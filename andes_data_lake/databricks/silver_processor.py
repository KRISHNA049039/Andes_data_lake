"""Silver layer processor for the Medallion Architecture.

Reads from Bronze, applies validation via the injected validation pipeline,
deduplicates records, and writes cleansed data to Silver Delta tables.
Failed records are routed to the Dead Letter Queue.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from andes_data_lake.databricks.errors import DeltaTableError, SilverProcessingError
from andes_data_lake.databricks.models import (
    DeltaColumn,
    DeltaSchema,
    MedallionLayer,
    SilverResult,
    ValidationSummary,
    WriteMode,
)


class _LogWriter(Protocol):
    """Minimal interface expected from a log store."""

    def write(self, entry: Any) -> None: ...


class _ValidationPipeline(Protocol):
    """Minimal interface expected from a validation pipeline."""

    def validate(self, record: dict, context: dict) -> tuple[Any, Any]: ...


class _DeadLetterQueue(Protocol):
    """Minimal interface expected from a dead letter queue."""

    def send(self, source_stream: str, record: dict, error: str) -> None: ...


class SilverLayerProcessor:
    """Reads from Bronze Delta tables, validates, deduplicates, and writes
    cleansed records to Silver Delta tables.

    Failed records are routed to the Dead Letter Queue with error details.
    A validation audit record is created for every processed record.
    """

    def __init__(
        self,
        delta_table_manager: Any,
        validation_pipeline: _ValidationPipeline,
        log_store: _LogWriter,
        dead_letter_queue: _DeadLetterQueue,
    ) -> None:
        self._dtm = delta_table_manager
        self._validation_pipeline = validation_pipeline
        self._log_store = log_store
        self._dlq = dead_letter_queue

    # ── Batch processing ──────────────────────────────────────────────────

    def process_batch(
        self,
        dataset_id: str,
        bronze_table_path: str,
        schema_context: dict,
    ) -> SilverResult:
        """Read records from a Bronze Delta table, validate, deduplicate,
        and write cleansed records to the Silver Delta table.

        Returns a :class:`SilverResult` where
        ``valid_count + rejected_count == total bronze records``.
        """
        try:
            bronze_records = self._dtm.read(bronze_table_path)
        except DeltaTableError as exc:
            raise SilverProcessingError(
                f"Failed to read Bronze table '{bronze_table_path}': {exc}",
                details={"dataset_id": dataset_id, "bronze_table_path": bronze_table_path},
            ) from exc

        return self._process_records(dataset_id, bronze_records, schema_context)

    # ── Micro-batch processing ────────────────────────────────────────────

    def process_micro_batch(
        self,
        dataset_id: str,
        records: list[dict],
        schema_context: dict,
    ) -> SilverResult:
        """Same validation/dedup logic as :meth:`process_batch` but operates
        on a provided list of records directly (no read from table).
        """
        return self._process_records(dataset_id, records, schema_context)

    # ── Deduplication ─────────────────────────────────────────────────────

    def deduplicate(
        self,
        records: list[dict],
        dedup_keys: list[str],
    ) -> list[dict]:
        """Remove duplicates based on *dedup_keys*.

        For duplicate groups, the record with the latest timestamp is
        retained (looks for ``timestamp`` or ``ingestion_timestamp``).
        The returned list is always a subset of the input — no fabrication.
        """
        if not dedup_keys or not records:
            return list(records)

        seen: dict[tuple, dict] = {}
        for record in records:
            key = tuple(record.get(k) for k in dedup_keys)
            existing = seen.get(key)
            if existing is None:
                seen[key] = record
            else:
                # Keep the record with the latest timestamp
                new_ts = self._extract_timestamp(record)
                old_ts = self._extract_timestamp(existing)
                if new_ts is not None and (old_ts is None or new_ts > old_ts):
                    seen[key] = record

        return list(seen.values())

    # ── Private helpers ───────────────────────────────────────────────────

    def _process_records(
        self,
        dataset_id: str,
        records: list[dict],
        schema_context: dict,
    ) -> SilverResult:
        """Core validation + dedup + write logic shared by batch and
        micro-batch paths."""
        valid_records: list[dict] = []
        rejected_count = 0
        audit_records: list[dict] = []

        # Validation summary counters
        summary = ValidationSummary(
            ingestion_pass=0,
            ingestion_fail=0,
            schema_pass=0,
            schema_fail=0,
            business_rule_pass=0,
            business_rule_fail=0,
        )

        for record in records:
            result, audit = self._validation_pipeline.validate(record, schema_context)
            audit_records.append(audit)

            # Update summary from audit info
            self._update_summary(summary, audit)

            if result.status.value == "PASS":
                valid_records.append(record)
            else:
                self._dlq.send(
                    source_stream=f"medallion_silver_{dataset_id}",
                    record=record,
                    error=result.error_message or "Validation failed",
                )
                rejected_count += 1

            self._log_store.write({
                "event": "silver_record_validation",
                "dataset_id": dataset_id,
                "status": result.status.value,
            })

        # Deduplicate valid records
        dedup_keys = schema_context.get("dedup_keys", [])
        deduped_records = self.deduplicate(valid_records, dedup_keys)
        deduplicated_count = len(valid_records) - len(deduped_records)

        # Write cleansed records to Silver Delta table
        silver_table = f"silver_{dataset_id}_cleansed"
        if deduped_records:
            self._ensure_table_exists(silver_table, deduped_records)
            try:
                self._dtm.write(silver_table, deduped_records, WriteMode.APPEND)
            except DeltaTableError as exc:
                raise SilverProcessingError(
                    f"Failed to write to Silver table '{silver_table}': {exc}",
                    details={"dataset_id": dataset_id, "record_count": len(deduped_records)},
                ) from exc

        now = datetime.now(timezone.utc)

        self._log_store.write({
            "event": "silver_batch_complete",
            "dataset_id": dataset_id,
            "valid_count": len(deduped_records),
            "rejected_count": rejected_count,
            "deduplicated_count": deduplicated_count,
            "timestamp": now.isoformat(),
        })

        return SilverResult(
            valid_count=len(deduped_records),
            rejected_count=rejected_count,
            deduplicated_count=deduplicated_count,
            table_path=silver_table,
            processing_timestamp=now,
            validation_summary=summary,
        )

    def _update_summary(self, summary: ValidationSummary, audit: Any) -> None:
        """Update the validation summary from an audit record.

        The audit object is expected to be a dict (or dict-like) with keys
        for each validation layer and their pass/fail status.  We handle
        both dict and object-with-attributes styles defensively.
        """
        layers = self._get_audit_layers(audit)

        # Track per-layer pass/fail.  If a layer wasn't reached (because
        # an earlier layer failed), we don't count it.
        for layer_name in ("ingestion", "schema", "business_rule"):
            layer_result = layers.get(layer_name)
            if layer_result is None:
                continue
            passed = self._is_layer_pass(layer_result)
            if passed:
                current = getattr(summary, f"{layer_name}_pass")
                setattr(summary, f"{layer_name}_pass", current + 1)
            else:
                current = getattr(summary, f"{layer_name}_fail")
                setattr(summary, f"{layer_name}_fail", current + 1)

    @staticmethod
    def _get_audit_layers(audit: Any) -> dict:
        """Extract layer results from an audit object."""
        if isinstance(audit, dict):
            return audit.get("layers", audit)
        if hasattr(audit, "layers"):
            layers = audit.layers
            return layers if isinstance(layers, dict) else {}
        return {}

    @staticmethod
    def _is_layer_pass(layer_result: Any) -> bool:
        """Determine if a single layer result indicates pass."""
        if isinstance(layer_result, str):
            return layer_result.upper() == "PASS"
        if isinstance(layer_result, bool):
            return layer_result
        if hasattr(layer_result, "value"):
            return str(layer_result.value).upper() == "PASS"
        if hasattr(layer_result, "status"):
            return str(layer_result.status).upper() == "PASS"
        return bool(layer_result)

    @staticmethod
    def _extract_timestamp(record: dict) -> Any:
        """Extract a comparable timestamp from a record.

        Looks for ``timestamp`` first, then ``ingestion_timestamp``.
        """
        return record.get("timestamp") or record.get("ingestion_timestamp")

    def _ensure_table_exists(
        self, table_name: str, sample_records: list[dict],
    ) -> None:
        """Auto-create the Silver table if it doesn't already exist."""
        try:
            self._dtm.get_table_metadata(table_name)
            self._evolve_schema_if_needed(table_name, sample_records)
        except DeltaTableError:
            columns = self._build_columns_from_records(sample_records)
            schema = DeltaSchema(columns=columns)
            self._dtm.create_table(
                table_name=table_name,
                schema=schema,
                layer=MedallionLayer.SILVER,
                partition_keys=[],
            )

    def _evolve_schema_if_needed(
        self, table_name: str, records: list[dict],
    ) -> None:
        """Evolve the table schema if records contain columns not yet in it."""
        metadata = self._dtm.get_table_metadata(table_name)
        existing_names = {col.name for col in metadata.delta_schema.columns}
        new_columns = self._build_columns_from_records(records)
        added = [c for c in new_columns if c.name not in existing_names]
        if added:
            new_schema = DeltaSchema(columns=list(metadata.delta_schema.columns) + added)
            self._dtm.evolve_schema(table_name, new_schema)

    @staticmethod
    def _build_columns_from_records(records: list[dict]) -> list[DeltaColumn]:
        """Derive a permissive Delta schema from a set of records."""
        all_keys: dict[str, None] = {}
        for rec in records:
            for key in rec:
                all_keys.setdefault(key, None)

        type_overrides = {
            "ingestion_timestamp": "TIMESTAMP",
            "processing_timestamp": "TIMESTAMP",
            "timestamp": "TIMESTAMP",
        }
        return [
            DeltaColumn(
                name=key,
                data_type=type_overrides.get(key, "STRING"),
                nullable=True,
            )
            for key in all_keys
        ]
