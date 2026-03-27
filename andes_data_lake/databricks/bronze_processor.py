"""Bronze layer processor for the Medallion Architecture.

Lands raw data as-is into Bronze Delta tables. No transformation or
validation — preserves source fidelity.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

from andes_data_lake.databricks.errors import BronzeIngestionError, DeltaTableError
from andes_data_lake.databricks.models import (
    BronzeResult,
    DeltaColumn,
    DeltaSchema,
    MedallionLayer,
    SourceMetadata,
    SourceType,
    WriteMode,
)


class _LogWriter(Protocol):
    """Minimal interface expected from a log store."""

    def write(self, entry: Any) -> None: ...


class BronzeLayerProcessor:
    """Lands raw data as-is into Bronze Delta tables.

    No transformation or validation is applied — source fidelity is preserved.
    Records are written in append-only mode with ingestion metadata appended.
    """

    def __init__(
        self,
        delta_table_manager: Any,
        log_store: _LogWriter,
    ) -> None:
        self._dtm = delta_table_manager
        self._log_store = log_store

    # ── Batch ingestion ───────────────────────────────────────────────────

    def ingest_batch(
        self,
        dataset_id: str,
        records: list[dict],
        source_metadata: SourceMetadata,
    ) -> BronzeResult:
        """Ingest a batch of raw records into the Bronze Delta table.

        Each record is written as-is with ingestion metadata appended
        (``ingestion_timestamp``, ``source_system``, ``batch_id``).
        The table is auto-created if it does not exist.
        """
        table_name = f"bronze_{dataset_id}"
        now = datetime.now(timezone.utc)
        batch_id = source_metadata.batch_id or str(uuid.uuid4())

        enriched_records = [
            {
                **record,
                "ingestion_timestamp": now.isoformat(),
                "source_system": source_metadata.source_system,
                "batch_id": batch_id,
            }
            for record in records
        ]

        self._ensure_table_exists(table_name, enriched_records)

        try:
            self._dtm.write(table_name, enriched_records, WriteMode.APPEND)
        except DeltaTableError as exc:
            raise BronzeIngestionError(
                f"Failed to ingest batch into '{table_name}': {exc}",
                details={"dataset_id": dataset_id, "record_count": len(records)},
            ) from exc

        self._log_store.write({
            "event": "bronze_batch_ingestion",
            "dataset_id": dataset_id,
            "table_name": table_name,
            "record_count": len(records),
            "batch_id": batch_id,
            "timestamp": now.isoformat(),
        })

        return BronzeResult(
            record_count=len(records),
            table_path=table_name,
            ingestion_timestamp=now,
            source_type=source_metadata.source_type,
            source_metadata=source_metadata,
        )

    # ── Stream ingestion ──────────────────────────────────────────────────

    def ingest_stream_record(
        self,
        dataset_id: str,
        record: dict,
        topic: str,
        offset: int,
    ) -> BronzeResult:
        """Append a single stream record to the Bronze Delta table.

        The record is enriched with ``topic``, ``offset``, and
        ``ingestion_timestamp`` metadata before writing.
        """
        table_name = f"bronze_{dataset_id}"
        now = datetime.now(timezone.utc)

        enriched_record = {
            **record,
            "ingestion_timestamp": now.isoformat(),
            "topic": topic,
            "offset": offset,
        }

        self._ensure_table_exists(table_name, [enriched_record])

        try:
            self._dtm.write(table_name, [enriched_record], WriteMode.APPEND)
        except DeltaTableError as exc:
            raise BronzeIngestionError(
                f"Failed to ingest stream record into '{table_name}': {exc}",
                details={
                    "dataset_id": dataset_id,
                    "topic": topic,
                    "offset": offset,
                },
            ) from exc

        source_metadata = SourceMetadata(
            source_type=SourceType.STREAM,
            source_system="kafka_consumer",
            topic=topic,
            offset=offset,
        )

        self._log_store.write({
            "event": "bronze_stream_ingestion",
            "dataset_id": dataset_id,
            "table_name": table_name,
            "topic": topic,
            "offset": offset,
            "timestamp": now.isoformat(),
        })

        return BronzeResult(
            record_count=1,
            table_path=table_name,
            ingestion_timestamp=now,
            source_type=SourceType.STREAM,
            source_metadata=source_metadata,
        )

    # ── Private helpers ───────────────────────────────────────────────────

    def _ensure_table_exists(
        self, table_name: str, sample_records: list[dict],
    ) -> None:
        """Auto-create the Bronze table if it doesn't already exist.

        If the table exists but the records contain new columns, the schema
        is evolved additively so that ``DeltaTableManager`` validation passes.
        """
        try:
            self._dtm.get_table_metadata(table_name)
            # Table exists — evolve schema if records have new columns
            self._evolve_schema_if_needed(table_name, sample_records)
        except DeltaTableError:
            columns = self._build_columns_from_records(sample_records)
            schema = DeltaSchema(columns=columns)
            self._dtm.create_table(
                table_name=table_name,
                schema=schema,
                layer=MedallionLayer.BRONZE,
                partition_keys=["ingestion_timestamp"],
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
        """Derive a permissive Delta schema from a set of records.

        All columns are nullable ``STRING`` except ``ingestion_timestamp``
        (``TIMESTAMP``) and ``offset`` (``LONG``).
        """
        all_keys: dict[str, None] = {}
        for rec in records:
            for key in rec:
                all_keys.setdefault(key, None)

        type_overrides = {
            "ingestion_timestamp": "TIMESTAMP",
            "offset": "LONG",
        }
        return [
            DeltaColumn(
                name=key,
                data_type=type_overrides.get(key, "STRING"),
                nullable=True,
            )
            for key in all_keys
        ]
