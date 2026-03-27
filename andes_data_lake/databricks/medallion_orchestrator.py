"""Top-level coordinator for Medallion pipeline runs.

Routes data through Bronze → Silver → Gold, manages idempotency,
integrates with RBAC and Log Store, and supports both batch and
streaming ingestion patterns.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Protocol

from andes_data_lake.databricks.models import (
    AggregationConfig,
    GoldResult,
    MedallionLayer,
    MedallionPipelineRun,
    MedallionPipelineStatus,
    SourceMetadata,
    SourceType,
)
from andes_data_lake.models.errors import AuthorizationDeniedError


# ── Protocol definitions for injected dependencies ────────────────────────


class _CatalogService(Protocol):
    def get_dataset(self, dataset_id: str) -> Any: ...
    def register_medallion_metadata(
        self,
        dataset_id: str,
        bronze_result: Any,
        silver_result: Any,
        gold_result: Any,
    ) -> None: ...


class _RBAC(Protocol):
    def authorize(self, user_id: str, action: str, resource: str) -> Any: ...


class _LogWriter(Protocol):
    def write(self, entry: Any) -> None: ...


class _IdempotencyStore(Protocol):
    def check_and_reserve(self, key: str) -> Any: ...
    def mark_completed(self, key: str, run: Any) -> None: ...


class MedallionOrchestrator:
    """Coordinates medallion pipeline runs (Bronze → Silver → Gold).

    All heavy-lifting components are injected via the constructor so the
    orchestrator itself stays thin and testable.
    """

    def __init__(
        self,
        bronze_processor: Any,
        silver_processor: Any,
        gold_processor: Any,
        catalog_service: Any,
        rbac: Any,
        log_store: Any,
        idempotency_store: Any,
    ) -> None:
        self._bronze = bronze_processor
        self._silver = silver_processor
        self._gold = gold_processor
        self._catalog = catalog_service
        self._rbac = rbac
        self._log_store = log_store
        self._idempotency = idempotency_store

        self._gold_configs: dict[str, AggregationConfig] = {}
        self._stream_buffer: list[dict] = []
        self._stream_dataset_id: str | None = None
        self._runs: dict[str, MedallionPipelineRun] = {}

    # ── Batch pipeline ────────────────────────────────────────────────────

    def submit_batch(
        self,
        dataset_id: str,
        payload: list[dict],
        idempotency_key: str,
        user_id: str,
    ) -> MedallionPipelineRun:
        """Execute a full Bronze → Silver → Gold pipeline for a batch.

        1. Idempotency check
        2. RBAC authorization
        3. Bronze ingestion
        4. Silver validation / dedup
        5. Gold aggregation (if configured)
        6. Catalog registration & completion
        """
        # Step 1 — idempotency
        existing = self._idempotency.check_and_reserve(idempotency_key)
        if existing is not None:
            return existing

        # Step 2 — authorization
        auth = self._rbac.authorize(user_id, "medallion:write", dataset_id)
        if not auth.granted:
            raise AuthorizationDeniedError(
                f"User '{user_id}' is not authorized for medallion:write on '{dataset_id}'"
            )

        # Step 3 — create pipeline run
        now = datetime.now(timezone.utc)
        run = MedallionPipelineRun(
            dataset_id=dataset_id,
            idempotency_key=idempotency_key,
            status=MedallionPipelineStatus.RUNNING,
            current_layer=MedallionLayer.BRONZE,
            start_time=now,
        )
        self._runs[str(run.run_id)] = run

        self._log_store.write({
            "event": "pipeline_start",
            "run_id": str(run.run_id),
            "dataset_id": dataset_id,
            "correlation_id": run.correlation_id,
        })

        try:
            # Step 4 — Bronze
            source_meta = SourceMetadata(
                source_type=SourceType.BATCH,
                source_system="ingestion_engine",
            )
            bronze_result = self._bronze.ingest_batch(
                dataset_id, payload, source_meta,
            )
            run.bronze_result = bronze_result
            run.current_layer = MedallionLayer.SILVER

            # Step 5 — Silver
            schema_context = self._catalog.get_dataset(dataset_id)
            silver_result = self._silver.process_batch(
                dataset_id, bronze_result.table_path, schema_context,
            )
            run.silver_result = silver_result
            run.current_layer = MedallionLayer.GOLD

            # Step 6 — Gold (optional)
            if self.has_gold_config(dataset_id):
                gold_config = self.get_gold_config(dataset_id)
                gold_result = self._gold.aggregate(
                    dataset_id, silver_result.table_path, gold_config,
                )
                run.gold_result = gold_result

            # Success
            run.status = MedallionPipelineStatus.COMPLETED
            run.end_time = datetime.now(timezone.utc)

            self._catalog.register_medallion_metadata(
                dataset_id, run.bronze_result, run.silver_result, run.gold_result,
            )
            self._idempotency.mark_completed(idempotency_key, run)

            self._log_store.write({
                "event": "pipeline_complete",
                "run_id": str(run.run_id),
                "dataset_id": dataset_id,
                "correlation_id": run.correlation_id,
                "status": run.status.value,
            })

        except Exception as exc:
            run.status = MedallionPipelineStatus.FAILED
            run.error_message = str(exc)
            run.end_time = datetime.now(timezone.utc)

            self._log_store.write({
                "event": "pipeline_error",
                "run_id": str(run.run_id),
                "dataset_id": dataset_id,
                "correlation_id": run.correlation_id,
                "current_layer": run.current_layer.value,
                "error": str(exc),
            })
            raise

        return run

    # ── Streaming ─────────────────────────────────────────────────────────

    def process_stream_record(
        self,
        record: dict,
        topic: str,
        offset: int,
        dataset_id: str,
    ) -> None:
        """Accumulate a stream record and trigger micro-batch when full."""
        self._stream_dataset_id = dataset_id
        self._stream_buffer.append(record)

        micro_batch_size = 1000
        if len(self._stream_buffer) >= micro_batch_size:
            self._flush_stream_buffer(dataset_id)

    def _flush_stream_buffer(self, dataset_id: str) -> None:
        """Process the accumulated micro-batch through Bronze + Silver."""
        if not self._stream_buffer:
            return

        batch = list(self._stream_buffer)
        self._stream_buffer.clear()

        source_meta = SourceMetadata(
            source_type=SourceType.STREAM,
            source_system="kafka_consumer",
        )
        bronze_result = self._bronze.ingest_batch(dataset_id, batch, source_meta)

        schema_context = self._catalog.get_dataset(dataset_id)
        self._silver.process_micro_batch(dataset_id, batch, schema_context)

    # ── Gold refresh ──────────────────────────────────────────────────────

    def trigger_gold_refresh(
        self,
        dataset_id: str,
        aggregation_config: AggregationConfig,
        user_id: str,
    ) -> GoldResult:
        """Run a Gold aggregation with RBAC authorization."""
        auth = self._rbac.authorize(user_id, "medallion:write", dataset_id)
        if not auth.granted:
            raise AuthorizationDeniedError(
                f"User '{user_id}' is not authorized for medallion:write on '{dataset_id}'"
            )

        silver_table = f"silver_{dataset_id}_cleansed"
        result = self._gold.aggregate(dataset_id, silver_table, aggregation_config)

        self._log_store.write({
            "event": "gold_refresh",
            "dataset_id": dataset_id,
            "aggregate_count": result.aggregate_count,
        })

        return result

    # ── Pipeline status ───────────────────────────────────────────────────

    def get_pipeline_status(self, run_id: str) -> MedallionPipelineStatus:
        """Return the status of a stored pipeline run."""
        run = self._runs.get(run_id)
        if run is None:
            raise KeyError(f"No pipeline run found with id '{run_id}'")
        return run.status

    # ── Gold config helpers ───────────────────────────────────────────────

    def has_gold_config(self, dataset_id: str) -> bool:
        return dataset_id in self._gold_configs

    def set_gold_config(self, dataset_id: str, config: AggregationConfig) -> None:
        self._gold_configs[dataset_id] = config

    def get_gold_config(self, dataset_id: str) -> AggregationConfig:
        return self._gold_configs[dataset_id]
