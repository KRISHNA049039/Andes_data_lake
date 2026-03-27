"""Gold layer processor for the Medallion Architecture.

Reads validated data from Silver Delta tables and produces business-level
aggregates for BI consumption.  Supports both full recomputation and
incremental refresh via watermark tracking.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from andes_data_lake.databricks.errors import DeltaTableError, GoldAggregationError
from andes_data_lake.databricks.models import (
    AggregationConfig,
    AggregationFunction,
    DeltaColumn,
    DeltaSchema,
    GoldResult,
    MedallionLayer,
    WriteMode,
)


class _LogWriter(Protocol):
    """Minimal interface expected from a log store."""

    def write(self, entry: Any) -> None: ...


class GoldLayerProcessor:
    """Reads from Silver Delta tables and produces business-level aggregates.

    Supports configurable aggregation functions (SUM, AVG, COUNT, MIN, MAX,
    COUNT_DISTINCT), group-by columns, optional filter conditions, and
    incremental refresh via watermark tracking.
    """

    def __init__(
        self,
        delta_table_manager: Any,
        log_store: _LogWriter,
    ) -> None:
        self._dtm = delta_table_manager
        self._log_store = log_store
        self._watermarks: dict[str, str] = {}

    # ── Public API ────────────────────────────────────────────────────────

    def aggregate(
        self,
        dataset_id: str,
        silver_table_path: str,
        config: AggregationConfig,
    ) -> GoldResult:
        """Aggregate Silver data into a Gold Delta table.

        Reads validated records from the Silver table, optionally filters
        them, groups by the configured columns, computes aggregation
        functions, and writes the results to a Gold table.

        When ``config.incremental`` is True, only records newer than the
        last watermark are processed.  When False, the Gold table is fully
        recomputed using OVERWRITE mode.
        """
        gold_table = f"gold_{dataset_id}_summary"
        now = datetime.now(timezone.utc)

        try:
            records = self._dtm.read(silver_table_path)
        except DeltaTableError as exc:
            raise GoldAggregationError(
                f"Failed to read Silver table '{silver_table_path}': {exc}",
                details={"dataset_id": dataset_id, "silver_table_path": silver_table_path},
            ) from exc

        # Incremental: filter to records newer than last watermark
        if config.incremental:
            watermark = self._watermarks.get(dataset_id)
            if watermark is not None:
                records = [
                    r for r in records
                    if str(r.get(config.watermark_column, "")) > watermark
                ]

        # Apply filter_condition (simple key=value filtering)
        if config.filter_condition:
            records = self._apply_filter(records, config.filter_condition)

        # Group and aggregate
        groups = self._group_records(records, config.group_by_columns)
        aggregated_rows = self._compute_aggregates(groups, config)

        # Determine write mode
        write_mode = WriteMode.APPEND if config.incremental else WriteMode.OVERWRITE

        # Write to Gold table
        if aggregated_rows:
            self._ensure_table_exists(gold_table, aggregated_rows)
            try:
                self._dtm.write(gold_table, aggregated_rows, write_mode)
            except DeltaTableError as exc:
                raise GoldAggregationError(
                    f"Failed to write to Gold table '{gold_table}': {exc}",
                    details={"dataset_id": dataset_id, "record_count": len(aggregated_rows)},
                ) from exc

        # Update watermark for incremental refresh
        if config.incremental and records:
            latest_watermark = max(
                str(r.get(config.watermark_column, "")) for r in records
            )
            self._watermarks[dataset_id] = latest_watermark

        # Build aggregation type description
        agg_funcs = ", ".join(
            f"{a.function.value}({a.column})" for a in config.aggregations
        )
        agg_type = f"GROUP BY {', '.join(config.group_by_columns)} | {agg_funcs}"

        self._log_store.write({
            "event": "gold_aggregation_complete",
            "dataset_id": dataset_id,
            "gold_table": gold_table,
            "aggregate_count": len(aggregated_rows),
            "is_incremental": config.incremental,
            "timestamp": now.isoformat(),
        })

        return GoldResult(
            aggregate_count=len(aggregated_rows),
            table_path=gold_table,
            aggregation_timestamp=now,
            aggregation_type=agg_type,
            is_incremental=config.incremental,
        )

    def refresh_materialized_view(
        self,
        view_name: str,
        config: AggregationConfig,
    ) -> GoldResult:
        """Refresh a materialized view by delegating to :meth:`aggregate`.

        The *view_name* is used as both the dataset_id and the Silver table
        path (``silver_{view_name}_cleansed``).
        """
        silver_table_path = f"silver_{view_name}_cleansed"
        return self.aggregate(view_name, silver_table_path, config)

    def get_last_watermark(self, dataset_id: str) -> str | None:
        """Return the last watermark for the given dataset, or None."""
        return self._watermarks.get(dataset_id)

    def set_watermark(self, dataset_id: str, watermark: str) -> None:
        """Explicitly set the watermark for the given dataset."""
        self._watermarks[dataset_id] = watermark

    # ── Private helpers ───────────────────────────────────────────────────

    @staticmethod
    def _apply_filter(records: list[dict], filter_condition: str) -> list[dict]:
        """Apply a simple ``key=value`` filter condition to records."""
        if "=" not in filter_condition:
            return records
        key, value = filter_condition.split("=", 1)
        key = key.strip()
        value = value.strip()
        return [r for r in records if str(r.get(key, "")) == value]

    @staticmethod
    def _group_records(
        records: list[dict],
        group_by_columns: list[str],
    ) -> dict[tuple, list[dict]]:
        """Group records by the specified columns."""
        groups: dict[tuple, list[dict]] = {}
        for record in records:
            key = tuple(record.get(col) for col in group_by_columns)
            groups.setdefault(key, []).append(record)
        return groups

    @staticmethod
    def _compute_aggregates(
        groups: dict[tuple, list[dict]],
        config: AggregationConfig,
    ) -> list[dict]:
        """Compute aggregation functions for each group."""
        rows: list[dict] = []
        for group_key, group_records in groups.items():
            row: dict[str, Any] = {}
            # Add group-by column values
            for i, col in enumerate(config.group_by_columns):
                row[col] = group_key[i]
            # Compute each aggregation
            for agg_spec in config.aggregations:
                values = [
                    r[agg_spec.column]
                    for r in group_records
                    if agg_spec.column in r and r[agg_spec.column] is not None
                ]
                row[agg_spec.alias] = _compute_function(agg_spec.function, values)
            rows.append(row)
        return rows

    def _ensure_table_exists(
        self, table_name: str, sample_records: list[dict],
    ) -> None:
        """Auto-create the Gold table if it doesn't already exist."""
        try:
            self._dtm.get_table_metadata(table_name)
            self._evolve_schema_if_needed(table_name, sample_records)
        except DeltaTableError:
            columns = self._build_columns_from_records(sample_records)
            schema = DeltaSchema(columns=columns)
            self._dtm.create_table(
                table_name=table_name,
                schema=schema,
                layer=MedallionLayer.GOLD,
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
        return [
            DeltaColumn(name=key, data_type="STRING", nullable=True)
            for key in all_keys
        ]


# ── Module-level aggregation helpers ──────────────────────────────────────


def _compute_function(func: AggregationFunction, values: list) -> Any:
    """Compute a single aggregation function over a list of values."""
    if func == AggregationFunction.COUNT:
        return len(values)

    if func == AggregationFunction.COUNT_DISTINCT:
        return len(set(values))

    # Numeric aggregations — coerce to float
    numeric = _to_numeric(values)

    if func == AggregationFunction.SUM:
        return sum(numeric) if numeric else 0

    if func == AggregationFunction.AVG:
        return (sum(numeric) / len(numeric)) if numeric else 0

    if func == AggregationFunction.MIN:
        return min(numeric) if numeric else None

    if func == AggregationFunction.MAX:
        return max(numeric) if numeric else None

    return None  # pragma: no cover


def _to_numeric(values: list) -> list[float]:
    """Convert values to floats, skipping non-numeric entries."""
    result: list[float] = []
    for v in values:
        try:
            result.append(float(v))
        except (TypeError, ValueError):
            continue
    return result
