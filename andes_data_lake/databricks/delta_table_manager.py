"""Delta Lake table lifecycle manager for the Medallion Architecture."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Protocol

from andes_data_lake.databricks.errors import DeltaTableError, SchemaViolationError
from andes_data_lake.databricks.models import (
    DeltaSchema,
    DeltaTableMetadata,
    MedallionLayer,
    WriteMode,
)
from andes_data_lake.databricks.workspace_client import DatabricksWorkspaceClient


class LogWriter(Protocol):
    """Minimal interface expected from a log store."""

    def write(self, entry: Any) -> None: ...


class DeltaTableManager:
    """Manages Delta Lake table lifecycle — creation, schema evolution,
    reads, writes, and metadata registration with Unity Catalog.

    Since there is no real Delta Lake runtime locally, this class acts as a
    clean abstraction that:
    * Uses :class:`DatabricksWorkspaceClient` to execute SQL for table ops.
    * Maintains an in-memory registry of table metadata for local tracking.
    * Can be easily mocked in tests.
    """

    def __init__(
        self,
        workspace_client: DatabricksWorkspaceClient,
        log_store: LogWriter,
    ) -> None:
        self._client = workspace_client
        self._log_store = log_store
        # In-memory registries
        self._tables: dict[str, DeltaTableMetadata] = {}
        self._data: dict[str, list[dict]] = {}

    # ── Table creation ────────────────────────────────────────────────────

    def create_table(
        self,
        table_name: str,
        schema: DeltaSchema,
        layer: MedallionLayer,
        partition_keys: list[str],
    ) -> DeltaTableMetadata:
        """Create a Delta table and register it in Unity Catalog.

        The table name must follow ``{layer}_{dataset_name}`` naming.
        Raises :class:`DeltaTableError` on failure.
        """
        if table_name in self._tables:
            raise DeltaTableError(
                f"Table '{table_name}' already exists",
                details={"table_name": table_name},
            )

        column_names = {col.name for col in schema.columns}
        for pk in partition_keys:
            if pk not in column_names:
                raise DeltaTableError(
                    f"Partition key '{pk}' is not in the schema",
                    details={"table_name": table_name, "partition_key": pk},
                )

        # Build and execute CREATE TABLE SQL
        col_defs = ", ".join(
            f"{col.name} {col.data_type}{'' if col.nullable else ' NOT NULL'}"
            for col in schema.columns
        )
        partition_clause = (
            f" PARTITIONED BY ({', '.join(partition_keys)})" if partition_keys else ""
        )
        location = f"dbfs:/mnt/delta/{layer.value}/{table_name}"
        sql = (
            f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs})"
            f" USING DELTA LOCATION '{location}'{partition_clause}"
        )

        try:
            self._client.execute_sql(sql, warehouse_id="default")
        except Exception as exc:
            raise DeltaTableError(
                f"Failed to create table '{table_name}': {exc}",
                details={"table_name": table_name, "sql": sql},
            ) from exc

        now = datetime.now(timezone.utc)
        metadata = DeltaTableMetadata(
            table_name=table_name,
            layer=layer,
            delta_schema=schema,
            partition_keys=partition_keys,
            location=location,
            created_at=now,
            last_modified=now,
            row_count=0,
            size_bytes=0,
        )
        self._tables[table_name] = metadata
        self._data[table_name] = []
        return metadata

    # ── Write ─────────────────────────────────────────────────────────────

    def write(
        self,
        table_path: str,
        records: list[dict],
        mode: WriteMode,
    ) -> dict:
        """Write records to a Delta table.

        Supports APPEND, OVERWRITE, and MERGE modes.  The write is atomic —
        either all records are committed or none are.

        Returns a dict with ``record_count``.
        Raises :class:`SchemaViolationError` on schema mismatch.
        Raises :class:`DeltaTableError` if the table does not exist.
        """
        if table_path not in self._tables:
            raise DeltaTableError(
                f"Table '{table_path}' does not exist",
                details={"table_path": table_path},
            )

        metadata = self._tables[table_path]
        self._validate_records_against_schema(table_path, records, metadata.delta_schema)

        try:
            if mode == WriteMode.APPEND:
                self._data[table_path].extend(records)
            elif mode == WriteMode.OVERWRITE:
                self._data[table_path] = list(records)
            elif mode == WriteMode.MERGE:
                # MERGE: upsert based on all keys present in the first record
                existing = self._data[table_path]
                if records:
                    merge_keys = list(records[0].keys())
                    existing_map = {
                        tuple(r.get(k) for k in merge_keys): r for r in existing
                    }
                    for rec in records:
                        key = tuple(rec.get(k) for k in merge_keys)
                        existing_map[key] = rec
                    self._data[table_path] = list(existing_map.values())
            else:  # pragma: no cover
                raise DeltaTableError(
                    f"Unsupported write mode: {mode}",
                    details={"table_path": table_path, "mode": mode.value},
                )
        except (SchemaViolationError, DeltaTableError):
            raise
        except Exception as exc:
            raise DeltaTableError(
                f"Write to '{table_path}' failed: {exc}",
                details={"table_path": table_path, "mode": mode.value},
            ) from exc

        # Update metadata
        metadata.row_count = len(self._data[table_path])
        metadata.last_modified = datetime.now(timezone.utc)
        metadata.size_bytes = sum(
            len(str(r)) for r in self._data[table_path]
        )

        return {"record_count": len(records)}

    # ── Read ──────────────────────────────────────────────────────────────

    def read(
        self,
        table_path: str,
        filters: dict | None = None,
    ) -> list[dict]:
        """Read records from a Delta table, optionally applying filters.

        Each filter key maps to a value (equality) or a dict of operators
        (e.g. ``{"gte": value}``).
        """
        if table_path not in self._tables:
            raise DeltaTableError(
                f"Table '{table_path}' does not exist",
                details={"table_path": table_path},
            )

        rows = self._data[table_path]
        if not filters:
            return list(rows)

        result: list[dict] = []
        for row in rows:
            if self._matches_filters(row, filters):
                result.append(row)
        return result

    # ── Metadata ───────────────────────────────────────────────────────────

    def get_table_metadata(self, table_path: str) -> DeltaTableMetadata:
        """Return metadata for the given table."""
        if table_path not in self._tables:
            raise DeltaTableError(
                f"Table '{table_path}' does not exist",
                details={"table_path": table_path},
            )
        return self._tables[table_path]

    # ── Schema evolution ──────────────────────────────────────────────────

    def evolve_schema(
        self,
        table_path: str,
        new_schema: DeltaSchema,
    ) -> DeltaTableMetadata:
        """Additive schema evolution — add new columns via ALTER TABLE.

        Only new columns are added; existing columns are never removed or
        modified.  Raises :class:`DeltaTableError` if the table does not exist.
        """
        if table_path not in self._tables:
            raise DeltaTableError(
                f"Table '{table_path}' does not exist",
                details={"table_path": table_path},
            )

        metadata = self._tables[table_path]
        existing_names = {col.name for col in metadata.delta_schema.columns}
        new_columns = [c for c in new_schema.columns if c.name not in existing_names]

        if not new_columns:
            return metadata

        for col in new_columns:
            alter_sql = (
                f"ALTER TABLE {table_path} ADD COLUMNS "
                f"({col.name} {col.data_type})"
            )
            try:
                self._client.execute_sql(alter_sql, warehouse_id="default")
            except Exception as exc:
                raise DeltaTableError(
                    f"Schema evolution failed for '{table_path}': {exc}",
                    details={"table_path": table_path, "column": col.name},
                ) from exc

        # Update in-memory schema
        merged_columns = list(metadata.delta_schema.columns) + new_columns
        metadata.delta_schema = DeltaSchema(columns=merged_columns)
        metadata.last_modified = datetime.now(timezone.utc)
        return metadata

    # ── Maintenance ────────────────────────────────────────────────────────

    def optimize_table(self, table_path: str) -> None:
        """Run OPTIMIZE on the Delta table for file compaction."""
        if table_path not in self._tables:
            raise DeltaTableError(
                f"Table '{table_path}' does not exist",
                details={"table_path": table_path},
            )
        sql = f"OPTIMIZE {table_path}"
        try:
            self._client.execute_sql(sql, warehouse_id="default")
        except Exception as exc:
            raise DeltaTableError(
                f"OPTIMIZE failed for '{table_path}': {exc}",
                details={"table_path": table_path},
            ) from exc

    def vacuum_table(self, table_path: str, retention_hours: int = 168) -> None:
        """Run VACUUM on the Delta table to remove old file versions."""
        if table_path not in self._tables:
            raise DeltaTableError(
                f"Table '{table_path}' does not exist",
                details={"table_path": table_path},
            )
        sql = f"VACUUM {table_path} RETAIN {retention_hours} HOURS"
        try:
            self._client.execute_sql(sql, warehouse_id="default")
        except Exception as exc:
            raise DeltaTableError(
                f"VACUUM failed for '{table_path}': {exc}",
                details={"table_path": table_path},
            ) from exc

    # ── Private helpers ───────────────────────────────────────────────────

    def _validate_records_against_schema(
        self,
        table_path: str,
        records: list[dict],
        schema: DeltaSchema,
    ) -> None:
        """Raise :class:`SchemaViolationError` if any record violates the schema."""
        non_nullable = {
            col.name for col in schema.columns if not col.nullable
        }
        column_names = {col.name for col in schema.columns}

        for idx, record in enumerate(records):
            # Check for unknown columns
            extra = set(record.keys()) - column_names
            if extra:
                raise SchemaViolationError(
                    f"Record {idx} contains columns not in schema: {extra}",
                    details={
                        "table_path": table_path,
                        "record_index": idx,
                        "extra_columns": sorted(extra),
                    },
                )
            # Check non-nullable columns
            for col_name in non_nullable:
                if col_name not in record or record[col_name] is None:
                    raise SchemaViolationError(
                        f"Record {idx} is missing non-nullable column '{col_name}'",
                        details={
                            "table_path": table_path,
                            "record_index": idx,
                            "column": col_name,
                        },
                    )

    @staticmethod
    def _matches_filters(row: dict, filters: dict) -> bool:
        """Return True if *row* satisfies all *filters*."""
        for key, condition in filters.items():
            value = row.get(key)
            if isinstance(condition, dict):
                for op, threshold in condition.items():
                    if op == "eq" and value != threshold:
                        return False
                    if op == "gte" and (value is None or value < threshold):
                        return False
                    if op == "lte" and (value is None or value > threshold):
                        return False
                    if op == "gt" and (value is None or value <= threshold):
                        return False
                    if op == "lt" and (value is None or value >= threshold):
                        return False
            else:
                if value != condition:
                    return False
        return True
