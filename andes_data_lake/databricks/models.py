"""Data models and enums for the Databricks Medallion integration."""

from datetime import datetime
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, field_validator, model_validator


# ── Enums ──────────────────────────────────────────────────────────────────────


class MedallionLayer(str, Enum):
    """Medallion architecture layer identifier."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class MedallionPipelineStatus(str, Enum):
    """Status of a medallion pipeline run."""

    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"
    SKIPPED = "SKIPPED"


class WriteMode(str, Enum):
    """Delta table write mode."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    MERGE = "merge"


class SourceType(str, Enum):
    """Data source type."""

    BATCH = "batch"
    STREAM = "stream"


class AggregationFunction(str, Enum):
    """Supported aggregation functions for Gold layer."""

    SUM = "SUM"
    AVG = "AVG"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"
    COUNT_DISTINCT = "COUNT_DISTINCT"


class JobRunStatus(str, Enum):
    """Status of a Databricks job run."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# ── Pydantic Models ───────────────────────────────────────────────────────────


class SourceMetadata(BaseModel):
    """Metadata about the data source for a Bronze ingestion."""

    source_type: SourceType
    source_system: str
    batch_id: str | None = None
    topic: str | None = None
    offset: int | None = None
    partition: int | None = None


class BronzeResult(BaseModel):
    """Result of a Bronze layer ingestion."""

    record_count: int
    table_path: str
    ingestion_timestamp: datetime
    source_type: SourceType
    source_metadata: SourceMetadata


class ValidationSummary(BaseModel):
    """Summary of validation outcomes across all three validation layers."""

    ingestion_pass: int
    ingestion_fail: int
    schema_pass: int
    schema_fail: int
    business_rule_pass: int
    business_rule_fail: int


class SilverResult(BaseModel):
    """Result of Silver layer processing."""

    valid_count: int
    rejected_count: int
    deduplicated_count: int
    table_path: str
    processing_timestamp: datetime
    validation_summary: ValidationSummary


class GoldResult(BaseModel):
    """Result of Gold layer aggregation."""

    aggregate_count: int
    table_path: str
    aggregation_timestamp: datetime
    aggregation_type: str
    is_incremental: bool


class DeltaColumn(BaseModel):
    """Column definition for a Delta table schema."""

    name: str
    data_type: str
    nullable: bool = True
    comment: str | None = None


class DeltaSchema(BaseModel):
    """Schema definition for a Delta table."""

    columns: list[DeltaColumn]


class DeltaTableMetadata(BaseModel):
    """Metadata for a Delta table."""

    table_name: str
    layer: MedallionLayer
    delta_schema: DeltaSchema
    partition_keys: list[str]
    location: str
    created_at: datetime
    last_modified: datetime
    row_count: int
    size_bytes: int


class AggregationSpec(BaseModel):
    """Specification for a single aggregation operation."""

    column: str
    function: AggregationFunction
    alias: str


class AggregationConfig(BaseModel):
    """Configuration for Gold layer aggregation."""

    group_by_columns: list[str]
    aggregations: list[AggregationSpec]
    filter_condition: str | None = None
    incremental: bool = True
    watermark_column: str = "processing_timestamp"

    @field_validator("group_by_columns")
    @classmethod
    def group_by_columns_must_be_non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("group_by_columns must be non-empty")
        return v


class ClusterConfig(BaseModel):
    """Databricks cluster configuration."""

    num_workers: int = 2
    spark_version: str = "13.3.x-scala2.12"
    node_type_id: str = "i3.xlarge"
    autoscale_min: int | None = None
    autoscale_max: int | None = None


class MedallionJobConfig(BaseModel):
    """Configuration for a medallion pipeline job."""

    dataset_id: str
    bronze_enabled: bool = True
    silver_enabled: bool = True
    gold_enabled: bool = True
    aggregation_config: AggregationConfig | None = None
    dedup_keys: list[str] = []
    micro_batch_size: int = 1000
    micro_batch_interval_seconds: int = 60


class DatabricksJobConfig(BaseModel):
    """Configuration for a Databricks workflow job."""

    job_id: str
    job_name: str
    dataset_id: str
    schedule_cron: str | None = None
    cluster_config: ClusterConfig
    medallion_config: MedallionJobConfig
    created_at: datetime
    updated_at: datetime


class MedallionPipelineRun(BaseModel):
    """Represents a single medallion pipeline execution."""

    run_id: UUID = None  # type: ignore[assignment]
    dataset_id: str
    idempotency_key: str
    status: MedallionPipelineStatus
    current_layer: MedallionLayer
    bronze_result: BronzeResult | None = None
    silver_result: SilverResult | None = None
    gold_result: GoldResult | None = None
    start_time: datetime
    end_time: datetime | None = None
    error_message: str | None = None
    correlation_id: str = ""

    def __init__(self, **data: object) -> None:
        if "run_id" not in data or data["run_id"] is None:
            data["run_id"] = uuid4()
        if "correlation_id" not in data or data["correlation_id"] == "":
            data["correlation_id"] = str(uuid4())
        super().__init__(**data)

    @field_validator("idempotency_key")
    @classmethod
    def idempotency_key_must_be_non_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("idempotency_key must be non-empty")
        return v

    @model_validator(mode="after")
    def end_time_must_be_gte_start_time(self) -> "MedallionPipelineRun":
        if self.end_time is not None and self.end_time < self.start_time:
            raise ValueError("end_time must be >= start_time")
        return self


class JobRunResult(BaseModel):
    """Result of a Databricks job run."""

    run_id: str
    status: JobRunStatus
    message: str | None = None
