"""Databricks Medallion Architecture integration for Andes Data Lake.

Public API exports for the ``andes_data_lake.databricks`` package.
"""

# ── Components ────────────────────────────────────────────────────────────
from andes_data_lake.databricks.bronze_processor import BronzeLayerProcessor
from andes_data_lake.databricks.delta_table_manager import DeltaTableManager
from andes_data_lake.databricks.gold_processor import GoldLayerProcessor
from andes_data_lake.databricks.job_manager import DatabricksJobManager
from andes_data_lake.databricks.medallion_orchestrator import MedallionOrchestrator
from andes_data_lake.databricks.silver_processor import SilverLayerProcessor
from andes_data_lake.databricks.workspace_client import DatabricksWorkspaceClient

# ── Errors ────────────────────────────────────────────────────────────────
from andes_data_lake.databricks.errors import (
    BronzeIngestionError,
    DatabricksConnectionError,
    DeltaTableError,
    GoldAggregationError,
    MedallionError,
    SchemaViolationError,
    SilverProcessingError,
)

# ── Data Models & Enums ──────────────────────────────────────────────────
from andes_data_lake.databricks.models import (
    AggregationConfig,
    AggregationFunction,
    AggregationSpec,
    BronzeResult,
    ClusterConfig,
    DatabricksJobConfig,
    DeltaColumn,
    DeltaSchema,
    DeltaTableMetadata,
    GoldResult,
    JobRunResult,
    JobRunStatus,
    MedallionJobConfig,
    MedallionLayer,
    MedallionPipelineRun,
    MedallionPipelineStatus,
    SilverResult,
    SourceMetadata,
    SourceType,
    ValidationSummary,
    WriteMode,
)

__all__ = [
    # Components
    "BronzeLayerProcessor",
    "DatabricksJobManager",
    "DatabricksWorkspaceClient",
    "DeltaTableManager",
    "GoldLayerProcessor",
    "MedallionOrchestrator",
    "SilverLayerProcessor",
    # Errors
    "BronzeIngestionError",
    "DatabricksConnectionError",
    "DeltaTableError",
    "GoldAggregationError",
    "MedallionError",
    "SchemaViolationError",
    "SilverProcessingError",
]
