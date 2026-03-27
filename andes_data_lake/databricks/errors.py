"""Error hierarchy for the Databricks Medallion integration."""

from andes_data_lake.models.errors import AndesError


class MedallionError(AndesError):
    """Base exception for all Medallion Architecture errors."""

    code = "MEDALLION_ERROR"


class BronzeIngestionError(MedallionError):
    """Raised when Bronze layer ingestion fails."""

    code = "BRONZE_INGESTION_ERROR"


class SilverProcessingError(MedallionError):
    """Raised when Silver layer processing fails."""

    code = "SILVER_PROCESSING_ERROR"


class GoldAggregationError(MedallionError):
    """Raised when Gold layer aggregation fails."""

    code = "GOLD_AGGREGATION_ERROR"


class DeltaTableError(MedallionError):
    """Raised when Delta table operations fail."""

    code = "DELTA_TABLE_ERROR"


class SchemaViolationError(MedallionError):
    """Raised when a schema mismatch is detected during a Delta write."""

    code = "SCHEMA_VIOLATION_ERROR"


class DatabricksConnectionError(MedallionError):
    """Raised when the Databricks workspace is unreachable."""

    code = "DATABRICKS_CONNECTION_ERROR"
