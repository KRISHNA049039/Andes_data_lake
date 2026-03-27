# Implementation Plan: Databricks Medallion Integration

## Overview

Implement the `andes_data_lake/databricks/` package that adds Medallion Architecture (Bronze → Silver → Gold) using Delta Lake on Databricks. Tasks are ordered so each builds on the previous: error classes and data models first, then low-level infrastructure (workspace client, delta table manager), then layer processors (bronze, silver, gold), then orchestration, job management, and finally wiring with existing Andes components.

## Tasks

- [x] 1. Create error hierarchy and data models
  - [x] 1.1 Create the `andes_data_lake/databricks/` package with `__init__.py` and the `MedallionError` hierarchy in `errors.py`
    - Define `MedallionError(AndesError)`, `BronzeIngestionError`, `SilverProcessingError`, `GoldAggregationError`, `DeltaTableError`, `SchemaViolationError`, `DatabricksConnectionError`
    - Each error class must have a unique `code` attribute
    - _Requirements: 5.2, 12.4, 16.1, 16.3, 16.4_

  - [x] 1.2 Create data models in `models.py`
    - Define all enums: `MedallionLayer`, `MedallionPipelineStatus`, `WriteMode`, `SourceType`, `AggregationFunction`, `JobRunStatus`
    - Define all Pydantic models: `SourceMetadata`, `BronzeResult`, `SilverResult`, `ValidationSummary`, `GoldResult`, `DeltaColumn`, `DeltaSchema`, `DeltaTableMetadata`, `AggregationSpec`, `AggregationConfig`, `ClusterConfig`, `MedallionJobConfig`, `DatabricksJobConfig`, `MedallionPipelineRun`, `JobRunResult`
    - Add validation rules: `end_time >= start_time`, `idempotency_key` non-empty, `group_by_columns` non-empty in AggregationConfig
    - _Requirements: 1.1, 1.3, 3.3, 4.3, 6.1, 7.1, 8.2, 10.1_

  - [ ]* 1.3 Write property test for data model validation rules
    - **Property 2: Medallion layer progression** — verify `MedallionPipelineRun.current_layer` only progresses BRONZE → SILVER → GOLD
    - **Validates: Requirements 8.1, 8.2**

  - [ ]* 1.4 Write unit tests for error classes and data models
    - Test error codes, inheritance from `AndesError`, model serialization round-trips, and validation constraints
    - _Requirements: 5.2, 12.4, 16.4_

- [x] 2. Implement DatabricksWorkspaceClient
  - [x] 2.1 Create `workspace_client.py` with `DatabricksWorkspaceClient`
    - Implement `__init__(host, token)` with token-based authentication
    - Implement `execute_sql(sql, warehouse_id)` to submit SQL to a Databricks SQL Warehouse and return results
    - Implement `submit_job(job_config)` returning a `JobRunResult`
    - Implement `get_job_status(run_id)` returning `JobRunStatus`
    - Implement `create_cluster(config)` and `terminate_cluster(cluster_id)`
    - Raise `DatabricksConnectionError` when workspace is unreachable
    - _Requirements: 12.1, 12.2, 12.3, 12.4_

  - [ ]* 2.2 Write unit tests for DatabricksWorkspaceClient
    - Test authentication setup, SQL execution, job submission, and connection error handling
    - Mock the Databricks SDK calls
    - _Requirements: 12.1, 12.2, 12.3, 12.4_

- [x] 3. Implement DeltaTableManager
  - [x] 3.1 Create `delta_table_manager.py` with `DeltaTableManager`
    - Implement `__init__(workspace_client, log_store)`
    - Implement `create_table(table_name, schema, layer, partition_keys)` following `{layer}_{dataset_name}` naming, registering in Unity Catalog
    - Implement `write(table_path, records, mode)` supporting APPEND, OVERWRITE, MERGE with atomic writes
    - Implement `read(table_path, filters)` returning list of dicts
    - Implement `get_table_metadata(table_path)` returning `DeltaTableMetadata`
    - Implement `evolve_schema(table_path, new_schema)` for additive schema evolution
    - Implement `optimize_table(table_path)` for file compaction
    - Implement `vacuum_table(table_path, retention_hours=168)` for old version cleanup
    - Raise `SchemaViolationError` on schema mismatch, `DeltaTableError` on creation failure
    - _Requirements: 5.1, 5.2, 10.1, 10.2, 10.3, 10.4, 10.5, 11.1, 11.2, 11.3, 16.4_

  - [ ]* 3.2 Write property test for Delta table write atomicity
    - **Property 11: Delta table write atomicity** — verify all-or-nothing write semantics across APPEND, OVERWRITE, MERGE modes
    - **Validates: Requirement 10.2**

  - [ ]* 3.3 Write property test for Delta schema enforcement
    - **Property 13: Delta table schema enforcement** — verify records violating the table schema are rejected with `SchemaViolationError`
    - **Validates: Requirement 5.1**

  - [ ]* 3.4 Write unit tests for DeltaTableManager
    - Test table creation with naming convention, partition keys, schema evolution, optimize, vacuum with default retention
    - _Requirements: 10.1, 10.3, 10.4, 10.5, 11.1, 11.2, 11.3_

- [ ] 4. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Implement BronzeLayerProcessor
  - [x] 5.1 Create `bronze_processor.py` with `BronzeLayerProcessor`
    - Implement `__init__(delta_table_manager, log_store)`
    - Implement `ingest_batch(dataset_id, records, source_metadata)` that writes raw records to Bronze Delta table with appended ingestion metadata (`ingestion_timestamp`, `source_system`, `batch_id`), partitioned by ingestion date, append-only mode
    - Implement `ingest_stream_record(dataset_id, record, topic, offset)` that appends a single stream record with topic, offset, and ingestion_timestamp metadata
    - Return `BronzeResult` where `record_count == len(records)`
    - Log Bronze ingestion events to Log Store
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 15.2_

  - [ ]* 5.2 Write property test for Bronze raw fidelity
    - **Property 1: Bronze layer raw fidelity** — verify original record content is preserved without modification, with ingestion metadata appended
    - **Validates: Requirements 1.1, 1.2, 1.3, 2.1**

  - [ ]* 5.3 Write property test for stream micro-batch Bronze landing
    - **Property 12: Stream micro-batch Bronze landing** — verify all records in a micro-batch land in Bronze before Silver processing, and record count matches batch size
    - **Validates: Requirement 2.3**

  - [ ]* 5.4 Write unit tests for BronzeLayerProcessor
    - Test batch ingestion metadata appending, stream record ingestion, append-only mode, partitioning by ingestion date
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 2.1_

- [x] 6. Implement SilverLayerProcessor
  - [x] 6.1 Create `silver_processor.py` with `SilverLayerProcessor`
    - Implement `__init__(delta_table_manager, validation_pipeline, log_store, dead_letter_queue)`
    - Implement `process_batch(dataset_id, bronze_table_path, schema_context)`:
      - Read records from Bronze Delta table
      - Pass each record through the existing `ValidationPipeline` (Ingestion → Schema → Business Rule) in order
      - Route failed records to `DeadLetterQueue` with error details; do NOT pass failed records to subsequent validation layers
      - Create a validation audit record for every processed record
      - Deduplicate valid records using configured dedup keys
      - Write cleansed records to Silver Delta table
      - Return `SilverResult` where `valid_count + rejected_count == total_bronze_records`
    - Implement `process_micro_batch(dataset_id, records, schema_context)` with same validation/dedup logic for streaming
    - Implement `deduplicate(records, dedup_keys)` retaining only the record with the latest timestamp per unique key combination
    - Log Silver processing events to Log Store
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 4.1, 4.2, 4.3, 4.4, 15.2, 16.2_

  - [ ]* 6.2 Write property test for Silver validation completeness
    - **Property 3: Silver validation completeness** — verify `valid_count + rejected_count == total_bronze_records` for any input set
    - **Validates: Requirements 3.3, 16.2**

  - [ ]* 6.3 Write property test for Silver deduplication correctness
    - **Property 4: Silver deduplication correctness** — verify exactly one record per unique dedup key, latest timestamp retained, output is subset of input
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4**

  - [ ]* 6.4 Write property test for Silver validation pipeline integration
    - **Property 5: Silver validation pipeline integration** — verify 3-layer validation order (Ingestion → Schema → Business Rule), early termination on failure, audit records reflect each executed layer
    - **Validates: Requirements 3.1, 3.2, 3.4**

  - [ ]* 6.5 Write unit tests for SilverLayerProcessor
    - Test validation pass/fail routing, DLQ integration, deduplication edge cases (all duplicates, no duplicates, single record), validation audit creation
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 4.1, 4.2, 4.3, 4.4, 16.2_

- [x] 7. Implement GoldLayerProcessor
  - [x] 7.1 Create `gold_processor.py` with `GoldLayerProcessor`
    - Implement `__init__(delta_table_manager, log_store)`
    - Implement `aggregate(dataset_id, silver_table_path, config)`:
      - Read validated records from Silver Delta table
      - Apply `filter_condition` before aggregation if specified
      - Group records by `group_by_columns`
      - Compute each aggregation function (SUM, AVG, COUNT, MIN, MAX, COUNT_DISTINCT) per group
      - Produce exactly one output row per unique group_by combination
      - Support incremental refresh: process only records newer than last watermark when `config.incremental == True`
      - Update watermark after incremental refresh
      - Full recompute with OVERWRITE mode when `config.incremental == False`
      - Write results to Gold Delta table
      - Return `GoldResult` with `aggregate_count`
    - Implement `refresh_materialized_view(view_name, config)` delegating to `aggregate`
    - Log Gold processing events to Log Store
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 7.1, 7.2, 7.3, 7.4, 15.2_

  - [ ]* 7.2 Write property test for Gold aggregation correctness
    - **Property 6: Gold aggregation correctness** — verify one row per group_by combination, mathematically correct SUM/AVG/COUNT/MIN/MAX values
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4**

  - [ ]* 7.3 Write property test for Gold incremental consistency
    - **Property 7: Gold incremental consistency** — verify incremental refresh produces results equivalent to full recomputation
    - **Validates: Requirements 7.1, 7.2, 7.3**

  - [ ]* 7.4 Write unit tests for GoldLayerProcessor
    - Test aggregation functions, filter conditions, incremental vs full refresh, watermark updates, empty Silver table
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 7.1, 7.2, 7.3, 7.4_

- [ ] 8. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement MedallionOrchestrator
  - [x] 9.1 Create `medallion_orchestrator.py` with `MedallionOrchestrator`
    - Implement `__init__(bronze_processor, silver_processor, gold_processor, catalog_service, rbac, log_store, idempotency_store)`
    - Implement `submit_batch(dataset_id, payload, idempotency_key, user_id)`:
      - Check idempotency store before any processing; return original result with SKIPPED status for duplicates
      - Authorize via RBAC; raise `AuthorizationDeniedError` if denied, write no data
      - Create `MedallionPipelineRun` with unique `correlation_id`
      - Log pipeline start to Log Store with run_id, dataset_id, correlation_id
      - Process Bronze → Silver → Gold in strict order, tracking `current_layer`
      - On success: set status to COMPLETED, register medallion metadata in Catalog, log completion with duration
      - On failure at any layer: set status to FAILED, log error with correlation_id and failed layer, stop subsequent layers
    - Implement `process_stream_record(record, topic, offset)` accumulating records and triggering micro-batch on count/time threshold
    - Implement `trigger_gold_refresh(dataset_id, aggregation_config, user_id)` with RBAC check before Gold execution
    - Implement `get_pipeline_status(run_id)` returning `MedallionPipelineStatus`
    - _Requirements: 2.2, 8.1, 8.2, 8.3, 8.4, 9.1, 9.2, 9.3, 14.1, 14.2, 14.3, 15.1, 15.2, 15.3, 15.4, 15.5, 16.1, 16.3, 16.5_

  - [ ]* 9.2 Write property test for Medallion pipeline idempotency
    - **Property 8: Medallion pipeline idempotency** — verify re-submission with same idempotency key returns original result, no re-execution, tables unchanged
    - **Validates: Requirements 9.1, 9.2, 9.3**

  - [ ]* 9.3 Write property test for RBAC enforcement on medallion operations
    - **Property 9: RBAC enforcement on medallion operations** — verify unauthorized users cannot write to any medallion layer, `AuthorizationDeniedError` raised
    - **Validates: Requirements 14.1, 14.2, 14.3**

  - [ ]* 9.4 Write property test for Medallion pipeline logging completeness
    - **Property 10: Medallion pipeline logging completeness** — verify Log Store contains entries for pipeline start, each layer start/complete, and pipeline end with correlation_id
    - **Validates: Requirements 15.1, 15.2, 15.3, 15.4, 15.5**

  - [ ]* 9.5 Write property test for Medallion pipeline failure rollback
    - **Property 14: Medallion pipeline failure rollback** — verify failed pipeline sets status to FAILED, logs error with correlation_id, does not corrupt previous successful data
    - **Validates: Requirements 8.4, 16.1, 16.3**

  - [ ]* 9.6 Write unit tests for MedallionOrchestrator
    - Test full batch flow (success), idempotency skip, RBAC denial, failure at each layer, stream micro-batch triggering, Gold refresh with RBAC, logging completeness
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 9.1, 9.2, 9.3, 14.1, 14.2, 14.3, 15.1, 15.2, 15.3, 15.4, 15.5, 16.1, 16.3, 16.5_

- [x] 10. Implement DatabricksJobManager
  - [x] 10.1 Create `job_manager.py` with `DatabricksJobManager`
    - Implement `__init__(workspace_client, log_store)`
    - Implement `create_medallion_job(dataset_id, schedule, config)` defining a Databricks workflow with cron schedule and medallion config
    - Implement `trigger_job(job_id)` returning `JobRunResult` and logging execution to Log Store
    - Implement `get_job_run_status(run_id)` returning `JobRunStatus`
    - Implement `list_jobs(dataset_id)` with optional filtering by dataset_id
    - Implement `delete_job(job_id)`
    - On job failure: set status to FAILED, log failure details to Log Store
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

  - [ ]* 10.2 Write unit tests for DatabricksJobManager
    - Test job creation with cron schedule, on-demand trigger, job listing with dataset filter, failure logging
    - _Requirements: 13.1, 13.2, 13.3, 13.4, 13.5_

- [ ] 11. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Wire integration with existing Andes components and create package exports
  - [x] 12.1 Update `andes_data_lake/databricks/__init__.py` with public API exports
    - Export all public classes: `MedallionOrchestrator`, `BronzeLayerProcessor`, `SilverLayerProcessor`, `GoldLayerProcessor`, `DeltaTableManager`, `DatabricksWorkspaceClient`, `DatabricksJobManager`
    - Export all data models and enums
    - Export all error classes
    - _Requirements: all_

  - [x] 12.2 Create a composition root example in `andes_data_lake/databricks/factory.py`
    - Implement a `create_medallion_orchestrator(config)` factory function that wires all dependencies: workspace client, delta table manager, bronze/silver/gold processors, and orchestrator
    - Inject existing Andes components: `ValidationPipeline`, `CatalogService`, `RBACModule`, `LogStore`, `DeadLetterQueue`, `IdempotencyStore`
    - _Requirements: 3.1, 8.1, 9.3, 14.1, 15.1_

  - [ ]* 12.3 Write integration tests for end-to-end medallion pipeline
    - Test full batch flow through factory-created orchestrator with mocked Databricks SDK
    - Verify Bronze → Silver → Gold data flow, RBAC checks, idempotency, logging, and catalog registration
    - _Requirements: 8.1, 8.2, 8.3, 9.1, 14.1, 15.1_

- [ ] 13. Final checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- Unit tests validate specific examples and edge cases
- All code is Python, using Pydantic for models, Hypothesis for property tests, and pytest as the test runner
- The implementation integrates with existing Andes components via dependency injection — no modifications to existing code required
