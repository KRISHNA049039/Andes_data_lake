# Implementation Plan: Andes Data Lake

## Overview

This plan implements the Andes Data Lake system in Python. The architecture follows a modular, event-driven design with components for catalog management, ingestion, stream validation, multi-layer validation, RBAC, concurrency management, and centralized logging. Apache Iceberg on S3 provides the storage layer, Kafka handles live streams, DynamoDB backs the catalog metadata and log store, and ZooKeeper/etcd handles distributed coordination. Property-based tests use Hypothesis; unit tests use pytest.

## Tasks

- [ ] 1. Set up project structure, core data models, and shared utilities
  - [ ] 1.1 Create project directory structure and install dependencies
    - Create top-level package `andes_data_lake/` with sub-packages: `models/`, `catalog/`, `ingestion/`, `validation/`, `streaming/`, `rbac/`, `concurrency/`, `logging_store/`, `api/`, `tests/`
    - Create `pyproject.toml` or `requirements.txt` with dependencies: `boto3`, `pydantic`, `kafka-python`, `pyiceberg`, `hypothesis`, `pytest`, `kazoo` (ZooKeeper client)
    - _Requirements: All_

  - [ ] 1.2 Implement core data models using Pydantic
    - Implement `DatasetMetadata`, `DatasetVersion`, `SchemaDefinition`, `FieldDefinition`, `Constraint` models in `models/dataset.py`
    - Implement `Subscription` model with status enum (PENDING, APPROVED, REJECTED, REVOKED) in `models/subscription.py`
    - Implement `PipelineRun` model with status enum (RUNNING, COMPLETED, FAILED, ROLLED_BACK, SKIPPED) in `models/pipeline.py`
    - Implement `LogEntry` model with severity enum (DEBUG, INFO, WARN, ERROR, FATAL) in `models/log_entry.py`
    - Implement `ValidationAuditRecord` model with per-layer result enums in `models/validation.py`
    - Implement `Role`, `Permission`, `UserRoleAssignment` models in `models/rbac.py`
    - Implement `DistributedLock`, `LeaderElection` models in `models/concurrency.py`
    - Implement `DLQRecord` model in `models/dlq.py`
    - Implement shared error response model (`ErrorResponse`) in `models/errors.py`
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 3.1, 3.4, 4.1, 4.5, 7.1, 8.8, 9.2, 10.1, 10.3_

  - [ ]* 1.3 Write unit tests for data model validation
    - Test Pydantic validation rules: required fields, enum constraints, UUID generation, timestamp formatting
    - Test edge cases: empty strings, boundary values, nullable fields
    - _Requirements: 1.1, 4.1, 9.2_

- [ ] 2. Implement the Log Store
  - [ ] 2.1 Implement `LogStore` class in `logging_store/log_store.py`
    - Implement `write(entry: LogEntry) → None` persisting to DynamoDB with partition key (sourceComponent) and sort key (timestamp)
    - Implement `query(time_range?, component?, correlation_id?) → list[LogEntry]` with DynamoDB query/scan and GSI on correlationId
    - Implement `buffer_locally(entry: LogEntry) → None` for local buffering during outages
    - Implement `flush_buffer() → None` to deliver buffered entries when DynamoDB becomes reachable
    - Implement `configure_retention(retention_days: int) → None` to set TTL on DynamoDB items
    - Implement retry logic with exponential backoff for DynamoDB write failures
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 9.6_

  - [ ]* 2.2 Write property test: Log entry structure (Property 18)
    - **Property 18: Log entry structure**
    - For any log entry persisted, it must contain timestamp, source component, severity, and correlation ID
    - **Validates: Requirements 9.2**

  - [ ]* 2.3 Write property test: Log query correctness and ordering (Property 19)
    - **Property 19: Log query correctness and ordering**
    - For any query by time range, component, or correlation ID, returned entries match filter and are ordered by timestamp
    - **Validates: Requirements 9.3**

  - [ ]* 2.4 Write property test: Log buffering and delivery guarantee (Property 20)
    - **Property 20: Log buffering and delivery guarantee**
    - For any period of Log Store unavailability, buffered entries are delivered once reachable, with zero loss
    - **Validates: Requirements 9.5**

  - [ ]* 2.5 Write unit tests for Log Store
    - Test query filtering by each parameter individually and combined
    - Test buffer overflow behavior (drop oldest entries, log warning)
    - Test retention configuration and TTL
    - _Requirements: 9.3, 9.5, 9.6_

- [ ] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 4. Implement the RBAC Module
  - [ ] 4.1 Implement `RBACModule` class in `rbac/rbac_module.py`
    - Define three default roles: `Controllership_Team_Role`, `BI_Team_Role`, `Administrator_Role` with their permission sets
    - Implement `authorize(user_id, action, resource) → AuthorizationResult` checking user roles against required permissions
    - Implement `assign_role(user_id, role) → None` and `revoke_role(user_id, role) → None` with immediate effect
    - Implement `get_roles(user_id) → list[Role]` and `list_roles() → list[Role]`
    - Log every authorization decision (grant/deny) to the Log Store with user identity, action, and outcome
    - Log every role assignment/revocation to the audit log
    - Return `AUTHORIZATION_DENIED` with action and role details for unauthorized actions
    - Return `INVALID_ROLE` error for invalid role assignments
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7_

  - [ ]* 4.2 Write property test: Role-permission mapping (Property 15)
    - **Property 15: Role-permission mapping**
    - For any user assigned a role, RBAC authorizes exactly the defined actions and denies all others
    - **Validates: Requirements 7.2, 7.3, 7.4, 7.5**

  - [ ]* 4.3 Write property test: RBAC audit logging (Property 16)
    - **Property 16: RBAC audit logging**
    - For any authorization decision, the RBAC module logs user identity, requested action, and outcome
    - **Validates: Requirements 7.6, 7.7**

  - [ ]* 4.4 Write unit tests for RBAC Module
    - Test each role's permission set explicitly
    - Test unauthorized action denial with descriptive error
    - Test immediate effect of role assignment and revocation
    - _Requirements: 7.2, 7.3, 7.4, 7.5, 7.6_

- [ ] 5. Implement the Validation Pipeline
  - [ ] 5.1 Implement `IngestionValidator` in `validation/ingestion_validator.py`
    - Implement `validate(record) → ValidationResult` checking format, encoding, and required header fields
    - Return `INVALID_FORMAT` error with field-level details on failure
    - _Requirements: 8.1, 8.2_

  - [ ] 5.2 Implement `SchemaValidator` in `validation/schema_validator.py`
    - Implement `validate(record, schema: SchemaDefinition) → ValidationResult` checking field types, required fields, and constraints
    - Return `SCHEMA_VIOLATION` error with field name and expected type on failure
    - _Requirements: 8.3, 8.4_

  - [ ] 5.3 Implement `BusinessRuleValidator` in `validation/business_rule_validator.py`
    - Implement `validate(record, rules) → ValidationResult` checking cross-field consistency and accounting constraints
    - Return `BUSINESS_RULE_VIOLATION` error with rule name and violated constraint on failure
    - _Requirements: 8.5, 8.6_

  - [ ] 5.4 Implement `ValidationPipeline` orchestrator in `validation/pipeline.py`
    - Chain validators in order: Ingestion → Schema → Business Rule
    - Stop at first failing layer and return descriptive error
    - Only commit records that pass all three layers
    - Record validation outcome of each executed layer in `ValidationAuditRecord`
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8_

  - [ ]* 5.5 Write property test: Multi-layer validation pipeline (Property 17)
    - **Property 17: Multi-layer validation pipeline**
    - For any record, validation executes layers in order, stops at first failure, rejects with descriptive error, and only commits records passing all layers. Audit log records each executed layer's outcome.
    - **Validates: Requirements 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8**

  - [ ]* 5.6 Write unit tests for Validation Pipeline
    - Test records that fail at each layer individually
    - Test records that pass all layers
    - Test validation audit record creation
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8_

- [ ] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Implement the Catalog Service
  - [ ] 7.1 Implement `CatalogService` class in `catalog/catalog_service.py`
    - Implement `register_dataset(name, schema, owner, description) → DatasetMetadata` storing metadata in DynamoDB
    - Implement `get_dataset(dataset_id) → DatasetMetadata` retrieval by partition key
    - Implement `list_datasets(filters?) → list[DatasetMetadata]` returning all published datasets with name, description, schema, owner, publication date
    - Implement `search_datasets(keyword) → list[DatasetMetadata]` with case-insensitive matching on name and description
    - Implement `get_version_history(dataset_id) → list[DatasetVersion]` querying by partition key with version sort key
    - Handle dataset name conflicts by creating new versions (not overwriting)
    - Integrate with RBAC module for authorization checks on all operations
    - Log all operations to the Log Store
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.3_

  - [ ]* 7.2 Write property test: Dataset registration round-trip (Property 1)
    - **Property 1: Dataset registration round-trip**
    - For any valid dataset submission, registering and retrieving by ID returns matching name, schema, owner, description
    - **Validates: Requirements 1.1, 1.2**

  - [ ]* 7.3 Write property test: Dataset versioning on re-publish (Property 2)
    - **Property 2: Dataset versioning on re-publish**
    - For any dataset published N times with the same name, Catalog contains exactly N versions, current version equals N
    - **Validates: Requirements 1.3**

  - [ ]* 7.4 Write property test: Catalog search completeness (Property 3)
    - **Property 3: Catalog search completeness**
    - For any set of datasets and keyword, search returns exactly those whose name or description contains the keyword (case-insensitive)
    - **Validates: Requirements 2.2**

  - [ ]* 7.5 Write property test: Catalog listing field completeness (Property 4)
    - **Property 4: Catalog listing field completeness**
    - For any set of published datasets, listing returns every dataset with name, description, schema, owner, publication date
    - **Validates: Requirements 2.1, 2.3**

  - [ ]* 7.6 Write unit tests for Catalog Service
    - Test dataset registration with valid and invalid inputs
    - Test version creation on duplicate name publish
    - Test search with various keywords including edge cases (empty string, special characters)
    - Test RBAC integration (authorized vs unauthorized access)
    - _Requirements: 1.1, 1.2, 1.3, 2.1, 2.2, 2.3_

- [ ] 8. Implement Subscription Management
  - [ ] 8.1 Implement subscription methods in `CatalogService`
    - Implement `create_subscription_request(dataset_id, user_id) → SubscriptionRequest` creating PENDING subscription
    - Implement `approve_subscription(subscription_id) → Subscription` transitioning to APPROVED and granting read access
    - Implement `reject_subscription(subscription_id, reason) → Subscription` transitioning to REJECTED with reason preserved
    - Implement `revoke_subscription(subscription_id) → Subscription` transitioning to REVOKED and removing read access
    - Implement `list_subscriptions(dataset_id) → list[Subscription]` returning all active and revoked subscriptions
    - Return `DATASET_NOT_FOUND` error for subscriptions to non-existent datasets
    - Integrate with RBAC for authorization on approve/reject/revoke actions
    - Log all subscription lifecycle events to the Log Store
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

  - [ ]* 8.2 Write property test: Subscription state machine (Property 5)
    - **Property 5: Subscription state machine**
    - For any subscription request, lifecycle follows: PENDING → APPROVED (grants access) or REJECTED (reason preserved); APPROVED → REVOKED (removes access). Complete transition record maintained.
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5**

  - [ ]* 8.3 Write unit tests for Subscription Management
    - Test full subscription lifecycle (request → approve → revoke)
    - Test rejection flow with reason preservation
    - Test subscription to non-existent dataset error
    - Test listing active and revoked subscriptions
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 9. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 10. Implement the Ingestion Engine
  - [ ] 10.1 Implement `IngestionEngine` class in `ingestion/ingestion_engine.py`
    - Implement `submit_batch(dataset_id, payload, idempotency_key) → PipelineRunResult` orchestrating the full ingestion pipeline
    - Assign unique idempotency key to each pipeline run; detect and skip duplicate runs returning original result
    - Integrate with `ValidationPipeline` for multi-layer validation of each record
    - Implement atomic commit of all validated records to Data Lake (Iceberg/S3) in a single transaction
    - Implement rollback of all partial writes on mid-run failure, setting status to FAILED
    - Implement `get_pipeline_run_status(run_id) → PipelineRunStatus`
    - Implement `rollback_run(run_id) → None` and `commit_run(run_id) → None`
    - Log start time, end time, record count, and status of each pipeline run to Log Store
    - Record validation audit for each processed record
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 8.7, 8.8_

  - [ ]* 10.2 Write property test: Pipeline idempotency (Property 6)
    - **Property 6: Pipeline idempotency**
    - For any completed pipeline run, re-submitting with same idempotency key returns original result without re-execution
    - **Validates: Requirements 4.1, 4.2**

  - [ ]* 10.3 Write property test: Pipeline failure rollback (Property 7)
    - **Property 7: Pipeline failure rollback**
    - For any pipeline run that fails midway, all partial writes are rolled back, Data Lake state identical to before the run
    - **Validates: Requirements 4.3**

  - [ ]* 10.4 Write property test: Pipeline run logging completeness (Property 8)
    - **Property 8: Pipeline run logging completeness**
    - For any pipeline run, log contains start time, end time, record count, and final status
    - **Validates: Requirements 4.5**

  - [ ]* 10.5 Write unit tests for Ingestion Engine
    - Test successful batch ingestion end-to-end
    - Test idempotency key duplicate detection
    - Test rollback on mid-run failure
    - Test atomic commit behavior
    - Test pipeline run status retrieval
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [ ] 11. Implement ACID Compliance Layer
  - [ ] 11.1 Implement transaction management in `ingestion/transaction_manager.py`
    - Implement atomic write operations using Apache Iceberg transaction API
    - Implement snapshot isolation for concurrent readers (readers see only committed data)
    - Implement durable commit ensuring records survive system restarts
    - Implement schema constraint enforcement at transaction level (reject violating transactions)
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

  - [ ]* 11.2 Write property test: Transaction atomicity (Property 12)
    - **Property 12: Transaction atomicity**
    - For any multi-record write transaction, either all records are committed and visible, or none are
    - **Validates: Requirements 6.1**

  - [ ]* 11.3 Write property test: Transaction isolation (Property 13)
    - **Property 13: Transaction isolation**
    - For any in-progress write, concurrent readers observe only previously committed data
    - **Validates: Requirements 6.3**

  - [ ]* 11.4 Write unit tests for ACID compliance
    - Test atomic commit of multi-record transactions
    - Test rollback leaves no partial records visible
    - Test concurrent read during write sees only committed data
    - Test schema constraint rejection at transaction level
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [ ] 12. Implement the Stream Validator and Live Ingestion
  - [ ] 12.1 Implement `StreamValidator` class in `streaming/stream_validator.py`
    - Implement `validate_record(record, schema) → ValidationResult` validating against registered schema
    - Implement `route_to_dead_letter_queue(record, error) → None` routing invalid records to DLQ with validation error logged
    - Implement `get_processing_latency() → Duration` for monitoring
    - Integrate with `ValidationPipeline` for multi-layer validation
    - _Requirements: 5.1, 5.2, 5.5_

  - [ ] 12.2 Implement Kafka consumer and stream ingestion in `streaming/stream_consumer.py`
    - Implement Kafka consumer with consumer group offset management
    - Maintain record ordering by source timestamp
    - Persist last committed offset for resumption after interruption
    - Resume from last committed offset on reconnection (no data loss, no duplicates)
    - Commit valid records to Data Lake via Ingestion Engine
    - _Requirements: 5.1, 5.3, 5.4_

  - [ ]* 12.3 Write property test: Stream validation routing (Property 9)
    - **Property 9: Stream validation routing**
    - For any streamed record, valid records committed to Data Lake, invalid routed to DLQ. No record both committed and in DLQ.
    - **Validates: Requirements 5.1, 5.2**

  - [ ]* 12.4 Write property test: Stream ordering preservation (Property 10)
    - **Property 10: Stream ordering preservation**
    - For any sequence of streamed records, committed records are ordered by source timestamp
    - **Validates: Requirements 5.3**

  - [ ]* 12.5 Write property test: Stream resumption from last offset (Property 11)
    - **Property 11: Stream resumption from last offset**
    - For any stream interruption, resumption starts from last committed offset with no data loss and no duplicates
    - **Validates: Requirements 5.4**

  - [ ]* 12.6 Write unit tests for Stream Validator
    - Test valid record passes through to Data Lake
    - Test invalid record routed to DLQ with error logged
    - Test ordering preservation with out-of-order source timestamps
    - Test resumption after simulated connection interruption
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [ ] 13. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 14. Implement the Concurrency Manager
  - [ ] 14.1 Implement `ConcurrencyManager` class in `concurrency/concurrency_manager.py`
    - Implement `acquire_lock(resource_id, node_id, lease_duration) → LockResult` using ZooKeeper/etcd for distributed locks
    - Implement `renew_lock(lock_id, node_id) → LockResult` for lease renewal
    - Implement `release_lock(lock_id, node_id) → None` for explicit release
    - Implement automatic lock release on lease expiry without renewal
    - Return `LOCK_HELD` with holder info on contention; caller retries with backoff
    - _Requirements: 10.1, 10.2_

  - [ ] 14.2 Implement leader election in `concurrency/leader_election.py`
    - Implement `elect_leader(pipeline_id) → LeaderElectionResult` electing exactly one leader per pipeline
    - Implement failover: detect unresponsive leader and trigger new election within configured timeout
    - _Requirements: 10.3, 10.4_

  - [ ] 14.3 Implement conflict resolution in `concurrency/conflict_resolver.py`
    - Implement `resolve_conflict(write_a, write_b, version_policy) → ConflictResolution` using OCC version comparison
    - Detect conflicts via version tokens, fail one transaction, retry so both eventually succeed
    - Implement `serialize_operations(op_a, op_b) → SerializedResult` for concurrent subscription modifications
    - _Requirements: 6.5, 10.5, 10.6_

  - [ ] 14.4 Integrate Concurrency Manager logging
    - Log every lock acquisition, release, leader election, and conflict resolution event to Log Store
    - _Requirements: 10.7_

  - [ ]* 14.5 Write property test: Distributed lock mutual exclusion (Property 21)
    - **Property 21: Distributed lock mutual exclusion**
    - For concurrent lock requests on same resource, exactly one succeeds; lock released on lease expiry allows re-acquisition
    - **Validates: Requirements 10.1, 10.2**

  - [ ]* 14.6 Write property test: Single leader election (Property 22)
    - **Property 22: Single leader election**
    - For any pipeline, exactly one leader at any time; unresponsive leader triggers new election within failover timeout
    - **Validates: Requirements 10.3, 10.4**

  - [ ]* 14.7 Write property test: OCC conflict resolution (Property 14)
    - **Property 14: Optimistic concurrency conflict resolution**
    - For two concurrent writes to same partition, conflict detected via version comparison, one fails and retries, both eventually succeed
    - **Validates: Requirements 6.5, 10.5**

  - [ ]* 14.8 Write property test: Concurrent subscription modification serialization (Property 23)
    - **Property 23: Concurrent subscription modification serialization**
    - For two concurrent modifications to same Subscription, operations serialized, final state reflects both changes
    - **Validates: Requirements 10.6**

  - [ ]* 14.9 Write property test: Concurrency event logging (Property 24)
    - **Property 24: Concurrency event logging**
    - For any lock, release, election, or conflict event, the event is logged to the Log Store
    - **Validates: Requirements 10.7**

  - [ ]* 14.10 Write unit tests for Concurrency Manager
    - Test lock acquisition and release
    - Test lease expiry and re-acquisition
    - Test leader election with single and multiple candidates
    - Test failover on unresponsive leader
    - Test OCC conflict detection and retry
    - _Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_

- [ ] 15. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 16. Implement the API Layer and wire all components together
  - [ ] 16.1 Implement REST API endpoints in `api/routes.py`
    - Implement dataset publishing endpoints: `POST /datasets`, `GET /datasets`, `GET /datasets/{id}`, `GET /datasets/search`
    - Implement subscription endpoints: `POST /datasets/{id}/subscriptions`, `PUT /subscriptions/{id}/approve`, `PUT /subscriptions/{id}/reject`, `PUT /subscriptions/{id}/revoke`, `GET /datasets/{id}/subscriptions`
    - Implement pipeline endpoints: `POST /datasets/{id}/ingest`, `GET /pipeline-runs/{id}`
    - Implement RBAC endpoints: `POST /roles/assign`, `DELETE /roles/revoke`, `GET /users/{id}/roles`
    - Implement log query endpoint: `GET /logs`
    - Apply RBAC authorization middleware to all endpoints
    - Use consistent error response format (`ErrorResponse`) for all API errors
    - _Requirements: 1.1, 2.1, 2.2, 3.1, 3.2, 3.3, 3.5, 4.1, 7.1, 7.5, 9.3_

  - [ ] 16.2 Wire all components with dependency injection
    - Initialize LogStore, RBACModule, CatalogService, ValidationPipeline, IngestionEngine, StreamValidator, ConcurrencyManager
    - Connect Ingestion Engine → Validation Pipeline → Data Lake
    - Connect Catalog Service → RBAC Module for authorization
    - Connect Ingestion Engine → Concurrency Manager for distributed locks
    - Connect all components → Log Store for centralized logging
    - _Requirements: All_

  - [ ]* 16.3 Write integration tests for API endpoints
    - Test dataset publishing end-to-end through API
    - Test subscription lifecycle through API
    - Test RBAC enforcement at API level
    - Test error response format consistency
    - _Requirements: 1.1, 3.1, 7.5_

- [ ] 17. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation after each major component
- Property tests use Hypothesis with minimum 100 iterations per property
- Unit tests use pytest
- All 24 correctness properties from the design are covered by property test tasks
- The Log Store is implemented first since all other components depend on it for centralized logging
