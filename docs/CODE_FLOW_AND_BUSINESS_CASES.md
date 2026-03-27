# Andes Data Lake — Code Flow & Business Use Cases

This document walks through the real code flow for each business use case,
tracing exactly which classes, methods, and files are involved at each step.

---

## Table of Contents

1. [Business Context](#1-business-context)
2. [Use Case 1: Controllership Team Publishes Quarterly Accounting Data](#2-use-case-1)
3. [Use Case 2: Live Stock Market Data Streaming](#3-use-case-2)
4. [Use Case 3: BI Team Discovers and Subscribes to a Dataset](#4-use-case-3)
5. [Use Case 4: Scheduled Daily Medallion Pipeline](#5-use-case-4)
6. [Use Case 5: On-Demand Gold Refresh for Month-End Reporting](#6-use-case-5)
7. [Use Case 6: Handling Invalid Records (DLQ Flow)](#7-use-case-6)
8. [Use Case 7: Duplicate Submission (Idempotency)](#8-use-case-7)
9. [Use Case 8: Unauthorized Access Attempt](#9-use-case-8)
10. [End-to-End Batch Pipeline Trace](#10-end-to-end-batch-pipeline-trace)
11. [End-to-End Streaming Pipeline Trace](#11-end-to-end-streaming-pipeline-trace)

---

## 1. Business Context

Andes Data Lake serves a financial organization where:

- The Controllership Team manages accounting datasets (journal entries, trial balances, reconciliations)
- The BI Team needs curated, validated data for dashboards and financial reporting
- Live stock market feeds (prices, trades, positions) stream in continuously from exchanges
- All data must be ACID-compliant, auditable, and role-controlled
- The Medallion Architecture (Bronze → Silver → Gold) ensures data quality at every stage

### Who does what?

| Role | Actions | System Access |
|---|---|---|
| Controllership Team | Publish datasets, approve/reject subscriptions | `dataset:publish`, `subscription:approve/reject/revoke` |
| BI Team | Discover datasets, request subscriptions, consume Gold tables | `catalog:search`, `subscription:request`, `dataset:read` |
| Data Engineer | Configure pipelines, manage Databricks jobs, monitor logs | `medallion:write`, `job:manage`, `logs:query` |
| Administrator | Manage roles, system config | `role:assign/revoke`, `system:configure` |

---

## 2. Use Case 1: Controllership Team Publishes Quarterly Accounting Data

### Business scenario
The Controllership Team has finalized Q4 2025 journal entries and wants to publish them to the data lake so the BI team can build financial reports.

### Code flow

```
User calls: orchestrator.submit_batch(
    dataset_id="journal-entries-2025-q4",
    payload=[{record_id: "je-001", debit: 50000, credit: 50000, account: "4100", ...}, ...],
    idempotency_key="q4-2025-journal-batch-001",
    user_id="controller-jane"
)
```

### Step-by-step trace

```
┌─ MedallionOrchestrator.submit_batch()          # medallion_orchestrator.py
│
├─ 1. IdempotencyStore.check_and_reserve("q4-2025-journal-batch-001")
│      → Returns None (first submission)
│
├─ 2. RBACModule.authorize("controller-jane", "medallion:write", "journal-entries-2025-q4")
│      → AuthorizationResult(granted=True)  # Jane has Controllership_Team_Role
│
├─ 3. Create MedallionPipelineRun(status=RUNNING, current_layer=BRONZE)
│      → run_id=uuid, correlation_id=uuid
│
├─ 4. LogStore.write({event: "pipeline_start", run_id, correlation_id})
│
├─ 5. BRONZE LAYER ─────────────────────────────────────────────────
│  │
│  └─ BronzeLayerProcessor.ingest_batch()         # bronze_processor.py
│     │
│     ├─ Enrich each record with metadata:
│     │    {**original_record, ingestion_timestamp, source_system, batch_id}
│     │
│     ├─ DeltaTableManager.create_table("bronze_journal-entries-2025-q4")
│     │    → Creates Delta table if not exists, partitioned by ingestion_timestamp
│     │
│     ├─ DeltaTableManager.write(table, enriched_records, WriteMode.APPEND)
│     │    → Atomic append — all records land or none do
│     │
│     ├─ LogStore.write({event: "bronze_batch_ingestion", record_count: 1500})
│     │
│     └─ Returns BronzeResult(record_count=1500, table_path="bronze_journal-entries-2025-q4")
│
├─ 6. SILVER LAYER ─────────────────────────────────────────────────
│  │
│  └─ SilverLayerProcessor.process_batch()         # silver_processor.py
│     │
│     ├─ DeltaTableManager.read("bronze_journal-entries-2025-q4")
│     │    → Returns 1500 raw records
│     │
│     ├─ FOR EACH record:
│     │  │
│     │  ├─ ValidationPipeline.validate(record, schema_context)
│     │  │    │
│     │  │    ├─ Layer 1: IngestionValidator.validate()
│     │  │    │    → Checks format, encoding, required headers (record_id, timestamp, source)
│     │  │    │
│     │  │    ├─ Layer 2: SchemaValidator.validate()
│     │  │    │    → Checks field types (debit=DECIMAL, credit=DECIMAL, account=STRING)
│     │  │    │
│     │  │    └─ Layer 3: BusinessRuleValidator.validate()
│     │  │         → Checks debit/credit non-negative, account code format
│     │  │
│     │  ├─ IF PASS → add to valid_records list
│     │  └─ IF FAIL → DeadLetterQueue.send(record, error_message)
│     │
│     ├─ SilverLayerProcessor.deduplicate(valid_records, dedup_keys=["record_id"])
│     │    → Removes duplicates, keeps latest timestamp per record_id
│     │    → 1485 valid, 15 rejected, 3 deduplicated
│     │
│     ├─ DeltaTableManager.write("silver_journal-entries-2025-q4_cleansed", records, APPEND)
│     │
│     ├─ LogStore.write({event: "silver_batch_complete", valid: 1482, rejected: 15})
│     │
│     └─ Returns SilverResult(valid_count=1482, rejected_count=15, deduplicated_count=3)
│
├─ 7. GOLD LAYER ───────────────────────────────────────────────────
│  │
│  └─ GoldLayerProcessor.aggregate()               # gold_processor.py
│     │
│     ├─ DeltaTableManager.read("silver_journal-entries-2025-q4_cleansed")
│     │
│     ├─ Group by ["account", "month"]
│     │
│     ├─ Compute: SUM(debit), SUM(credit), COUNT(record_id)
│     │    → Produces 45 aggregate rows (one per account+month combo)
│     │
│     ├─ DeltaTableManager.write("gold_journal-entries-2025-q4_summary", rows, OVERWRITE)
│     │
│     └─ Returns GoldResult(aggregate_count=45)
│
├─ 8. CatalogService.register_medallion_metadata(dataset_id, bronze, silver, gold)
│
├─ 9. IdempotencyStore.mark_completed("q4-2025-journal-batch-001", run)
│
├─ 10. LogStore.write({event: "pipeline_complete", status: "COMPLETED"})
│
└─ Returns MedallionPipelineRun(status=COMPLETED, bronze/silver/gold results)
```

### What the BI team sees after this
- Gold table `gold_journal-entries-2025-q4_summary` with 45 rows:

| account | month | total_debit | total_credit | transaction_count |
|---|---|---|---|---|
| 4100 | 2025-10 | 125,000 | 125,000 | 42 |
| 4200 | 2025-11 | 89,500 | 89,500 | 31 |
| ... | ... | ... | ... | ... |

---

## 3. Use Case 2: Live Stock Market Data Streaming

### Business scenario
Real-time stock trade data flows from the NYSE exchange via Kafka. Each trade must be validated (price > 0, valid ticker symbol, timestamp within market hours) before entering the data lake.

### Code flow

```
Kafka Consumer receives records continuously:
  {ticker: "AAPL", price: 178.52, volume: 100, trade_time: "2025-03-27T14:30:00Z"}
  {ticker: "MSFT", price: 425.10, volume: 50, trade_time: "2025-03-27T14:30:01Z"}
  ...
```

### Step-by-step trace

```
┌─ MedallionOrchestrator.process_stream_record()   # medallion_orchestrator.py
│
├─ Record added to _stream_buffer (in-memory list)
│
├─ Buffer size check: len(buffer) < 1000?
│    → YES: return immediately (accumulate more records)
│    → NO (buffer full): trigger micro-batch processing ↓
│
├─ _flush_stream_buffer("stock-trades-nyse")
│  │
│  ├─ BRONZE: BronzeLayerProcessor.ingest_batch()
│  │    → 1000 raw trade records land in "bronze_stock-trades-nyse"
│  │    → Each enriched with: ingestion_timestamp, source_system="kafka_consumer"
│  │
│  └─ SILVER: SilverLayerProcessor.process_micro_batch()
│       │
│       ├─ FOR EACH of 1000 records:
│       │    ValidationPipeline.validate(record, schema_context)
│       │      Layer 1: format check (has ticker, price, volume, trade_time)
│       │      Layer 2: schema check (price=DECIMAL, volume=INTEGER)
│       │      Layer 3: business rules (price > 0, valid ticker, market hours)
│       │
│       ├─ 997 pass → written to "silver_stock-trades-nyse_cleansed"
│       ├─ 3 fail → routed to Dead Letter Queue:
│       │    {ticker: "INVALID", price: -5.00, ...} → DLQ with "price must be positive"
│       │    {ticker: "AAPL", price: null, ...}     → DLQ with "required field 'price' missing"
│       │
│       └─ Returns SilverResult(valid=997, rejected=3)
│
└─ Gold is NOT triggered per micro-batch (runs on schedule)
```

### Why Gold doesn't run per micro-batch
Stock market data arrives at thousands of records per second. Running aggregation on every micro-batch would be wasteful. Instead, Gold runs on a schedule (e.g., every 5 minutes or end-of-day) to produce summaries like:

| ticker | hour | total_volume | avg_price | trade_count |
|---|---|---|---|---|
| AAPL | 14:00 | 45,200 | 178.63 | 312 |
| MSFT | 14:00 | 22,100 | 425.08 | 187 |

---

## 4. Use Case 3: BI Team Discovers and Subscribes to a Dataset

### Business scenario
A BI analyst needs Q4 journal entry data for a financial dashboard. They search the catalog, find the dataset, and request access.

### Code flow (existing Andes components, not Databricks-specific)

```
1. BI analyst → API: GET /datasets/search?keyword=journal
   → CatalogService.search_datasets("journal")
   → Returns: [{name: "journal-entries-2025-q4", owner: "controller-jane", ...}]

2. BI analyst → API: POST /datasets/journal-entries-2025-q4/subscriptions
   → CatalogService.create_subscription_request(dataset_id, "bi-analyst-bob")
   → Subscription created with status=PENDING
   → Controllership Team notified

3. Controller Jane → API: PUT /subscriptions/{id}/approve
   → CatalogService.approve_subscription(subscription_id)
   → State machine: PENDING → APPROVED
   → Bob now has read access to the Gold table

4. Bob queries: SELECT * FROM gold_journal-entries-2025-q4_summary
   → Returns the 45 aggregate rows for dashboarding
```

---

## 5. Use Case 4: Scheduled Daily Medallion Pipeline

### Business scenario
The data engineering team wants journal entry data to be processed automatically every day at 6 AM.

### Code flow

```python
# One-time setup by data engineer:
job_manager = DatabricksJobManager(workspace_client, log_store)

job = job_manager.create_medallion_job(
    dataset_id="journal-entries-daily",
    schedule="0 6 * * *",                    # Cron: daily at 6 AM
    config=MedallionJobConfig(
        dataset_id="journal-entries-daily",
        bronze_enabled=True,
        silver_enabled=True,
        gold_enabled=True,
        dedup_keys=["record_id"],
        aggregation_config=AggregationConfig(
            group_by_columns=["account", "cost_center"],
            aggregations=[
                AggregationSpec(column="debit", function=AggregationFunction.SUM, alias="total_debit"),
                AggregationSpec(column="credit", function=AggregationFunction.SUM, alias="total_credit"),
            ],
        ),
    ),
)
```

### What happens at 6 AM daily

```
┌─ Databricks scheduler triggers job
│
├─ DatabricksJobManager.trigger_job(job_id)        # job_manager.py
│    → DatabricksWorkspaceClient.submit_job(job_config)
│    → Returns JobRunResult(run_id, status=PENDING)
│
├─ On the Databricks cluster, the job executes:
│    orchestrator.submit_batch(
│        dataset_id="journal-entries-daily",
│        payload=<today's journal entries from source system>,
│        idempotency_key="daily-2025-03-27",
│        user_id="service-account-etl"
│    )
│
├─ Full Bronze → Silver → Gold flow executes (same as Use Case 1)
│
└─ LogStore records: job started, each layer completed, job finished
```

---

## 6. Use Case 5: On-Demand Gold Refresh for Month-End Reporting

### Business scenario
It's month-end. The CFO needs updated financial summaries immediately, not at the next scheduled run.

### Code flow

```python
result = orchestrator.trigger_gold_refresh(
    dataset_id="journal-entries-2025-q4",
    aggregation_config=AggregationConfig(
        group_by_columns=["account", "department", "month"],
        aggregations=[
            AggregationSpec(column="debit", function=AggregationFunction.SUM, alias="total_debit"),
            AggregationSpec(column="credit", function=AggregationFunction.SUM, alias="total_credit"),
            AggregationSpec(column="record_id", function=AggregationFunction.COUNT, alias="entry_count"),
        ],
        incremental=False,  # Full recompute for month-end accuracy
    ),
    user_id="controller-jane",
)
```

### Step-by-step trace

```
┌─ MedallionOrchestrator.trigger_gold_refresh()    # medallion_orchestrator.py
│
├─ RBACModule.authorize("controller-jane", "medallion:write", dataset_id)
│    → Granted (Controllership_Team_Role)
│
├─ GoldLayerProcessor.aggregate()                   # gold_processor.py
│  │
│  ├─ DeltaTableManager.read("silver_journal-entries-2025-q4_cleansed")
│  │    → Returns all validated Silver records
│  │
│  ├─ config.incremental == False → process ALL Silver records
│  │
│  ├─ Group by [account, department, month]
│  │
│  ├─ Compute SUM(debit), SUM(credit), COUNT(record_id) per group
│  │
│  ├─ DeltaTableManager.write(gold_table, rows, WriteMode.OVERWRITE)
│  │    → Full table replacement with fresh aggregates
│  │
│  └─ Returns GoldResult(aggregate_count=120)
│
└─ LogStore.write({event: "gold_refresh", aggregate_count: 120})
```

---

## 7. Use Case 6: Handling Invalid Records (DLQ Flow)

### Business scenario
A batch of journal entries contains some records with negative debit values and missing account codes. These must not enter the Silver layer but should be preserved for investigation.

### Code flow

```
Record: {record_id: "je-bad-001", debit: -500, credit: 500, account: null}

┌─ SilverLayerProcessor._process_records()          # silver_processor.py
│
├─ ValidationPipeline.validate(record, context)
│  │
│  ├─ Layer 1 (IngestionValidator): PASS
│  │    → record_id present, timestamp present, source present
│  │
│  ├─ Layer 2 (SchemaValidator): PASS
│  │    → debit is numeric, credit is numeric (account nullable allowed at schema level)
│  │
│  └─ Layer 3 (BusinessRuleValidator): FAIL
│       → "debit must be non-negative" AND "account code is required"
│       → Validation STOPS here — record rejected
│
├─ DeadLetterQueue.send(
│      source_stream="medallion_silver_journal-entries-2025-q4",
│      record={record_id: "je-bad-001", debit: -500, ...},
│      error="debit must be non-negative"
│  )
│
├─ LogStore.write({event: "silver_record_validation", status: "FAIL"})
│
└─ Record is NOT written to Silver table
   → It lives in the DLQ for ops team to inspect and fix
```

### DLQ record in DynamoDB

```json
{
  "dlqId": "uuid",
  "sourceStream": "medallion_silver_journal-entries-2025-q4",
  "originalRecord": {"record_id": "je-bad-001", "debit": -500, "credit": 500, "account": null},
  "validationError": "debit must be non-negative",
  "failedAt": "2025-03-27T06:15:23Z",
  "retryCount": 0
}
```

---

## 8. Use Case 7: Duplicate Submission (Idempotency)

### Business scenario
A network retry causes the same batch to be submitted twice with the same idempotency key.

### Code flow

```
# First submission (succeeds normally):
orchestrator.submit_batch(..., idempotency_key="q4-batch-001", ...)
→ Full Bronze → Silver → Gold pipeline executes
→ IdempotencyStore.mark_completed("q4-batch-001", run)

# Second submission (duplicate):
orchestrator.submit_batch(..., idempotency_key="q4-batch-001", ...)

┌─ MedallionOrchestrator.submit_batch()
│
├─ IdempotencyStore.check_and_reserve("q4-batch-001")
│    → Returns the EXISTING completed run (not None)
│
└─ Returns the original MedallionPipelineRun immediately
   → NO Bronze writes, NO Silver processing, NO Gold aggregation
   → Data lake state is UNCHANGED
   → Status: SKIPPED
```

### Why this matters
Without idempotency, the duplicate submission would double-count every journal entry in the Gold aggregates. The CFO would see $100M in revenue instead of $50M.

---

## 9. Use Case 8: Unauthorized Access Attempt

### Business scenario
A BI analyst tries to publish data (which only the Controllership Team can do).

### Code flow

```
orchestrator.submit_batch(
    dataset_id="journal-entries-2025-q4",
    payload=[...],
    idempotency_key="unauthorized-attempt",
    user_id="bi-analyst-bob"    # Bob has BI_Team_Role, NOT Controllership_Team_Role
)

┌─ MedallionOrchestrator.submit_batch()
│
├─ IdempotencyStore.check_and_reserve("unauthorized-attempt")
│    → None (new key)
│
├─ RBACModule.authorize("bi-analyst-bob", "medallion:write", "journal-entries-2025-q4")
│    → AuthorizationResult(granted=False)
│    → BI_Team_Role does NOT include "medallion:write" permission
│
├─ RAISE AuthorizationDeniedError(
│      "User 'bi-analyst-bob' is not authorized for medallion:write"
│  )
│
└─ NO data written to ANY layer (Bronze, Silver, or Gold)
   → LogStore records the denied access attempt
```

---

## 10. End-to-End Batch Pipeline Trace

### Complete file-by-file trace for a single batch submission

```
ENTRY POINT
  → api/routes.py: POST /datasets/{id}/ingest
    → Parses HTTP request, extracts payload

ORCHESTRATION
  → databricks/medallion_orchestrator.py: MedallionOrchestrator.submit_batch()
    → Idempotency check, RBAC check, creates pipeline run

BRONZE
  → databricks/bronze_processor.py: BronzeLayerProcessor.ingest_batch()
    → databricks/delta_table_manager.py: DeltaTableManager.create_table()
    → databricks/delta_table_manager.py: DeltaTableManager.write(APPEND)
    → databricks/workspace_client.py: DatabricksWorkspaceClient.execute_sql()

SILVER
  → databricks/silver_processor.py: SilverLayerProcessor.process_batch()
    → databricks/delta_table_manager.py: DeltaTableManager.read()
    → validation/pipeline.py: ValidationPipeline.validate()
      → validation/ingestion_validator.py: IngestionValidator.validate()
      → validation/schema_validator.py: SchemaValidator.validate()
      → validation/business_rule_validator.py: BusinessRuleValidator.validate()
    → streaming/dead_letter_queue.py: DeadLetterQueue.send() [for failures]
    → databricks/silver_processor.py: SilverLayerProcessor.deduplicate()
    → databricks/delta_table_manager.py: DeltaTableManager.write(APPEND)

GOLD
  → databricks/gold_processor.py: GoldLayerProcessor.aggregate()
    → databricks/delta_table_manager.py: DeltaTableManager.read()
    → gold_processor.py: _group_records(), _compute_aggregates()
    → databricks/delta_table_manager.py: DeltaTableManager.write(OVERWRITE)

COMPLETION
  → catalog/catalog_service.py: CatalogService.register_medallion_metadata()
  → ingestion/idempotency.py: IdempotencyStore.mark_completed()
  → logging_store/log_store.py: LogStore.write() [throughout every step]
```

---

## 11. End-to-End Streaming Pipeline Trace

### Complete file-by-file trace for streaming records

```
KAFKA CONSUMER
  → streaming/stream_consumer.py: receives record from Kafka topic

ORCHESTRATION
  → databricks/medallion_orchestrator.py: MedallionOrchestrator.process_stream_record()
    → Adds record to _stream_buffer
    → When buffer reaches 1000 records → _flush_stream_buffer()

BRONZE (micro-batch)
  → databricks/bronze_processor.py: BronzeLayerProcessor.ingest_batch()
    → 1000 records land in Bronze Delta table with stream metadata

SILVER (micro-batch)
  → databricks/silver_processor.py: SilverLayerProcessor.process_micro_batch()
    → Same 3-layer validation as batch
    → Valid records → Silver table
    → Invalid records → Dead Letter Queue

GOLD (scheduled, NOT per micro-batch)
  → Triggered by DatabricksJobManager on cron schedule
  → databricks/gold_processor.py: GoldLayerProcessor.aggregate()
    → Reads all Silver data (or incremental since last watermark)
    → Produces business aggregates
```

### Timing

| Event | Frequency | Latency |
|---|---|---|
| Kafka record received | Continuous (ms) | < 1ms |
| Bronze micro-batch write | Every 1000 records or 60s | ~100ms |
| Silver validation + write | Same as Bronze trigger | ~500ms per record |
| Gold aggregation | Scheduled (e.g., every 5 min) | Depends on data volume |
