# Andes Data Lake — Design Patterns, Concepts & Project Structure Guide

This document explains the architectural patterns, design principles, and project structure conventions used in the Andes Data Lake. Each concept includes rationale and example code specific to this project.

---

## Table of Contents

1. [Project Structure Philosophy](#1-project-structure-philosophy)
2. [Layered Architecture](#2-layered-architecture)
3. [Dependency Injection](#3-dependency-injection)
4. [Repository Pattern](#4-repository-pattern)
5. [Chain of Responsibility — Validation Pipeline](#5-chain-of-responsibility--validation-pipeline)
6. [State Machine — Subscription Lifecycle](#6-state-machine--subscription-lifecycle)
7. [Strategy Pattern — Conflict Resolution](#7-strategy-pattern--conflict-resolution)
8. [Unit of Work — ACID Transactions](#8-unit-of-work--acid-transactions)
9. [Circuit Breaker — Log Store Resilience](#9-circuit-breaker--log-store-resilience)
10. [Observer / Event-Driven Logging](#10-observer--event-driven-logging)
11. [Idempotency Pattern](#11-idempotency-pattern)
12. [Leader Election Pattern](#12-leader-election-pattern)
13. [Dead Letter Queue Pattern](#13-dead-letter-queue-pattern)
14. [Error Handling Strategy](#14-error-handling-strategy)
15. [Testing Patterns](#15-testing-patterns)

---

## 1. Project Structure Philosophy

The project follows a **domain-driven modular structure** where each top-level package
represents a bounded context. This keeps coupling low and cohesion high.

### Why this structure?

```
andes_data_lake/
├── models/          # Pure data structures — no business logic, no I/O
├── catalog/         # Dataset metadata + subscription domain
├── ingestion/       # Batch ingestion + transaction management
├── validation/      # 3-layer validation pipeline
├── streaming/       # Kafka consumer + stream validation
├── rbac/            # Access control domain
├── concurrency/     # Distributed locks, leader election, conflict resolution
├── logging_store/   # Centralized logging abstraction
├── api/             # HTTP layer — thin, delegates to services
└── tests/           # Mirrors the source structure
```

**Key rules:**

- `models/` is a **shared kernel** — every other package can import from it, but it imports from nothing.
- Each domain package (`catalog/`, `ingestion/`, etc.) owns its business logic and exposes a clean interface.
- `api/` is the **outermost shell** — it only calls domain services, never contains business logic.
- `tests/` mirrors the source tree: `test_catalog.py` tests `catalog/`, etc.

### File naming conventions

| File | Purpose |
|---|---|
| `*_service.py` | Business logic orchestrator (e.g., `catalog_service.py`) |
| `*_validator.py` | Validation logic (e.g., `schema_validator.py`) |
| `*_manager.py` | Infrastructure coordination (e.g., `concurrency_manager.py`) |
| `*_repository.py` | Data access abstraction (e.g., `dataset_repository.py`) |
| `models/*.py` | Pydantic data models — one file per aggregate |

---

## 2. Layered Architecture

The system uses a **clean layered architecture** where dependencies flow inward:

```
┌─────────────────────────────────────────┐
│              API Layer (api/)            │  ← HTTP, routing, serialization
├─────────────────────────────────────────┤
│          Service Layer (catalog/,       │  ← Business logic, orchestration
│          ingestion/, rbac/, etc.)        │
├─────────────────────────────────────────┤
│        Repository Layer (internal)      │  ← Data access abstraction
├─────────────────────────────────────────┤
│     Infrastructure (DynamoDB, S3,       │  ← External systems
│     Kafka, ZooKeeper)                   │
└─────────────────────────────────────────┘
```

**Rule: Each layer only calls the layer directly below it. Never skip layers.**

Example — the API layer delegates to the service, never touches DynamoDB directly:

```python
# api/routes.py — THIN controller, no business logic
from fastapi import APIRouter, Depends
from catalog.catalog_service import CatalogService

router = APIRouter()

@router.post("/datasets")
async def publish_dataset(
    request: PublishDatasetRequest,
    catalog: CatalogService = Depends(get_catalog_service),
):
    # API layer only: parse request, call service, return response
    result = catalog.register_dataset(
        name=request.name,
        schema=request.schema,
        owner=request.owner,
        description=request.description,
    )
    return result
```

---

## 3. Dependency Injection

Every service declares its dependencies through **constructor injection**. This makes
testing trivial (swap real DynamoDB with an in-memory mock) and keeps components decoupled.

### Pattern: Constructor Injection

```python
# catalog/catalog_service.py
from logging_store.log_store import LogStore
from rbac.rbac_module import RBACModule

class CatalogService:
    """Manages dataset metadata and subscriptions.
    
    Dependencies are injected through the constructor — never created internally.
    This allows swapping DynamoDB for an in-memory store in tests.
    """

    def __init__(
        self,
        dynamo_client,          # boto3 DynamoDB resource
        log_store: LogStore,    # centralized logging
        rbac: RBACModule,       # access control
        table_name: str = "andes_catalog",
    ):
        self._table = dynamo_client.Table(table_name)
        self._log = log_store
        self._rbac = rbac

    def register_dataset(self, name: str, schema, owner: str, description: str):
        # Business logic here — uses injected dependencies
        self._rbac.authorize(owner, "dataset:publish", name)
        # ... store in DynamoDB via self._table
        self._log.write(LogEntry(
            source_component="CatalogService",
            message=f"Dataset '{name}' registered by {owner}",
            severity=Severity.INFO,
        ))
```

### Wiring it all together

```python
# api/dependencies.py — composition root
import boto3
from logging_store.log_store import LogStore
from rbac.rbac_module import RBACModule
from catalog.catalog_service import CatalogService

def create_services():
    """Single place where all dependencies are wired together."""
    dynamo = boto3.resource("dynamodb")

    log_store = LogStore(dynamo_client=dynamo)
    rbac = RBACModule(dynamo_client=dynamo, log_store=log_store)
    catalog = CatalogService(dynamo_client=dynamo, log_store=log_store, rbac=rbac)

    return {"catalog": catalog, "rbac": rbac, "log_store": log_store}
```

**Why?** If you ever need to swap DynamoDB for PostgreSQL, you change `create_services()` — not every service file.

---

## 4. Repository Pattern

The **Repository Pattern** abstracts data access behind a clean interface. Services never
know whether data lives in DynamoDB, S3, or an in-memory dict.

```python
# catalog/dataset_repository.py
from abc import ABC, abstractmethod
from models.dataset import DatasetMetadata

class DatasetRepository(ABC):
    """Abstract interface for dataset storage."""

    @abstractmethod
    def save(self, dataset: DatasetMetadata) -> None: ...

    @abstractmethod
    def find_by_id(self, dataset_id: str) -> DatasetMetadata | None: ...

    @abstractmethod
    def search(self, keyword: str) -> list[DatasetMetadata]: ...

    @abstractmethod
    def list_all(self) -> list[DatasetMetadata]: ...


class DynamoDatasetRepository(DatasetRepository):
    """DynamoDB implementation of DatasetRepository."""

    def __init__(self, dynamo_client, table_name: str = "andes_datasets"):
        self._table = dynamo_client.Table(table_name)

    def save(self, dataset: DatasetMetadata) -> None:
        self._table.put_item(Item=dataset.model_dump())

    def find_by_id(self, dataset_id: str) -> DatasetMetadata | None:
        response = self._table.get_item(Key={"datasetId": dataset_id})
        item = response.get("Item")
        return DatasetMetadata(**item) if item else None

    def search(self, keyword: str) -> list[DatasetMetadata]:
        # Scan with filter — for production, consider OpenSearch for full-text
        response = self._table.scan(
            FilterExpression="contains(#n, :kw) OR contains(description, :kw)",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":kw": keyword.lower()},
        )
        return [DatasetMetadata(**item) for item in response["Items"]]

    def list_all(self) -> list[DatasetMetadata]:
        response = self._table.scan()
        return [DatasetMetadata(**item) for item in response["Items"]]


class InMemoryDatasetRepository(DatasetRepository):
    """In-memory implementation for unit tests — no DynamoDB needed."""

    def __init__(self):
        self._store: dict[str, DatasetMetadata] = {}

    def save(self, dataset: DatasetMetadata) -> None:
        self._store[str(dataset.dataset_id)] = dataset

    def find_by_id(self, dataset_id: str) -> DatasetMetadata | None:
        return self._store.get(dataset_id)

    def search(self, keyword: str) -> list[DatasetMetadata]:
        kw = keyword.lower()
        return [
            d for d in self._store.values()
            if kw in d.name.lower() or kw in d.description.lower()
        ]

    def list_all(self) -> list[DatasetMetadata]:
        return list(self._store.values())
```

**Why?** Tests run instantly against `InMemoryDatasetRepository` without spinning up DynamoDB Local.

---

## 5. Chain of Responsibility — Validation Pipeline

The 3-layer validation pipeline uses the **Chain of Responsibility** pattern. Each validator
is a link in the chain — if it passes, the record moves to the next link; if it fails, the
chain stops and returns the error.

```python
# validation/pipeline.py
from dataclasses import dataclass
from enum import Enum
from models.validation import ValidationAuditRecord

class ValidationStatus(Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    SKIPPED = "SKIPPED"

@dataclass
class ValidationResult:
    status: ValidationStatus
    error_code: str | None = None
    error_message: str | None = None

class BaseValidator:
    """Base class for all validators in the chain."""

    def __init__(self, next_validator: "BaseValidator | None" = None):
        self._next = next_validator

    def validate(self, record: dict, context: dict) -> ValidationResult:
        raise NotImplementedError

    def _pass_to_next(self, record: dict, context: dict) -> ValidationResult:
        if self._next:
            return self._next.validate(record, context)
        return ValidationResult(status=ValidationStatus.PASS)


class IngestionValidator(BaseValidator):
    """Layer 1: Checks format, encoding, required headers."""

    def validate(self, record: dict, context: dict) -> ValidationResult:
        # Check required header fields
        required_headers = {"record_id", "timestamp", "source"}
        missing = required_headers - set(record.keys())
        if missing:
            return ValidationResult(
                status=ValidationStatus.FAIL,
                error_code="INVALID_FORMAT",
                error_message=f"Missing required headers: {missing}",
            )
        return self._pass_to_next(record, context)


class SchemaValidator(BaseValidator):
    """Layer 2: Checks field types, required fields, constraints."""

    def validate(self, record: dict, context: dict) -> ValidationResult:
        schema = context.get("schema")
        if not schema:
            return ValidationResult(
                status=ValidationStatus.FAIL,
                error_code="SCHEMA_VIOLATION",
                error_message="No schema registered for this dataset",
            )
        for field in schema.fields:
            if field.required and field.name not in record:
                return ValidationResult(
                    status=ValidationStatus.FAIL,
                    error_code="SCHEMA_VIOLATION",
                    error_message=f"Required field '{field.name}' missing",
                )
        return self._pass_to_next(record, context)


class BusinessRuleValidator(BaseValidator):
    """Layer 3: Cross-field consistency, accounting constraints."""

    def validate(self, record: dict, context: dict) -> ValidationResult:
        # Example: debit and credit must balance
        debit = record.get("debit", 0)
        credit = record.get("credit", 0)
        if debit < 0 or credit < 0:
            return ValidationResult(
                status=ValidationStatus.FAIL,
                error_code="BUSINESS_RULE_VIOLATION",
                error_message="Debit and credit values must be non-negative",
            )
        return self._pass_to_next(record, context)


class ValidationPipeline:
    """Orchestrates the 3-layer validation chain.
    
    Builds the chain: Ingestion → Schema → BusinessRule
    Records audit trail for each layer executed.
    """

    def __init__(self):
        # Build chain from last to first (each wraps the next)
        business_rule = BusinessRuleValidator(next_validator=None)
        schema = SchemaValidator(next_validator=business_rule)
        self._chain = IngestionValidator(next_validator=schema)

    def validate(self, record: dict, context: dict) -> tuple[ValidationResult, ValidationAuditRecord]:
        result = self._chain.validate(record, context)
        audit = self._build_audit(record, result)
        return result, audit

    def _build_audit(self, record, result) -> ValidationAuditRecord:
        # Build audit record tracking which layers executed and their outcomes
        ...
```

**Why Chain of Responsibility?**
- Each validator is independent and testable in isolation
- Adding a 4th validation layer = add one class, plug it into the chain
- The pipeline stops early on failure — no wasted computation

---

## 6. State Machine — Subscription Lifecycle

Subscriptions follow a strict state machine: `PENDING → APPROVED → REVOKED` or
`PENDING → REJECTED`. The **State Machine pattern** enforces valid transitions and
prevents illegal state changes.

```python
# catalog/subscription_state_machine.py
from enum import Enum
from models.errors import InvalidStateTransitionError

class SubscriptionStatus(Enum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    REVOKED = "REVOKED"

# Define allowed transitions as a directed graph
ALLOWED_TRANSITIONS: dict[SubscriptionStatus, set[SubscriptionStatus]] = {
    SubscriptionStatus.PENDING: {SubscriptionStatus.APPROVED, SubscriptionStatus.REJECTED},
    SubscriptionStatus.APPROVED: {SubscriptionStatus.REVOKED},
    SubscriptionStatus.REJECTED: set(),   # terminal state
    SubscriptionStatus.REVOKED: set(),    # terminal state
}

def transition(current: SubscriptionStatus, target: SubscriptionStatus) -> SubscriptionStatus:
    """Enforce valid state transitions.
    
    Raises InvalidStateTransitionError if the transition is not allowed.
    """
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise InvalidStateTransitionError(
            f"Cannot transition from {current.value} to {target.value}"
        )
    return target


# Usage in CatalogService:
class CatalogService:
    def approve_subscription(self, subscription_id: str) -> Subscription:
        sub = self._get_subscription(subscription_id)
        sub.status = transition(sub.status, SubscriptionStatus.APPROVED)
        sub.decided_at = datetime.utcnow()
        self._save_subscription(sub)
        self._grant_read_access(sub.user_id, sub.dataset_id)
        return sub

    def reject_subscription(self, subscription_id: str, reason: str) -> Subscription:
        sub = self._get_subscription(subscription_id)
        sub.status = transition(sub.status, SubscriptionStatus.REJECTED)
        sub.rejection_reason = reason
        sub.decided_at = datetime.utcnow()
        self._save_subscription(sub)
        return sub
```

**Why?** You can never accidentally approve an already-revoked subscription. The state machine is the single source of truth for what transitions are legal.

---

## 7. Strategy Pattern — Conflict Resolution

The Concurrency Manager uses the **Strategy Pattern** for conflict resolution. Different
datasets might use different resolution strategies (OCC, last-write-wins, merge).

```python
# concurrency/conflict_resolver.py
from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class WriteOperation:
    dataset_id: str
    partition: str
    version: int
    payload: dict

@dataclass
class ConflictResolution:
    winner: WriteOperation
    loser: WriteOperation
    should_retry: bool

class ConflictResolutionStrategy(ABC):
    """Abstract strategy for resolving write conflicts."""

    @abstractmethod
    def resolve(self, write_a: WriteOperation, write_b: WriteOperation) -> ConflictResolution:
        ...


class OptimisticConcurrencyStrategy(ConflictResolutionStrategy):
    """OCC: Compare version tokens. Higher version wins, loser retries."""

    def resolve(self, write_a: WriteOperation, write_b: WriteOperation) -> ConflictResolution:
        if write_a.version >= write_b.version:
            return ConflictResolution(winner=write_a, loser=write_b, should_retry=True)
        return ConflictResolution(winner=write_b, loser=write_a, should_retry=True)


class LastWriteWinsStrategy(ConflictResolutionStrategy):
    """LWW: Most recent write wins, no retry needed."""

    def resolve(self, write_a: WriteOperation, write_b: WriteOperation) -> ConflictResolution:
        # In LWW, the second write always wins
        return ConflictResolution(winner=write_b, loser=write_a, should_retry=False)


class ConflictResolver:
    """Uses a pluggable strategy to resolve conflicts.
    
    Default is OCC. Can be overridden per dataset.
    """

    def __init__(self, default_strategy: ConflictResolutionStrategy | None = None):
        self._default = default_strategy or OptimisticConcurrencyStrategy()
        self._dataset_strategies: dict[str, ConflictResolutionStrategy] = {}

    def register_strategy(self, dataset_id: str, strategy: ConflictResolutionStrategy):
        """Override the conflict resolution strategy for a specific dataset."""
        self._dataset_strategies[dataset_id] = strategy

    def resolve(self, write_a: WriteOperation, write_b: WriteOperation) -> ConflictResolution:
        strategy = self._dataset_strategies.get(write_a.dataset_id, self._default)
        return strategy.resolve(write_a, write_b)
```

**Why Strategy?** Different datasets may have different consistency requirements. Stock market data might use OCC (correctness matters), while a logging dataset might use last-write-wins (throughput matters). The resolver doesn't need to know which — it just delegates.

---

## 8. Unit of Work — ACID Transactions

The **Unit of Work** pattern tracks all changes within a transaction and commits or rolls
back atomically. This maps directly to Apache Iceberg's transaction API.

```python
# ingestion/transaction_manager.py
from contextlib import contextmanager
from dataclasses import dataclass, field

@dataclass
class PendingWrite:
    dataset_id: str
    partition: str
    records: list[dict]

class TransactionManager:
    """Manages ACID transactions against the Iceberg data lake.
    
    Collects writes, then commits atomically or rolls back entirely.
    Implements snapshot isolation — readers never see uncommitted data.
    """

    def __init__(self, iceberg_catalog):
        self._catalog = iceberg_catalog
        self._pending: list[PendingWrite] = []

    def add_records(self, dataset_id: str, partition: str, records: list[dict]):
        """Stage records for atomic commit."""
        self._pending.append(PendingWrite(dataset_id, partition, records))

    @contextmanager
    def transaction(self):
        """Context manager for atomic commit/rollback.
        
        Usage:
            with tx_manager.transaction():
                tx_manager.add_records("ds-1", "2024-Q1", records)
                tx_manager.add_records("ds-1", "2024-Q2", more_records)
            # All committed atomically here, or rolled back on exception
        """
        self._pending.clear()
        try:
            yield self
            self._commit()
        except Exception:
            self._rollback()
            raise

    def _commit(self):
        """Atomically commit all pending writes using Iceberg transactions."""
        for write in self._pending:
            table = self._catalog.load_table(write.dataset_id)
            # Iceberg's append operation is atomic per table
            table.append(write.records)
        self._pending.clear()

    def _rollback(self):
        """Discard all pending writes — nothing was committed."""
        self._pending.clear()


# Usage in IngestionEngine:
class IngestionEngine:
    def submit_batch(self, dataset_id, payload, idempotency_key):
        # Check idempotency first
        if self._is_duplicate(idempotency_key):
            return self._get_original_result(idempotency_key)

        with self._tx_manager.transaction():
            for record in payload:
                result, audit = self._validation_pipeline.validate(record, context)
                if result.status == ValidationStatus.FAIL:
                    raise ValidationError(result.error_message)
                self._tx_manager.add_records(dataset_id, partition, [record])
        # If we get here, everything committed atomically
```

**Why Unit of Work?** A pipeline run might process 10,000 records. If record #7,432 fails validation, all 7,431 previously staged writes are discarded. The data lake never sees partial data.

---

## 9. Circuit Breaker — Log Store Resilience

The **Circuit Breaker** pattern prevents cascading failures when DynamoDB (Log Store) is
temporarily unavailable. Instead of every component failing, they buffer locally and retry.

```python
# logging_store/circuit_breaker.py
import time
from enum import Enum
from collections import deque

class CircuitState(Enum):
    CLOSED = "CLOSED"      # Normal operation — requests flow through
    OPEN = "OPEN"          # Failure detected — requests short-circuited
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered

class CircuitBreaker:
    """Protects the Log Store from cascading failures.
    
    - CLOSED: All writes go to DynamoDB normally
    - OPEN: Writes go to local buffer (DynamoDB assumed down)
    - HALF_OPEN: Try one write to DynamoDB to test recovery
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._failure_count = 0
        self._state = CircuitState.CLOSED
        self._last_failure_time = 0.0

    @property
    def state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            # Check if recovery timeout has elapsed
            if time.time() - self._last_failure_time >= self._recovery_timeout:
                self._state = CircuitState.HALF_OPEN
        return self._state

    def record_success(self):
        self._failure_count = 0
        self._state = CircuitState.CLOSED

    def record_failure(self):
        self._failure_count += 1
        self._last_failure_time = time.time()
        if self._failure_count >= self._failure_threshold:
            self._state = CircuitState.OPEN

    def allow_request(self) -> bool:
        state = self.state
        if state == CircuitState.CLOSED:
            return True
        if state == CircuitState.HALF_OPEN:
            return True  # Allow one probe request
        return False  # OPEN — don't even try


# Usage in LogStore:
class LogStore:
    def __init__(self, dynamo_client):
        self._table = dynamo_client.Table("andes_logs")
        self._circuit = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        self._buffer: deque = deque(maxlen=10000)

    def write(self, entry):
        if self._circuit.allow_request():
            try:
                self._table.put_item(Item=entry.model_dump())
                self._circuit.record_success()
                self._flush_buffer()  # Try to deliver buffered entries too
            except Exception:
                self._circuit.record_failure()
                self._buffer.append(entry)
        else:
            # Circuit is OPEN — buffer locally, don't hit DynamoDB
            self._buffer.append(entry)

    def _flush_buffer(self):
        while self._buffer:
            entry = self._buffer[0]
            try:
                self._table.put_item(Item=entry.model_dump())
                self._buffer.popleft()
            except Exception:
                break  # Stop flushing, DynamoDB might be struggling
```

**Why?** Without a circuit breaker, if DynamoDB goes down for 30 seconds, every single component in the system would fail on every log write. With it, they buffer silently and recover automatically.

---

## 10. Observer / Event-Driven Logging

Every component needs to log to the centralized Log Store. Rather than scattering log calls
everywhere, use the **Observer pattern** — components emit events, and the logging
infrastructure subscribes to them.

```python
# logging_store/event_bus.py
from typing import Callable
from collections import defaultdict

class EventBus:
    """Simple in-process event bus for decoupled logging.
    
    Components publish events. The Log Store subscribes and persists them.
    This keeps logging concerns out of business logic.
    """

    def __init__(self):
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Callable):
        self._subscribers[event_type].append(handler)

    def publish(self, event_type: str, data: dict):
        for handler in self._subscribers.get(event_type, []):
            handler(data)


# Wiring:
event_bus = EventBus()
log_store = LogStore(dynamo_client)

# Log Store subscribes to all events
event_bus.subscribe("dataset.published", lambda data: log_store.write(
    LogEntry(source_component="CatalogService", message=f"Published: {data['name']}", ...)
))
event_bus.subscribe("pipeline.completed", lambda data: log_store.write(
    LogEntry(source_component="IngestionEngine", message=f"Run {data['run_id']} completed", ...)
))
event_bus.subscribe("auth.denied", lambda data: log_store.write(
    LogEntry(source_component="RBACModule", severity=Severity.WARN, ...)
))

# Components just publish — they don't know about the Log Store
class CatalogService:
    def __init__(self, event_bus: EventBus, ...):
        self._events = event_bus

    def register_dataset(self, name, schema, owner, description):
        # ... business logic ...
        self._events.publish("dataset.published", {"name": name, "owner": owner})
```

**Why?** Business logic stays clean. If you later want to add Slack notifications on `auth.denied`, you add one more subscriber — zero changes to the RBAC module.

---

## 11. Idempotency Pattern

Pipeline runs must be idempotent — running the same batch twice produces the same result
without duplicating data. This uses an **idempotency key store**.

```python
# ingestion/idempotency.py
from models.pipeline import PipelineRun, PipelineRunStatus

class IdempotencyStore:
    """Tracks completed pipeline runs by idempotency key.
    
    Before executing a pipeline run:
    1. Check if the key exists
    2. If yes → return the original result (skip execution)
    3. If no → execute, then store the result with the key
    """

    def __init__(self, dynamo_client, table_name: str = "andes_pipeline_runs"):
        self._table = dynamo_client.Table(table_name)

    def check_and_reserve(self, idempotency_key: str) -> PipelineRun | None:
        """Check if this key was already processed.
        
        Uses DynamoDB conditional put to atomically reserve the key.
        Returns the existing run if duplicate, None if new.
        """
        try:
            self._table.put_item(
                Item={
                    "idempotencyKey": idempotency_key,
                    "status": PipelineRunStatus.RUNNING.value,
                },
                ConditionExpression="attribute_not_exists(idempotencyKey)",
            )
            return None  # Key is new — proceed with execution
        except self._table.meta.client.exceptions.ConditionalCheckFailedException:
            # Key already exists — return the original run
            response = self._table.get_item(Key={"idempotencyKey": idempotency_key})
            return PipelineRun(**response["Item"])

    def mark_completed(self, idempotency_key: str, result: PipelineRun):
        """Mark a pipeline run as completed with its result."""
        self._table.update_item(
            Key={"idempotencyKey": idempotency_key},
            UpdateExpression="SET #s = :status, endTime = :end, recordCount = :count",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":status": PipelineRunStatus.COMPLETED.value,
                ":end": result.end_time.isoformat(),
                ":count": result.record_count,
            },
        )


# Usage:
class IngestionEngine:
    def submit_batch(self, dataset_id, payload, idempotency_key):
        existing = self._idempotency.check_and_reserve(idempotency_key)
        if existing:
            return existing  # Duplicate — return original result, no re-execution

        # Not a duplicate — proceed with ingestion
        run = self._execute_pipeline(dataset_id, payload)
        self._idempotency.mark_completed(idempotency_key, run)
        return run
```

**Why?** Network retries, Kafka redeliveries, and user double-clicks can all cause duplicate submissions. The idempotency store ensures the data lake never gets corrupted by duplicates.

---

## 12. Leader Election Pattern

For each Kafka-consumed data pipeline, exactly one node should be the leader coordinating
partition assignment and checkpointing. Uses ZooKeeper ephemeral nodes.

```python
# concurrency/leader_election.py
from kazoo.client import KazooClient
from kazoo.recipe.election import Election

class LeaderElector:
    """Elects a single leader per pipeline using ZooKeeper.
    
    - Ephemeral nodes auto-delete when the holder disconnects
    - This triggers automatic re-election on node failure
    - Only the leader processes partition assignments
    """

    def __init__(self, zk_hosts: str = "localhost:2181"):
        self._zk = KazooClient(hosts=zk_hosts)
        self._zk.start()
        self._elections: dict[str, Election] = {}

    def run_for_leader(self, pipeline_id: str, node_id: str, on_elected: callable):
        """Participate in leader election for a pipeline.
        
        Args:
            pipeline_id: Which pipeline to lead
            node_id: This node's unique identifier
            on_elected: Callback invoked when this node becomes leader
        """
        path = f"/andes/leader/{pipeline_id}"
        election = self._zk.Election(path, node_id)
        self._elections[pipeline_id] = election

        # This blocks until this node is elected leader
        # If the current leader dies, ZK auto-promotes the next candidate
        election.run(on_elected)

    def get_leader(self, pipeline_id: str) -> str | None:
        """Get the current leader for a pipeline."""
        path = f"/andes/leader/{pipeline_id}"
        contenders = self._zk.Election(path).contenders()
        return contenders[0] if contenders else None

    def resign(self, pipeline_id: str):
        """Voluntarily resign leadership."""
        if pipeline_id in self._elections:
            self._elections[pipeline_id].cancel()
```

---

## 13. Dead Letter Queue Pattern

Invalid streamed records don't get dropped — they go to a **Dead Letter Queue** for
later inspection, debugging, and potential reprocessing.

```python
# streaming/dead_letter_queue.py
from models.dlq import DLQRecord
from datetime import datetime
import uuid

class DeadLetterQueue:
    """Stores invalid records that failed validation.
    
    Records land here when:
    - Stream validation fails (schema mismatch, missing fields)
    - Business rule validation fails on streamed data
    
    Operations teams can inspect, fix, and replay these records.
    """

    def __init__(self, dynamo_client, table_name: str = "andes_dlq"):
        self._table = dynamo_client.Table(table_name)

    def send(self, source_stream: str, record: dict, error: str):
        dlq_record = DLQRecord(
            dlq_id=str(uuid.uuid4()),
            source_stream=source_stream,
            original_record=record,
            validation_error=error,
            failed_at=datetime.utcnow().isoformat(),
            retry_count=0,
        )
        self._table.put_item(Item=dlq_record.model_dump())

    def list_failed(self, source_stream: str | None = None) -> list[DLQRecord]:
        if source_stream:
            response = self._table.query(
                IndexName="source-stream-index",
                KeyConditionExpression="sourceStream = :ss",
                ExpressionAttributeValues={":ss": source_stream},
            )
        else:
            response = self._table.scan()
        return [DLQRecord(**item) for item in response["Items"]]

    def retry(self, dlq_id: str) -> DLQRecord:
        """Increment retry count and return the record for reprocessing."""
        response = self._table.update_item(
            Key={"dlqId": dlq_id},
            UpdateExpression="SET retryCount = retryCount + :inc",
            ExpressionAttributeValues={":inc": 1},
            ReturnValues="ALL_NEW",
        )
        return DLQRecord(**response["Attributes"])
```

---

## 14. Error Handling Strategy

All errors follow a **structured error hierarchy** with consistent error codes.

```python
# models/errors.py
from dataclasses import dataclass
from datetime import datetime
import uuid

class AndesError(Exception):
    """Base exception for all Andes Data Lake errors."""
    code: str = "INTERNAL_ERROR"

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.correlation_id = str(uuid.uuid4())
        self.timestamp = datetime.utcnow().isoformat()

    def to_response(self) -> dict:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "correlationId": self.correlation_id,
                "timestamp": self.timestamp,
                "details": self.details,
            }
        }


class ValidationError(AndesError):
    """Raised when any validation layer fails."""
    code = "VALIDATION_ERROR"

class SchemaViolationError(ValidationError):
    code = "SCHEMA_VIOLATION"

class BusinessRuleViolationError(ValidationError):
    code = "BUSINESS_RULE_VIOLATION"

class InvalidFormatError(ValidationError):
    code = "INVALID_FORMAT"

class AuthorizationDeniedError(AndesError):
    code = "AUTHORIZATION_DENIED"

class DatasetNotFoundError(AndesError):
    code = "DATASET_NOT_FOUND"

class DuplicateRunError(AndesError):
    code = "DUPLICATE_RUN"

class VersionConflictError(AndesError):
    code = "VERSION_CONFLICT"

class LockHeldError(AndesError):
    code = "LOCK_HELD"

class InvalidStateTransitionError(AndesError):
    code = "INVALID_STATE_TRANSITION"


# API layer catches these and returns consistent JSON:
# api/error_handler.py
from fastapi import Request
from fastapi.responses import JSONResponse

async def andes_error_handler(request: Request, exc: AndesError):
    status_map = {
        "AUTHORIZATION_DENIED": 403,
        "DATASET_NOT_FOUND": 404,
        "VERSION_CONFLICT": 409,
        "LOCK_HELD": 409,
        "DUPLICATE_RUN": 200,  # Not an error — idempotent success
    }
    status = status_map.get(exc.code, 400)
    return JSONResponse(status_code=status, content=exc.to_response())
```

**Why a hierarchy?** The API layer catches `AndesError` once and maps it to HTTP. Business logic throws specific subclasses. No one needs to remember status codes — the error knows its own code.

---

## 15. Testing Patterns

### 15.1 Test Structure

Tests mirror the source structure and use clear naming:

```
tests/
├── unit/                    # Fast, no external dependencies
│   ├── test_models.py
│   ├── test_validation_pipeline.py
│   ├── test_subscription_state_machine.py
│   └── test_rbac.py
├── property/                # Hypothesis property-based tests
│   ├── test_prop_catalog.py
│   ├── test_prop_pipeline.py
│   └── test_prop_rbac.py
└── integration/             # Requires DynamoDB Local / moto
    ├── test_catalog_dynamo.py
    └── test_log_store_dynamo.py
```

### 15.2 Property-Based Testing with Hypothesis

```python
# tests/property/test_prop_catalog.py
from hypothesis import given, strategies as st, settings
from models.dataset import SchemaDefinition, FieldDefinition, FieldType

# Custom strategy: generate random valid dataset metadata
dataset_name = st.text(min_size=1, max_size=100, alphabet=st.characters(whitelist_categories=("L", "N")))
dataset_owner = st.text(min_size=1, max_size=50)
dataset_description = st.text(min_size=0, max_size=500)

field_def = st.builds(
    FieldDefinition,
    name=st.text(min_size=1, max_size=30, alphabet="abcdefghijklmnopqrstuvwxyz_"),
    type=st.sampled_from(list(FieldType)),
    required=st.booleans(),
    constraints=st.just([]),
)

schema_def = st.builds(
    SchemaDefinition,
    fields=st.lists(field_def, min_size=1, max_size=10),
    primary_key=st.just(["id"]),
    partition_keys=st.just([]),
)


# Feature: andes-data-lake, Property 1: Dataset registration round-trip
@settings(max_examples=100)
@given(name=dataset_name, owner=dataset_owner, desc=dataset_description, schema=schema_def)
def test_dataset_registration_roundtrip(name, owner, desc, schema):
    """For any valid dataset submission, registering and retrieving by ID
    returns matching name, schema, owner, and description."""
    repo = InMemoryDatasetRepository()
    catalog = CatalogService(repo=repo, log_store=MockLogStore(), rbac=MockRBAC())

    registered = catalog.register_dataset(name=name, schema=schema, owner=owner, description=desc)
    retrieved = catalog.get_dataset(registered.dataset_id)

    assert retrieved is not None
    assert retrieved.name == name
    assert retrieved.owner == owner
    assert retrieved.description == desc
    assert retrieved.schema == schema


# Feature: andes-data-lake, Property 5: Subscription state machine
@settings(max_examples=100)
@given(action_sequence=st.lists(
    st.sampled_from(["approve", "reject", "revoke"]),
    min_size=1, max_size=5,
))
def test_subscription_state_machine(action_sequence):
    """Subscription lifecycle follows valid state transitions only."""
    catalog = create_test_catalog()
    dataset = catalog.register_dataset("test", mock_schema, "owner", "desc")
    sub = catalog.create_subscription_request(dataset.dataset_id, "bi-user-1")

    assert sub.status == SubscriptionStatus.PENDING

    for action in action_sequence:
        try:
            if action == "approve":
                sub = catalog.approve_subscription(sub.subscription_id)
            elif action == "reject":
                sub = catalog.reject_subscription(sub.subscription_id, "reason")
            elif action == "revoke":
                sub = catalog.revoke_subscription(sub.subscription_id)
        except InvalidStateTransitionError:
            pass  # Expected — invalid transitions are rejected

    # Final state must be one of the valid states
    assert sub.status in {s for s in SubscriptionStatus}
```

### 15.3 Using Mocks for Unit Tests

```python
# tests/unit/test_rbac.py
import pytest
from rbac.rbac_module import RBACModule
from models.rbac import Role, RoleName

class MockLogStore:
    """Captures log entries for assertion in tests."""
    def __init__(self):
        self.entries = []
    def write(self, entry):
        self.entries.append(entry)

def test_controllership_can_publish():
    log = MockLogStore()
    rbac = RBACModule(log_store=log)
    rbac.assign_role("user-1", RoleName.CONTROLLERSHIP_TEAM_ROLE)

    result = rbac.authorize("user-1", "dataset:publish", "any-dataset")
    assert result.granted is True
    assert len(log.entries) == 2  # 1 for role assignment + 1 for auth decision

def test_bi_team_cannot_publish():
    log = MockLogStore()
    rbac = RBACModule(log_store=log)
    rbac.assign_role("user-2", RoleName.BI_TEAM_ROLE)

    result = rbac.authorize("user-2", "dataset:publish", "any-dataset")
    assert result.granted is False
    assert result.error_code == "AUTHORIZATION_DENIED"
```

---

## Summary of Patterns Used

| Pattern | Where Used | Why |
|---|---|---|
| Layered Architecture | Entire system | Separation of concerns, testability |
| Dependency Injection | All services | Decoupling, easy testing with mocks |
| Repository | Catalog, Log Store | Abstract data access, swap implementations |
| Chain of Responsibility | Validation Pipeline | Composable, extensible validation layers |
| State Machine | Subscription lifecycle | Enforce valid transitions, prevent illegal states |
| Strategy | Conflict Resolution | Pluggable resolution policies per dataset |
| Unit of Work | Transaction Manager | Atomic commit/rollback of multi-record writes |
| Circuit Breaker | Log Store | Resilience during DynamoDB outages |
| Observer / Event Bus | Centralized Logging | Decouple logging from business logic |
| Idempotency Key | Ingestion Engine | Prevent duplicate pipeline runs |
| Leader Election | Stream Consumer | Single coordinator per pipeline |
| Dead Letter Queue | Stream Validator | Preserve invalid records for debugging |
| Error Hierarchy | All components | Consistent error codes and API responses |
