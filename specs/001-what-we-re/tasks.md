# Tasks: ma114tsdb Core System

**Input**: Design documents from `/home/pss/AIExperiments/ma114tsdb/specs/001-what-we-re/`
**Prerequisites**: plan.md, research.md, data-model.md, contracts/adapter-interfaces.py, quickstart.md

## Execution Flow (main)
```
1. Load plan.md from feature directory
   → Extract: tech stack (Python 3.11+, Polylith, NX, Kombu, etc.)
   → Extract: project structure (components/, bases/, projects/)
2. Load design documents:
   → data-model.md: 11 entities (DataStream, Event, TraceContext, Version, etc.)
   → contracts/adapter-interfaces.py: 5 interfaces (IAdapter, IProducer, IConsumer, IProcessor, ITransport)
   → quickstart.md: 6 integration scenarios
3. Generate tasks by category:
   → Setup: Polylith workspace, uv, NX, project structure
   → Core models: DataStream, Event, TraceContext, Version + pydantic validation
   → Adapter interfaces: IAdapter, IProducer, IConsumer, IProcessor, ITransport
   → Contract tests: One per interface (must fail initially - TDD)
   → Observability: Prometheus metrics, structlog logging, tracing
   → Transports: Memory, IPC, MQTT, Kombu implementations
   → Pipeline runtime: Lifecycle management, configuration loader
   → Infrastructure: NX projects for inf_core_*, inf_shared_*, dev_*
   → Integration tests: 6 quickstart scenarios
4. Apply task rules:
   → Different files/components = mark [P] for parallel
   → Same file = sequential (no [P])
   → Tests before implementation (TDD)
   → Core infrastructure before app infrastructure
5. Number tasks sequentially (T001, T002...)
6. Return: tasks.md ready for execution
```

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Polylith monorepo**: Root `/home/pss/AIExperiments/ma114tsdb/`
- **Components**: `components/core/`, `components/transports/`, `components/observability/`
- **Bases**: `bases/api/`, `bases/runtime/`
- **Projects**: `projects/ma114tsdb-runtime/`, `projects/inf_core_network/`, etc.
- **Tests**: `test/contract/`, `test/integration/`, `test/unit/`

---

## Phase 3.1: Setup & Workspace Initialization

### T001 [P] Initialize Polylith workspace
Create Polylith workspace structure with `workspace.toml` configuration.
```bash
# At repository root
polylith create workspace ma114tsdb --python
# Verify: workspace.toml exists with correct structure
```
**File**: `/home/pss/AIExperiments/ma114tsdb/workspace.toml`
**Dependencies**: None
**Validation**: `polylith check` passes

### T002 [P] Configure uv workspace with pyproject.toml
Create root `pyproject.toml` with workspace dependencies and Python 3.11+ requirement.
```toml
[project]
name = "ma114tsdb"
version = "1.0.0"
requires-python = ">=3.11"
dependencies = [
    "kombu>=5.3.0",
    "faststream>=0.3.0",
    "msgpack>=1.0.0",
    "prometheus-client>=0.18.0",
    "structlog>=23.0.0",
    "pydantic>=2.0.0",
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
]
```
**File**: `/home/pss/AIExperiments/ma114tsdb/pyproject.toml`
**Dependencies**: None
**Validation**: `uv pip list` shows all dependencies

### T003 [P] Initialize NX workspace
Create `nx.json` and root `project.json` for NX task orchestration.
```json
// nx.json
{
  "tasksRunnerOptions": {
    "default": {
      "runner": "nx/tasks-runners/default",
      "options": {
        "cacheableOperations": ["build", "test", "lint"]
      }
    }
  }
}

// project.json (root)
{
  "name": "ma114tsdb-workspace",
  "targets": {
    "test-all": {
      "executor": "nx:run-commands",
      "options": {
        "command": "pytest test/"
      }
    }
  }
}
```
**Files**: `/home/pss/AIExperiments/ma114tsdb/nx.json`, `/home/pss/AIExperiments/ma114tsdb/project.json`
**Dependencies**: None
**Validation**: `nx show projects` lists workspace

### T004 [P] Create Polylith component directories
Create directory structure for all Polylith components.
```bash
mkdir -p components/core/{models,interfaces,versioning}
mkdir -p components/transports/{memory,ipc,mqtt,kombu}
mkdir -p components/observability/{metrics,logging,tracing}
mkdir -p components/pipeline/{runtime,config,lifecycle}
mkdir -p components/serialization/msgpack
```
**Path**: `/home/pss/AIExperiments/ma114tsdb/components/`
**Dependencies**: T001 (Polylith workspace)

### T005 [P] Create Polylith bases and projects directories
Create directory structure for bases and projects.
```bash
mkdir -p bases/{api,runtime}
mkdir -p projects/{ma114tsdb-runtime,ma114tsdb-api}
mkdir -p projects/{inf_core_network,inf_core_storage}
mkdir -p projects/{inf_shared_monitoring}
mkdir -p projects/{dev_local_cluster,dev_testing_utils}
```
**Path**: `/home/pss/AIExperiments/ma114tsdb/{bases,projects}/`
**Dependencies**: T001 (Polylith workspace)

### T006 [P] Create test directories
Create test directory structure for contract, integration, and unit tests.
```bash
mkdir -p test/{contract,integration,unit}
```
**Path**: `/home/pss/AIExperiments/ma114tsdb/test/`
**Dependencies**: None

### T007 [P] Configure pytest with pytest.ini
Create `pytest.ini` with async support and test discovery settings.
```ini
[pytest]
testpaths = test
python_files = test_*.py
python_classes = Test*
python_functions = test_*
asyncio_mode = auto
```
**File**: `/home/pss/AIExperiments/ma114tsdb/pytest.ini`
**Dependencies**: T002 (uv workspace with pytest)

### T008 [P] Create .gitignore
Create `.gitignore` for Python, NX, and Polylith artifacts.
```
__pycache__/
*.py[cod]
.venv/
.pytest_cache/
.nx/
dist/
*.egg-info/
```
**File**: `/home/pss/AIExperiments/ma114tsdb/.gitignore`
**Dependencies**: None

---

## Phase 3.2: NX Project Configurations

### T009 [P] Create ma114tsdb-runtime project.json
Create NX project configuration for runtime project.
```json
{
  "name": "ma114tsdb-runtime",
  "sourceRoot": "projects/ma114tsdb-runtime/src",
  "targets": {
    "build": {
      "executor": "nx:run-commands",
      "options": {
        "command": "uv build",
        "cwd": "projects/ma114tsdb-runtime"
      }
    },
    "test": {
      "executor": "nx:run-commands",
      "options": {
        "command": "pytest test/",
        "cwd": "projects/ma114tsdb-runtime"
      },
      "dependsOn": ["build"]
    }
  }
}
```
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-runtime/project.json`
**Dependencies**: T003 (NX workspace), T005 (project directories)

### T010 [P] Create ma114tsdb-api project.json
Create NX project configuration for API project.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-api/project.json`
**Dependencies**: T003, T005

### T011 [P] Create inf_core_network project.json
Create NX project configuration for core network infrastructure.
```json
{
  "name": "inf_core_network",
  "sourceRoot": "projects/inf_core_network/cdk",
  "targets": {
    "deploy-infra": {
      "executor": "nx:run-commands",
      "options": {
        "command": "cdk deploy",
        "cwd": "projects/inf_core_network/cdk"
      }
    }
  }
}
```
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_core_network/project.json`
**Dependencies**: T003, T005

### T012 [P] Create inf_core_storage project.json
Create NX project configuration for core storage infrastructure.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_core_storage/project.json`
**Dependencies**: T003, T005

### T013 [P] Create inf_shared_monitoring project.json
Create NX project configuration for shared monitoring infrastructure.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_shared_monitoring/project.json`
**Dependencies**: T003, T005

### T014 [P] Create dev_local_cluster project.json
Create NX project configuration for local k3d cluster development project.
```json
{
  "name": "dev_local_cluster",
  "sourceRoot": "projects/dev_local_cluster/scripts",
  "targets": {
    "start": {
      "executor": "nx:run-commands",
      "options": {
        "command": "bash create-cluster.sh",
        "cwd": "projects/dev_local_cluster/scripts"
      }
    }
  }
}
```
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/dev_local_cluster/project.json`
**Dependencies**: T003, T005

### T015 [P] Create dev_testing_utils project.json
Create NX project configuration for development testing utilities.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/dev_testing_utils/project.json`
**Dependencies**: T003, T005

---

## Phase 3.3: Core Infrastructure Projects

### T016 [P] Create inf_core_network CDK stack for VPC
Create AWS CDK stack for VPC, subnets, and security groups in `projects/inf_core_network/cdk/app.py`.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_core_network/cdk/app.py`
**Dependencies**: T011 (inf_core_network project.json)

### T017 [P] Create inf_core_storage CDK stack for databases
Create AWS CDK stack for RDS (optional), S3 buckets, and DLQ storage in `projects/inf_core_storage/cdk/app.py`.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_core_storage/cdk/app.py`
**Dependencies**: T012 (inf_core_storage project.json)

### T018 [P] Create inf_shared_monitoring CDK stack for Prometheus
Create AWS CDK stack for Prometheus, Grafana in `projects/inf_shared_monitoring/cdk/app.py`.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_shared_monitoring/cdk/app.py`
**Dependencies**: T013 (inf_shared_monitoring project.json)

### T019 [P] Create inf_shared_monitoring Kubernetes manifests
Create K8s manifests for Prometheus, Grafana in `projects/inf_shared_monitoring/k8s/`.
**Files**: `/home/pss/AIExperiments/ma114tsdb/projects/inf_shared_monitoring/k8s/{prometheus.yaml,grafana.yaml}`
**Dependencies**: T013

---

## Phase 3.4: Core Data Models (Tests First - TDD)

### T020 [P] Create Version model in components/core/versioning
Implement `Version` dataclass with `is_compatible()` method and pydantic validation.
```python
@dataclass
class Version:
    major: int
    minor: int

    def is_compatible(self, other: 'Version') -> bool:
        if self.major == other.major:
            return True  # Same major
        if self.major == other.major + 1:
            return True  # N-1 backward compat
        return False
```
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/versioning/version.py`
**Dependencies**: T004 (component directories), T002 (pydantic dependency)

### T021 [P] Create TraceContext model in components/core/models
Implement `TraceContext` dataclass with `create_child_span()` method.
```python
@dataclass
class TraceContext:
    trace_id: UUID
    span_id: UUID
    parent_span: UUID | None = None

    def create_child_span(self) -> 'TraceContext':
        return TraceContext(
            trace_id=self.trace_id,
            span_id=uuid4(),
            parent_span=self.span_id
        )
```
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/trace_context.py`
**Dependencies**: T004, T002

### T022 [P] Create DataStream model with pydantic validation
Implement `DataStream` dataclass with all fields from data-model.md and pydantic validators.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/datastream.py`
**Dependencies**: T004, T002, T020 (Version), T021 (TraceContext)

### T023 [P] Create Event model with pydantic validation
Implement `Event` dataclass with all fields from data-model.md and pydantic validators.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/event.py`
**Dependencies**: T004, T002, T020, T021

### T024 [P] Create AdapterMetadata model
Implement `AdapterMetadata`, `AdapterCapabilities`, `AdapterType`, `DataType` enums and dataclasses.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/adapter_metadata.py`
**Dependencies**: T004, T002, T020

### T025 [P] Create RoutingInfo and SubscriptionPattern models
Implement `RoutingInfo` and `SubscriptionPattern` for transport abstraction.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/routing.py`
**Dependencies**: T004, T002

### T026 [P] Create PipelineConfig and DLQConfig models
Implement `PipelineConfig`, `PipelineRuntimeConfig`, `DLQConfig`, `ErrorPolicy` for pipeline configuration.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/config.py`
**Dependencies**: T004, T002

### T027 [P] Create Result and HealthStatus models
Implement `Result`, `HealthStatus`, `AdapterState` enum for adapter lifecycle.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/models/lifecycle.py`
**Dependencies**: T004, T002

---

## Phase 3.5: Adapter Interface Definitions

### T028 [P] Create IAdapter base interface in components/core/interfaces
Implement `IAdapter` protocol with `init()`, `start()`, `stop()`, `health()`, `metadata` property.
```python
class IAdapter(Protocol):
    async def init(self, config: dict[str, Any]) -> Result: ...
    async def start(self) -> Result: ...
    async def stop(self) -> Result: ...
    async def health(self) -> HealthStatus: ...

    @property
    def metadata(self) -> AdapterMetadata: ...
```
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/interfaces/adapter.py`
**Dependencies**: T004, T024 (AdapterMetadata), T027 (Result, HealthStatus)

### T029 [P] Create IProducer interface
Implement `IProducer` protocol extending `IAdapter` with `produce()` async generator.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/interfaces/producer.py`
**Dependencies**: T004, T028 (IAdapter), T022 (DataStream), T023 (Event)

### T030 [P] Create IConsumer interface
Implement `IConsumer` protocol extending `IAdapter` with `consume()` method.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/interfaces/consumer.py`
**Dependencies**: T004, T028, T022, T023

### T031 [P] Create IProcessor interface
Implement `IProcessor` protocol extending `IAdapter` with `process()` method (returns None for filtering).
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/interfaces/processor.py`
**Dependencies**: T004, T028, T022, T023

### T032 [P] Create ITransport interface
Implement `ITransport` protocol with `publish()`, `subscribe()`, `ack()`, `declare()` methods.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/core/interfaces/transport.py`
**Dependencies**: T004, T028, T022, T023, T025 (RoutingInfo, SubscriptionPattern)

---

## Phase 3.6: Contract Tests (MUST FAIL Initially - TDD)

### T033 [P] Contract test for IAdapter interface
Create contract test base class in `test/contract/test_adapter_contract.py` that validates:
- `init()` with valid config succeeds
- Lifecycle transitions: init → start → stop
- `health()` returns valid HealthStatus
- `metadata` includes capabilities
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_adapter_contract.py`
**Dependencies**: T028 (IAdapter), T007 (pytest config)
**Expected**: FAILS (no implementations yet)

### T034 [P] Contract test for IProducer interface
Create contract test that validates:
- `produce()` yields DataStream or Event
- All data includes TraceContext and Version
- Source field matches adapter ID
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_producer_contract.py`
**Dependencies**: T029 (IProducer), T007
**Expected**: FAILS (no implementations yet)

### T035 [P] Contract test for IConsumer interface
Create contract test that validates:
- `consume()` processes data without blocking pipeline
- Returns Result with success/failure
- Handles N-1 version compatibility
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_consumer_contract.py`
**Dependencies**: T030 (IConsumer), T007
**Expected**: FAILS (no implementations yet)

### T036 [P] Contract test for IProcessor interface
Create contract test that validates:
- `process()` transforms data or returns None (filter)
- Creates new span_id, preserves trace_id
- Sequential execution order
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_processor_contract.py`
**Dependencies**: T031 (IProcessor), T007
**Expected**: FAILS (no implementations yet)

### T037 [P] Contract test for ITransport interface
Create contract test that validates:
- `publish()` handles DataStream (lossy) and Event (durable) differently
- `subscribe()` returns async iterator
- `ack()` confirms delivery
- `declare()` creates resources
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_transport_contract.py`
**Dependencies**: T032 (ITransport), T007
**Expected**: FAILS (no implementations yet)

### T038 [P] Unit tests for Version compatibility
Create unit tests for `Version.is_compatible()` method covering:
- Same major version: compatible
- N-1 major version: compatible with warning
- Older than N-1: incompatible
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_version.py`
**Dependencies**: T020 (Version), T007
**Expected**: PASSES (Version model complete)

### T039 [P] Unit tests for TraceContext span creation
Create unit tests for `TraceContext.create_child_span()` method.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_trace_context.py`
**Dependencies**: T021 (TraceContext), T007
**Expected**: PASSES (TraceContext model complete)

### T040 [P] Unit tests for DataStream pydantic validation
Create unit tests for DataStream field validation (timestamps, non-empty lists, etc.).
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_datastream.py`
**Dependencies**: T022 (DataStream), T007
**Expected**: PASSES (DataStream model complete)

### T041 [P] Unit tests for Event pydantic validation
Create unit tests for Event field validation (UUID, timestamps, event_type, etc.).
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_event.py`
**Dependencies**: T023 (Event), T007
**Expected**: PASSES (Event model complete)

---

## Phase 3.7: Observability Components

### T042 [P] Create Prometheus metrics collector in components/observability/metrics
Implement metrics collection for adapters:
- `events_processed` (Counter)
- `events_failed` (Counter)
- `processing_time` (Histogram)
- `queue_depth` (Gauge)
- `adapter_state` (Gauge)
**File**: `/home/pss/AIExperiments/ma114tsdb/components/observability/metrics/prometheus_collector.py`
**Dependencies**: T004, T002 (prometheus-client dependency)

### T043 [P] Create structlog logger configuration in components/observability/logging
Implement structured logging with JSON/EDN formatters and automatic context injection (timestamp, level, adapter_id).
**File**: `/home/pss/AIExperiments/ma114tsdb/components/observability/logging/structured_logger.py`
**Dependencies**: T004, T002 (structlog dependency)

### T044 [P] Create trace propagator in components/observability/tracing
Implement trace context propagation with automatic span creation.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/observability/tracing/trace_propagator.py`
**Dependencies**: T004, T021 (TraceContext)

### T045 [P] Unit tests for Prometheus metrics collector
Test metric collection (counter increment, histogram recording, gauge set).
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_prometheus_collector.py`
**Dependencies**: T042, T007

### T046 [P] Unit tests for structured logging
Test log formatting (JSON output, context binding, adapter_id injection).
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_structured_logger.py`
**Dependencies**: T043, T007

### T047 [P] Unit tests for trace propagation
Test span creation (trace_id preservation, parent_span linkage).
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_trace_propagator.py`
**Dependencies**: T044, T007

---

## Phase 3.8: Serialization (MessagePack)

### T048 [P] Create MessagePack serializer in components/serialization/msgpack
Implement envelope-based serialization with version and trace_context:
```python
envelope = {
    "type": "datastream" | "event",
    "version": {"major": 1, "minor": 0},
    "trace_context": {...},
    "payload": {...}
}
```
**File**: `/home/pss/AIExperiments/ma114tsdb/components/serialization/msgpack/serializer.py`
**Dependencies**: T004, T002 (msgpack dependency), T020 (Version), T021 (TraceContext)

### T049 [P] Unit tests for MessagePack serialization
Test serialization/deserialization with version compatibility checks.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_msgpack_serializer.py`
**Dependencies**: T048, T007

---

## Phase 3.9: Transport Implementations

### T050 [P] Implement Memory transport in components/transports/memory
Implement in-memory transport with best-effort delivery using Python queues.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/transports/memory/transport.py`
**Dependencies**: T004, T032 (ITransport), T048 (serializer)

### T051 [P] Contract test for Memory transport
Run transport contract tests against Memory implementation (should PASS now).
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_memory_transport.py`
**Dependencies**: T050, T037 (transport contract test)
**Expected**: PASSES (Memory transport implements ITransport)

### T052 Unit tests for Memory transport
Test in-memory queue operations, buffer overflow handling.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_memory_transport.py`
**Dependencies**: T050, T007

### T053 Implement IPC transport in components/transports/ipc
Implement Unix domain socket transport using AWS Greengrass Python SDK.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/transports/ipc/transport.py`
**Dependencies**: T004, T032, T048, T050 (reference implementation)

### T054 Contract test for IPC transport
Run transport contract tests against IPC implementation.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_ipc_transport.py`
**Dependencies**: T053, T037

### T055 Unit tests for IPC transport
Test socket creation, path resolution, connection handling.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_ipc_transport.py`
**Dependencies**: T053, T007

### T056 Implement MQTT transport in components/transports/mqtt
Implement MQTT transport using AWS Greengrass Python SDK with QoS 0 (DataStream) and QoS 1 (Event).
**File**: `/home/pss/AIExperiments/ma114tsdb/components/transports/mqtt/transport.py`
**Dependencies**: T004, T032, T048, T050

### T057 Contract test for MQTT transport
Run transport contract tests against MQTT implementation.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_mqtt_transport.py`
**Dependencies**: T056, T037

### T058 Unit tests for MQTT transport
Test topic wildcards (+, #), QoS levels, connection handling.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_mqtt_transport.py`
**Dependencies**: T056, T007

### T059 Implement Kombu transport in components/transports/kombu
Implement Kombu transport for AMQP, Redis, SQS with configurable backends.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/transports/kombu/transport.py`
**Dependencies**: T004, T032, T048, T050, T002 (kombu dependency)

### T060 Contract test for Kombu transport
Run transport contract tests against Kombu implementation.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/contract/test_kombu_transport.py`
**Dependencies**: T059, T037

### T061 Unit tests for Kombu transport
Test exchange declaration, routing keys, persistent delivery mode.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_kombu_transport.py`
**Dependencies**: T059, T007

---

## Phase 3.10: Pipeline Runtime & Lifecycle

### T062 [X] Create adapter lifecycle manager in components/pipeline/lifecycle
Implement state machine with exponential backoff (1s, 5s, 30s) and circuit breaker.
```python
class AdapterLifecycle:
    async def start_with_retry(self, adapter: IAdapter):
        for attempt, delay in enumerate([1, 5, 30]):
            # Retry logic with health event emission
```
**File**: `/home/pss/AIExperiments/ma114tsdb/components/pipeline/lifecycle/adapter_lifecycle.py`
**Dependencies**: T004, T028 (IAdapter), T027 (AdapterState)

### T063 [X] Create pipeline runtime engine in components/pipeline/runtime
Implement pipeline execution: Producer → Transport → Processor → Transport → Consumer.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/pipeline/runtime/pipeline_engine.py`
**Dependencies**: T004, T028-T032 (all interfaces), T062 (lifecycle manager)

### T064 [X] Unit tests for adapter lifecycle manager
Test state transitions, exponential backoff delays, circuit breaker after 3 failures.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_adapter_lifecycle.py`
**Dependencies**: T062, T007

### T065 [X] Unit tests for pipeline runtime engine
Test pipeline data flow, processor ordering, consumer parallelism.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_pipeline_engine.py`
**Dependencies**: T063, T007

---

## Phase 3.11: Configuration Loader

### T066 Create TOML configuration loader in components/pipeline/config
Implement configuration file parser with pydantic validation for `PipelineConfig`.
**File**: `/home/pss/AIExperiments/ma114tsdb/components/pipeline/config/config_loader.py`
**Dependencies**: T004, T026 (PipelineConfig), T002 (pydantic)

### T067 Unit tests for configuration loader
Test TOML parsing, validation errors, pipeline config structure.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/unit/test_config_loader.py`
**Dependencies**: T066, T007

---

## Phase 3.12: Development Projects

### T068 [P] Create dev_local_cluster k3d setup script
Create bash script to create k3d cluster with Prometheus and Grafana.
```bash
# create-cluster.sh
k3d cluster create ma114tsdb-dev \
  --agents 2 \
  --port "8080:80@loadbalancer" \
  --port "9090:9090@loadbalancer"
```
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/dev_local_cluster/scripts/create-cluster.sh`
**Dependencies**: T014 (dev_local_cluster project.json)

### T069 [P] Create dev_testing_utils mock adapters
Create mock producer, consumer, processor adapters for testing.
- `mock_temp_sensor.py` - Temperature sensor producer
- `mock_flaky_consumer.py` - Consumer with configurable failure rate
- `mock_failing_producer.py` - Producer that fails after N seconds
**Files**: `/home/pss/AIExperiments/ma114tsdb/projects/dev_testing_utils/src/mock_*.py`
**Dependencies**: T015 (dev_testing_utils project.json), T029-T031 (adapter interfaces)

### T070 [P] Create dev_testing_utils console logger adapter
Create console logger consumer for quickstart scenarios.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/dev_testing_utils/src/console_logger.py`
**Dependencies**: T015, T030 (IConsumer)

### T071 [P] Create dev_testing_utils DLQ inspector tool
Create CLI tool to inspect Dead Letter Queue entries.
```bash
python -m ma114tsdb.tools.dlq_inspector dlq/
```
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/dev_testing_utils/src/dlq_inspector.py`
**Dependencies**: T015, T022-T023 (data models)

---

## Phase 3.13: Integration Tests (Quickstart Scenarios)

### T072 [P] Integration test: Scenario 1 - High-volume DataStream collection
Implement test for temperature sensor producing readings with lossy delivery.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_scenario1_datastream.py`
**Dependencies**: T063 (pipeline engine), T050 (memory transport), T069 (mock adapters)

### T073 [P] Integration test: Scenario 2 - Critical Event durable delivery
Implement test for device emitting critical events with at-least-once guarantee.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_scenario2_event.py`
**Dependencies**: T063, T059 (kombu transport), T069

### T074 [P] Integration test: Scenario 3 - In-flight data transformation
Implement test for processor filtering anomalous readings.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_scenario3_processor.py`
**Dependencies**: T063, T050, T069

### T075 [P] Integration test: Scenario 4 - Pipeline isolation
Implement test for multiple pipelines where one fails without affecting others.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_scenario4_isolation.py`
**Dependencies**: T063, T062 (lifecycle manager), T069

### T076 [P] Integration test: Scenario 5 - Dead Letter Queue
Implement test for failed data preservation in DLQ.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_scenario5_dlq.py`
**Dependencies**: T063, T066 (config loader with DLQ), T069

### T077 [P] Integration test: Scenario 6 - Observability
Implement test for metrics, logs, traces across all adapters.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_scenario6_observability.py`
**Dependencies**: T063, T042-T044 (observability components), T069

### T078 [P] Performance test: Throughput validation
Implement test to validate 100-10,000 streams/second throughput.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_performance_throughput.py`
**Dependencies**: T063, T050, T069

### T079 [P] Performance test: Version compatibility
Implement test matrix for N-1 version compatibility.
**File**: `/home/pss/AIExperiments/ma114tsdb/test/integration/test_version_compatibility.py`
**Dependencies**: T063, T020 (Version), T048 (serializer with version check)

---

## Phase 3.14: Bases (Application Entry Points)

### T080 Create runtime base in bases/runtime
Implement main entry point for pipeline runtime that loads config and starts pipeline.
```python
async def main(config_path: str):
    config = load_config(config_path)
    engine = PipelineEngine(config)
    await engine.start()
```
**File**: `/home/pss/AIExperiments/ma114tsdb/bases/runtime/main.py`
**Dependencies**: T063 (pipeline engine), T066 (config loader)

### T081 Create API base in bases/api
Implement FastStream API entry point for observability queries (future v2.0 runtime API).
**File**: `/home/pss/AIExperiments/ma114tsdb/bases/api/main.py`
**Dependencies**: T002 (faststream dependency), T042 (metrics)

---

## Phase 3.15: Project Packaging

### T082 Package ma114tsdb-runtime project
Create `src/` with runtime application using runtime base + all components.
**Directory**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-runtime/src/`
**Dependencies**: T080 (runtime base), all components (T020-T067)

### T083 Package ma114tsdb-api project
Create `src/` with API application using api base + observability components.
**Directory**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-api/src/`
**Dependencies**: T081 (api base), T042-T044 (observability)

---

## Phase 3.16: Per-Project Infrastructure

### T084 [P] Create ma114tsdb-runtime AWS CDK stack
Create CDK stack for runtime ECS/Fargate deployment in `projects/ma114tsdb-runtime/cdk/app.py`.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-runtime/cdk/app.py`
**Dependencies**: T082 (runtime project), T016-T017 (core infrastructure)

### T085 [P] Create ma114tsdb-runtime Kubernetes manifests
Create K8s deployment, service, configmap in `projects/ma114tsdb-runtime/k8s/`.
**Files**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-runtime/k8s/{deployment.yaml,service.yaml,configmap.yaml}`
**Dependencies**: T082

### T086 [P] Create ma114tsdb-api AWS CDK stack
Create CDK stack for API ECS/Fargate deployment.
**File**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-api/cdk/app.py`
**Dependencies**: T083 (api project), T016-T017

### T087 [P] Create ma114tsdb-api Kubernetes manifests
Create K8s deployment, service for API.
**Files**: `/home/pss/AIExperiments/ma114tsdb/projects/ma114tsdb-api/k8s/{deployment.yaml,service.yaml}`
**Dependencies**: T083

---

## Dependencies Summary

**Critical path** (must be sequential):
1. Setup (T001-T008) → NX configs (T009-T015) → Infrastructure (T016-T019)
2. Core models (T020-T027) → Interfaces (T028-T032) → Contract tests (T033-T037)
3. Transports (T050, T053, T056, T059) must pass contract tests before integration tests
4. Pipeline runtime (T062-T063) → Integration tests (T072-T079)
5. Bases (T080-T081) → Project packaging (T082-T083) → Per-project infra (T084-T087)

**Parallel execution groups**:
- Setup tasks T001-T008 [P]
- NX project configs T009-T015 [P]
- Core infrastructure T016-T019 [P]
- Core models T020-T027 [P]
- Adapter interfaces T028-T032 [P]
- Contract tests T033-T041 [P]
- Observability components T042-T047 [P]
- Transport implementations can run in parallel AFTER interfaces complete
- Integration tests T072-T079 [P] (can all run concurrently)
- Per-project infrastructure T084-T087 [P]

---

## Parallel Execution Example

```bash
# Execute setup tasks in parallel
nx run-many --target=init --projects=workspace,uv,nx --parallel=3

# After interfaces complete, run all contract tests in parallel
pytest test/contract/ -n auto  # pytest-xdist for parallelization

# Build all transport implementations in parallel
nx run-many --target=build --projects=memory-transport,ipc-transport,mqtt-transport,kombu-transport --parallel=4

# Run all integration tests in parallel
pytest test/integration/ -n 6  # 6 scenarios in parallel

# Deploy all infrastructure in dependency order
nx run-many --target=deploy-infra --all
```

---

## Validation

After all tasks complete:
- [ ] All contract tests PASS
- [ ] All unit tests PASS
- [ ] All integration tests PASS (6 quickstart scenarios)
- [ ] Performance tests achieve 100-10K streams/sec
- [ ] NX dependency graph shows all projects
- [ ] Constitution principles validated (all adapters implement interfaces, observability mandatory, versioning enforced)
- [ ] Local k3d deployment successful
- [ ] AWS infrastructure deployable via NX

---

**Total Tasks**: 87 (Setup: 8, NX: 7, Infrastructure: 4, Models: 8, Interfaces: 5, Tests: 26, Observability: 6, Serialization: 2, Transports: 16, Pipeline: 4, Config: 2, Dev Projects: 4, Integration: 8, Bases: 2, Packaging: 2, Per-project Infra: 4)

**Estimated Completion Time**: 3-4 weeks (with parallel execution and task caching via NX)

**Next Steps**: Execute tasks in order, marking [P] tasks for parallel execution. Use `nx affected:test` to run only affected tests after changes.
