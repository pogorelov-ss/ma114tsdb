# Research: ma114tsdb Core System

**Date**: 2025-10-04
**Feature**: 001-what-we-re
**Context**: Technical decisions for time-series data acquisition system with pluggable adapters

---

## Research Areas

### 1. Polylith Monorepo with Python + uv

**Decision**: Use Polylith architecture with uv package manager

**Rationale**:
- **Component reusability**: Polylith's component-based architecture (bricks) enables sharing core interfaces, models, and observability across all adapter implementations
- **Independent evolution**: Transport implementations (Memory, IPC, MQTT, Kombu) can evolve independently while maintaining stable interfaces
- **Cross-language compatibility**: Components expose clean Python protocols that can be mirrored in other languages (Go, Rust) for adapter development
- **uv advantages**: Faster dependency resolution than pip, lockfile-based reproducibility, workspace support for monorepo
- **Testing isolation**: Components tested independently, projects tested end-to-end

**Alternatives considered**:
- Traditional multi-package repo: Rejected due to duplication of core interfaces and observability code
- Single package: Rejected due to inability to version transports independently
- Poetry workspaces: Rejected in favor of uv's superior performance and PEP 621 compliance

**References**:
- Polylith architecture: https://polylith.gitbook.io/
- uv package manager: https://github.com/astral-sh/uv

---

### 2. Transport Layer: Kombu + AWS Greengrass Python Client

**Decision**:
- Kombu for memory/RabbitMQ/Redis/SQS transports
- AWS Greengrass Python SDK for IPC and MQTT on edge devices

**Rationale**:
- **Kombu**:
  - Unified abstraction over AMQP, Redis, SQS, in-memory backends
  - Battle-tested in Celery for high-throughput message passing
  - Supports QoS levels matching DataStream (lossy) vs Event (durable) semantics
  - Native serializer support (JSON, MessagePack)
- **AWS Greengrass**:
  - Standard for IoT edge deployments
  - Built-in IPC via Unix sockets for inter-process communication
  - MQTT client for pub/sub with AWS IoT Core
  - Supports local and cloud messaging patterns

**Alternatives considered**:
- Pure Paho MQTT: Rejected due to lack of IPC support and limited transport abstraction
- Custom transport layer: Rejected due to reinventing tested message queue patterns
- ZeroMQ: Rejected due to fewer deployment-ready patterns for IoT/cloud hybrid

**Best practices**:
- Use Kombu's connection pooling for high-throughput pipelines
- Implement heartbeat/keepalive for MQTT connections (FR-036: graceful adapter restart)
- Leverage Kombu's autoretry for Event delivery (at-least-once semantics)

**References**:
- Kombu documentation: https://docs.celeryq.dev/projects/kombu/
- AWS Greengrass SDK: https://github.com/aws/aws-iot-device-sdk-python-v2

---

### 3. MessagePack for Wire Format (Cross-Language Compatibility)

**Decision**: MessagePack for DataStream/Event serialization

**Rationale**:
- **Compact binary format**: 5-10x smaller than JSON for time-series data with repeated timestamps
- **Cross-language support**: Implementations in Python, Go, Rust, JavaScript, Java (critical for FR-018)
- **Schema-less evolution**: Supports FR-048 (ignore unknown fields) for forward compatibility
- **Performance**: Faster serialization than JSON, comparable to Protocol Buffers without schema compilation
- **Type preservation**: Maintains timestamp precision (sub-second) and UUID integrity

**Alternatives considered**:
- JSON: Rejected due to size overhead for high-volume DataStreams (100-10K/sec)
- Protocol Buffers: Rejected due to schema compilation overhead for rapid adapter development
- CBOR: Rejected due to less mature Python library support

**Implementation approach**:
- Envelope format: `{type: "datastream"|"event", version: {major, minor}, trace_context: {...}, payload: {...}}`
- Use msgpack.packb() with datetime=True for automatic timestamp handling
- Validate version compatibility on deserialization (N-1 check per FR-046)

**References**:
- MessagePack spec: https://msgpack.org/
- Python msgpack: https://github.com/msgpack/msgpack-python

---

### 4. Observability: Prometheus + Structured Logging + OpenTelemetry-Compatible Tracing

**Decision**:
- Prometheus client for metrics (FR-030)
- Python structlog for structured logging (FR-031)
- Lightweight trace context propagation (FR-032), OpenTelemetry-compatible in future

**Rationale**:
- **Prometheus**:
  - Industry standard for time-series metrics
  - Counter, Gauge, Histogram types match FR-030 requirements (events-processed, queue-depth, processing-time)
  - Pull model reduces adapter overhead
  - Native Kubernetes/container integration
- **structlog**:
  - Structured logging with JSON/EDN output (FR-031 format requirements)
  - Context binding for trace-id, adapter-id propagation
  - Performance-optimized for high-throughput systems
- **Trace propagation**:
  - v1.0: Simple trace-id/span-id propagation in TraceContext (FR-032 minimal overhead)
  - Future: OpenTelemetry SDK integration for full distributed tracing

**Alternatives considered**:
- StatsD: Rejected in favor of Prometheus pull model (less adapter responsibility)
- Python logging: Rejected in favor of structlog for structured output
- Full OpenTelemetry now: Deferred to v2.0 to minimize v1.0 complexity

**Best practices**:
- Expose Prometheus metrics on `/metrics` endpoint per adapter
- Use structlog processors for automatic timestamp/level/adapter-id injection
- Generate trace-id at pipeline ingress, propagate through all adapters

**References**:
- Prometheus Python client: https://github.com/prometheus/client_python
- structlog: https://www.structlog.org/

---

### 5. Configuration Management: File-based (v1.0) + Runtime API (v2.0)

**Decision**: Static file configuration (YAML/TOML) for v1.0, REST API deferred to v2.0

**Rationale**:
- **v1.0 scope** (FR-056, FR-057):
  - File-based configuration read at startup eliminates runtime API complexity
  - TOML format (pythonic, type-safe, comments for documentation)
  - Validation with pydantic models before pipeline initialization
  - Changes require restart (acceptable for v1.0)
- **v2.0 evolution**:
  - FastStream backend enables GraphQL/REST API for pipeline management
  - Hot-reload pipelines without full restart
  - Web UI for pipeline visualization

**Alternatives considered**:
- JSON configuration: Rejected in favor of TOML (comments, readability)
- Environment variables only: Rejected due to complexity for nested pipeline configs
- Immediate REST API: Deferred per clarifications (reduce v1.0 scope)

**Configuration schema**:
```toml
[system]
id = "ma114tsdb-prod"
environment = "production"

[transport]
type = "mqtt"
broker = "mqtt://broker:1883"
topic_prefix = "ma114tsdb"
qos_datastream = 0
qos_event = 1

[[adapters]]
id = "temp-sensor"
type = "producer"
impl = "ma114tsdb.adapters.http_poller"
config = { url = "http://sensor/data", interval_ms = 1000 }

[[pipelines]]
pipeline_id = "sensor-pipeline"
sources = ["temp-sensor"]
sinks = ["timescaledb-consumer"]
config = { parallelism = 4, time_window_minutes = 5 }
```

**References**:
- TOML spec: https://toml.io/
- pydantic: https://docs.pydantic.dev/

---

### 6. Adapter Lifecycle & Error Handling

**Decision**: State machine with exponential backoff retry, circuit breaker pattern

**Rationale**:
- **State transitions** (FR-009): init → running → stopped, error state transitions from any state
- **Exponential backoff** (FR-036): 1s, 5s, 30s delays with max 3 attempts prevents thundering herd
- **Circuit breaker**: After 3 failures, adapter marked failed, manual intervention required
- **Isolation** (FR-039): Adapter failures don't cascade to other adapters or pipelines

**Implementation pattern**:
```python
class AdapterState(Enum):
    INIT = "init"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    FAILED = "failed"

class AdapterLifecycle:
    async def start_with_retry(self, adapter: IAdapter):
        for attempt, delay in enumerate([1, 5, 30]):
            try:
                await adapter.start()
                self.state = AdapterState.RUNNING
                return
            except Exception as e:
                await emit_health_event(status="degraded", attempt=attempt)
                if attempt < 2:
                    await asyncio.sleep(delay)

        self.state = AdapterState.FAILED
        await emit_health_event(status="unhealthy")
        # Manual intervention required
```

**Alternatives considered**:
- Unlimited retries: Rejected due to resource exhaustion risk
- Immediate failure: Rejected in favor of transient error resilience
- Automatic recovery after failure: Deferred to operator decision (security/stability)

**References**:
- Circuit breaker pattern: https://martinfowler.com/bliki/CircuitBreaker.html

---

### 7. Testing Strategy: Contract Tests + Integration Tests + Quickstart Validation

**Decision**:
- Contract tests for all adapter interfaces (IAdapter, IProducer, IConsumer, IProcessor, ITransport)
- Integration tests for end-to-end pipeline flows
- Quickstart scenarios as executable validation tests

**Rationale**:
- **Contract tests**:
  - Validate adapter implementations conform to interfaces (FR-008, FR-010-013)
  - Reusable test suite for any adapter (Python or other languages)
  - Example: `test_producer_lifecycle(adapter: IProducer)` verifies init→start→stop→health
- **Integration tests**:
  - Match acceptance scenarios from spec (temperature sensor, event delivery, pipeline isolation)
  - Use in-memory transport for deterministic testing
  - Verify observability outputs (metrics, logs, traces)
- **Quickstart validation**:
  - Executable quickstart.md runs end-to-end scenarios
  - Validates deployment configurations
  - Smoke test for releases

**Testing frameworks**:
- pytest for test execution
- pytest-asyncio for async adapter testing
- hypothesis for property-based testing (version compatibility, timestamp handling)

**Test organization** (Polylith structure):
```
test/
├── contract/
│   ├── test_adapter_interface.py      # IAdapter contract tests
│   ├── test_producer_contract.py      # IProducer contract tests
│   ├── test_consumer_contract.py      # IConsumer contract tests
│   └── test_transport_contract.py     # ITransport contract tests
├── integration/
│   ├── test_datastream_pipeline.py    # High-volume DataStream flow
│   ├── test_event_pipeline.py         # Durable Event delivery
│   └── test_pipeline_isolation.py     # Failure isolation
└── unit/
    └── components/                     # Per-component unit tests
```

**References**:
- pytest: https://docs.pytest.org/
- Contract testing principles: https://martinfowler.com/bliki/ContractTest.html

---

### 8. Version Compatibility & Migration

**Decision**: Semantic versioning with N-1 compatibility checks at runtime

**Rationale**:
- **Version encoding** (FR-005): All DataStream/Event messages carry {major, minor} version
- **Compatibility check** (FR-046): Consumers validate version on deserialization:
  - Same major version: accept (minor version forward/backward compatible)
  - Previous major version (N-1): accept with compatibility layer
  - Older than N-1 or future major: reject with clear error
- **Deprecation warnings** (FR-050): Log warnings for N-1 version usage, document migration timeline

**Implementation approach**:
```python
@dataclass
class Version:
    major: int
    minor: int

    def is_compatible(self, other: Version) -> bool:
        # Same major version always compatible
        if self.major == other.major:
            return True
        # N-1 backward compatibility
        if self.major == other.major + 1:
            return True
        return False

def deserialize_with_version_check(data: bytes) -> DataStream | Event:
    envelope = msgpack.unpackb(data)
    msg_version = Version(**envelope['version'])

    if not CURRENT_VERSION.is_compatible(msg_version):
        raise VersionMismatchError(f"Version {msg_version} incompatible with {CURRENT_VERSION}")

    if msg_version.major < CURRENT_VERSION.major:
        logger.warning(f"Deprecated version {msg_version}, migrate to {CURRENT_VERSION}")

    return parse_payload(envelope['payload'], msg_version)
```

**Migration workflow**:
1. Announce deprecation (e.g., v1.5 deprecated when v2.0 released)
2. Coexistence period (v1.5 and v2.0 both supported for 6 months)
3. Hard cutoff (v1.5 rejected after coexistence period)

**References**:
- Semantic Versioning: https://semver.org/

---

---

### 9. Build System: NX for Task Orchestration and CI/CD

**Decision**: Use NX for monorepo build orchestration, CI/CD, and task caching

**Rationale**:
- **Task graph analysis**: NX automatically builds dependency graph from `project.json` files
- **Incremental builds**: Only builds affected projects (uses git diff to detect changes)
- **Distributed caching**: Task results cached locally and remotely (NX Cloud optional)
- **Parallel execution**: Runs independent tasks concurrently (maxes out CPU cores)
- **Python support**: NX supports Python projects via custom executors
- **CI/CD optimization**: Dramatically reduces CI time by running only affected tests/builds

**Project structure**:
- Root `nx.json`: Workspace-level configuration, task runners, caching
- Root `project.json`: Workspace-level tasks (e.g., `nx run workspace:test-all`)
- Per-project `project.json`: Project-specific tasks (build, test, deploy, lint)
  - `projects/ma114tsdb-runtime/project.json`
  - `projects/ma114tsdb-api/project.json`
  - `projects/inf_core_network/project.json`
  - `projects/inf_shared_monitoring/project.json`
  - `projects/dev_local_cluster/project.json`

**Infrastructure organization**:
- **Per-project infrastructure**: Each deployable project has optional `cdk/`, `k8s/`, `k8s.cdk/` subfolders
- **Core infrastructure projects**: `inf_core_*` (network, storage) — foundational resources
- **Shared infrastructure projects**: `inf_shared_*` (monitoring, observability) — shared services
- **Development projects**: `dev_*` — local cluster setup, testing utilities (not deployed to production)

**Task examples**:
```json
// projects/ma114tsdb-runtime/project.json
{
  "name": "ma114tsdb-runtime",
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
    },
    "deploy-infra": {
      "executor": "nx:run-commands",
      "options": {
        "command": "cd cdk && cdk deploy",
        "cwd": "projects/ma114tsdb-runtime"
      },
      "dependsOn": ["^deploy-infra"]  // Deploy core infrastructure first
    },
    "deploy": {
      "executor": "nx:run-commands",
      "options": {
        "command": "kubectl apply -f k8s/",
        "cwd": "projects/ma114tsdb-runtime"
      },
      "dependsOn": ["build", "deploy-infra"]
    }
  }
}
```

**Alternatives considered**:
- Make/Makefiles: Rejected due to poor dependency graph analysis and no caching
- Pants build: Rejected due to Python-specific (NX supports multi-language)
- Bazel: Rejected due to complexity and steep learning curve for small team
- Plain shell scripts: Rejected due to no parallelization or caching

**Best practices**:
- Use `dependsOn` for task dependencies (e.g., test depends on build)
- Use `^` prefix for dependencies on upstream projects (e.g., deploy depends on `^deploy-infra`)
- Enable NX Cloud for distributed caching in CI/CD (optional, free tier available)
- Run `nx affected:test` in CI to test only changed projects
- Use `nx graph` to visualize project dependencies

**References**:
- NX documentation: https://nx.dev/
- NX with Python: https://nx.dev/recipes/other/misc-python

---

## Summary

All technical unknowns resolved. No NEEDS CLARIFICATION items remain. Implementation can proceed to Phase 1 (Design & Contracts) with:

1. **Architecture**: Polylith monorepo with uv (Python 3.11+)
2. **Build System**: NX for task orchestration, incremental builds, CI/CD optimization
3. **Transports**: Kombu (memory/AMQP/Redis/SQS) + AWS Greengrass (IPC/MQTT)
4. **Serialization**: MessagePack for cross-language wire format
5. **Observability**: Prometheus metrics, structlog logging, trace context propagation
6. **Configuration**: TOML files with pydantic validation (v1.0)
7. **Error handling**: State machine with exponential backoff (1s, 5s, 30s), circuit breaker after 3 failures
8. **Testing**: Contract tests (adapter interfaces), integration tests (pipelines), quickstart validation
9. **Versioning**: Semantic versioning with N-1 runtime compatibility checks
10. **Infrastructure**: Per-project cdk/k8s folders, core/shared/dev naming conventions

Next phase: Generate data models, adapter interface contracts, and integration test scenarios.
