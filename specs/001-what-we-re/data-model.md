# Data Model: ma114tsdb Core System

**Date**: 2025-10-04
**Feature**: 001-what-we-re
**Version**: 1.0.0

---

## Core Data Types

### 1. DataStream (High-Volume, Lossy-Tolerant)

**Purpose**: Compact representation of high-volume time-series sensor data where some loss is acceptable.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `stream_id` | `str` | Yes | Unique stream identifier | Non-empty string |
| `source` | `str` | Yes | Adapter ID that produced this stream | Valid adapter ID |
| `timestamp_start` | `datetime` | Yes | First data point timestamp | Sub-second precision, timezone-aware |
| `timestamp_end` | `datetime` | Yes | Last data point timestamp | >= timestamp_start |
| `interval` | `timedelta \| None` | No | Regular interval between points (optional) | Positive duration |
| `data_points` | `list[Any]` | Yes | Array of data points (internal format deferred v1.1) | Non-empty |
| `trace_context` | `TraceContext` | Yes | Distributed tracing metadata | Valid TraceContext |
| `metadata` | `dict[str, Any]` | No | Optional additional context | Key-value pairs |
| `version` | `Version` | Yes | Data format version | Valid semantic version |

**Characteristics**:
- Delivery: Best-effort, lossy-tolerant (some loss acceptable)
- Ordering: NOT guaranteed by timestamp
- Prioritization: Latest-first within time window (configurable, default 1-5 minutes)
- Throughput target: 100-10,000 streams/second per pipeline

**Use cases**: Temperature readings, CPU metrics, network bandwidth, sensor arrays

**Example** (Python):
```python
@dataclass
class DataStream:
    stream_id: str
    source: str
    timestamp_start: datetime
    timestamp_end: datetime
    data_points: list[Any]
    trace_context: TraceContext
    version: Version
    interval: timedelta | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

---

### 2. Event (Critical, Durable)

**Purpose**: Critical occurrences that must never be lost, with flexible payload structure.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `event_id` | `UUID` | Yes | Unique event identifier | UUID v4 |
| `timestamp` | `datetime` | Yes | When event occurred | Sub-second precision, timezone-aware |
| `source` | `str` | Yes | Adapter ID that produced this event | Valid adapter ID |
| `event_type` | `str` | Yes | Event classification (e.g., "health-check", "alarm") | Non-empty string |
| `stream_id` | `str \| None` | No | Optional reference to DataStream | Valid stream ID if present |
| `data` | `dict[str, Any]` | Yes | Flexible event payload | Non-empty dict |
| `trace_context` | `TraceContext` | Yes | Distributed tracing metadata | Valid TraceContext |
| `metadata` | `dict[str, Any]` | No | Additional context | Key-value pairs |
| `version` | `Version` | Yes | Data format version | Valid semantic version |

**Characteristics**:
- Delivery: Durable, at-least-once guarantee (NEVER lost)
- Ordering: FIFO (First-In-First-Out) by default
- Idempotency: Consumers responsible for handling duplicates
- Durability: Persisted before acknowledgment

**Use cases**: Device state changes, alarms, system events, configuration changes, health checks

**Example** (Python):
```python
@dataclass
class Event:
    event_id: UUID
    timestamp: datetime
    source: str
    event_type: str
    data: dict[str, Any]
    trace_context: TraceContext
    version: Version
    stream_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
```

---

### 3. TraceContext (Lightweight Correlation)

**Purpose**: Minimal distributed tracing correlation without full span overhead.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `trace_id` | `UUID` | Yes | Unique trace identifier | UUID v4 |
| `span_id` | `UUID` | Yes | Current span identifier | UUID v4 |
| `parent_span` | `UUID \| None` | No | Optional parent span | UUID v4 if present |

**Propagation**:
- **Producer**: Generates trace_id at ingress, creates initial span_id
- **Processor**: Preserves trace_id, creates new span_id, sets parent_span to incoming span_id
- **Consumer**: Preserves trace_id for correlation, creates final span_id
- **Transport**: Passes through trace_context unchanged

**Example** (Python):
```python
@dataclass
class TraceContext:
    trace_id: UUID
    span_id: UUID
    parent_span: UUID | None = None

    def create_child_span(self) -> 'TraceContext':
        """Generate new span while preserving trace lineage."""
        return TraceContext(
            trace_id=self.trace_id,
            span_id=uuid4(),
            parent_span=self.span_id
        )
```

---

### 4. Version (Semantic Versioning)

**Purpose**: Data format version for compatibility checking.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `major` | `int` | Yes | Breaking changes increment | >= 0 |
| `minor` | `int` | Yes | Backward-compatible additions | >= 0 |

**Compatibility rules**:
- Same major version: Always compatible
- N-1 major version: Compatible (with deprecation warning)
- Older than N-1 or future major: Incompatible (reject with error)

**Example** (Python):
```python
@dataclass
class Version:
    major: int
    minor: int

    def is_compatible(self, other: 'Version') -> bool:
        """Check if other version is compatible with this version."""
        if self.major == other.major:
            return True  # Same major version
        if self.major == other.major + 1:
            return True  # N-1 backward compatibility
        return False

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}"
```

---

## Adapter Metadata Types

### 5. AdapterMetadata (Self-Description)

**Purpose**: Self-describing information for adapter discovery and capability negotiation.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `id` | `str` | Yes | Unique adapter identifier | Non-empty string |
| `name` | `str` | Yes | Human-readable name | Non-empty string |
| `version` | `Version` | Yes | Adapter version | Valid semantic version |
| `type` | `AdapterType` | Yes | Adapter role | One of: producer, consumer, processor, transport |
| `capabilities` | `AdapterCapabilities` | Yes | Adapter capabilities | Valid capabilities |

**Example** (Python):
```python
class AdapterType(Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"
    PROCESSOR = "processor"
    TRANSPORT = "transport"

@dataclass
class AdapterMetadata:
    id: str
    name: str
    version: Version
    type: AdapterType
    capabilities: AdapterCapabilities
```

---

### 6. AdapterCapabilities

**Purpose**: Declare adapter capabilities for compatibility checking.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `data_types` | `set[DataType]` | Yes | Supported data types | Non-empty set |
| `ordering_guarantee` | `bool` | Yes | Maintains temporal order | Boolean |
| `lossy_allowed` | `bool` | Yes | Can drop data | Boolean |
| `throughput_hint` | `int` | No | Estimated events/second | Positive integer |

**Example** (Python):
```python
class DataType(Enum):
    DATASTREAM = "datastream"
    EVENT = "event"

@dataclass
class AdapterCapabilities:
    data_types: set[DataType]
    ordering_guarantee: bool
    lossy_allowed: bool
    throughput_hint: int | None = None
```

---

## Transport Layer Types

### 7. RoutingInfo (Flexible Addressing)

**Purpose**: Transport-agnostic routing abstraction.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `destination` | `str` | Yes | Transport-specific destination | Non-empty string |
| `properties` | `dict[str, Any]` | No | Transport-specific metadata | Key-value pairs |

**Transport-specific interpretations**:
- **Memory**: destination = topic name (string)
- **IPC**: destination = socket path (e.g., "/var/run/ma114tsdb/sensor-data")
- **MQTT**: destination = topic hierarchy (e.g., "sensors/temperature/room1")
- **Kombu**: destination = exchange + routing key (e.g., "ma114tsdb:sensor.temp.room1")

**Example** (Python):
```python
@dataclass
class RoutingInfo:
    destination: str
    properties: dict[str, Any] = field(default_factory=dict)
```

---

### 8. SubscriptionPattern (Pattern Matching)

**Purpose**: Transport-agnostic subscription filtering.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `pattern` | `str` | Yes | Transport-specific pattern | Non-empty string |
| `properties` | `dict[str, Any]` | No | Transport-specific options | Key-value pairs |

**Transport-specific interpretations**:
- **Memory**: pattern = exact topic name or "*" wildcard
- **IPC**: pattern = socket path glob (e.g., "/var/run/ma114tsdb/*.sock")
- **MQTT**: pattern = topic wildcards (+ single-level, # multi-level)
- **Kombu**: pattern = routing key pattern (e.g., "sensor.*.room1")

**Example** (Python):
```python
@dataclass
class SubscriptionPattern:
    pattern: str
    properties: dict[str, Any] = field(default_factory=dict)
```

---

## Configuration Types

### 9. PipelineConfig

**Purpose**: Declarative pipeline definition.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `pipeline_id` | `str` | Yes | Unique pipeline identifier | Non-empty string |
| `name` | `str` | Yes | Human-readable name | Non-empty string |
| `transport` | `TransportConfig` | Yes | Transport configuration | Valid transport config |
| `sources` | `list[str]` | Yes | Producer adapter IDs | Non-empty list |
| `processors` | `list[str]` | No | Processor adapter IDs (ordered) | Ordered list |
| `sinks` | `list[str]` | Yes | Consumer adapter IDs | Non-empty list |
| `config` | `PipelineRuntimeConfig` | Yes | Runtime configuration | Valid runtime config |

**Example** (Python):
```python
@dataclass
class PipelineConfig:
    pipeline_id: str
    name: str
    transport: TransportConfig
    sources: list[str]
    sinks: list[str]
    config: PipelineRuntimeConfig
    processors: list[str] = field(default_factory=list)
```

---

### 10. PipelineRuntimeConfig

**Purpose**: Pipeline runtime behavior configuration.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `parallelism` | `int` | No | Concurrent consumer count | Positive integer, default=1 |
| `buffer_size` | `int` | No | Transport buffer capacity | Positive integer, default=1000 |
| `time_window_minutes` | `int` | No | DataStream prioritization window | Positive integer, default=5 (1-5 minutes) |
| `error_policy` | `ErrorPolicy` | No | Error handling strategy | Valid policy, default=CONTINUE |
| `dlq` | `DLQConfig \| None` | No | Dead Letter Queue config | Valid DLQ config if present |

**Example** (Python):
```python
class ErrorPolicy(Enum):
    CONTINUE = "continue"  # Skip failed data, continue processing
    DLQ = "dlq"            # Send failed data to DLQ
    HALT = "halt"          # Stop pipeline on error

@dataclass
class PipelineRuntimeConfig:
    parallelism: int = 1
    buffer_size: int = 1000
    time_window_minutes: int = 5
    error_policy: ErrorPolicy = ErrorPolicy.CONTINUE
    dlq: DLQConfig | None = None
```

---

### 11. DLQConfig (Dead Letter Queue)

**Purpose**: Failed data preservation configuration.

**Fields**:
| Field | Type | Required | Description | Validation |
|-------|------|----------|-------------|------------|
| `enabled` | `bool` | Yes | Enable DLQ | Boolean |
| `storage` | `str` | Yes | Storage backend type | One of: file, s3, database |
| `retention` | `str` | Yes | Retention duration | ISO 8601 duration (e.g., "P7D" = 7 days) |
| `retry_policy` | `RetryPolicy \| None` | No | Optional automatic retry | Valid retry policy |

**Example** (Python):
```python
@dataclass
class DLQConfig:
    enabled: bool
    storage: str
    retention: str  # ISO 8601 duration
    retry_policy: RetryPolicy | None = None
```

---

## Relationships

### Entity Relationship Diagram

```
┌─────────────────┐
│  DataStream     │
│  - stream_id    │◄───────┐
│  - data_points  │        │ optional reference
│  - trace_context│        │
│  - version      │        │
└─────────────────┘        │
                           │
┌─────────────────┐        │
│  Event          │        │
│  - event_id     │────────┘
│  - stream_id?   │
│  - trace_context│
│  - version      │
└─────────────────┘
         │
         │ carries
         ▼
┌─────────────────┐
│  TraceContext   │
│  - trace_id     │
│  - span_id      │
│  - parent_span? │
└─────────────────┘

┌─────────────────┐
│  Pipeline       │
│  - pipeline_id  │
│  - sources[]    │──┐
│  - processors[] │  │ references
│  - sinks[]      │  │
│  - transport    │  │
└─────────────────┘  │
         │           │
         │ uses      │
         ▼           ▼
┌─────────────────┐ ┌─────────────────┐
│  Transport      │ │  Adapter        │
│  - type         │ │  - id           │
│  - config       │ │  - metadata     │
└─────────────────┘ │  - capabilities │
                    └─────────────────┘
```

### Key relationships:
- **Event → DataStream**: Optional reference via `stream_id` (not enforced)
- **DataStream/Event → TraceContext**: Mandatory 1:1 composition
- **DataStream/Event → Version**: Mandatory 1:1 composition
- **Pipeline → Adapter**: Many-to-many (sources, processors, sinks)
- **Pipeline → Transport**: Many-to-one

---

## State Transitions

### Adapter Lifecycle States

```
    ┌──────┐
    │ INIT │
    └──┬───┘
       │ start()
       ▼
    ┌─────────┐
    │ RUNNING │◄────────┐
    └──┬──────┘         │ retry success
       │                │
       │ error          │
       ▼                │
    ┌───────┐           │
    │ ERROR │───────────┘
    └──┬────┘
       │ 3 failures
       ▼
    ┌────────┐
    │ FAILED │ (manual intervention)
    └────────┘
       │
       │ stop()
       ▼
    ┌─────────┐
    │ STOPPED │
    └─────────┘
```

**Transition rules**:
- INIT → RUNNING: `start()` succeeds
- RUNNING → ERROR: Exception during execution
- ERROR → RUNNING: Retry succeeds (max 3 attempts with exponential backoff)
- ERROR → FAILED: 3 retry attempts exhausted
- Any state → STOPPED: `stop()` called (graceful shutdown)

---

## Validation Rules

### Cross-cutting validation:
1. **Timestamps**: All timestamps must be timezone-aware with sub-second precision
2. **UUIDs**: All UUIDs must be version 4 (random)
3. **Versions**: major >= 0, minor >= 0
4. **Adapter IDs**: Must match pattern `[a-z0-9-_]+` (lowercase, numbers, hyphens, underscores)
5. **Time windows**: `timestamp_end >= timestamp_start` for DataStreams
6. **Version compatibility**: Check at deserialization using `Version.is_compatible()`
7. **Non-empty collections**: `data_points`, `sources`, `sinks` must have at least one element

### Pydantic model examples:

```python
from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone
from uuid import UUID, uuid4

class DataStreamModel(BaseModel):
    stream_id: str = Field(min_length=1)
    source: str = Field(pattern=r"^[a-z0-9-_]+$")
    timestamp_start: datetime
    timestamp_end: datetime
    data_points: list[Any] = Field(min_length=1)
    trace_context: TraceContext
    version: Version
    interval: timedelta | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp_start", "timestamp_end")
    def validate_timezone_aware(cls, v):
        if v.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware")
        return v

    @field_validator("timestamp_end")
    def validate_time_order(cls, v, info):
        if "timestamp_start" in info.data and v < info.data["timestamp_start"]:
            raise ValueError("timestamp_end must be >= timestamp_start")
        return v
```

---

## Wire Format (MessagePack Envelope)

All DataStreams and Events are serialized with this envelope structure:

```python
envelope = {
    "type": "datastream" | "event",  # Message discriminator
    "version": {"major": 1, "minor": 0},
    "trace_context": {
        "trace_id": "uuid-string",
        "span_id": "uuid-string",
        "parent_span": "uuid-string" | None
    },
    "payload": {
        # DataStream or Event fields here
        # Serialized as MessagePack binary
    }
}

# Serialize
wire_bytes = msgpack.packb(envelope, datetime=True, use_bin_type=True)

# Deserialize with version check
envelope = msgpack.unpackb(wire_bytes, timestamp=3)
version = Version(**envelope["version"])
if not CURRENT_VERSION.is_compatible(version):
    raise VersionMismatchError(f"{version} incompatible with {CURRENT_VERSION}")
```

---

## Summary

This data model supports:
- ✅ **Two data types** with distinct guarantees (FR-001)
- ✅ **Time-first design** (FR-002, FR-003: sub-second timestamps)
- ✅ **Distributed tracing** (FR-004: lightweight TraceContext)
- ✅ **Version evolution** (FR-005, FR-045-050: semantic versioning + N-1 compatibility)
- ✅ **Adapter uniformity** (FR-007, FR-014: metadata-driven capabilities)
- ✅ **Transport abstraction** (FR-019: flexible routing/subscription)
- ✅ **Configuration-driven pipelines** (FR-051, FR-056: file-based config)
- ✅ **Error handling** (FR-035-044: DLQ, retry policies)

All entities validated with pydantic, serialized with MessagePack for cross-language compatibility.
