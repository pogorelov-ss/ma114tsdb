# Feature Specification: ma114tsdb Core System

**Feature Branch**: `001-what-we-re`
**Created**: 2025-10-04
**Status**: Draft
**Input**: User description: "A time-series data acquisition system with modular architecture that handles high-volume sensor data and critical events through a pluggable adapter system."

## Execution Flow (main)
```
1. Parse user description from Input
   → If empty: ERROR "No feature description provided"
2. Extract key concepts from description
   → Identify: actors, actions, data, constraints
3. For each unclear aspect:
   → Mark with [NEEDS CLARIFICATION: specific question]
4. Fill User Scenarios & Testing section
   → If no clear user flow: ERROR "Cannot determine user scenarios"
5. Generate Functional Requirements
   → Each requirement must be testable
   → Mark ambiguous requirements
6. Identify Key Entities (if data involved)
7. Run Review Checklist
   → If any [NEEDS CLARIFICATION]: WARN "Spec has uncertainties"
   → If implementation details found: ERROR "Remove tech details"
8. Return: SUCCESS (spec ready for planning)
```

---

## ⚡ Quick Guidelines
- ✅ Focus on WHAT users need and WHY
- ❌ Avoid HOW to implement (no tech stack, APIs, code structure)
- 👥 Written for business stakeholders, not developers

### Section Requirements
- **Mandatory sections**: Must be completed for every feature
- **Optional sections**: Include only when relevant to the feature
- When a section doesn't apply, remove it entirely (don't leave as "N/A")

---

## Clarifications

### Session 2025-10-04

- Q: What is the target throughput for DataStream ingestion per pipeline? → A: Variable range from low to medium (100-10,000 streams/second). Adapters batch multiple timestamp-value pairs per packet; actual throughput depends on packet send frequency.

- Q: How should the system handle DataStreams/Events with invalid timestamps (future dates or missing)? → A: Configurable per-adapter policy. Default behavior: Events use FIFO order; DataStreams prioritize latest data within time window first, then historical data FIFO after connection recovery. System designed to handle out-of-order data.

- Q: What is the default time window duration for DataStream latest-first prioritization? → A: Configurable per-pipeline with recommended default of 1-5 minutes (real-time focus).

- Q: How are pipelines configured and managed? → A: Hybrid approach (file-based with optional runtime API). For v1.0 implementation: static configuration files read at startup only. Runtime API deferred to future versions.

- Q: What security model applies to pipeline and adapter operations in v1.0? → A: Out-of-scope for v1.0. Adapters deployed by external services. Transport layer will use unique per-adapter tokens provided at deployment stage. Full security model deferred to future versions.

---

## User Scenarios & Testing *(mandatory)*

### Primary User Story

As a system operator, I need to collect time-series data from multiple sensors and devices, where some data is high-volume and can tolerate loss (metrics), while other data is critical and must never be lost (events). The system must allow me to configure different data sources, apply transformations, and route data to appropriate storage or alerting systems, all while maintaining complete visibility into the pipeline's operation.

### Acceptance Scenarios

1. **Given** a temperature sensor producing readings every second, **When** the sensor publishes data to the system, **Then** the readings are collected as DataStreams with timestamps and forwarded to storage, with acceptable loss of some readings during network issues

2. **Given** a device that emits critical state change events, **When** the device publishes an event, **Then** the event is durably persisted and delivered at-least-once to all configured consumers, even if delivery is out of temporal order

3. **Given** a pipeline with a processor that filters anomalous readings, **When** data flows through the pipeline, **Then** the processor transforms the data in-flight and only valid data reaches consumers

4. **Given** multiple concurrent pipelines operating independently, **When** one pipeline experiences a failure, **Then** other pipelines continue operating without disruption

5. **Given** an adapter fails to process incoming data, **When** a Dead Letter Queue is configured, **Then** the failed data is preserved for later inspection and processing continues

6. **Given** the system is operating with multiple adapters, **When** I query observability data, **Then** I can see metrics, structured logs, and distributed traces for all components

### Edge Cases

- **Transport connectivity loss**: System buffers latest DataStreams within configured time window, sends fresh data first upon reconnection, then backfills historical data FIFO. Events are queued and delivered FIFO with at-least-once guarantee.
- **Invalid timestamps**: Configurable per-adapter validation policy (reject vs. fallback to system time). Default accepts future timestamps; missing timestamps trigger adapter-specific handling.
- **Repeated health check failures**: After 3 failed restart attempts (exponential backoff: 1s, 5s, 30s), adapter marked failed and requires manual intervention while other adapters continue.
- **Version mismatches**: System supports N-1 backward compatibility; consumers handle older formats, ignore unknown fields for forward compatibility.
- **Backpressure**: Transport layer handles flow control; DataStreams may be dropped (lossy), Events are buffered and persisted (durable).
- **Duplicate event delivery**: Consumers responsible for idempotent processing (at-least-once delivery semantics).

## Requirements *(mandatory)*

### Functional Requirements

**Data Management**
- **FR-001**: System MUST support two distinct data types: DataStream (lossy, high-volume) and Event (durable, critical)
- **FR-002**: All DataStreams MUST include timestamp-start, timestamp-end, and sub-second timestamp precision
- **FR-003**: All Events MUST include unique event-id (UUID), timestamp with sub-second precision, and event-type classification
- **FR-004**: Both data types MUST carry TraceContext (trace-id, span-id, optional parent-span) for distributed tracing
- **FR-005**: Both data types MUST carry version information (major, minor) for format evolution
- **FR-006**: Events MAY optionally reference a DataStream via stream-id (system does not enforce referential integrity)

**Adapter System**
- **FR-007**: System MUST support four adapter roles: Producer, Consumer, Processor, and Transport
- **FR-008**: All adapters MUST implement lifecycle methods: init, start, stop, health
- **FR-009**: All adapters MUST transition through defined states: init → running → stopped, with error state possible at any time
- **FR-010**: Producer adapters MUST generate and publish DataStreams or Events
- **FR-011**: Consumer adapters MUST receive and process DataStreams or Events
- **FR-012**: Processor adapters MUST transform data in-flight and MAY filter data by returning null
- **FR-013**: Transport adapters MUST provide publish, subscribe, ack, and declare operations
- **FR-014**: All adapters MUST declare metadata including id, name, version, type, and capabilities
- **FR-015**: Adapters MAY emit health status as regular Events with health-check event-type

**Transport Layer**
- **FR-016**: Every pipeline MUST specify exactly one transport
- **FR-017**: System MUST support multiple transport types: Memory (in-process), IPC (Unix sockets), MQTT (IoT protocol), and Kombu (Python interop)
- **FR-018**: Transport layer MUST be language-agnostic to enable cross-language adapter compatibility
- **FR-019**: Transport MUST support flexible routing via destination strings and pattern-based subscriptions
- **FR-020**: Each transport implementation MUST document destination interpretation, pattern matching, supported properties, and delivery guarantees

**Delivery Guarantees**
- **FR-021**: DataStream delivery MUST be lossy-tolerant (some data loss acceptable) with best-effort, transport-dependent delivery
- **FR-022**: Event delivery MUST be durable (never lost) with at-least-once delivery guarantee
- **FR-023**: System MUST NOT guarantee timestamp-ordered delivery for either data type
- **FR-024**: System MUST support latest-first delivery prioritization for DataStreams within configured time windows
- **FR-025**: System MUST support out-of-order delivery, backfill after network recovery, and adaptive buffering
- **FR-026**: Events MUST be delivered in FIFO order (First-In-First-Out) by default
- **FR-027**: DataStreams MUST prioritize latest data within time window first, then historical data FIFO after connection recovery
- **FR-028**: Adapters MUST support configurable timestamp validation policies (reject invalid vs. fallback to system time)
- **FR-029**: System MUST accept future timestamps by default; missing timestamp handling is adapter-configurable

**Observability**
- **FR-030**: Every adapter MUST emit telemetry metrics: events-processed, events-failed, processing-time, queue-depth, last-event-time, adapter-state
- **FR-031**: Every adapter MUST emit structured logs with timestamp, level, adapter-id, message, context, and optional error information
- **FR-032**: System MUST propagate trace IDs through the entire pipeline with minimal overhead
- **FR-033**: System MUST provide default implementations for metric collectors, log formatters, and trace propagators
- **FR-034**: System-level metrics MUST aggregate data across all adapters

**Error Handling**
- **FR-035**: System MUST follow failure philosophy: Fail → Retry → Isolate → Continue
- **FR-036**: Failed adapters MUST be restarted with exponential backoff (1s, 5s, 30s delays) for up to 3 attempts
- **FR-037**: After 3 failed restart attempts, adapters MUST be marked as failed and require manual intervention
- **FR-038**: Failed adapters MUST emit health events with degraded or unhealthy status
- **FR-039**: System MUST continue operating other adapters when individual components fail (isolation)
- **FR-040**: System MUST never block pipeline processing due to bad data
- **FR-041**: When Dead Letter Queue is configured, failed data MUST be preserved for debugging and potential reprocessing
- **FR-042**: When Dead Letter Queue is not configured, failed data MUST be skipped with error logging
- **FR-043**: System MUST support graceful shutdown with buffer flushing and clean exit
- **FR-044**: On restart, system MUST resume from last known good state when supported by transport

**Versioning**
- **FR-045**: All contracts MUST use semantic versioning (major.minor.patch)
- **FR-046**: System MUST support N-1 backward compatibility (one version back)
- **FR-047**: Consumers MUST handle older data format versions
- **FR-048**: Consumers SHOULD ignore unknown fields for forward compatibility
- **FR-049**: System MUST support coexistence of version N and N-1 during migration
- **FR-050**: Deprecation warnings MUST precede breaking changes

**Pipeline Management**
- **FR-051**: Pipelines MUST define: pipeline-id, name, transport config, sources, processors, sinks, and configuration
- **FR-052**: Processors MUST execute in sequential order within a pipeline
- **FR-053**: Consumers MAY execute in parallel (unordered) within a pipeline
- **FR-054**: Transport MUST handle backpressure within the pipeline
- **FR-055**: Pipeline failures MUST NOT affect other pipelines (isolation)
- **FR-056**: System MUST load pipeline configurations from static files at startup (v1.0)
- **FR-057**: Pipeline configuration changes MUST require system restart in v1.0 (runtime API deferred to future versions)

**Performance & Scalability**
- **FR-058**: System MUST support variable throughput targets per pipeline ranging from 100 to 10,000 DataStreams per second
- **FR-059**: DataStream packets MUST support batching multiple timestamp-value pairs to optimize throughput based on configurable send frequency
- **FR-060**: Pipelines MUST support configurable time window duration for DataStream prioritization with recommended default of 1-5 minutes
- **FR-061**: Time window configuration MUST affect buffer sizing and connection recovery behavior

**Security (v1.0 Scope)**
- **FR-062**: Transport layer MUST support unique per-adapter authentication tokens
- **FR-063**: Adapter tokens MUST be provided during deployment stage by external services
- **FR-064**: System assumes trusted environment; comprehensive authentication/authorization model deferred to future versions

### Out-of-Scope (v1.0)

The following are explicitly out-of-scope for v1.0 and deferred to future versions:

- **Runtime configuration API**: Pipeline management via REST/GraphQL API (v1.0 uses static files only)
- **Role-based access control**: User/operator permission management
- **Authentication system**: Login, session management, user accounts
- **Authorization policies**: Fine-grained access control for observability data
- **Data encryption at rest**: Storage-level encryption (transport-level security only via adapter tokens)
- **Multi-tenancy**: Isolation between different organizations or user groups
- **Audit logging**: Comprehensive security event tracking beyond observability logs
- **Advanced security**: Field-level encryption, key rotation, secrets management

### Key Entities

- **DataStream**: High-volume, compact time-series data type containing stream-id, source adapter, timestamp range, data points, trace context, metadata, and version. Optimized for throughput with lossy delivery acceptable.

- **Event**: Critical, durable occurrence containing unique event-id, timestamp, source adapter, event-type, flexible data payload, trace context, metadata, and version. Guaranteed delivery with at-least-once semantics.

- **Adapter**: Pluggable component implementing lifecycle methods (init, start, stop, health) with one of four roles: Producer (generates data), Consumer (processes data), Processor (transforms data), or Transport (moves data between adapters).

- **Pipeline**: Configured data flow defining sources (producers), optional processors (ordered transformations), sinks (consumers), transport, and operational configuration (parallelism, buffer size, error policy, optional DLQ).

- **TraceContext**: Lightweight correlation structure containing trace-id, span-id, and optional parent-span for distributed tracing without full span overhead.

- **AdapterMetadata**: Self-describing information for each adapter including id, name, version, type, and capabilities (supported data types, ordering guarantees, lossy tolerance, throughput hints).

- **RoutingInfo**: Flexible addressing abstraction for transport layer containing destination string and transport-specific properties for message routing.

- **SubscriptionPattern**: Pattern matching abstraction for transport subscriptions containing pattern string and transport-specific options for filtering incoming data.

- **DLQConfig**: Dead Letter Queue configuration specifying enabled status, storage backend, retention duration, and optional retry policy for failed data.

---

## Review & Acceptance Checklist
*GATE: Automated checks run during main() execution*

### Content Quality
- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

### Requirement Completeness
- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

---

## Execution Status
*Updated by main() during processing*

- [x] User description parsed
- [x] Key concepts extracted
- [x] Ambiguities marked
- [x] User scenarios defined
- [x] Requirements generated
- [x] Entities identified
- [x] Review checklist passed

---
