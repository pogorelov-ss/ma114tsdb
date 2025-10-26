"""
Adapter Interface Contracts for ma114tsdb

These interfaces define the core contracts that all adapters must implement.
Based on constitutional principle: "Everything is an Adapter"

Version: 1.0.0
Date: 2025-10-04
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, AsyncIterator, Protocol
from uuid import UUID


# ============================================================================
# Core Data Types
# ============================================================================

@dataclass
class Version:
    """Semantic version for data format evolution."""
    major: int
    minor: int

    def is_compatible(self, other: 'Version') -> bool:
        """Check N-1 backward compatibility."""
        if self.major == other.major:
            return True
        if self.major == other.major + 1:
            return True
        return False


@dataclass
class TraceContext:
    """Lightweight distributed tracing correlation."""
    trace_id: UUID
    span_id: UUID
    parent_span: UUID | None = None


@dataclass
class DataStream:
    """High-volume, lossy-tolerant time-series data."""
    stream_id: str
    source: str
    timestamp_start: datetime
    timestamp_end: datetime
    data_points: list[Any]
    trace_context: TraceContext
    version: Version
    interval: float | None = None  # seconds
    metadata: dict[str, Any] | None = None


@dataclass
class Event:
    """Critical, durable occurrence that must never be lost."""
    event_id: UUID
    timestamp: datetime
    source: str
    event_type: str
    data: dict[str, Any]
    trace_context: TraceContext
    version: Version
    stream_id: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass
class RoutingInfo:
    """Transport-agnostic routing destination."""
    destination: str
    properties: dict[str, Any] | None = None


@dataclass
class SubscriptionPattern:
    """Transport-agnostic subscription pattern."""
    pattern: str
    properties: dict[str, Any] | None = None


# ============================================================================
# Adapter States & Results
# ============================================================================

class AdapterState(Enum):
    """Adapter lifecycle states."""
    INIT = "init"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    FAILED = "failed"


@dataclass
class Result:
    """Operation result with success/failure."""
    success: bool
    message: str | None = None
    error: Exception | None = None


@dataclass
class HealthStatus:
    """Adapter health status."""
    status: str  # "healthy" | "degraded" | "unhealthy"
    uptime: float  # seconds
    state: AdapterState
    details: dict[str, Any] | None = None


class DataType(Enum):
    """Supported data types."""
    DATASTREAM = "datastream"
    EVENT = "event"


class AdapterType(Enum):
    """Adapter roles."""
    PRODUCER = "producer"
    CONSUMER = "consumer"
    PROCESSOR = "processor"
    TRANSPORT = "transport"


@dataclass
class AdapterCapabilities:
    """Adapter capability declaration."""
    data_types: set[DataType]
    ordering_guarantee: bool
    lossy_allowed: bool
    throughput_hint: int | None = None


@dataclass
class AdapterMetadata:
    """Self-describing adapter metadata."""
    id: str
    name: str
    version: Version
    type: AdapterType
    capabilities: AdapterCapabilities


# ============================================================================
# Core Adapter Interface (Base)
# ============================================================================

class IAdapter(Protocol):
    """
    Base adapter interface - ALL adapters must implement these methods.

    Lifecycle: init() → start() → [running] → stop()
    Error state can occur at any time.

    Constitutional requirements:
    - FR-008: All adapters MUST implement lifecycle methods
    - FR-009: State transitions: init → running → stopped, error possible
    - FR-014: All adapters MUST declare metadata
    """

    @abstractmethod
    async def init(self, config: dict[str, Any]) -> Result:
        """
        Initialize adapter with configuration.

        Args:
            config: Adapter-specific configuration dictionary

        Returns:
            Result indicating success/failure

        Postconditions:
            - State transitions to INIT if successful
            - Resources allocated but not active
            - Config validated and stored
        """
        ...

    @abstractmethod
    async def start(self) -> Result:
        """
        Start adapter operation.

        Preconditions:
            - init() must have been called successfully

        Returns:
            Result indicating success/failure

        Postconditions:
            - State transitions to RUNNING if successful
            - Adapter actively processing data
            - Resources fully initialized
        """
        ...

    @abstractmethod
    async def stop(self) -> Result:
        """
        Gracefully stop adapter.

        Returns:
            Result indicating success/failure

        Postconditions:
            - State transitions to STOPPED
            - In-flight operations completed
            - Resources released
            - Buffers flushed (FR-043)
        """
        ...

    @abstractmethod
    async def health(self) -> HealthStatus:
        """
        Get current health status.

        Returns:
            HealthStatus with current state and diagnostics

        Constitutional requirement:
            - FR-015: Adapters MAY emit health status as Events
            - FR-030: Must include adapter-state metric
        """
        ...

    @property
    @abstractmethod
    def metadata(self) -> AdapterMetadata:
        """
        Adapter metadata for discovery and capability negotiation.

        Constitutional requirement:
            - FR-014: All adapters MUST declare metadata
        """
        ...


# ============================================================================
# Producer Adapter Interface
# ============================================================================

class IProducer(IAdapter, Protocol):
    """
    Producer adapter interface - generates and publishes data.

    Constitutional requirements:
    - FR-010: Producer adapters MUST generate and publish DataStreams or Events
    - FR-004: All data MUST carry TraceContext
    - FR-005: All data MUST carry version information
    """

    @abstractmethod
    async def produce(self) -> AsyncIterator[DataStream | Event]:
        """
        Generate stream of DataStreams or Events.

        Preconditions:
            - Adapter in RUNNING state

        Yields:
            DataStream or Event instances with:
            - Valid timestamps (sub-second precision, FR-002, FR-003)
            - TraceContext attached (FR-004)
            - Version information (FR-005)
            - Source set to adapter ID

        Constitutional requirements:
            - FR-010: Generate and publish DataStreams or Events
            - FR-026: Events delivered in FIFO order
            - FR-027: DataStreams prioritize latest data in time window

        Example:
            async for data in producer.produce():
                if isinstance(data, DataStream):
                    # Handle high-volume stream
                elif isinstance(data, Event):
                    # Handle critical event
        """
        ...


# ============================================================================
# Consumer Adapter Interface
# ============================================================================

class IConsumer(IAdapter, Protocol):
    """
    Consumer adapter interface - receives and processes data.

    Constitutional requirements:
    - FR-011: Consumer adapters MUST receive and process DataStreams or Events
    - FR-040: System MUST never block pipeline on bad data
    """

    @abstractmethod
    async def consume(self, data: DataStream | Event) -> Result:
        """
        Process incoming DataStream or Event.

        Args:
            data: DataStream or Event to process

        Returns:
            Result indicating success/failure

        Preconditions:
            - Adapter in RUNNING state
            - data has valid TraceContext and version

        Postconditions:
            - Data processed or error logged
            - If DLQ configured (FR-041): failed data sent to DLQ
            - If no DLQ (FR-042): failed data skipped with error log
            - Pipeline never blocked (FR-040)

        Constitutional requirements:
            - FR-011: Receive and process DataStreams or Events
            - FR-036: Never block pipeline on bad data
            - FR-047: Handle older data format versions (N-1 compatibility)

        Example:
            result = await consumer.consume(event)
            if not result.success:
                logger.error(f"Failed to consume: {result.message}")
                # Error logged, processing continues
        """
        ...


# ============================================================================
# Processor Adapter Interface
# ============================================================================

class IProcessor(IAdapter, Protocol):
    """
    Processor adapter interface - transforms data in-flight.

    Constitutional requirements:
    - FR-012: Processor adapters MUST transform data and MAY filter by returning None
    - FR-052: Processors execute in sequential order within pipeline
    """

    @abstractmethod
    async def process(self, data: DataStream | Event) -> DataStream | Event | None:
        """
        Transform data in-flight, optionally filtering.

        Args:
            data: Input DataStream or Event

        Returns:
            - Transformed DataStream or Event, OR
            - None to filter out (drop) this data

        Preconditions:
            - Adapter in RUNNING state

        Postconditions:
            - If not None: data transformed with new trace span
            - If None: data filtered out
            - Original trace_id preserved (FR-032)

        Constitutional requirements:
            - FR-012: Transform data or filter by returning None
            - FR-032: Propagate trace IDs with minimal overhead
            - FR-052: Sequential execution order maintained

        Example:
            async def process(self, data):
                # Filter anomalous data
                if is_anomalous(data):
                    return None

                # Transform and create new span
                return DataStream(
                    ...
                    trace_context=data.trace_context.create_child_span(),
                    ...
                )
        """
        ...


# ============================================================================
# Transport Adapter Interface
# ============================================================================

class Receipt:
    """Acknowledgment receipt for message delivery."""
    receipt_id: str
    timestamp: datetime


class ResourceSpec:
    """Transport resource declaration (e.g., topic, queue, exchange)."""
    name: str
    type: str  # "topic" | "queue" | "exchange" | etc.
    properties: dict[str, Any] | None = None


class ITransport(IAdapter, Protocol):
    """
    Transport adapter interface - moves data between adapters.

    Constitutional requirements:
    - FR-013: Transport adapters MUST provide publish, subscribe, ack, declare
    - FR-016: Every pipeline MUST specify exactly one transport
    - FR-018: Transport layer MUST be language-agnostic
    - FR-050: Transport MUST handle backpressure
    """

    @abstractmethod
    async def publish(self, data: DataStream | Event, routing: RoutingInfo) -> Result:
        """
        Publish data to transport destination.

        Args:
            data: DataStream or Event to publish
            routing: Transport-specific routing information

        Returns:
            Result indicating delivery success/failure

        Preconditions:
            - Adapter in RUNNING state
            - routing.destination valid for this transport

        Postconditions:
            - DataStream: Best-effort delivery (FR-021, lossy allowed)
            - Event: Durable delivery with at-least-once guarantee (FR-022)

        Constitutional requirements:
            - FR-013: Provide publish operation
            - FR-021: DataStream delivery lossy-tolerant
            - FR-022: Event delivery durable, never lost
            - FR-062: Support per-adapter authentication tokens

        Example:
            result = await transport.publish(
                event,
                RoutingInfo(destination="sensors/temperature/room1")
            )
        """
        ...

    @abstractmethod
    async def subscribe(self, pattern: SubscriptionPattern) -> AsyncIterator[DataStream | Event]:
        """
        Subscribe to data matching pattern.

        Args:
            pattern: Transport-specific subscription pattern

        Yields:
            DataStreams or Events matching subscription

        Preconditions:
            - Adapter in RUNNING state
            - pattern valid for this transport

        Postconditions:
            - Continuous stream of matching data
            - Backpressure handled by transport (FR-054)

        Constitutional requirements:
            - FR-013: Provide subscribe operation
            - FR-019: Support flexible pattern-based subscriptions
            - FR-054: Handle backpressure within pipeline

        Transport-specific patterns:
            - Memory: exact topic or "*" wildcard
            - IPC: socket path glob (e.g., "*.sock")
            - MQTT: topic wildcards (+ single, # multi-level)
            - Kombu: routing key patterns (e.g., "sensor.*.room1")

        Example:
            async for data in transport.subscribe(SubscriptionPattern("sensors/#")):
                # Process data from all sensor topics
        """
        ...

    @abstractmethod
    async def ack(self, receipt: Receipt) -> Result:
        """
        Acknowledge message delivery for durable transports.

        Args:
            receipt: Receipt from previous publish/subscribe operation

        Returns:
            Result indicating acknowledgment success/failure

        Constitutional requirements:
            - FR-013: Provide ack operation
            - FR-022: Enable at-least-once delivery for Events

        Example:
            # Consumer acknowledges after successful processing
            result = await consumer.consume(event)
            if result.success:
                await transport.ack(receipt)
        """
        ...

    @abstractmethod
    async def declare(self, resource: ResourceSpec) -> Result:
        """
        Declare transport resource (topic, queue, exchange, etc.).

        Args:
            resource: Resource specification

        Returns:
            Result indicating declaration success/failure

        Constitutional requirements:
            - FR-013: Provide declare operation for resource setup

        Example:
            await transport.declare(ResourceSpec(
                name="ma114tsdb",
                type="exchange",
                properties={"type": "topic", "durable": True}
            ))
        """
        ...


# ============================================================================
# Contract Testing Base Classes
# ============================================================================

class AdapterContractTest(ABC):
    """
    Base class for adapter contract tests.

    All adapter implementations must pass these tests.
    Constitutional requirement: FR-070 (contract tests for all adapters)
    """

    @abstractmethod
    async def test_init_success(self, adapter: IAdapter, valid_config: dict):
        """Test init() with valid configuration."""
        result = await adapter.init(valid_config)
        assert result.success, f"init() failed: {result.message}"
        assert adapter.metadata.id is not None

    @abstractmethod
    async def test_lifecycle_transitions(self, adapter: IAdapter, config: dict):
        """Test init → start → stop lifecycle."""
        await adapter.init(config)
        start_result = await adapter.start()
        assert start_result.success

        health = await adapter.health()
        assert health.state == AdapterState.RUNNING

        stop_result = await adapter.stop()
        assert stop_result.success

    @abstractmethod
    async def test_health_returns_valid_status(self, adapter: IAdapter):
        """Test health() returns valid HealthStatus."""
        health = await adapter.health()
        assert health.status in ["healthy", "degraded", "unhealthy"]
        assert health.uptime >= 0

    @abstractmethod
    async def test_metadata_includes_capabilities(self, adapter: IAdapter):
        """Test metadata declares valid capabilities."""
        metadata = adapter.metadata
        assert metadata.version.major >= 0
        assert metadata.version.minor >= 0
        assert len(metadata.capabilities.data_types) > 0


# ============================================================================
# Version Information
# ============================================================================

CONTRACT_VERSION = Version(major=1, minor=0)

__all__ = [
    # Data types
    "DataStream",
    "Event",
    "TraceContext",
    "Version",
    "RoutingInfo",
    "SubscriptionPattern",
    # States & results
    "AdapterState",
    "Result",
    "HealthStatus",
    "DataType",
    "AdapterType",
    "AdapterCapabilities",
    "AdapterMetadata",
    # Interfaces
    "IAdapter",
    "IProducer",
    "IConsumer",
    "IProcessor",
    "ITransport",
    # Transport types
    "Receipt",
    "ResourceSpec",
    # Testing
    "AdapterContractTest",
    # Version
    "CONTRACT_VERSION",
]
