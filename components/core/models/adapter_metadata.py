"""
Adapter Metadata Models for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Self-describing adapters with capability declaration.
"""

from dataclasses import dataclass
from enum import Enum

from ..versioning import Version


class DataType(Enum):
    """Supported data types per FR-001."""

    DATASTREAM = "datastream"
    EVENT = "event"


class AdapterType(Enum):
    """Adapter roles per FR-007."""

    PRODUCER = "producer"
    CONSUMER = "consumer"
    PROCESSOR = "processor"
    TRANSPORT = "transport"


@dataclass
class AdapterCapabilities:
    """
    Adapter capability declaration.

    Per FR-014: All adapters MUST declare metadata including capabilities.

    Attributes:
        data_types: Supported data types (non-empty set)
        ordering_guarantee: Maintains temporal order
        lossy_allowed: Can drop data (False for Events)
        throughput_hint: Estimated events/second (optional)

    Examples:
        >>> # High-throughput producer, lossy allowed
        >>> AdapterCapabilities(
        ...     data_types={DataType.DATASTREAM},
        ...     ordering_guarantee=False,
        ...     lossy_allowed=True,
        ...     throughput_hint=10000
        ... )

        >>> # Critical event consumer, no loss
        >>> AdapterCapabilities(
        ...     data_types={DataType.EVENT},
        ...     ordering_guarantee=True,
        ...     lossy_allowed=False
        ... )
    """

    data_types: set[DataType]
    ordering_guarantee: bool
    lossy_allowed: bool
    throughput_hint: int | None = None

    def __post_init__(self) -> None:
        """Validate capabilities."""
        if not self.data_types:
            raise ValueError("data_types must be non-empty")
        if self.throughput_hint is not None and self.throughput_hint <= 0:
            raise ValueError(f"throughput_hint must be positive, got {self.throughput_hint}")


@dataclass
class AdapterMetadata:
    """
    Self-describing adapter metadata.

    Per FR-014: All adapters MUST declare metadata for discovery and capability negotiation.

    Attributes:
        id: Unique adapter identifier (lowercase, numbers, hyphens, underscores)
        name: Human-readable name
        version: Adapter version
        type: Adapter role
        capabilities: Adapter capabilities

    Example:
        >>> AdapterMetadata(
        ...     id="temp-sensor-01",
        ...     name="Temperature Sensor HTTP Poller",
        ...     version=Version(1, 0),
        ...     type=AdapterType.PRODUCER,
        ...     capabilities=AdapterCapabilities(
        ...         data_types={DataType.DATASTREAM},
        ...         ordering_guarantee=False,
        ...         lossy_allowed=True,
        ...         throughput_hint=100
        ...     )
        ... )
    """

    id: str
    name: str
    version: Version
    type: AdapterType
    capabilities: AdapterCapabilities

    def __post_init__(self) -> None:
        """Validate metadata."""
        if not self.id:
            raise ValueError("id must be non-empty")
        if not self.name:
            raise ValueError("name must be non-empty")
        # Validate adapter ID format: [a-z0-9-_]+
        import re
        if not re.match(r"^[a-z0-9-_]+$", self.id):
            raise ValueError(
                f"id must match [a-z0-9-_]+ pattern, got '{self.id}'"
            )


# State and result types for adapter lifecycle

class AdapterState(Enum):
    """
    Adapter lifecycle states per FR-009.

    State transitions:
        INIT → RUNNING → STOPPED
        ERROR possible from any state
        FAILED after 3 retry attempts
    """

    INIT = "init"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    FAILED = "failed"


@dataclass
class Result:
    """
    Operation result with success/failure.

    Used by adapter lifecycle methods (init, start, stop, consume).

    Attributes:
        success: Operation succeeded
        message: Optional human-readable message
        error: Optional exception if failed
    """

    success: bool
    message: str | None = None
    error: Exception | None = None

    @classmethod
    def ok(cls, message: str | None = None) -> "Result":
        """Create successful result."""
        return cls(success=True, message=message)

    @classmethod
    def fail(cls, message: str, error: Exception | None = None) -> "Result":
        """Create failed result."""
        return cls(success=False, message=message, error=error)


@dataclass
class HealthStatus:
    """
    Adapter health status per FR-015.

    Attributes:
        status: "healthy" | "degraded" | "unhealthy"
        uptime: Seconds since adapter started
        state: Current adapter state
        details: Optional diagnostic information
    """

    status: str  # "healthy" | "degraded" | "unhealthy"
    uptime: float  # seconds
    state: AdapterState
    details: dict[str, any] | None = None

    def __post_init__(self) -> None:
        """Validate health status."""
        if self.status not in ("healthy", "degraded", "unhealthy"):
            raise ValueError(
                f"status must be healthy/degraded/unhealthy, got '{self.status}'"
            )
        if self.uptime < 0:
            raise ValueError(f"uptime must be non-negative, got {self.uptime}")
