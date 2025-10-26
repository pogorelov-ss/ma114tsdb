"""
Pipeline Configuration Models for ma114tsdb

Constitutional principle: IV. Programmed with Strict Contracts
Configuration models with validation.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ErrorPolicy(Enum):
    """
    Error handling strategy per FR-040-042.

    Attributes:
        CONTINUE: Skip failed data, continue processing (default)
        DLQ: Send failed data to Dead Letter Queue
        HALT: Stop pipeline on error (use with caution)
    """

    CONTINUE = "continue"
    DLQ = "dlq"
    HALT = "halt"


@dataclass
class DLQConfig:
    """
    Dead Letter Queue configuration per FR-041.

    Attributes:
        enabled: Enable DLQ
        storage: Storage backend type ("file" | "s3" | "database")
        retention: Retention duration (ISO 8601, e.g., "P7D" = 7 days)
        retry_policy: Optional automatic retry configuration

    Example:
        >>> DLQConfig(
        ...     enabled=True,
        ...     storage="file",
        ...     retention="P7D",
        ...     retry_policy=None
        ... )
    """

    enabled: bool
    storage: str
    retention: str  # ISO 8601 duration
    retry_policy: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validate DLQ config."""
        valid_storage = {"file", "s3", "database"}
        if self.storage not in valid_storage:
            raise ValueError(
                f"storage must be one of {valid_storage}, got '{self.storage}'"
            )
        if not self.retention:
            raise ValueError("retention must be non-empty ISO 8601 duration")


@dataclass
class PipelineRuntimeConfig:
    """
    Pipeline runtime behavior configuration per FR-051, FR-053, FR-054, FR-060.

    Attributes:
        parallelism: Concurrent consumer count (default=1, FR-053)
        buffer_size: Transport buffer capacity (default=1000, FR-054)
        time_window_minutes: DataStream prioritization window (default=5, FR-060)
        error_policy: Error handling strategy (default=CONTINUE)
        dlq: Dead Letter Queue config (optional, FR-041)

    Example:
        >>> PipelineRuntimeConfig(
        ...     parallelism=4,
        ...     buffer_size=5000,
        ...     time_window_minutes=5,
        ...     error_policy=ErrorPolicy.DLQ,
        ...     dlq=DLQConfig(enabled=True, storage="file", retention="P7D")
        ... )
    """

    parallelism: int = 1
    buffer_size: int = 1000
    time_window_minutes: int = 5
    error_policy: ErrorPolicy = ErrorPolicy.CONTINUE
    dlq: DLQConfig | None = None

    def __post_init__(self) -> None:
        """Validate runtime config."""
        if self.parallelism <= 0:
            raise ValueError(f"parallelism must be positive, got {self.parallelism}")
        if self.buffer_size <= 0:
            raise ValueError(f"buffer_size must be positive, got {self.buffer_size}")
        if self.time_window_minutes <= 0:
            raise ValueError(
                f"time_window_minutes must be positive, got {self.time_window_minutes}"
            )
        if self.error_policy == ErrorPolicy.DLQ and self.dlq is None:
            raise ValueError("DLQ error_policy requires dlq configuration")


@dataclass
class TransportConfig:
    """
    Transport configuration.

    Attributes:
        type: Transport type ("memory" | "ipc" | "mqtt" | "kombu")
        config: Transport-specific configuration

    Example:
        >>> # MQTT transport
        >>> TransportConfig(
        ...     type="mqtt",
        ...     config={
        ...         "broker": "mqtt://broker:1883",
        ...         "topic_prefix": "ma114tsdb",
        ...         "qos_datastream": 0,
        ...         "qos_event": 1
        ...     }
        ... )

        >>> # Kombu transport
        >>> TransportConfig(
        ...     type="kombu",
        ...     config={
        ...         "url": "amqp://guest:guest@localhost:5672//",
        ...         "exchange": "ma114tsdb",
        ...         "exchange_type": "topic"
        ...     }
        ... )
    """

    type: str
    config: dict[str, Any]

    def __post_init__(self) -> None:
        """Validate transport config."""
        valid_types = {"memory", "ipc", "mqtt", "kombu"}
        if self.type not in valid_types:
            raise ValueError(
                f"type must be one of {valid_types}, got '{self.type}'"
            )


@dataclass
class PipelineConfig:
    """
    Declarative pipeline definition per FR-051.

    Attributes:
        pipeline_id: Unique pipeline identifier
        name: Human-readable name
        transport: Transport configuration
        sources: Producer adapter IDs (non-empty list)
        sinks: Consumer adapter IDs (non-empty list)
        config: Runtime configuration
        processors: Processor adapter IDs (ordered, optional)

    Example:
        >>> PipelineConfig(
        ...     pipeline_id="sensor-pipeline",
        ...     name="Temperature Sensor Pipeline",
        ...     transport=TransportConfig(type="mqtt", config={...}),
        ...     sources=["temp-sensor-01", "temp-sensor-02"],
        ...     sinks=["timescaledb-consumer"],
        ...     processors=["anomaly-filter", "unit-converter"],
        ...     config=PipelineRuntimeConfig(parallelism=4)
        ... )
    """

    pipeline_id: str
    name: str
    transport: TransportConfig
    sources: list[str]
    sinks: list[str]
    config: PipelineRuntimeConfig
    processors: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate pipeline config."""
        if not self.pipeline_id:
            raise ValueError("pipeline_id must be non-empty")
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.sources:
            raise ValueError("sources must be non-empty list")
        if not self.sinks:
            raise ValueError("sinks must be non-empty list")
