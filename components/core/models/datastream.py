"""
DataStream Model for ma114tsdb

Constitutional principle: I. Time is First-Class
All data carries timestamps with sub-second precision.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..versioning import Version
from .trace_context import TraceContext


@dataclass
class DataStream:
    """
    High-volume, lossy-tolerant time-series data.

    Per FR-001, FR-002: Compact representation where some loss is acceptable.
    Optimized for throughput (100-10K streams/sec per FR-058).

    Attributes:
        stream_id: Unique stream identifier
        source: Adapter ID that produced this stream
        timestamp_start: First data point timestamp (sub-second, timezone-aware)
        timestamp_end: Last data point timestamp (>= timestamp_start)
        data_points: Array of data points (non-empty)
        trace_context: Distributed tracing metadata
        version: Data format version
        interval: Optional regular interval between points
        metadata: Optional additional context

    Characteristics (FR-021, FR-024, FR-027):
        - Delivery: Best-effort, lossy-tolerant
        - Ordering: NOT guaranteed by timestamp
        - Prioritization: Latest-first within time window (1-5 min default)
        - Throughput: 100-10,000 streams/second per pipeline

    Use cases:
        Temperature readings, CPU metrics, network bandwidth, sensor arrays
    """

    stream_id: str
    source: str
    timestamp_start: datetime
    timestamp_end: datetime
    data_points: list[Any]
    trace_context: TraceContext
    version: Version
    interval: float | None = None  # seconds between points
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate invariants."""
        if not self.stream_id:
            raise ValueError("stream_id must be non-empty")
        if not self.source:
            raise ValueError("source must be non-empty")
        if not self.data_points:
            raise ValueError("data_points must be non-empty")
        if self.timestamp_end < self.timestamp_start:
            raise ValueError(
                f"timestamp_end ({self.timestamp_end}) must be >= "
                f"timestamp_start ({self.timestamp_start})"
            )
        if self.timestamp_start.tzinfo is None:
            raise ValueError("timestamp_start must be timezone-aware")
        if self.timestamp_end.tzinfo is None:
            raise ValueError("timestamp_end must be timezone-aware")
        if self.interval is not None and self.interval <= 0:
            raise ValueError(f"interval must be positive, got {self.interval}")
