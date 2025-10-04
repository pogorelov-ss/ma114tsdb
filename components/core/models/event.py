"""
Event Model for ma114tsdb

Constitutional principle: I. Time is First-Class
Critical occurrences with durable, at-least-once delivery.
"""

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID, uuid4
from typing import Any

from ..versioning import Version
from .trace_context import TraceContext


@dataclass
class Event:
    """
    Critical, durable occurrence that must never be lost.

    Per FR-001, FR-003: Guaranteed delivery with at-least-once semantics.

    Attributes:
        event_id: Unique event identifier (UUID v4)
        timestamp: When event occurred (sub-second, timezone-aware)
        source: Adapter ID that produced this event
        event_type: Event classification (e.g., "health-check", "alarm")
        data: Flexible event payload (non-empty dict)
        trace_context: Distributed tracing metadata
        version: Data format version
        stream_id: Optional reference to DataStream
        metadata: Optional additional context

    Characteristics (FR-022, FR-026):
        - Delivery: Durable, at-least-once guarantee (NEVER lost)
        - Ordering: FIFO (First-In-First-Out) by default
        - Idempotency: Consumers responsible for handling duplicates
        - Durability: Persisted before acknowledgment

    Use cases:
        Device state changes, alarms, system events, configuration changes, health checks
    """

    event_id: UUID
    timestamp: datetime
    source: str
    event_type: str
    data: dict[str, Any]
    trace_context: TraceContext
    version: Version
    stream_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate invariants."""
        if not self.source:
            raise ValueError("source must be non-empty")
        if not self.event_type:
            raise ValueError("event_type must be non-empty")
        if not self.data:
            raise ValueError("data must be non-empty dict")
        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")

    @classmethod
    def create(
        cls,
        source: str,
        event_type: str,
        data: dict[str, Any],
        trace_context: TraceContext,
        version: Version,
        stream_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "Event":
        """
        Factory method for creating Events with auto-generated ID and timestamp.

        Args:
            source: Adapter ID producing the event
            event_type: Event classification
            data: Event payload
            trace_context: Tracing metadata
            version: Data format version
            stream_id: Optional DataStream reference
            metadata: Optional additional context

        Returns:
            Event with generated event_id and current timestamp (UTC)

        Example:
            >>> evt = Event.create(
            ...     source="temp-sensor-01",
            ...     event_type="alarm",
            ...     data={"threshold_exceeded": True, "value": 95.2},
            ...     trace_context=TraceContext.new(),
            ...     version=Version(1, 0)
            ... )
        """
        from datetime import timezone

        return cls(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source=source,
            event_type=event_type,
            data=data,
            trace_context=trace_context,
            version=version,
            stream_id=stream_id,
            metadata=metadata or {},
        )
