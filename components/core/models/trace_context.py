"""
TraceContext Model for ma114tsdb

Constitutional principle: III. Observable by Default
Distributed tracing is mandatory with minimal overhead.
"""

from dataclasses import dataclass
from uuid import UUID, uuid4
from typing import Self


@dataclass(frozen=True)
class TraceContext:
    """
    Lightweight distributed tracing correlation.

    Enables request correlation across pipeline stages without full span overhead.
    Per FR-004, FR-032: All DataStreams and Events carry TraceContext.

    Attributes:
        trace_id: Unique trace identifier (UUID v4)
        span_id: Current span identifier (UUID v4)
        parent_span: Optional parent span identifier (UUID v4)

    Propagation pattern:
        - Producer: Generates trace_id at ingress, creates initial span_id
        - Processor: Preserves trace_id, creates new span_id, sets parent_span
        - Consumer: Preserves trace_id for correlation
        - Transport: Passes through unchanged
    """

    trace_id: UUID
    span_id: UUID
    parent_span: UUID | None = None

    def create_child_span(self) -> Self:
        """
        Generate new span while preserving trace lineage.

        Used by Processor adapters to create new span for transformation stage.

        Returns:
            New TraceContext with same trace_id, new span_id, parent set to current span

        Example:
            >>> parent_ctx = TraceContext.new()
            >>> child_ctx = parent_ctx.create_child_span()
            >>> child_ctx.trace_id == parent_ctx.trace_id
            True
            >>> child_ctx.parent_span == parent_ctx.span_id
            True
        """
        return TraceContext(
            trace_id=self.trace_id,
            span_id=uuid4(),
            parent_span=self.span_id,
        )

    @classmethod
    def new(cls) -> Self:
        """
        Create new trace context (root span).

        Used by Producer adapters at data ingress point.

        Returns:
            TraceContext with new trace_id, span_id, no parent

        Example:
            >>> ctx = TraceContext.new()
            >>> ctx.parent_span is None
            True
        """
        return cls(
            trace_id=uuid4(),
            span_id=uuid4(),
            parent_span=None,
        )
