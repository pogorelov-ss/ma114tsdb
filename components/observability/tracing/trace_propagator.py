"""
Trace Context Propagator for ma114tsdb

Implements lightweight trace context propagation with automatic span creation.
Based on FR-032: Distributed tracing requirement.
"""

from uuid import UUID, uuid4
from typing import Optional
from components.core.models.trace_context import TraceContext


class TracePropagator:
    """
    Propagates trace context across adapters with automatic span management.

    Responsibilities:
    - Generate new trace_id at pipeline ingress
    - Create child spans while preserving trace lineage
    - Validate trace context integrity
    """

    @staticmethod
    def create_root_trace() -> TraceContext:
        """
        Create root trace context for pipeline ingress.

        Returns:
            New TraceContext with fresh trace_id and span_id
        """
        return TraceContext(
            trace_id=uuid4(),
            span_id=uuid4(),
            parent_span=None
        )

    @staticmethod
    def create_child_span(parent: TraceContext) -> TraceContext:
        """
        Create child span from parent trace context.

        Args:
            parent: Parent trace context

        Returns:
            New TraceContext with same trace_id, new span_id, parent linkage
        """
        return parent.create_child_span()

    @staticmethod
    def validate_trace_context(trace: TraceContext) -> bool:
        """
        Validate trace context integrity.

        Args:
            trace: Trace context to validate

        Returns:
            True if valid, False otherwise
        """
        if not isinstance(trace.trace_id, UUID):
            return False
        if not isinstance(trace.span_id, UUID):
            return False
        if trace.parent_span is not None and not isinstance(trace.parent_span, UUID):
            return False
        return True

    @staticmethod
    def extract_trace_id(trace: TraceContext) -> str:
        """
        Extract trace ID as string for logging/correlation.

        Args:
            trace: Trace context

        Returns:
            Trace ID as string
        """
        return str(trace.trace_id)

    @staticmethod
    def extract_span_id(trace: TraceContext) -> str:
        """
        Extract span ID as string for logging/correlation.

        Args:
            trace: Trace context

        Returns:
            Span ID as string
        """
        return str(trace.span_id)

    @staticmethod
    def has_parent(trace: TraceContext) -> bool:
        """
        Check if trace context has parent span.

        Args:
            trace: Trace context

        Returns:
            True if has parent, False otherwise
        """
        return trace.parent_span is not None


__all__ = ['TracePropagator']
