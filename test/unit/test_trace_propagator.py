"""
Unit tests for trace propagation.

Tests span creation (trace_id preservation, parent_span linkage).
"""

import pytest
from uuid import UUID
from components.observability.tracing import TracePropagator
from components.core.models.trace_context import TraceContext


class TestTracePropagator:
    """Test suite for TracePropagator."""

    def test_create_root_trace(self):
        """Test root trace creation."""
        trace = TracePropagator.create_root_trace()

        assert isinstance(trace, TraceContext)
        assert isinstance(trace.trace_id, UUID)
        assert isinstance(trace.span_id, UUID)
        assert trace.parent_span is None

    def test_root_traces_are_unique(self):
        """Test each root trace has unique IDs."""
        trace1 = TracePropagator.create_root_trace()
        trace2 = TracePropagator.create_root_trace()

        assert trace1.trace_id != trace2.trace_id
        assert trace1.span_id != trace2.span_id

    def test_create_child_span_preserves_trace_id(self):
        """Test child span preserves parent's trace_id."""
        parent = TracePropagator.create_root_trace()
        child = TracePropagator.create_child_span(parent)

        assert child.trace_id == parent.trace_id
        assert child.span_id != parent.span_id
        assert child.parent_span == parent.span_id

    def test_create_child_span_creates_new_span_id(self):
        """Test child span gets new span_id."""
        parent = TracePropagator.create_root_trace()
        child1 = TracePropagator.create_child_span(parent)
        child2 = TracePropagator.create_child_span(parent)

        assert child1.span_id != child2.span_id
        assert child1.parent_span == parent.span_id
        assert child2.parent_span == parent.span_id

    def test_multi_level_span_hierarchy(self):
        """Test multi-level span hierarchy."""
        root = TracePropagator.create_root_trace()
        level1 = TracePropagator.create_child_span(root)
        level2 = TracePropagator.create_child_span(level1)
        level3 = TracePropagator.create_child_span(level2)

        # All preserve root trace_id
        assert root.trace_id == level1.trace_id == level2.trace_id == level3.trace_id

        # Parent linkage
        assert level1.parent_span == root.span_id
        assert level2.parent_span == level1.span_id
        assert level3.parent_span == level2.span_id

        # Unique span IDs
        span_ids = {root.span_id, level1.span_id, level2.span_id, level3.span_id}
        assert len(span_ids) == 4

    def test_validate_trace_context_valid(self):
        """Test validation of valid trace context."""
        trace = TracePropagator.create_root_trace()
        assert TracePropagator.validate_trace_context(trace) is True

        child = TracePropagator.create_child_span(trace)
        assert TracePropagator.validate_trace_context(child) is True

    def test_validate_trace_context_invalid_trace_id(self):
        """Test validation rejects invalid trace_id."""
        trace = TraceContext(
            trace_id="not-a-uuid",  # Invalid type
            span_id=UUID('12345678-1234-5678-1234-567812345678'),
            parent_span=None
        )
        assert TracePropagator.validate_trace_context(trace) is False

    def test_validate_trace_context_invalid_span_id(self):
        """Test validation rejects invalid span_id."""
        trace = TraceContext(
            trace_id=UUID('12345678-1234-5678-1234-567812345678'),
            span_id="not-a-uuid",  # Invalid type
            parent_span=None
        )
        assert TracePropagator.validate_trace_context(trace) is False

    def test_validate_trace_context_invalid_parent_span(self):
        """Test validation rejects invalid parent_span."""
        trace = TraceContext(
            trace_id=UUID('12345678-1234-5678-1234-567812345678'),
            span_id=UUID('87654321-4321-8765-4321-876543218765'),
            parent_span="not-a-uuid"  # Invalid type
        )
        assert TracePropagator.validate_trace_context(trace) is False

    def test_extract_trace_id(self):
        """Test trace ID extraction as string."""
        trace = TracePropagator.create_root_trace()
        trace_id_str = TracePropagator.extract_trace_id(trace)

        assert isinstance(trace_id_str, str)
        assert trace_id_str == str(trace.trace_id)
        # Verify it's a valid UUID string
        UUID(trace_id_str)

    def test_extract_span_id(self):
        """Test span ID extraction as string."""
        trace = TracePropagator.create_root_trace()
        span_id_str = TracePropagator.extract_span_id(trace)

        assert isinstance(span_id_str, str)
        assert span_id_str == str(trace.span_id)
        # Verify it's a valid UUID string
        UUID(span_id_str)

    def test_has_parent_root_trace(self):
        """Test has_parent() returns False for root trace."""
        trace = TracePropagator.create_root_trace()
        assert TracePropagator.has_parent(trace) is False

    def test_has_parent_child_trace(self):
        """Test has_parent() returns True for child trace."""
        parent = TracePropagator.create_root_trace()
        child = TracePropagator.create_child_span(parent)
        assert TracePropagator.has_parent(child) is True

    def test_trace_context_create_child_span_method(self):
        """Test TraceContext.create_child_span() method directly."""
        parent = TracePropagator.create_root_trace()
        child = parent.create_child_span()

        assert child.trace_id == parent.trace_id
        assert child.span_id != parent.span_id
        assert child.parent_span == parent.span_id
