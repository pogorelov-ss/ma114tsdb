"""
Unit tests for structured logging.

Tests log formatting (JSON output, context binding, adapter_id injection).
"""

import pytest
import json
import structlog
from io import StringIO
import logging
from components.observability.logging import StructuredLogger, configure_logging
from uuid import uuid4


class TestStructuredLogger:
    """Test suite for StructuredLogger."""

    def setup_method(self):
        """Setup test environment before each test."""
        # Clear structlog configuration
        structlog.reset_defaults()

    def test_logger_initialization(self):
        """Test logger initializes with adapter_id."""
        logger = StructuredLogger("test-adapter", format="json")
        assert logger.adapter_id == "test-adapter"
        assert logger.format == "json"

    def test_json_output_format(self, caplog):
        """Test JSON output formatting."""
        configure_logging(format="json", log_level="info")
        logger = StructuredLogger("test-adapter", format="json")

        with caplog.at_level(logging.INFO):
            logger.info("Test message", key1="value1")

        # Verify log output contains expected fields
        assert len(caplog.records) > 0
        log_output = caplog.records[0].getMessage()

        # Parse JSON
        log_data = json.loads(log_output)
        assert log_data['adapter_id'] == 'test-adapter'
        assert log_data['event'] == 'Test message'
        assert log_data['key1'] == 'value1'
        assert 'timestamp' in log_data
        assert 'level' in log_data

    def test_context_binding(self, caplog):
        """Test context binding persists across log calls."""
        configure_logging(format="json", log_level="info")
        logger = StructuredLogger("test-adapter", format="json")
        bound_logger = logger.bind(request_id="req-123", user_id="user-456")

        with caplog.at_level(logging.INFO):
            bound_logger.info("First message")
            bound_logger.info("Second message")

        # Both messages should have bound context
        assert len(caplog.records) >= 2
        for record in caplog.records[:2]:
            log_data = json.loads(record.getMessage())
            assert log_data.get('request_id') == 'req-123'
            assert log_data.get('user_id') == 'user-456'

    def test_adapter_id_injection(self, caplog):
        """Test adapter_id automatically injected in all logs."""
        configure_logging(format="json", log_level="info")
        logger = StructuredLogger("my-adapter", format="json")

        with caplog.at_level(logging.INFO):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")

        # Filter to only INFO and above
        info_records = [r for r in caplog.records if r.levelno >= logging.INFO]

        for record in info_records:
            log_data = json.loads(record.getMessage())
            assert log_data['adapter_id'] == 'my-adapter'

    def test_trace_context_binding(self, caplog):
        """Test with_trace() binds trace context."""
        configure_logging(format="json", log_level="info")
        logger = StructuredLogger("test-adapter", format="json")

        trace_id = uuid4()
        span_id = uuid4()
        parent_span = uuid4()

        traced_logger = logger.with_trace(trace_id, span_id, parent_span)

        with caplog.at_level(logging.INFO):
            traced_logger.info("Traced message")

        log_data = json.loads(caplog.records[0].getMessage())
        assert log_data['trace_id'] == str(trace_id)
        assert log_data['span_id'] == str(span_id)
        assert log_data['parent_span'] == str(parent_span)

    def test_trace_context_without_parent(self, caplog):
        """Test with_trace() without parent span."""
        configure_logging(format="json", log_level="info")
        logger = StructuredLogger("test-adapter", format="json")

        trace_id = uuid4()
        span_id = uuid4()

        traced_logger = logger.with_trace(trace_id, span_id)

        with caplog.at_level(logging.INFO):
            traced_logger.info("Root span message")

        log_data = json.loads(caplog.records[0].getMessage())
        assert log_data['trace_id'] == str(trace_id)
        assert log_data['span_id'] == str(span_id)
        assert 'parent_span' not in log_data

    def test_log_levels(self, caplog):
        """Test different log levels."""
        configure_logging(format="json", log_level="debug")
        logger = StructuredLogger("test-adapter", format="json")

        with caplog.at_level(logging.DEBUG):
            logger.debug("Debug message")
            logger.info("Info message")
            logger.warning("Warning message")
            logger.error("Error message")

        assert len(caplog.records) == 4

        levels = [json.loads(r.getMessage())['level'] for r in caplog.records]
        assert levels == ['debug', 'info', 'warning', 'error']

    def test_edn_format(self, caplog):
        """Test EDN (Extensible Data Notation) output format."""
        configure_logging(format="edn", log_level="info")
        logger = StructuredLogger("test-adapter", format="edn")

        with caplog.at_level(logging.INFO):
            logger.info("Test message", key1="value1")

        # EDN format uses key='value' pairs
        log_output = caplog.records[0].getMessage()
        assert "adapter_id='test-adapter'" in log_output
        assert "event='Test message'" in log_output
