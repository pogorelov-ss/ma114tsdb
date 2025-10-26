"""
Structured Logger for ma114tsdb

Provides structured logging with JSON/EDN formatters and automatic context injection.
Based on FR-031: Structured logging requirement.
"""

import structlog
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import UUID


def add_timestamp(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add ISO 8601 timestamp to log event."""
    event_dict['timestamp'] = datetime.now(timezone.utc).isoformat()
    return event_dict


def add_level(logger: Any, method_name: str, event_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Add log level to event."""
    event_dict['level'] = method_name
    return event_dict


class StructuredLogger:
    """
    Structured logger with automatic context injection.

    Automatically injects:
    - timestamp (ISO 8601)
    - level (debug, info, warning, error)
    - adapter_id
    - trace_id (if available in context)
    """

    def __init__(
        self,
        adapter_id: str,
        format: str = "json",
        log_level: str = "info"
    ):
        """
        Initialize structured logger.

        Args:
            adapter_id: Unique adapter identifier
            format: Output format ("json" or "edn")
            log_level: Minimum log level (debug, info, warning, error)
        """
        self.adapter_id = adapter_id
        self.format = format

        # Configure structlog
        processors = [
            structlog.stdlib.filter_by_level,
            add_timestamp,
            add_level,
            structlog.stdlib.add_logger_name,
            structlog.contextvars.merge_contextvars,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
        ]

        # Add renderer based on format
        if format == "json":
            processors.append(structlog.processors.JSONRenderer())
        elif format == "edn":
            # EDN format (Extensible Data Notation)
            processors.append(structlog.processors.KeyValueRenderer(
                key_order=['timestamp', 'level', 'adapter_id', 'message'],
                drop_missing=True
            ))
        else:
            processors.append(structlog.dev.ConsoleRenderer())

        structlog.configure(
            processors=processors,
            wrapper_class=structlog.stdlib.BoundLogger,
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        self.logger = structlog.get_logger().bind(adapter_id=adapter_id)

    def bind(self, **kwargs: Any) -> 'StructuredLogger':
        """
        Bind additional context to logger.

        Args:
            **kwargs: Context key-value pairs

        Returns:
            New logger instance with bound context
        """
        new_logger = StructuredLogger(
            adapter_id=self.adapter_id,
            format=self.format
        )
        new_logger.logger = self.logger.bind(**kwargs)
        return new_logger

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.logger.debug(message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.logger.info(message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.logger.warning(message, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self.logger.error(message, **kwargs)

    def with_trace(self, trace_id: UUID, span_id: UUID, parent_span: Optional[UUID] = None) -> 'StructuredLogger':
        """
        Bind trace context to logger.

        Args:
            trace_id: Trace identifier
            span_id: Span identifier
            parent_span: Optional parent span identifier

        Returns:
            New logger instance with trace context
        """
        context = {
            'trace_id': str(trace_id),
            'span_id': str(span_id)
        }
        if parent_span:
            context['parent_span'] = str(parent_span)

        return self.bind(**context)


def configure_logging(format: str = "json", log_level: str = "info") -> None:
    """
    Configure global structlog settings.

    Args:
        format: Output format ("json" or "edn")
        log_level: Minimum log level
    """
    import logging
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, log_level.upper())
    )


__all__ = ['StructuredLogger', 'configure_logging']
