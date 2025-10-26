"""
Console Logger Consumer

Simple console logger for testing and quickstart scenarios per T070.
Constitutional principle: II. Everything is an Adapter
"""

import json
from typing import Any

from components.core.interfaces.consumer import IConsumer
from components.core.models.adapter_metadata import (
    AdapterCapabilities,
    AdapterMetadata,
    AdapterType,
    DataType,
)
from components.core.models.datastream import DataStream
from components.core.models.event import Event
from components.core.models.lifecycle import HealthStatus, Result
from components.core.versioning.version import Version


class ConsoleLogger:
    """
    Console logger consumer for development and testing.

    Logs DataStreams and Events to stdout with configurable formatting.

    Configuration:
        - format: Output format ("json" | "compact" | "verbose", default="json")
        - adapter_id: Consumer identifier (default="console-logger")
        - show_trace: Include trace context in output (default=True)

    Example:
        >>> logger = ConsoleLogger()
        >>> await logger.init({"format": "json"})
        >>> await logger.start()
        >>> result = await logger.consume(datastream)
        # Output: {"type": "datastream", "stream_id": "...", ...}
    """

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._running = False
        self._adapter_id = "console-logger"
        self._format = "json"
        self._show_trace = True
        self._consumed_count = 0

    async def init(self, config: dict[str, Any]) -> Result:
        """Initialize console logger with configuration."""
        self._config = config
        self._adapter_id = config.get("adapter_id", "console-logger")
        self._format = config.get("format", "json")
        self._show_trace = config.get("show_trace", True)

        valid_formats = {"json", "compact", "verbose"}
        if self._format not in valid_formats:
            return Result(
                success=False,
                message=f"Invalid format '{self._format}'. Must be one of: {valid_formats}",
            )

        return Result(success=True, message=f"Initialized {self._adapter_id}")

    async def start(self) -> Result:
        """Start consuming data."""
        self._running = True
        print(f"[{self._adapter_id}] Started (format={self._format})")
        return Result(success=True, message=f"Started {self._adapter_id}")

    async def stop(self) -> Result:
        """Stop consuming data."""
        self._running = False
        print(
            f"[{self._adapter_id}] Stopped (consumed {self._consumed_count} items)"
        )
        return Result(success=True, message=f"Stopped {self._adapter_id}")

    async def health(self) -> HealthStatus:
        """Return health status."""
        if not self._running:
            return HealthStatus(status="stopped", message=f"{self._adapter_id} is stopped")

        return HealthStatus(
            status="healthy",
            message=f"{self._adapter_id}: Consumed {self._consumed_count} items",
        )

    @property
    def metadata(self) -> AdapterMetadata:
        """Return adapter metadata."""
        return AdapterMetadata(
            id=self._adapter_id,
            name="Console Logger",
            version=Version(major=1, minor=0),
            type=AdapterType.CONSUMER,
            capabilities=AdapterCapabilities(
                data_types={DataType.DATASTREAM, DataType.EVENT},
                ordering_guarantee=True,  # Logs in order received
                lossy_allowed=False,  # Never drops data
                throughput_hint=1000,
            ),
        )

    async def consume(self, data: DataStream | Event) -> Result:
        """
        Consume and log data to console.

        Format options:
        - json: Full JSON representation
        - compact: One-line summary
        - verbose: Multi-line detailed output
        """
        if not self._running:
            return Result(success=False, message="Consumer not running")

        try:
            if self._format == "json":
                self._log_json(data)
            elif self._format == "compact":
                self._log_compact(data)
            else:  # verbose
                self._log_verbose(data)

            self._consumed_count += 1
            return Result(success=True, message=f"Logged {type(data).__name__}")

        except Exception as e:
            return Result(success=False, message=f"Failed to log data: {e}")

    def _log_json(self, data: DataStream | Event) -> None:
        """Log data as JSON."""
        if isinstance(data, DataStream):
            output = {
                "type": "datastream",
                "stream_id": data.stream_id,
                "source": data.source,
                "timestamp_start": data.timestamp_start.isoformat(),
                "timestamp_end": data.timestamp_end.isoformat(),
                "data_points_count": len(data.data_points),
                "data_points": data.data_points,
                "version": f"{data.version.major}.{data.version.minor}",
            }
            if self._show_trace:
                output["trace_id"] = str(data.trace_context.trace_id)
                output["span_id"] = str(data.trace_context.span_id)
        else:  # Event
            output = {
                "type": "event",
                "event_id": str(data.event_id),
                "event_type": data.event_type,
                "source": data.source,
                "timestamp": data.timestamp.isoformat(),
                "data": data.data,
                "version": f"{data.version.major}.{data.version.minor}",
            }
            if self._show_trace:
                output["trace_id"] = str(data.trace_context.trace_id)
                output["span_id"] = str(data.trace_context.span_id)

        print(json.dumps(output, indent=None))

    def _log_compact(self, data: DataStream | Event) -> None:
        """Log data as compact one-line summary."""
        if isinstance(data, DataStream):
            trace = f"trace={data.trace_context.trace_id}" if self._show_trace else ""
            print(
                f"[DATASTREAM] {data.stream_id} | source={data.source} | "
                f"points={len(data.data_points)} | {trace}"
            )
        else:  # Event
            trace = f"trace={data.trace_context.trace_id}" if self._show_trace else ""
            print(
                f"[EVENT] {data.event_id} | type={data.event_type} | "
                f"source={data.source} | {trace}"
            )

    def _log_verbose(self, data: DataStream | Event) -> None:
        """Log data with detailed multi-line output."""
        print("=" * 80)
        if isinstance(data, DataStream):
            print(f"Type:           DataStream")
            print(f"Stream ID:      {data.stream_id}")
            print(f"Source:         {data.source}")
            print(f"Timestamp:      {data.timestamp_start} - {data.timestamp_end}")
            print(f"Data Points:    {len(data.data_points)}")
            print(f"Version:        {data.version.major}.{data.version.minor}")
            if self._show_trace:
                print(f"Trace ID:       {data.trace_context.trace_id}")
                print(f"Span ID:        {data.trace_context.span_id}")
            print(f"\nData:")
            for i, point in enumerate(data.data_points[:10]):  # Show first 10
                print(f"  [{i}] {point}")
            if len(data.data_points) > 10:
                print(f"  ... ({len(data.data_points) - 10} more)")
        else:  # Event
            print(f"Type:           Event")
            print(f"Event ID:       {data.event_id}")
            print(f"Event Type:     {data.event_type}")
            print(f"Source:         {data.source}")
            print(f"Timestamp:      {data.timestamp}")
            print(f"Version:        {data.version.major}.{data.version.minor}")
            if self._show_trace:
                print(f"Trace ID:       {data.trace_context.trace_id}")
                print(f"Span ID:        {data.trace_context.span_id}")
            print(f"\nData:")
            print(json.dumps(data.data, indent=2))
        print("=" * 80)


# Adapter registration for dynamic loading
def create_adapter() -> IConsumer:
    """Factory function for adapter instantiation."""
    return ConsoleLogger()  # type: ignore
