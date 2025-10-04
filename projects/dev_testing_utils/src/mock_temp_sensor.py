"""
Mock Temperature Sensor Producer

Generates DataStreams with temperature readings for testing per T069.
Constitutional principle: II. Everything is an Adapter
"""

import asyncio
import random
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncIterator
from uuid import uuid4

from components.core.interfaces.producer import IProducer
from components.core.models.adapter_metadata import (
    AdapterCapabilities,
    AdapterMetadata,
    AdapterType,
    DataType,
)
from components.core.models.datastream import DataStream
from components.core.models.lifecycle import HealthStatus, Result
from components.core.models.trace_context import TraceContext
from components.core.versioning.version import Version


class MockTempSensor:
    """
    Mock temperature sensor producer for testing.

    Generates realistic temperature readings with configurable interval.

    Configuration:
        - interval_seconds: Reading interval (default=1)
        - sensor_id: Sensor identifier (default="mock-temp-sensor")
        - min_temp: Minimum temperature (default=18.0°C)
        - max_temp: Maximum temperature (default=28.0°C)
        - noise: Temperature noise stddev (default=0.5°C)

    Example:
        >>> sensor = MockTempSensor()
        >>> await sensor.init({"interval_seconds": 1, "sensor_id": "room1"})
        >>> await sensor.start()
        >>> async for stream in sensor.produce():
        ...     print(stream.stream_id, stream.data_points)
    """

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._running = False
        self._interval_seconds = 1.0
        self._sensor_id = "mock-temp-sensor"
        self._min_temp = 18.0
        self._max_temp = 28.0
        self._noise = 0.5
        self._current_temp = 22.0  # Starting temperature

    async def init(self, config: dict[str, Any]) -> Result:
        """Initialize sensor with configuration."""
        self._config = config
        self._interval_seconds = config.get("interval_seconds", 1.0)
        self._sensor_id = config.get("sensor_id", "mock-temp-sensor")
        self._min_temp = config.get("min_temp", 18.0)
        self._max_temp = config.get("max_temp", 28.0)
        self._noise = config.get("noise", 0.5)

        return Result(success=True, message=f"Initialized {self._sensor_id}")

    async def start(self) -> Result:
        """Start producing temperature readings."""
        self._running = True
        return Result(success=True, message=f"Started {self._sensor_id}")

    async def stop(self) -> Result:
        """Stop producing temperature readings."""
        self._running = False
        return Result(success=True, message=f"Stopped {self._sensor_id}")

    async def health(self) -> HealthStatus:
        """Return health status."""
        return HealthStatus(
            status="healthy" if self._running else "stopped",
            message=f"{self._sensor_id} is {'running' if self._running else 'stopped'}",
        )

    @property
    def metadata(self) -> AdapterMetadata:
        """Return adapter metadata."""
        return AdapterMetadata(
            id=self._sensor_id,
            name="Mock Temperature Sensor",
            version=Version(major=1, minor=0),
            type=AdapterType.PRODUCER,
            capabilities=AdapterCapabilities(
                data_types={DataType.DATASTREAM},
                ordering_guarantee=False,
                lossy_allowed=True,
                throughput_hint=int(1 / self._interval_seconds),
            ),
        )

    async def produce(self) -> AsyncIterator[DataStream]:
        """
        Produce temperature readings as DataStreams.

        Yields DataStreams with simulated temperature data.
        Temperature drifts slowly with random noise.
        """
        while self._running:
            # Generate temperature reading
            # Drift: Random walk with pull towards center
            center_temp = (self._min_temp + self._max_temp) / 2
            drift = (center_temp - self._current_temp) * 0.1  # 10% pull towards center
            noise = random.gauss(0, self._noise)
            self._current_temp += drift + noise

            # Clamp to min/max
            self._current_temp = max(
                self._min_temp, min(self._max_temp, self._current_temp)
            )

            # Create trace context
            trace_context = TraceContext(
                trace_id=uuid4(), span_id=uuid4(), parent_span=None
            )

            # Create DataStream with single reading
            timestamp = datetime.now(UTC)
            stream = DataStream(
                stream_id=f"{self._sensor_id}-{timestamp.isoformat()}",
                source=self._sensor_id,
                timestamp_start=timestamp,
                timestamp_end=timestamp,
                interval=None,  # Single reading
                data_points=[
                    {
                        "temperature": round(self._current_temp, 2),
                        "unit": "celsius",
                        "timestamp": timestamp.isoformat(),
                    }
                ],
                trace_context=trace_context,
                metadata={
                    "sensor_type": "temperature",
                    "location": self._config.get("location", "unknown"),
                },
                version=Version(major=1, minor=0),
            )

            yield stream

            # Wait for next reading
            await asyncio.sleep(self._interval_seconds)


# Adapter registration for dynamic loading
def create_adapter() -> IProducer:
    """Factory function for adapter instantiation."""
    return MockTempSensor()  # type: ignore
