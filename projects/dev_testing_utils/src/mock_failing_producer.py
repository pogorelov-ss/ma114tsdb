"""
Mock Failing Producer

Producer that fails after N seconds for testing adapter lifecycle per T069.
Constitutional principle: II. Everything is an Adapter
"""

import asyncio
from datetime import UTC, datetime
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


class MockFailingProducer:
    """
    Mock producer that fails after configured duration.

    Simulates adapter failure for testing lifecycle management and retry logic.

    Configuration:
        - fail_after_seconds: Seconds before failure (default=5)
        - adapter_id: Producer identifier (default="failing-producer")
        - interval_seconds: Reading interval before failure (default=1)

    Example:
        >>> producer = MockFailingProducer()
        >>> await producer.init({"fail_after_seconds": 10})
        >>> await producer.start()
        >>> async for stream in producer.produce():
        ...     # Produces for 10 seconds, then raises exception
        ...     print(stream)
    """

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._running = False
        self._adapter_id = "failing-producer"
        self._fail_after_seconds = 5.0
        self._interval_seconds = 1.0
        self._start_time: float | None = None
        self._produced_count = 0

    async def init(self, config: dict[str, Any]) -> Result:
        """Initialize producer with configuration."""
        self._config = config
        self._adapter_id = config.get("adapter_id", "failing-producer")
        self._fail_after_seconds = config.get("fail_after_seconds", 5.0)
        self._interval_seconds = config.get("interval_seconds", 1.0)

        return Result(success=True, message=f"Initialized {self._adapter_id}")

    async def start(self) -> Result:
        """Start producing data."""
        self._running = True
        self._start_time = asyncio.get_event_loop().time()
        return Result(success=True, message=f"Started {self._adapter_id}")

    async def stop(self) -> Result:
        """Stop producing data."""
        self._running = False
        return Result(success=True, message=f"Stopped {self._adapter_id}")

    async def health(self) -> HealthStatus:
        """Return health status."""
        if not self._running:
            return HealthStatus(status="stopped", message=f"{self._adapter_id} is stopped")

        if self._start_time is None:
            return HealthStatus(status="init", message=f"{self._adapter_id} not started")

        elapsed = asyncio.get_event_loop().time() - self._start_time
        remaining = self._fail_after_seconds - elapsed

        if remaining > 0:
            return HealthStatus(
                status="healthy",
                message=f"{self._adapter_id}: {remaining:.1f}s until failure (produced {self._produced_count})",
            )
        else:
            return HealthStatus(
                status="unhealthy",
                message=f"{self._adapter_id}: Failed after {self._fail_after_seconds}s",
            )

    @property
    def metadata(self) -> AdapterMetadata:
        """Return adapter metadata."""
        return AdapterMetadata(
            id=self._adapter_id,
            name="Mock Failing Producer",
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
        Produce DataStreams until configured failure time.

        Raises RuntimeError after fail_after_seconds elapsed.
        """
        if self._start_time is None:
            raise RuntimeError(f"{self._adapter_id} not started")

        while self._running:
            # Check if time to fail
            elapsed = asyncio.get_event_loop().time() - self._start_time
            if elapsed >= self._fail_after_seconds:
                raise RuntimeError(
                    f"{self._adapter_id} intentionally failed after {self._fail_after_seconds}s "
                    f"(produced {self._produced_count} streams)"
                )

            # Generate dummy DataStream
            trace_context = TraceContext(
                trace_id=uuid4(), span_id=uuid4(), parent_span=None
            )

            timestamp = datetime.now(UTC)
            stream = DataStream(
                stream_id=f"{self._adapter_id}-{self._produced_count}",
                source=self._adapter_id,
                timestamp_start=timestamp,
                timestamp_end=timestamp,
                interval=None,
                data_points=[
                    {
                        "count": self._produced_count,
                        "elapsed_seconds": round(elapsed, 2),
                        "timestamp": timestamp.isoformat(),
                    }
                ],
                trace_context=trace_context,
                metadata={
                    "fail_after_seconds": self._fail_after_seconds,
                    "remaining_seconds": round(self._fail_after_seconds - elapsed, 2),
                },
                version=Version(major=1, minor=0),
            )

            self._produced_count += 1
            yield stream

            await asyncio.sleep(self._interval_seconds)


# Adapter registration for dynamic loading
def create_adapter() -> IProducer:
    """Factory function for adapter instantiation."""
    return MockFailingProducer()  # type: ignore
