"""
Mock Flaky Consumer

Consumer with configurable failure rate for testing error handling per T069.
Constitutional principle: II. Everything is an Adapter
"""

import random
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


class MockFlakyConsumer:
    """
    Mock consumer with configurable failure rate.

    Simulates unreliable consumer for testing DLQ and retry logic.

    Configuration:
        - failure_rate: Probability of failure (0.0-1.0, default=0.2)
        - adapter_id: Consumer identifier (default="flaky-consumer")

    Example:
        >>> consumer = MockFlakyConsumer()
        >>> await consumer.init({"failure_rate": 0.3})
        >>> result = await consumer.consume(datastream)
        >>> result.success  # 70% True, 30% False
    """

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._running = False
        self._adapter_id = "flaky-consumer"
        self._failure_rate = 0.2
        self._processed_count = 0
        self._failed_count = 0

    async def init(self, config: dict[str, Any]) -> Result:
        """Initialize consumer with configuration."""
        self._config = config
        self._adapter_id = config.get("adapter_id", "flaky-consumer")
        self._failure_rate = config.get("failure_rate", 0.2)

        if not 0.0 <= self._failure_rate <= 1.0:
            return Result(
                success=False,
                message=f"failure_rate must be between 0.0 and 1.0, got {self._failure_rate}",
            )

        return Result(success=True, message=f"Initialized {self._adapter_id}")

    async def start(self) -> Result:
        """Start consuming data."""
        self._running = True
        return Result(success=True, message=f"Started {self._adapter_id}")

    async def stop(self) -> Result:
        """Stop consuming data."""
        self._running = False
        return Result(success=True, message=f"Stopped {self._adapter_id}")

    async def health(self) -> HealthStatus:
        """Return health status."""
        if not self._running:
            return HealthStatus(status="stopped", message=f"{self._adapter_id} is stopped")

        total = self._processed_count + self._failed_count
        success_rate = (
            self._processed_count / total if total > 0 else 1.0
        )

        # Degrade health if success rate too low
        if success_rate < 0.5:
            return HealthStatus(
                status="degraded",
                message=f"{self._adapter_id}: Success rate {success_rate:.1%} (expected {1-self._failure_rate:.1%})",
            )

        return HealthStatus(
            status="healthy",
            message=f"{self._adapter_id}: Processed {self._processed_count}, Failed {self._failed_count}",
        )

    @property
    def metadata(self) -> AdapterMetadata:
        """Return adapter metadata."""
        return AdapterMetadata(
            id=self._adapter_id,
            name="Mock Flaky Consumer",
            version=Version(major=1, minor=0),
            type=AdapterType.CONSUMER,
            capabilities=AdapterCapabilities(
                data_types={DataType.DATASTREAM, DataType.EVENT},
                ordering_guarantee=False,
                lossy_allowed=True,
                throughput_hint=100,
            ),
        )

    async def consume(self, data: DataStream | Event) -> Result:
        """
        Consume data with configurable failure rate.

        Randomly fails based on failure_rate configuration.
        """
        if not self._running:
            return Result(success=False, message="Consumer not running")

        # Simulate failure
        if random.random() < self._failure_rate:
            self._failed_count += 1
            return Result(
                success=False,
                message=f"Simulated failure (failure_rate={self._failure_rate})",
            )

        # Successful consumption
        self._processed_count += 1

        if isinstance(data, DataStream):
            return Result(
                success=True,
                message=f"Consumed DataStream {data.stream_id} with {len(data.data_points)} points",
            )
        else:  # Event
            return Result(
                success=True,
                message=f"Consumed Event {data.event_id} (type={data.event_type})",
            )


# Adapter registration for dynamic loading
def create_adapter() -> IConsumer:
    """Factory function for adapter instantiation."""
    return MockFlakyConsumer()  # type: ignore
