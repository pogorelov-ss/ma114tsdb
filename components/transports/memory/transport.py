"""
Memory Transport Implementation

In-memory transport with best-effort delivery using Python queues.
Based on FR-013: Transport interface requirements.
"""

import asyncio
from datetime import datetime, timezone
from typing import Union, AsyncIterator
from uuid import uuid4
from asyncio import Queue
from dataclasses import dataclass

from components.core.interfaces import ITransport, Receipt, ResourceSpec
from components.core.models import (
    DataStream, Event, RoutingInfo, SubscriptionPattern,
    Result, HealthStatus, AdapterState, AdapterMetadata,
    AdapterType, AdapterCapabilities, DataType, Version
)


@dataclass
class MemoryReceipt(Receipt):
    """Receipt for memory transport (acknowledgment)."""
    receipt_id: str
    timestamp: datetime


class MemoryTransport(ITransport):
    """
    In-memory transport with best-effort delivery.

    Characteristics:
    - Best-effort delivery (no durability)
    - Topic-based routing
    - Buffer overflow handling
    - Supports wildcard subscriptions (*)
    """

    def __init__(self, buffer_size: int = 1000):
        """
        Initialize memory transport.

        Args:
            buffer_size: Maximum queue size per topic
        """
        self.buffer_size = buffer_size
        self._topics: dict[str, Queue] = {}
        self._state = AdapterState.INIT
        self._start_time = None

    async def init(self, config: dict) -> Result:
        """Initialize transport with configuration."""
        try:
            self.buffer_size = config.get('buffer_size', self.buffer_size)
            self._state = AdapterState.INIT
            return Result(success=True, message="Memory transport initialized")
        except Exception as e:
            return Result(success=False, message=f"Init failed: {e}", error=e)

    async def start(self) -> Result:
        """Start transport operation."""
        try:
            self._state = AdapterState.RUNNING
            self._start_time = datetime.now(timezone.utc)
            return Result(success=True, message="Memory transport started")
        except Exception as e:
            self._state = AdapterState.ERROR
            return Result(success=False, message=f"Start failed: {e}", error=e)

    async def stop(self) -> Result:
        """Stop transport and flush buffers."""
        try:
            # Clear all topic queues
            for topic_queue in self._topics.values():
                while not topic_queue.empty():
                    try:
                        topic_queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

            self._topics.clear()
            self._state = AdapterState.STOPPED
            return Result(success=True, message="Memory transport stopped")
        except Exception as e:
            return Result(success=False, message=f"Stop failed: {e}", error=e)

    async def health(self) -> HealthStatus:
        """Get transport health status."""
        uptime = 0.0
        if self._start_time:
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        return HealthStatus(
            status="healthy" if self._state == AdapterState.RUNNING else "unhealthy",
            uptime=uptime,
            state=self._state,
            details={
                "topics": list(self._topics.keys()),
                "buffer_size": self.buffer_size
            }
        )

    @property
    def metadata(self) -> AdapterMetadata:
        """Transport metadata."""
        return AdapterMetadata(
            id="memory-transport",
            name="In-Memory Transport",
            version=Version(major=1, minor=0),
            type=AdapterType.TRANSPORT,
            capabilities=AdapterCapabilities(
                data_types={DataType.DATASTREAM, DataType.EVENT},
                ordering_guarantee=True,  # FIFO within topic
                lossy_allowed=True,  # Best-effort delivery
                throughput_hint=10000
            )
        )

    async def publish(self, data: Union[DataStream, Event], routing: RoutingInfo) -> Result:
        """
        Publish data to topic.

        Args:
            data: DataStream or Event to publish
            routing: Routing info with destination as topic name

        Returns:
            Result indicating success/failure
        """
        try:
            topic = routing.destination

            # Create topic queue if doesn't exist
            if topic not in self._topics:
                self._topics[topic] = Queue(maxsize=self.buffer_size)

            topic_queue = self._topics[topic]

            # Try to put data in queue (non-blocking)
            try:
                topic_queue.put_nowait(data)
                return Result(success=True, message=f"Published to topic: {topic}")
            except asyncio.QueueFull:
                # Buffer overflow - drop data (best-effort delivery)
                return Result(
                    success=False,
                    message=f"Topic {topic} buffer full, data dropped"
                )
        except Exception as e:
            return Result(success=False, message=f"Publish failed: {e}", error=e)

    async def subscribe(self, pattern: SubscriptionPattern) -> AsyncIterator[Union[DataStream, Event]]:
        """
        Subscribe to topic pattern.

        Args:
            pattern: Subscription pattern (exact topic or "*" for all)

        Yields:
            DataStreams or Events from matching topics
        """
        topic_pattern = pattern.pattern

        # Create topic queue if doesn't exist
        if topic_pattern != "*" and topic_pattern not in self._topics:
            self._topics[topic_pattern] = Queue(maxsize=self.buffer_size)

        while self._state == AdapterState.RUNNING:
            try:
                if topic_pattern == "*":
                    # Subscribe to all topics
                    for topic_queue in self._topics.values():
                        if not topic_queue.empty():
                            data = await asyncio.wait_for(topic_queue.get(), timeout=0.1)
                            yield data
                else:
                    # Subscribe to specific topic
                    topic_queue = self._topics[topic_pattern]
                    data = await asyncio.wait_for(topic_queue.get(), timeout=0.1)
                    yield data
            except asyncio.TimeoutError:
                # No data available, continue polling
                await asyncio.sleep(0.01)
            except KeyError:
                # Topic doesn't exist yet, wait
                await asyncio.sleep(0.1)
            except Exception:
                # Stop iteration on error
                break

    async def ack(self, receipt: Receipt) -> Result:
        """
        Acknowledge message delivery.

        Note: Memory transport is best-effort, so ack is a no-op.
        """
        return Result(success=True, message="Ack received (no-op for memory transport)")

    async def declare(self, resource: ResourceSpec) -> Result:
        """
        Declare topic resource.

        Args:
            resource: Resource spec with topic name

        Returns:
            Result indicating success/failure
        """
        try:
            topic_name = resource.name
            if topic_name not in self._topics:
                self._topics[topic_name] = Queue(maxsize=self.buffer_size)
            return Result(success=True, message=f"Topic declared: {topic_name}")
        except Exception as e:
            return Result(success=False, message=f"Declare failed: {e}", error=e)


__all__ = ['MemoryTransport', 'MemoryReceipt']
