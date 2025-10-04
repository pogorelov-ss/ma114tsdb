"""
Producer Adapter Interface for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Producers generate and publish DataStreams or Events.
"""

from abc import abstractmethod
from typing import AsyncIterator, Protocol

from ..models import DataStream, Event
from .adapter import IAdapter


class IProducer(IAdapter, Protocol):
    """
    Producer adapter interface - generates and publishes data.

    Constitutional requirements:
    - FR-010: Producer adapters MUST generate and publish DataStreams or Events
    - FR-004: All data MUST carry TraceContext
    - FR-005: All data MUST carry version information

    Example implementation:
        >>> class TemperatureSensorProducer:
        ...     async def produce(self) -> AsyncIterator[DataStream]:
        ...         while self._running:
        ...             data = await self._fetch_sensor_data()
        ...             yield DataStream(
        ...                 stream_id="temp-sensor-01",
        ...                 source=self.metadata.id,
        ...                 timestamp_start=data.start_time,
        ...                 timestamp_end=data.end_time,
        ...                 data_points=data.readings,
        ...                 trace_context=TraceContext.new(),
        ...                 version=Version(1, 0)
        ...             )
        ...             await asyncio.sleep(1.0)
    """

    @abstractmethod
    async def produce(self) -> AsyncIterator[DataStream | Event]:
        """
        Generate stream of DataStreams or Events.

        Preconditions:
            - Adapter in RUNNING state

        Yields:
            DataStream or Event instances with:
            - Valid timestamps (sub-second precision, FR-002, FR-003)
            - TraceContext attached (FR-004)
            - Version information (FR-005)
            - Source set to adapter ID

        Constitutional requirements:
            - FR-010: Generate and publish DataStreams or Events
            - FR-026: Events delivered in FIFO order
            - FR-027: DataStreams prioritize latest data in time window

        Example:
            >>> async for data in producer.produce():
            ...     if isinstance(data, DataStream):
            ...         # Handle high-volume stream
            ...         await transport.publish(data, routing)
            ...     elif isinstance(data, Event):
            ...         # Handle critical event
            ...         await transport.publish(data, routing)
        """
        ...
        yield  # Make this a generator for type checking
