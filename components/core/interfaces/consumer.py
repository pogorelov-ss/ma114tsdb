"""
Consumer Adapter Interface for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Consumers receive and process DataStreams or Events.
"""

from abc import abstractmethod
from typing import Protocol

from ..models import DataStream, Event, Result
from .adapter import IAdapter


class IConsumer(IAdapter, Protocol):
    """
    Consumer adapter interface - receives and processes data.

    Constitutional requirements:
    - FR-011: Consumer adapters MUST receive and process DataStreams or Events
    - FR-040: System MUST never block pipeline on bad data

    Example implementation:
        >>> class TimescaleDBConsumer:
        ...     async def consume(self, data: DataStream | Event) -> Result:
        ...         try:
        ...             if isinstance(data, DataStream):
        ...                 await self._insert_datastream(data)
        ...             else:
        ...                 await self._insert_event(data)
        ...             return Result.ok()
        ...         except Exception as e:
        ...             # Per FR-040: Never block pipeline
        ...             logger.error(f"Failed to consume: {e}")
        ...             if self._dlq_enabled:
        ...                 await self._send_to_dlq(data, e)
        ...             return Result.fail(str(e), e)
    """

    @abstractmethod
    async def consume(self, data: DataStream | Event) -> Result:
        """
        Process incoming DataStream or Event.

        Args:
            data: DataStream or Event to process

        Returns:
            Result indicating success/failure

        Preconditions:
            - Adapter in RUNNING state
            - data has valid TraceContext and version

        Postconditions:
            - Data processed or error logged
            - If DLQ configured (FR-041): failed data sent to DLQ
            - If no DLQ (FR-042): failed data skipped with error log
            - Pipeline never blocked (FR-040)

        Constitutional requirements:
            - FR-011: Receive and process DataStreams or Events
            - FR-040: Never block pipeline on bad data
            - FR-047: Handle older data format versions (N-1 compatibility)

        Example:
            >>> result = await consumer.consume(event)
            >>> if not result.success:
            ...     logger.error(f"Failed to consume: {result.message}")
            ...     # Error logged, processing continues
        """
        ...
