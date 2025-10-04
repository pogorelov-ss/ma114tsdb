"""
Transport Adapter Interface for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Transports move data between adapters with language-agnostic design.
"""

from abc import abstractmethod
from typing import AsyncIterator, Protocol

from ..models import DataStream, Event, Receipt, ResourceSpec, Result, RoutingInfo, SubscriptionPattern
from .adapter import IAdapter


class ITransport(IAdapter, Protocol):
    """
    Transport adapter interface - moves data between adapters.

    Constitutional requirements:
    - FR-013: Transport adapters MUST provide publish, subscribe, ack, declare
    - FR-016: Every pipeline MUST specify exactly one transport
    - FR-018: Transport layer MUST be language-agnostic
    - FR-054: Transport MUST handle backpressure

    Example implementation:
        >>> class MQTTTransport:
        ...     async def publish(self, data: DataStream | Event, routing: RoutingInfo) -> Result:
        ...         try:
        ...             # Serialize to MessagePack
        ...             payload = msgpack.packb({
        ...                 'type': 'datastream' if isinstance(data, DataStream) else 'event',
        ...                 'version': {'major': data.version.major, 'minor': data.version.minor},
        ...                 'trace_context': {...},
        ...                 'payload': {...}
        ...             })
        ...
        ...             # Determine QoS based on data type
        ...             qos = 0 if isinstance(data, DataStream) else 1
        ...
        ...             # Publish to MQTT topic
        ...             await self._mqtt_client.publish(
        ...                 topic=routing.destination,
        ...                 payload=payload,
        ...                 qos=qos
        ...             )
        ...             return Result.ok()
        ...         except Exception as e:
        ...             return Result.fail(str(e), e)
    """

    @abstractmethod
    async def publish(self, data: DataStream | Event, routing: RoutingInfo) -> Result:
        """
        Publish data to transport destination.

        Args:
            data: DataStream or Event to publish
            routing: Transport-specific routing information

        Returns:
            Result indicating delivery success/failure

        Preconditions:
            - Adapter in RUNNING state
            - routing.destination valid for this transport

        Postconditions:
            - DataStream: Best-effort delivery (FR-021, lossy allowed)
            - Event: Durable delivery with at-least-once guarantee (FR-022)

        Constitutional requirements:
            - FR-013: Provide publish operation
            - FR-021: DataStream delivery lossy-tolerant
            - FR-022: Event delivery durable, never lost
            - FR-062: Support per-adapter authentication tokens

        Example:
            >>> result = await transport.publish(
            ...     event,
            ...     RoutingInfo(destination="sensors/temperature/room1")
            ... )
        """
        ...

    @abstractmethod
    async def subscribe(self, pattern: SubscriptionPattern) -> AsyncIterator[DataStream | Event]:
        """
        Subscribe to data matching pattern.

        Args:
            pattern: Transport-specific subscription pattern

        Yields:
            DataStreams or Events matching subscription

        Preconditions:
            - Adapter in RUNNING state
            - pattern valid for this transport

        Postconditions:
            - Continuous stream of matching data
            - Backpressure handled by transport (FR-054)

        Constitutional requirements:
            - FR-013: Provide subscribe operation
            - FR-019: Support flexible pattern-based subscriptions
            - FR-054: Handle backpressure within pipeline

        Transport-specific patterns:
            - Memory: exact topic or "*" wildcard
            - IPC: socket path glob (e.g., "*.sock")
            - MQTT: topic wildcards (+ single, # multi-level)
            - Kombu: routing key patterns (e.g., "sensor.*.room1")

        Example:
            >>> async for data in transport.subscribe(SubscriptionPattern("sensors/#")):
            ...     # Process data from all sensor topics
            ...     await consumer.consume(data)
        """
        ...
        yield  # Make this a generator for type checking

    @abstractmethod
    async def ack(self, receipt: Receipt) -> Result:
        """
        Acknowledge message delivery for durable transports.

        Args:
            receipt: Receipt from previous publish/subscribe operation

        Returns:
            Result indicating acknowledgment success/failure

        Constitutional requirements:
            - FR-013: Provide ack operation
            - FR-022: Enable at-least-once delivery for Events

        Example:
            >>> # Consumer acknowledges after successful processing
            >>> result = await consumer.consume(event)
            >>> if result.success:
            ...     await transport.ack(receipt)
        """
        ...

    @abstractmethod
    async def declare(self, resource: ResourceSpec) -> Result:
        """
        Declare transport resource (topic, queue, exchange, etc.).

        Args:
            resource: Resource specification

        Returns:
            Result indicating declaration success/failure

        Constitutional requirements:
            - FR-013: Provide declare operation for resource setup

        Example:
            >>> await transport.declare(ResourceSpec(
            ...     name="ma114tsdb",
            ...     type="exchange",
            ...     properties={"type": "topic", "durable": True}
            ... ))
        """
        ...
