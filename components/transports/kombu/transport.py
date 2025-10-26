"""
Kombu Transport Implementation

Kombu transport for AMQP, Redis, SQS with configurable backends.
Based on FR-013: Transport interface requirements.
"""

from datetime import datetime, timezone
from typing import Union, AsyncIterator, Optional
from uuid import uuid4
import asyncio

from kombu import Connection, Exchange, Queue, Producer, Consumer
from kombu.exceptions import KombuError

from components.core.interfaces import ITransport, Receipt, ResourceSpec
from components.core.models import (
    DataStream, Event, RoutingInfo, SubscriptionPattern,
    Result, HealthStatus, AdapterState, AdapterMetadata,
    AdapterType, AdapterCapabilities, DataType, Version
)
from components.serialization.msgpack import MessagePackSerializer


class KombuReceipt(Receipt):
    """Receipt for Kombu transport."""
    def __init__(self, receipt_id: str, timestamp: datetime):
        self.receipt_id = receipt_id
        self.timestamp = timestamp


class KombuTransport(ITransport):
    """
    Kombu transport with pluggable backends.

    Supports:
    - AMQP (RabbitMQ)
    - Redis
    - SQS
    - In-memory (for testing)
    """

    def __init__(self):
        """Initialize Kombu transport."""
        self._connection: Optional[Connection] = None
        self._exchange: Optional[Exchange] = None
        self._state = AdapterState.INIT
        self._start_time = None
        self._broker_url = None
        self._exchange_name = "ma114tsdb"
        self._exchange_type = "topic"

    async def init(self, config: dict) -> Result:
        """Initialize transport with configuration."""
        try:
            self._broker_url = config.get('url', 'memory://')
            self._exchange_name = config.get('exchange_name', 'ma114tsdb')
            self._exchange_type = config.get('exchange_type', 'topic')

            # Create connection
            self._connection = Connection(self._broker_url)

            # Create exchange
            self._exchange = Exchange(
                name=self._exchange_name,
                type=self._exchange_type,
                durable=True
            )

            self._state = AdapterState.INIT
            return Result(success=True, message="Kombu transport initialized")
        except Exception as e:
            return Result(success=False, message=f"Init failed: {e}", error=e)

    async def start(self) -> Result:
        """Start transport operation."""
        try:
            # Establish connection
            self._connection.connect()
            self._state = AdapterState.RUNNING
            self._start_time = datetime.now(timezone.utc)
            return Result(success=True, message="Kombu transport started")
        except Exception as e:
            self._state = AdapterState.ERROR
            return Result(success=False, message=f"Start failed: {e}", error=e)

    async def stop(self) -> Result:
        """Stop transport and close connection."""
        try:
            if self._connection:
                self._connection.release()
            self._state = AdapterState.STOPPED
            return Result(success=True, message="Kombu transport stopped")
        except Exception as e:
            return Result(success=False, message=f"Stop failed: {e}", error=e)

    async def health(self) -> HealthStatus:
        """Get transport health status."""
        uptime = 0.0
        if self._start_time:
            uptime = (datetime.now(timezone.utc) - self._start_time).total_seconds()

        is_connected = self._connection and self._connection.connected

        return HealthStatus(
            status="healthy" if (self._state == AdapterState.RUNNING and is_connected) else "unhealthy",
            uptime=uptime,
            state=self._state,
            details={
                "broker_url": self._broker_url,
                "exchange": self._exchange_name,
                "connected": is_connected
            }
        )

    @property
    def metadata(self) -> AdapterMetadata:
        """Transport metadata."""
        return AdapterMetadata(
            id="kombu-transport",
            name="Kombu Transport",
            version=Version(major=1, minor=0),
            type=AdapterType.TRANSPORT,
            capabilities=AdapterCapabilities(
                data_types={DataType.DATASTREAM, DataType.EVENT},
                ordering_guarantee=True,  # FIFO with routing key
                lossy_allowed=False,  # Durable delivery
                throughput_hint=5000
            )
        )

    async def publish(self, data: Union[DataStream, Event], routing: RoutingInfo) -> Result:
        """
        Publish data to exchange with routing key.

        Args:
            data: DataStream or Event to publish
            routing: Routing info with destination as routing key

        Returns:
            Result indicating success/failure
        """
        try:
            routing_key = routing.destination

            # Serialize data
            wire_bytes = MessagePackSerializer.serialize(data)

            # Publish with Kombu
            with Producer(self._connection) as producer:
                # DataStream: best-effort (non-persistent)
                # Event: durable (persistent)
                delivery_mode = 1 if isinstance(data, DataStream) else 2

                producer.publish(
                    wire_bytes,
                    exchange=self._exchange,
                    routing_key=routing_key,
                    delivery_mode=delivery_mode,
                    content_type='application/msgpack'
                )

            return Result(success=True, message=f"Published to routing key: {routing_key}")
        except KombuError as e:
            return Result(success=False, message=f"Kombu publish failed: {e}", error=e)
        except Exception as e:
            return Result(success=False, message=f"Publish failed: {e}", error=e)

    async def subscribe(self, pattern: SubscriptionPattern) -> AsyncIterator[Union[DataStream, Event]]:
        """
        Subscribe to routing key pattern.

        Args:
            pattern: Subscription pattern (routing key with wildcards)

        Yields:
            DataStreams or Events from matching routing keys
        """
        routing_pattern = pattern.pattern

        # Create queue bound to exchange with routing pattern
        queue_name = f"ma114tsdb-{uuid4()}"
        queue = Queue(
            name=queue_name,
            exchange=self._exchange,
            routing_key=routing_pattern,
            auto_delete=True
        )

        try:
            with Consumer(
                self._connection,
                queues=[queue],
                callbacks=[self._process_message],
                accept=['application/msgpack']
            ):
                # Message processing loop
                while self._state == AdapterState.RUNNING:
                    try:
                        # Drain events (non-blocking)
                        self._connection.drain_events(timeout=0.1)

                        # Yield received messages
                        if hasattr(self, '_received_data'):
                            for data in self._received_data:
                                yield data
                            self._received_data.clear()

                        await asyncio.sleep(0.01)
                    except Exception:
                        await asyncio.sleep(0.1)
        except Exception:
            pass

    def _process_message(self, body, message):
        """Process received message callback."""
        try:
            # Deserialize
            data = MessagePackSerializer.deserialize(body)

            # Store for yielding
            if not hasattr(self, '_received_data'):
                self._received_data = []
            self._received_data.append(data)

            # Acknowledge message
            message.ack()
        except Exception:
            # Reject message on error
            message.reject()

    async def ack(self, receipt: Receipt) -> Result:
        """
        Acknowledge message delivery.

        Note: Kombu auto-acks in callback, so this is informational.
        """
        return Result(success=True, message=f"Ack for receipt: {receipt.receipt_id}")

    async def declare(self, resource: ResourceSpec) -> Result:
        """
        Declare exchange resource.

        Args:
            resource: Resource spec with exchange name and properties

        Returns:
            Result indicating success/failure
        """
        try:
            exchange_name = resource.name
            exchange_type = resource.properties.get('type', 'topic') if resource.properties else 'topic'
            durable = resource.properties.get('durable', True) if resource.properties else True

            exchange = Exchange(
                name=exchange_name,
                type=exchange_type,
                durable=durable
            )

            # Declare exchange
            with self._connection.channel() as channel:
                exchange.declare(channel=channel)

            return Result(success=True, message=f"Exchange declared: {exchange_name}")
        except Exception as e:
            return Result(success=False, message=f"Declare failed: {e}", error=e)


__all__ = ['KombuTransport', 'KombuReceipt']
