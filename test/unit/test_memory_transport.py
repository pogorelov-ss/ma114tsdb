"""
Unit tests for Memory transport.

Tests in-memory queue operations and buffer overflow handling.
"""

import pytest
import asyncio
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.memory import MemoryTransport
from components.core.models import (
    Event, TraceContext, Version, RoutingInfo, SubscriptionPattern
)


class TestMemoryTransportUnits:
    """Unit tests for Memory transport specifics."""

    @pytest.mark.asyncio
    async def test_multiple_topics_isolation(self):
        """Test that different topics are isolated."""
        transport = MemoryTransport()
        await transport.init({})
        await transport.start()

        event1 = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="type1",
            data={"topic": 1},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(1, 0)
        )

        event2 = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="type2",
            data={"topic": 2},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(1, 0)
        )

        # Publish to different topics
        await transport.publish(event1, RoutingInfo("topic1"))
        await transport.publish(event2, RoutingInfo("topic2"))

        # Subscribe to topic1 only
        pattern = SubscriptionPattern("topic1")
        async for received in transport.subscribe(pattern):
            assert received.event_id == event1.event_id
            assert received.data["topic"] == 1
            break

    @pytest.mark.asyncio
    async def test_wildcard_subscription(self):
        """Test wildcard subscription receives from all topics."""
        transport = MemoryTransport()
        await transport.init({})
        await transport.start()

        events = []
        for i in range(3):
            event = Event(
                event_id=uuid4(),
                timestamp=datetime.now(timezone.utc),
                source="test",
                event_type=f"type{i}",
                data={"index": i},
                trace_context=TraceContext(uuid4(), uuid4()),
                version=Version(1, 0)
            )
            events.append(event)
            await transport.publish(event, RoutingInfo(f"topic{i}"))

        # Subscribe to all topics
        received_count = 0
        pattern = SubscriptionPattern("*")
        async for received in transport.subscribe(pattern):
            received_count += 1
            if received_count == 3:
                break

        assert received_count == 3

    @pytest.mark.asyncio
    async def test_fifo_ordering(self):
        """Test FIFO ordering within topic."""
        transport = MemoryTransport()
        await transport.init({})
        await transport.start()

        # Publish events in order
        event_ids = []
        for i in range(5):
            event_id = uuid4()
            event_ids.append(event_id)
            event = Event(
                event_id=event_id,
                timestamp=datetime.now(timezone.utc),
                source="test",
                event_type="test",
                data={"order": i},
                trace_context=TraceContext(uuid4(), uuid4()),
                version=Version(1, 0)
            )
            await transport.publish(event, RoutingInfo("ordered-topic"))

        # Receive in same order
        received_ids = []
        pattern = SubscriptionPattern("ordered-topic")
        async for received in transport.subscribe(pattern):
            received_ids.append(received.event_id)
            if len(received_ids) == 5:
                break

        assert received_ids == event_ids

    @pytest.mark.asyncio
    async def test_config_buffer_size(self):
        """Test buffer size configuration."""
        transport = MemoryTransport()
        await transport.init({"buffer_size": 50})
        assert transport.buffer_size == 50

    @pytest.mark.asyncio
    async def test_health_details(self):
        """Test health status includes topic details."""
        transport = MemoryTransport()
        await transport.init({})
        await transport.start()

        # Declare some topics
        await transport.publish(
            Event(
                event_id=uuid4(),
                timestamp=datetime.now(timezone.utc),
                source="test",
                event_type="test",
                data={"test": "data"},
                trace_context=TraceContext(uuid4(), uuid4()),
                version=Version(1, 0)
            ),
            RoutingInfo("topic1")
        )

        health = await transport.health()
        assert "topics" in health.details
        assert "topic1" in health.details["topics"]
        assert health.details["buffer_size"] == 1000  # default

    @pytest.mark.asyncio
    async def test_ack_is_noop(self):
        """Test ack operation is no-op for memory transport."""
        transport = MemoryTransport()
        await transport.init({})
        await transport.start()

        from components.transports.memory import MemoryReceipt
        receipt = MemoryReceipt(
            receipt_id="test",
            timestamp=datetime.now(timezone.utc)
        )

        result = await transport.ack(receipt)
        assert result.success
        assert "no-op" in result.message.lower()
