"""
Contract test for Memory transport implementation.

Tests that Memory transport correctly implements ITransport interface.
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.memory import MemoryTransport
from components.core.models import (
    DataStream, Event, TraceContext, Version,
    RoutingInfo, SubscriptionPattern, ResourceSpec
)


class TestMemoryTransportContract:
    """Test Memory transport against ITransport contract."""

    @pytest.fixture
    def transport(self):
        """Provide Memory transport instance."""
        return MemoryTransport(buffer_size=100)

    @pytest.fixture
    def sample_datastream(self):
        """Provide sample DataStream."""
        return DataStream(
            stream_id="test-stream",
            source="test-producer",
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc),
            data_points=[1.0, 2.0, 3.0],
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(1, 0)
        )

    @pytest.fixture
    def sample_event(self):
        """Provide sample Event."""
        return Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test-producer",
            event_type="test",
            data={"test": "data"},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(1, 0)
        )

    @pytest.mark.asyncio
    async def test_init_and_start(self, transport):
        """Test transport initialization and start."""
        result = await transport.init({})
        assert result.success

        result = await transport.start()
        assert result.success

        health = await transport.health()
        assert health.status == "healthy"

    @pytest.mark.asyncio
    async def test_publish_datastream(self, transport, sample_datastream):
        """Test publishing DataStream."""
        await transport.init({})
        await transport.start()

        routing = RoutingInfo(destination="test-topic")
        result = await transport.publish(sample_datastream, routing)
        assert result.success

    @pytest.mark.asyncio
    async def test_publish_event(self, transport, sample_event):
        """Test publishing Event."""
        await transport.init({})
        await transport.start()

        routing = RoutingInfo(destination="events")
        result = await transport.publish(sample_event, routing)
        assert result.success

    @pytest.mark.asyncio
    async def test_publish_subscribe_roundtrip(self, transport, sample_event):
        """Test publish-subscribe roundtrip."""
        await transport.init({})
        await transport.start()

        # Publish event
        routing = RoutingInfo(destination="test-topic")
        await transport.publish(sample_event, routing)

        # Subscribe and receive
        pattern = SubscriptionPattern("test-topic")
        async for received_event in transport.subscribe(pattern):
            assert received_event.event_id == sample_event.event_id
            break  # Received one event, done

    @pytest.mark.asyncio
    async def test_declare_resource(self, transport):
        """Test declaring topic resource."""
        await transport.init({})
        await transport.start()

        resource = ResourceSpec(name="my-topic", type="topic")
        result = await transport.declare(resource)
        assert result.success

    @pytest.mark.asyncio
    async def test_buffer_overflow_handling(self, transport, sample_datastream):
        """Test buffer overflow drops data (best-effort)."""
        small_buffer_transport = MemoryTransport(buffer_size=2)
        await small_buffer_transport.init({})
        await small_buffer_transport.start()

        routing = RoutingInfo(destination="small-topic")

        # Fill buffer
        result1 = await small_buffer_transport.publish(sample_datastream, routing)
        assert result1.success

        result2 = await small_buffer_transport.publish(sample_datastream, routing)
        assert result2.success

        # Overflow should fail (buffer full)
        result3 = await small_buffer_transport.publish(sample_datastream, routing)
        assert not result3.success
        assert "full" in result3.message.lower()

    @pytest.mark.asyncio
    async def test_metadata(self, transport):
        """Test transport metadata."""
        metadata = transport.metadata
        assert metadata.id == "memory-transport"
        assert metadata.type.value == "transport"
        assert len(metadata.capabilities.data_types) == 2

    @pytest.mark.asyncio
    async def test_stop_flushes_buffers(self, transport, sample_event):
        """Test stop clears topic queues."""
        await transport.init({})
        await transport.start()

        # Publish some data
        routing = RoutingInfo(destination="flush-topic")
        await transport.publish(sample_event, routing)

        # Stop should flush
        result = await transport.stop()
        assert result.success

        health = await transport.health()
        assert health.status == "unhealthy"  # Stopped
