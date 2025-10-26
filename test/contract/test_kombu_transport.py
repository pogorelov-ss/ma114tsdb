"""
Contract test for Kombu transport implementation.

Tests that Kombu transport correctly implements ITransport interface.
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.kombu import KombuTransport
from components.core.models import (
    DataStream, Event, TraceContext, Version,
    RoutingInfo, SubscriptionPattern, ResourceSpec
)


class TestKombuTransportContract:
    """Test Kombu transport against ITransport contract."""

    @pytest.fixture
    def transport(self):
        """Provide Kombu transport instance with in-memory backend."""
        return KombuTransport()

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
    async def test_init_with_memory_backend(self, transport):
        """Test transport initialization with in-memory backend."""
        result = await transport.init({'url': 'memory://'})
        assert result.success

    @pytest.mark.asyncio
    async def test_init_and_start(self, transport):
        """Test transport initialization and start."""
        await transport.init({'url': 'memory://'})
        result = await transport.start()
        assert result.success

        health = await transport.health()
        assert health.status == "healthy"

    @pytest.mark.asyncio
    async def test_publish_event(self, transport, sample_event):
        """Test publishing Event with durable delivery."""
        await transport.init({'url': 'memory://'})
        await transport.start()

        routing = RoutingInfo(destination="test.events")
        result = await transport.publish(sample_event, routing)
        assert result.success

    @pytest.mark.asyncio
    async def test_declare_exchange(self, transport):
        """Test declaring exchange resource."""
        await transport.init({'url': 'memory://'})
        await transport.start()

        resource = ResourceSpec(
            name="test-exchange",
            type="exchange",
            properties={"type": "topic", "durable": True}
        )
        result = await transport.declare(resource)
        assert result.success

    @pytest.mark.asyncio
    async def test_metadata(self, transport):
        """Test transport metadata."""
        metadata = transport.metadata
        assert metadata.id == "kombu-transport"
        assert metadata.type.value == "transport"
        assert not metadata.capabilities.lossy_allowed  # Durable delivery

    @pytest.mark.asyncio
    async def test_stop_closes_connection(self, transport):
        """Test stop closes Kombu connection."""
        await transport.init({'url': 'memory://'})
        await transport.start()

        result = await transport.stop()
        assert result.success

        health = await transport.health()
        assert health.status == "unhealthy"  # Stopped
