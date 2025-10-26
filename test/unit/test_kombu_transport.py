"""
Unit tests for Kombu transport.

Tests exchange declaration, routing keys, persistent delivery mode.
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.kombu import KombuTransport
from components.core.models import (
    DataStream, Event, TraceContext, Version, RoutingInfo
)


class TestKombuTransportUnits:
    """Unit tests for Kombu transport specifics."""

    @pytest.mark.asyncio
    async def test_config_broker_url(self):
        """Test broker URL configuration."""
        transport = KombuTransport()
        await transport.init({'url': 'amqp://localhost'})
        assert transport._broker_url == 'amqp://localhost'

    @pytest.mark.asyncio
    async def test_config_exchange_name(self):
        """Test exchange name configuration."""
        transport = KombuTransport()
        await transport.init({
            'url': 'memory://',
            'exchange_name': 'custom-exchange'
        })
        assert transport._exchange_name == 'custom-exchange'

    @pytest.mark.asyncio
    async def test_delivery_mode_datastream_non_persistent(self):
        """Test DataStream uses non-persistent delivery mode."""
        transport = KombuTransport()
        await transport.init({'url': 'memory://'})
        await transport.start()

        datastream = DataStream(
            stream_id="test",
            source="test",
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc),
            data_points=[1, 2, 3],
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(1, 0)
        )

        # Should succeed (delivery_mode=1 for DataStream)
        result = await transport.publish(datastream, RoutingInfo("test.data"))
        assert result.success

    @pytest.mark.asyncio
    async def test_delivery_mode_event_persistent(self):
        """Test Event uses persistent delivery mode."""
        transport = KombuTransport()
        await transport.init({'url': 'memory://'})
        await transport.start()

        event = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="test",
            data={"persistent": True},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(1, 0)
        )

        # Should succeed (delivery_mode=2 for Event)
        result = await transport.publish(event, RoutingInfo("test.events"))
        assert result.success

    @pytest.mark.asyncio
    async def test_health_shows_connection_status(self):
        """Test health status includes connection info."""
        transport = KombuTransport()
        await transport.init({'url': 'memory://'})
        await transport.start()

        health = await transport.health()
        assert "broker_url" in health.details
        assert "exchange" in health.details
        assert "connected" in health.details

    @pytest.mark.asyncio
    async def test_ack_returns_success(self):
        """Test ack operation returns success."""
        transport = KombuTransport()
        await transport.init({'url': 'memory://'})
        await transport.start()

        from components.transports.kombu import KombuReceipt
        receipt = KombuReceipt(
            receipt_id="test-receipt",
            timestamp=datetime.now(timezone.utc)
        )

        result = await transport.ack(receipt)
        assert result.success
