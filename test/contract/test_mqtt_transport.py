"""
Contract test for MQTT transport implementation.

Tests that MQTT transport correctly implements ITransport interface.
Note: Uses stub implementation (AWS Greengrass SDK not required for tests).
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.mqtt import MQTTTransport
from components.core.models import (
    DataStream, Event, TraceContext, Version, RoutingInfo, ResourceSpec
)


class TestMQTTTransportContract:
    """Test MQTT transport against ITransport contract (stub mode)."""

    @pytest.fixture
    def transport(self):
        """Provide MQTT transport instance."""
        return MQTTTransport()

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

    @pytest.mark.asyncio
    async def test_init_with_broker(self, transport):
        """Test transport initialization with broker URL."""
        result = await transport.init({'broker': 'mqtt://test-broker:1883'})
        assert result.success
        assert transport._broker_endpoint == 'mqtt://test-broker:1883'

    @pytest.mark.asyncio
    async def test_init_and_start(self, transport):
        """Test transport initialization and start (stub)."""
        await transport.init({})
        result = await transport.start()
        assert result.success

        health = await transport.health()
        assert health.status == "healthy"

    @pytest.mark.asyncio
    async def test_publish_event_qos1(self, transport, sample_event):
        """Test publishing Event with QoS 1 (stub)."""
        await transport.init({})
        await transport.start()

        routing = RoutingInfo(destination="sensors/temperature")
        result = await transport.publish(sample_event, routing)
        assert result.success
        assert "QoS=1" in result.message  # Events use QoS 1

    @pytest.mark.asyncio
    async def test_publish_datastream_qos0(self, transport, sample_datastream):
        """Test publishing DataStream with QoS 0 (stub)."""
        await transport.init({})
        await transport.start()

        routing = RoutingInfo(destination="sensors/data")
        result = await transport.publish(sample_datastream, routing)
        assert result.success
        assert "QoS=0" in result.message  # DataStreams use QoS 0

    @pytest.mark.asyncio
    async def test_declare_topic(self, transport):
        """Test declaring MQTT topic (stub)."""
        await transport.init({})
        await transport.start()

        resource = ResourceSpec(name="sensors/#", type="topic")
        result = await transport.declare(resource)
        assert result.success

    @pytest.mark.asyncio
    async def test_metadata(self, transport):
        """Test transport metadata."""
        metadata = transport.metadata
        assert metadata.id == "mqtt-transport"
        assert metadata.type.value == "transport"
        assert metadata.capabilities.lossy_allowed  # QoS 0 allows loss

    @pytest.mark.asyncio
    async def test_stop(self, transport):
        """Test stop disconnects MQTT (stub)."""
        await transport.init({})
        await transport.start()

        result = await transport.stop()
        assert result.success

        health = await transport.health()
        assert health.status == "unhealthy"  # Stopped
