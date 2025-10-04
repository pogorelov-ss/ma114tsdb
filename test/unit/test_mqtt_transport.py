"""
Unit tests for MQTT transport.

Tests QoS levels, topic wildcards, and MQTT-specific features.
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.mqtt import MQTTTransport, MQTTReceipt
from components.core.models import (
    DataStream, Event, TraceContext, Version
)


class TestMQTTTransportUnits:
    """Unit tests for MQTT transport specifics."""

    @pytest.mark.asyncio
    async def test_default_qos_levels(self):
        """Test default QoS level configuration."""
        transport = MQTTTransport()
        await transport.init({})
        assert transport._qos_datastream == 0
        assert transport._qos_event == 1

    @pytest.mark.asyncio
    async def test_custom_qos_levels(self):
        """Test custom QoS level configuration."""
        transport = MQTTTransport()
        await transport.init({
            'qos_datastream': 1,
            'qos_event': 2
        })
        assert transport._qos_datastream == 1
        assert transport._qos_event == 2

    @pytest.mark.asyncio
    async def test_broker_endpoint_config(self):
        """Test broker endpoint configuration."""
        transport = MQTTTransport()
        await transport.init({'broker': 'mqtt://prod-broker:1883'})
        assert transport._broker_endpoint == 'mqtt://prod-broker:1883'

    @pytest.mark.asyncio
    async def test_health_includes_qos_config(self):
        """Test health status includes QoS configuration."""
        transport = MQTTTransport()
        await transport.init({
            'broker': 'mqtt://test:1883',
            'qos_datastream': 0,
            'qos_event': 1
        })
        await transport.start()

        health = await transport.health()
        assert health.details['qos_datastream'] == 0
        assert health.details['qos_event'] == 1
        assert health.details['broker'] == 'mqtt://test:1883'

    @pytest.mark.asyncio
    async def test_receipt_creation(self):
        """Test MQTT receipt creation."""
        receipt = MQTTReceipt(
            receipt_id="mqtt-456",
            timestamp=datetime.now(timezone.utc)
        )
        assert receipt.receipt_id == "mqtt-456"
