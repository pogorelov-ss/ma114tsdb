"""
Contract test for IPC transport implementation.

Tests that IPC transport correctly implements ITransport interface.
Note: Uses stub implementation (AWS Greengrass SDK not required for tests).
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.transports.ipc import IPCTransport
from components.core.models import (
    Event, TraceContext, Version, RoutingInfo, ResourceSpec
)


class TestIPCTransportContract:
    """Test IPC transport against ITransport contract (stub mode)."""

    @pytest.fixture
    def transport(self):
        """Provide IPC transport instance."""
        return IPCTransport()

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
    async def test_init_with_socket_path(self, transport):
        """Test transport initialization with socket path."""
        result = await transport.init({'socket_path': '/tmp/test.sock'})
        assert result.success
        assert transport._socket_path == '/tmp/test.sock'

    @pytest.mark.asyncio
    async def test_init_and_start(self, transport):
        """Test transport initialization and start (stub)."""
        await transport.init({})
        result = await transport.start()
        assert result.success

        health = await transport.health()
        assert health.status == "healthy"

    @pytest.mark.asyncio
    async def test_publish_event(self, transport, sample_event):
        """Test publishing Event via IPC (stub)."""
        await transport.init({})
        await transport.start()

        routing = RoutingInfo(destination="ipc/events")
        result = await transport.publish(sample_event, routing)
        assert result.success

    @pytest.mark.asyncio
    async def test_declare_topic(self, transport):
        """Test declaring IPC topic (stub)."""
        await transport.init({})
        await transport.start()

        resource = ResourceSpec(name="ipc/data", type="topic")
        result = await transport.declare(resource)
        assert result.success

    @pytest.mark.asyncio
    async def test_metadata(self, transport):
        """Test transport metadata."""
        metadata = transport.metadata
        assert metadata.id == "ipc-transport"
        assert metadata.type.value == "transport"
        assert not metadata.capabilities.lossy_allowed  # Reliable IPC

    @pytest.mark.asyncio
    async def test_stop(self, transport):
        """Test stop closes IPC connection (stub)."""
        await transport.init({})
        await transport.start()

        result = await transport.stop()
        assert result.success

        health = await transport.health()
        assert health.status == "unhealthy"  # Stopped
