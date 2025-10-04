"""
Unit tests for IPC transport.

Tests socket path handling and IPC-specific features.
"""

import pytest
from datetime import datetime, timezone

from components.transports.ipc import IPCTransport, IPCReceipt


class TestIPCTransportUnits:
    """Unit tests for IPC transport specifics."""

    @pytest.mark.asyncio
    async def test_default_socket_path(self):
        """Test default socket path configuration."""
        transport = IPCTransport()
        await transport.init({})
        assert transport._socket_path == '/var/run/ma114tsdb/ipc.sock'

    @pytest.mark.asyncio
    async def test_custom_socket_path(self):
        """Test custom socket path configuration."""
        transport = IPCTransport()
        await transport.init({'socket_path': '/custom/path.sock'})
        assert transport._socket_path == '/custom/path.sock'

    @pytest.mark.asyncio
    async def test_health_includes_socket_path(self):
        """Test health status includes socket path."""
        transport = IPCTransport()
        await transport.init({'socket_path': '/test/ipc.sock'})
        await transport.start()

        health = await transport.health()
        assert health.details['socket_path'] == '/test/ipc.sock'
        assert health.details['transport_type'] == 'ipc'

    @pytest.mark.asyncio
    async def test_receipt_creation(self):
        """Test IPC receipt creation."""
        receipt = IPCReceipt(
            receipt_id="ipc-123",
            timestamp=datetime.now(timezone.utc)
        )
        assert receipt.receipt_id == "ipc-123"
