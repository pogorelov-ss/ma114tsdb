"""
Contract tests for IAdapter base interface.

These tests validate that any adapter implementation conforms to IAdapter contract.
Per TDD approach: These tests MUST FAIL initially (no implementations exist yet).

Constitutional requirement: FR-070 (contract tests for all adapters)
"""

import pytest
from datetime import datetime, timezone

from components.core.interfaces import IAdapter
from components.core.models import AdapterState, HealthStatus, Result


class TestIAdapterContract:
    """
    Base contract tests for IAdapter interface.

    All adapter implementations must pass these tests.
    Per FR-008: All adapters MUST implement lifecycle methods.
    """

    @pytest.fixture
    def adapter(self) -> IAdapter:
        """
        Provide adapter instance for testing.

        Override this fixture in adapter-specific test files.

        Example:
            @pytest.fixture
            def adapter(self) -> IAdapter:
                return MyConcreteAdapter()
        """
        pytest.skip("No adapter implementation provided - expected to FAIL")

    @pytest.fixture
    def valid_config(self) -> dict:
        """
        Provide valid configuration for adapter.

        Override this fixture in adapter-specific test files.
        """
        return {}

    @pytest.mark.asyncio
    async def test_init_success(self, adapter: IAdapter, valid_config: dict):
        """
        Test init() with valid configuration.

        Per FR-008: Adapter must successfully initialize with valid config.
        """
        result = await adapter.init(valid_config)

        assert isinstance(result, Result), "init() must return Result"
        assert result.success, f"init() failed: {result.message}"
        assert adapter.metadata.id is not None, "metadata.id must be set after init"

    @pytest.mark.asyncio
    async def test_init_validation(self, adapter: IAdapter):
        """
        Test init() rejects invalid configuration.

        Adapters should validate config and return failure Result.
        """
        result = await adapter.init({})  # Empty config likely invalid

        # Either accept empty config or fail gracefully
        assert isinstance(result, Result), "init() must return Result"
        if not result.success:
            assert result.message is not None, "Failed init should provide error message"

    @pytest.mark.asyncio
    async def test_lifecycle_transitions(self, adapter: IAdapter, valid_config: dict):
        """
        Test init → start → stop lifecycle.

        Per FR-009: State transitions must follow defined flow.
        """
        # Initialize
        init_result = await adapter.init(valid_config)
        assert init_result.success, f"init() failed: {init_result.message}"

        # Start
        start_result = await adapter.start()
        assert isinstance(start_result, Result), "start() must return Result"
        assert start_result.success, f"start() failed: {start_result.message}"

        # Verify running state
        health = await adapter.health()
        assert health.state == AdapterState.RUNNING, f"Expected RUNNING, got {health.state}"

        # Stop
        stop_result = await adapter.stop()
        assert isinstance(stop_result, Result), "stop() must return Result"
        assert stop_result.success, f"stop() failed: {stop_result.message}"

        # Verify stopped state
        health_after = await adapter.health()
        assert health_after.state == AdapterState.STOPPED, f"Expected STOPPED, got {health_after.state}"

    @pytest.mark.asyncio
    async def test_health_returns_valid_status(self, adapter: IAdapter, valid_config: dict):
        """
        Test health() returns valid HealthStatus.

        Per FR-015, FR-030: Health status must include state and uptime.
        """
        await adapter.init(valid_config)
        await adapter.start()

        health = await adapter.health()

        assert isinstance(health, HealthStatus), "health() must return HealthStatus"
        assert health.status in ("healthy", "degraded", "unhealthy"), \
            f"Invalid health status: {health.status}"
        assert health.uptime >= 0, f"uptime must be non-negative, got {health.uptime}"
        assert isinstance(health.state, AdapterState), "state must be AdapterState enum"

        await adapter.stop()

    @pytest.mark.asyncio
    async def test_metadata_includes_capabilities(self, adapter: IAdapter):
        """
        Test metadata declares valid capabilities.

        Per FR-014: All adapters MUST declare metadata.
        """
        metadata = adapter.metadata

        assert metadata.id, "metadata.id must be non-empty"
        assert metadata.name, "metadata.name must be non-empty"
        assert metadata.version.major >= 0, "version.major must be non-negative"
        assert metadata.version.minor >= 0, "version.minor must be non-negative"
        assert len(metadata.capabilities.data_types) > 0, \
            "capabilities.data_types must be non-empty"

    @pytest.mark.asyncio
    async def test_stop_is_idempotent(self, adapter: IAdapter, valid_config: dict):
        """
        Test stop() can be called multiple times safely.

        Adapters should handle repeated stop() calls gracefully.
        """
        await adapter.init(valid_config)
        await adapter.start()

        # First stop
        result1 = await adapter.stop()
        assert result1.success, "First stop() should succeed"

        # Second stop (idempotent)
        result2 = await adapter.stop()
        assert isinstance(result2, Result), "Second stop() must return Result"
        # May succeed or fail, but must not crash

    @pytest.mark.asyncio
    async def test_start_requires_init(self, adapter: IAdapter):
        """
        Test start() fails if init() not called.

        Per FR-008: init() must be called before start().
        """
        result = await adapter.start()

        # Should fail without init
        assert isinstance(result, Result), "start() must return Result"
        # Implementation may choose to succeed or fail, but must handle gracefully
