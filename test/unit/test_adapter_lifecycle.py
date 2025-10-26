"""
Unit tests for AdapterLifecycle

Tests state transitions, exponential backoff delays, circuit breaker after 3 failures.

Version: 1.0.0
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, Mock, patch
import time

from components.pipeline.lifecycle.adapter_lifecycle import AdapterLifecycle
from components.core.interfaces.adapter import IAdapter
from components.core.models.adapter_metadata import (
    Result, HealthStatus, AdapterState, AdapterMetadata,
    AdapterCapabilities, AdapterType, DataType
)
from components.core.versioning.version import Version


# Test fixtures

@pytest.fixture
def mock_adapter():
    """Create a mock adapter for testing."""
    adapter = AsyncMock(spec=IAdapter)
    adapter.metadata = AdapterMetadata(
        id="test-adapter",
        name="Test Adapter",
        version=Version(major=1, minor=0),
        type=AdapterType.PRODUCER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    return adapter


@pytest.fixture
def lifecycle(mock_adapter):
    """Create AdapterLifecycle instance with mock adapter."""
    return AdapterLifecycle(
        adapter=mock_adapter,
        metrics_collector=None,
        emit_health_events=False  # Disable for testing
    )


# Test cases

@pytest.mark.asyncio
async def test_initialize_success(lifecycle, mock_adapter):
    """Test successful adapter initialization."""
    config = {"test": "config"}
    mock_adapter.init.return_value = Result(success=True)

    result = await lifecycle.initialize(config)

    assert result.success
    assert lifecycle.state == AdapterState.INIT
    mock_adapter.init.assert_called_once_with(config)


@pytest.mark.asyncio
async def test_initialize_failure(lifecycle, mock_adapter):
    """Test adapter initialization failure."""
    config = {"test": "config"}
    mock_adapter.init.return_value = Result(
        success=False,
        message="Init failed"
    )

    result = await lifecycle.initialize(config)

    assert not result.success
    assert result.message == "Init failed"


@pytest.mark.asyncio
async def test_start_success_on_first_attempt(lifecycle, mock_adapter):
    """Test successful start on first attempt."""
    mock_adapter.start.return_value = Result(success=True)

    result = await lifecycle.start_with_retry()

    assert result.success
    assert lifecycle.state == AdapterState.RUNNING
    assert lifecycle.retry_count == 0
    mock_adapter.start.assert_called_once()


@pytest.mark.asyncio
async def test_exponential_backoff_delays(lifecycle, mock_adapter):
    """Test exponential backoff delays: 1s, 5s, 30s."""
    # Fail first two attempts, succeed on third
    mock_adapter.start.side_effect = [
        Result(success=False, message="Attempt 1 failed"),
        Result(success=False, message="Attempt 2 failed"),
        Result(success=True)
    ]

    start_time = time.time()
    result = await lifecycle.start_with_retry()
    elapsed = time.time() - start_time

    assert result.success
    assert lifecycle.state == AdapterState.RUNNING
    assert lifecycle.retry_count == 0  # Reset after success

    # Should have delays of 1s + 5s = 6s (approximately)
    assert 5.5 <= elapsed <= 7.0  # Allow some margin for timing


@pytest.mark.asyncio
async def test_circuit_breaker_after_3_failures(lifecycle, mock_adapter):
    """Test circuit breaker trips after 3 failures."""
    mock_adapter.start.return_value = Result(
        success=False,
        message="Start failed"
    )

    result = await lifecycle.start_with_retry()

    assert not result.success
    assert lifecycle.state == AdapterState.FAILED
    assert lifecycle.retry_count == 3
    assert "3 retry attempts" in result.message
    assert mock_adapter.start.call_count == 3


@pytest.mark.asyncio
async def test_state_transitions(lifecycle, mock_adapter):
    """Test state transitions: init → running → stopped."""
    # Initialize
    mock_adapter.init.return_value = Result(success=True)
    await lifecycle.initialize({})
    assert lifecycle.state == AdapterState.INIT

    # Start
    mock_adapter.start.return_value = Result(success=True)
    await lifecycle.start_with_retry()
    assert lifecycle.state == AdapterState.RUNNING

    # Stop
    mock_adapter.stop.return_value = Result(success=True)
    await lifecycle.stop()
    assert lifecycle.state == AdapterState.STOPPED


@pytest.mark.asyncio
async def test_error_state_during_retries(lifecycle, mock_adapter):
    """Test adapter enters ERROR state during retries."""
    # Fail all attempts
    mock_adapter.start.return_value = Result(
        success=False,
        message="Always fails"
    )

    # Start retry process in background
    task = asyncio.create_task(lifecycle.start_with_retry())

    # Give it time to fail first attempt
    await asyncio.sleep(0.5)

    # Should be in ERROR state during retries
    assert lifecycle.state == AdapterState.ERROR

    # Wait for completion
    await task

    # Should be FAILED after all retries
    assert lifecycle.state == AdapterState.FAILED


@pytest.mark.asyncio
async def test_stop_success(lifecycle, mock_adapter):
    """Test successful adapter stop."""
    mock_adapter.stop.return_value = Result(success=True)

    result = await lifecycle.stop()

    assert result.success
    assert lifecycle.state == AdapterState.STOPPED
    mock_adapter.stop.assert_called_once()


@pytest.mark.asyncio
async def test_stop_failure(lifecycle, mock_adapter):
    """Test adapter stop failure."""
    mock_adapter.stop.return_value = Result(
        success=False,
        message="Stop failed"
    )

    result = await lifecycle.stop()

    assert not result.success
    assert result.message == "Stop failed"


@pytest.mark.asyncio
async def test_health_check_success(lifecycle, mock_adapter):
    """Test successful health check."""
    expected_health = HealthStatus(
        status="healthy",
        uptime=10.0,
        state=AdapterState.RUNNING
    )
    mock_adapter.health.return_value = expected_health

    health = await lifecycle.health()

    assert health == expected_health
    mock_adapter.health.assert_called_once()


@pytest.mark.asyncio
async def test_health_check_exception(lifecycle, mock_adapter):
    """Test health check returns degraded on exception."""
    mock_adapter.health.side_effect = Exception("Health check failed")

    health = await lifecycle.health()

    assert health.status == "degraded"
    assert "Health check failed" in health.details["error"]


@pytest.mark.asyncio
async def test_metrics_collector_integration(mock_adapter):
    """Test metrics collector state updates."""
    metrics_collector = Mock()
    lifecycle = AdapterLifecycle(
        adapter=mock_adapter,
        metrics_collector=metrics_collector,
        emit_health_events=False
    )

    # Start adapter
    mock_adapter.start.return_value = Result(success=True)
    await lifecycle.start_with_retry()

    # Should set state to RUNNING
    metrics_collector.set_adapter_state.assert_called_with(
        adapter_id="test-adapter",
        state=AdapterState.RUNNING
    )

    # Stop adapter
    mock_adapter.stop.return_value = Result(success=True)
    await lifecycle.stop()

    # Should set state to STOPPED
    metrics_collector.set_adapter_state.assert_called_with(
        adapter_id="test-adapter",
        state=AdapterState.STOPPED
    )


@pytest.mark.asyncio
async def test_exception_during_start(lifecycle, mock_adapter):
    """Test exception handling during start."""
    mock_adapter.start.side_effect = Exception("Unexpected error")

    result = await lifecycle.start_with_retry()

    assert not result.success
    assert lifecycle.state == AdapterState.FAILED
    assert mock_adapter.start.call_count == 3  # All retries exhausted


@pytest.mark.asyncio
async def test_retry_count_increments_on_failure(lifecycle, mock_adapter):
    """Test retry count increments on each failure."""
    call_count = 0

    def failing_start():
        nonlocal call_count
        call_count += 1
        # Verify retry_count matches call_count - 1 (before increment)
        assert lifecycle.retry_count == call_count - 1
        return Result(success=False, message=f"Attempt {call_count}")

    mock_adapter.start.side_effect = failing_start

    await lifecycle.start_with_retry()

    assert call_count == 3
    assert lifecycle.retry_count == 3


@pytest.mark.asyncio
async def test_uptime_tracking(lifecycle, mock_adapter):
    """Test uptime tracking after start."""
    mock_adapter.start.return_value = Result(success=True)

    start_time = time.time()
    await lifecycle.start_with_retry()

    await asyncio.sleep(0.1)  # Wait a bit

    uptime = time.time() - lifecycle.start_time
    assert uptime >= 0.1
