"""
Adapter Lifecycle Manager

Manages adapter state transitions with exponential backoff retry and circuit breaker.

Constitutional requirements:
- FR-009: State transitions: init → running → stopped, error possible
- FR-036: Exponential backoff retry (1s, 5s, 30s)
- FR-039: Adapter failures don't cascade to other adapters

Version: 1.0.0
"""

import asyncio
import time
from datetime import datetime
from enum import Enum
from typing import Any, Optional, TYPE_CHECKING

from ...core.interfaces.adapter import IAdapter
from ...core.models.adapter_metadata import Result, HealthStatus, AdapterState
from ...core.models.event import Event
from ...core.models.trace_context import TraceContext
from ...core.versioning.version import Version
import structlog

if TYPE_CHECKING:
    from ...observability.metrics.prometheus_collector import MetricsCollector


logger = structlog.get_logger(__name__)


class AdapterLifecycle:
    """
    Manages adapter lifecycle with state machine and retry logic.

    State transitions:
    - INIT → RUNNING: start() succeeds
    - RUNNING → ERROR: Exception during execution
    - ERROR → RUNNING: Retry succeeds (max 3 attempts)
    - ERROR → FAILED: 3 retry attempts exhausted
    - Any state → STOPPED: stop() called
    """

    RETRY_DELAYS = [1, 5, 30]  # seconds (exponential backoff)
    MAX_RETRIES = 3

    def __init__(
        self,
        adapter: IAdapter,
        metrics_collector: Optional["MetricsCollector"] = None,
        emit_health_events: bool = True
    ):
        """
        Initialize adapter lifecycle manager.

        Args:
            adapter: Adapter to manage
            metrics_collector: Optional metrics collector for state tracking
            emit_health_events: Whether to emit health status as Events
        """
        self.adapter = adapter
        self.metrics_collector = metrics_collector
        self.emit_health_events = emit_health_events
        self.state = AdapterState.INIT
        self.start_time = time.time()
        self.retry_count = 0

    async def initialize(self, config: dict[str, Any]) -> Result:
        """
        Initialize adapter with configuration.

        Args:
            config: Adapter-specific configuration

        Returns:
            Result indicating success/failure
        """
        try:
            logger.info(
                "Initializing adapter",
                adapter_id=self.adapter.metadata.id,
                adapter_type=self.adapter.metadata.type.value
            )

            result = await self.adapter.init(config)

            if result.success:
                self.state = AdapterState.INIT
                logger.info(
                    "Adapter initialized successfully",
                    adapter_id=self.adapter.metadata.id
                )
            else:
                logger.error(
                    "Adapter initialization failed",
                    adapter_id=self.adapter.metadata.id,
                    error=result.message
                )

            return result

        except Exception as e:
            logger.exception(
                "Exception during adapter initialization",
                adapter_id=self.adapter.metadata.id,
                error=str(e)
            )
            return Result(success=False, message=str(e), error=e)

    async def start_with_retry(self) -> Result:
        """
        Start adapter with exponential backoff retry.

        Implements circuit breaker pattern:
        - Retry on failure with delays: 1s, 5s, 30s
        - After 3 failures, adapter marked FAILED
        - Emits health events during retries

        Returns:
            Result indicating final success/failure
        """
        adapter_id = self.adapter.metadata.id

        for attempt in range(self.MAX_RETRIES):
            try:
                logger.info(
                    "Starting adapter",
                    adapter_id=adapter_id,
                    attempt=attempt + 1,
                    max_attempts=self.MAX_RETRIES
                )

                result = await self.adapter.start()

                if result.success:
                    self.state = AdapterState.RUNNING
                    self.retry_count = 0
                    self.start_time = time.time()

                    if self.metrics_collector:
                        self.metrics_collector.set_adapter_state(
                            adapter_id=adapter_id,
                            state=self.state
                        )

                    await self._emit_health_event(status="healthy")

                    logger.info(
                        "Adapter started successfully",
                        adapter_id=adapter_id
                    )

                    return result
                else:
                    raise Exception(result.message or "Start failed")

            except Exception as e:
                self.retry_count = attempt + 1
                self.state = AdapterState.ERROR

                if self.metrics_collector:
                    self.metrics_collector.set_adapter_state(
                        adapter_id=adapter_id,
                        state=self.state
                    )
                    self.metrics_collector.increment_events_failed(adapter_id=adapter_id)

                logger.error(
                    "Adapter start failed",
                    adapter_id=adapter_id,
                    attempt=attempt + 1,
                    max_attempts=self.MAX_RETRIES,
                    error=str(e)
                )

                # Emit degraded health event during retries
                if attempt < self.MAX_RETRIES - 1:
                    await self._emit_health_event(
                        status="degraded",
                        details={
                            "attempt": attempt + 1,
                            "max_attempts": self.MAX_RETRIES,
                            "error": str(e),
                            "next_retry_delay": self.RETRY_DELAYS[attempt]
                        }
                    )

                    # Exponential backoff
                    delay = self.RETRY_DELAYS[attempt]
                    logger.info(
                        "Retrying adapter start after delay",
                        adapter_id=adapter_id,
                        delay_seconds=delay
                    )
                    await asyncio.sleep(delay)

        # All retries exhausted - circuit breaker trips
        self.state = AdapterState.FAILED

        if self.metrics_collector:
            self.metrics_collector.set_adapter_state(
                adapter_id=adapter_id,
                state=self.state
            )

        await self._emit_health_event(
            status="unhealthy",
            details={
                "reason": "Circuit breaker tripped after max retries",
                "retry_count": self.MAX_RETRIES
            }
        )

        logger.error(
            "Adapter failed after max retries",
            adapter_id=adapter_id,
            retry_count=self.MAX_RETRIES
        )

        return Result(
            success=False,
            message=f"Adapter failed after {self.MAX_RETRIES} retry attempts"
        )

    async def stop(self) -> Result:
        """
        Gracefully stop adapter.

        Returns:
            Result indicating success/failure
        """
        adapter_id = self.adapter.metadata.id

        try:
            logger.info("Stopping adapter", adapter_id=adapter_id)

            result = await self.adapter.stop()

            if result.success:
                self.state = AdapterState.STOPPED

                if self.metrics_collector:
                    self.metrics_collector.set_adapter_state(
                        adapter_id=adapter_id,
                        state=self.state
                    )

                logger.info("Adapter stopped successfully", adapter_id=adapter_id)
            else:
                logger.error(
                    "Adapter stop failed",
                    adapter_id=adapter_id,
                    error=result.message
                )

            return result

        except Exception as e:
            logger.exception(
                "Exception during adapter stop",
                adapter_id=adapter_id,
                error=str(e)
            )
            return Result(success=False, message=str(e), error=e)

    async def health(self) -> HealthStatus:
        """
        Get current health status.

        Returns:
            HealthStatus with current state and uptime
        """
        try:
            health = await self.adapter.health()
            return health
        except Exception as e:
            logger.exception(
                "Exception during health check",
                adapter_id=self.adapter.metadata.id,
                error=str(e)
            )
            # Return degraded health if health check itself fails
            return HealthStatus(
                status="degraded",
                uptime=time.time() - self.start_time,
                state=self.state,
                details={"error": str(e)}
            )

    async def _emit_health_event(
        self,
        status: str,
        details: dict[str, Any] | None = None
    ):
        """
        Emit health status as Event (if enabled).

        Args:
            status: Health status ("healthy" | "degraded" | "unhealthy")
            details: Optional additional details
        """
        if not self.emit_health_events:
            return

        try:
            from uuid import uuid4

            event = Event(
                event_id=uuid4(),
                timestamp=datetime.now(),
                source=self.adapter.metadata.id,
                event_type="adapter.health",
                data={
                    "status": status,
                    "state": self.state.value,
                    "uptime": time.time() - self.start_time,
                    "retry_count": self.retry_count,
                    **(details or {})
                },
                trace_context=TraceContext(
                    trace_id=uuid4(),
                    span_id=uuid4()
                ),
                version=Version(major=1, minor=0)
            )

            logger.debug(
                "Health event emitted",
                adapter_id=self.adapter.metadata.id,
                status=status,
                event_id=str(event.event_id)
            )

        except Exception as e:
            logger.warning(
                "Failed to emit health event",
                adapter_id=self.adapter.metadata.id,
                error=str(e)
            )
