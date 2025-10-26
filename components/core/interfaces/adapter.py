"""
Base Adapter Interface for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Uniform lifecycle methods for all adapter types.
"""

from abc import abstractmethod
from typing import Any, Protocol

from ..models import AdapterMetadata, HealthStatus, Result


class IAdapter(Protocol):
    """
    Base adapter interface - ALL adapters must implement these methods.

    Lifecycle: init() → start() → [running] → stop()
    Error state can occur at any time.

    Constitutional requirements:
    - FR-008: All adapters MUST implement lifecycle methods
    - FR-009: State transitions: init → running → stopped, error possible
    - FR-014: All adapters MUST declare metadata

    Example:
        >>> class MyAdapter:
        ...     async def init(self, config: dict) -> Result:
        ...         # Initialize resources
        ...         return Result.ok("Initialized")
        ...
        ...     async def start(self) -> Result:
        ...         # Start processing
        ...         return Result.ok("Started")
        ...
        ...     async def stop(self) -> Result:
        ...         # Cleanup and shutdown
        ...         return Result.ok("Stopped")
        ...
        ...     async def health(self) -> HealthStatus:
        ...         return HealthStatus(
        ...             status="healthy",
        ...             uptime=100.0,
        ...             state=AdapterState.RUNNING
        ...         )
        ...
        ...     @property
        ...     def metadata(self) -> AdapterMetadata:
        ...         return AdapterMetadata(...)
    """

    @abstractmethod
    async def init(self, config: dict[str, Any]) -> Result:
        """
        Initialize adapter with configuration.

        Args:
            config: Adapter-specific configuration dictionary

        Returns:
            Result indicating success/failure

        Postconditions:
            - State transitions to INIT if successful
            - Resources allocated but not active
            - Config validated and stored

        Example:
            >>> result = await adapter.init({"url": "http://sensor/data"})
            >>> assert result.success
        """
        ...

    @abstractmethod
    async def start(self) -> Result:
        """
        Start adapter operation.

        Preconditions:
            - init() must have been called successfully

        Returns:
            Result indicating success/failure

        Postconditions:
            - State transitions to RUNNING if successful
            - Adapter actively processing data
            - Resources fully initialized

        Per FR-036: May retry with exponential backoff (1s, 5s, 30s) on failure.

        Example:
            >>> result = await adapter.start()
            >>> assert result.success
        """
        ...

    @abstractmethod
    async def stop(self) -> Result:
        """
        Gracefully stop adapter.

        Returns:
            Result indicating success/failure

        Postconditions:
            - State transitions to STOPPED
            - In-flight operations completed
            - Resources released
            - Buffers flushed (FR-043)

        Example:
            >>> result = await adapter.stop()
            >>> assert result.success
        """
        ...

    @abstractmethod
    async def health(self) -> HealthStatus:
        """
        Get current health status.

        Returns:
            HealthStatus with current state and diagnostics

        Constitutional requirement:
            - FR-015: Adapters MAY emit health status as Events
            - FR-030: Must include adapter-state metric

        Example:
            >>> health = await adapter.health()
            >>> assert health.status in ("healthy", "degraded", "unhealthy")
        """
        ...

    @property
    @abstractmethod
    def metadata(self) -> AdapterMetadata:
        """
        Adapter metadata for discovery and capability negotiation.

        Constitutional requirement:
            - FR-014: All adapters MUST declare metadata

        Returns:
            AdapterMetadata with id, name, version, type, capabilities

        Example:
            >>> meta = adapter.metadata
            >>> assert meta.id == "temp-sensor-01"
        """
        ...
