"""
Processor Adapter Interface for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Processors transform data in-flight with optional filtering.
"""

from abc import abstractmethod
from typing import Protocol

from ..models import DataStream, Event
from .adapter import IAdapter


class IProcessor(IAdapter, Protocol):
    """
    Processor adapter interface - transforms data in-flight.

    Constitutional requirements:
    - FR-012: Processor adapters MUST transform data and MAY filter by returning None
    - FR-052: Processors execute in sequential order within pipeline

    Example implementation:
        >>> class AnomalyFilterProcessor:
        ...     async def process(self, data: DataStream | Event) -> DataStream | Event | None:
        ...         if isinstance(data, DataStream):
        ...             # Filter anomalous readings
        ...             if self._is_anomalous(data):
        ...                 return None  # Drop this data
        ...
        ...             # Transform: convert units
        ...             converted_data = self._convert_units(data)
        ...
        ...             # Create new span for tracing
        ...             return DataStream(
        ...                 **{**converted_data.__dict__,
        ...                    'trace_context': data.trace_context.create_child_span()}
        ...             )
        ...         return data  # Pass through Events unchanged
    """

    @abstractmethod
    async def process(self, data: DataStream | Event) -> DataStream | Event | None:
        """
        Transform data in-flight, optionally filtering.

        Args:
            data: Input DataStream or Event

        Returns:
            - Transformed DataStream or Event, OR
            - None to filter out (drop) this data

        Preconditions:
            - Adapter in RUNNING state

        Postconditions:
            - If not None: data transformed with new trace span
            - If None: data filtered out
            - Original trace_id preserved (FR-032)

        Constitutional requirements:
            - FR-012: Transform data or filter by returning None
            - FR-032: Propagate trace IDs with minimal overhead
            - FR-052: Sequential execution order maintained

        Example:
            >>> async def process(self, data):
            ...     # Filter anomalous data
            ...     if is_anomalous(data):
            ...         return None
            ...
            ...     # Transform and create new span
            ...     return DataStream(
            ...         ...
            ...         trace_context=data.trace_context.create_child_span(),
            ...         ...
            ...     )
        """
        ...
