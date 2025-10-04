"""
Transport Layer Types for ma114tsdb

Constitutional principle: II. Everything is an Adapter
Transport-agnostic routing and subscription abstractions.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class RoutingInfo:
    """
    Transport-agnostic routing destination.

    Per FR-019: Flexible routing via destination strings.

    Attributes:
        destination: Transport-specific destination string
        properties: Transport-specific metadata (optional)

    Transport-specific interpretations:
        - Memory: destination = topic name (string)
        - IPC: destination = socket path (e.g., "/var/run/ma114tsdb/sensor-data")
        - MQTT: destination = topic hierarchy (e.g., "sensors/temperature/room1")
        - Kombu: destination = exchange + routing key (e.g., "ma114tsdb:sensor.temp.room1")

    Example:
        >>> # MQTT routing
        >>> RoutingInfo(
        ...     destination="sensors/temperature/room1",
        ...     properties={"qos": 1, "retain": False}
        ... )

        >>> # Kombu routing
        >>> RoutingInfo(
        ...     destination="ma114tsdb:sensor.temp.room1",
        ...     properties={"delivery_mode": 2}  # persistent
        ... )
    """

    destination: str
    properties: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate routing info."""
        if not self.destination:
            raise ValueError("destination must be non-empty")


@dataclass
class SubscriptionPattern:
    """
    Transport-agnostic subscription pattern.

    Per FR-019: Pattern-based subscriptions with transport-specific matching.

    Attributes:
        pattern: Transport-specific pattern string
        properties: Transport-specific options (optional)

    Transport-specific interpretations:
        - Memory: pattern = exact topic name or "*" wildcard
        - IPC: pattern = socket path glob (e.g., "/var/run/ma114tsdb/*.sock")
        - MQTT: pattern = topic wildcards (+ single-level, # multi-level)
        - Kombu: pattern = routing key pattern (e.g., "sensor.*.room1")

    Example:
        >>> # MQTT wildcard subscription
        >>> SubscriptionPattern(
        ...     pattern="sensors/#",
        ...     properties={"qos": 1}
        ... )

        >>> # Kombu routing key pattern
        >>> SubscriptionPattern(
        ...     pattern="sensor.*.room1",
        ...     properties={"exclusive": False}
        ... )
    """

    pattern: str
    properties: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate subscription pattern."""
        if not self.pattern:
            raise ValueError("pattern must be non-empty")


@dataclass
class Receipt:
    """
    Acknowledgment receipt for message delivery.

    Per FR-013: Transport adapters provide ack operation for durable delivery.

    Attributes:
        receipt_id: Transport-specific receipt identifier
        timestamp: When receipt was issued

    Used for Event delivery acknowledgment (at-least-once guarantee per FR-022).
    """

    receipt_id: str
    timestamp: datetime

    def __post_init__(self) -> None:
        """Validate receipt."""
        if not self.receipt_id:
            raise ValueError("receipt_id must be non-empty")


@dataclass
class ResourceSpec:
    """
    Transport resource declaration.

    Per FR-013: Transport adapters provide declare operation for resource setup.

    Attributes:
        name: Resource name (e.g., topic, queue, exchange)
        type: Resource type ("topic" | "queue" | "exchange" | transport-specific)
        properties: Resource configuration (optional)

    Example:
        >>> # MQTT topic
        >>> ResourceSpec(
        ...     name="sensors/temperature",
        ...     type="topic",
        ...     properties={"qos": 1}
        ... )

        >>> # Kombu exchange
        >>> ResourceSpec(
        ...     name="ma114tsdb",
        ...     type="exchange",
        ...     properties={"type": "topic", "durable": True}
        ... )

        >>> # Kombu queue
        >>> ResourceSpec(
        ...     name="events-queue",
        ...     type="queue",
        ...     properties={"durable": True, "auto_delete": False}
        ... )
    """

    name: str
    type: str
    properties: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate resource spec."""
        if not self.name:
            raise ValueError("name must be non-empty")
        if not self.type:
            raise ValueError("type must be non-empty")
