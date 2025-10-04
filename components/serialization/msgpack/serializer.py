"""
MessagePack Serializer for ma114tsdb

Implements envelope-based serialization with version and trace context.
Based on FR-018: Cross-language wire format compatibility.

Wire Format Envelope:
{
    "type": "datastream" | "event",
    "version": {"major": int, "minor": int},
    "trace_context": {
        "trace_id": "uuid-string",
        "span_id": "uuid-string",
        "parent_span": "uuid-string" | None
    },
    "payload": {...}
}
"""

import msgpack
from typing import Union
from datetime import datetime, timezone
from uuid import UUID
from components.core.models import DataStream, Event, TraceContext, Version


class VersionMismatchError(Exception):
    """Raised when data format version is incompatible."""
    pass


class MessagePackSerializer:
    """
    Serializes and deserializes DataStream/Event with MessagePack.

    Features:
    - Envelope-based format with version and trace context
    - Automatic version compatibility checking (N-1 support)
    - Sub-second timestamp precision preservation
    - UUID integrity maintenance
    """

    CURRENT_VERSION = Version(major=1, minor=0)

    @staticmethod
    def serialize(data: Union[DataStream, Event]) -> bytes:
        """
        Serialize DataStream or Event to MessagePack bytes.

        Args:
            data: DataStream or Event to serialize

        Returns:
            MessagePack-encoded bytes with envelope structure
        """
        # Determine data type
        data_type = "datastream" if isinstance(data, DataStream) else "event"

        # Build envelope
        envelope = {
            "type": data_type,
            "version": {
                "major": data.version.major,
                "minor": data.version.minor
            },
            "trace_context": {
                "trace_id": str(data.trace_context.trace_id),
                "span_id": str(data.trace_context.span_id),
                "parent_span": str(data.trace_context.parent_span) if data.trace_context.parent_span else None
            },
            "payload": MessagePackSerializer._serialize_payload(data)
        }

        # Serialize with datetime support
        return msgpack.packb(envelope, datetime=True, use_bin_type=True)

    @staticmethod
    def deserialize(wire_bytes: bytes) -> Union[DataStream, Event]:
        """
        Deserialize MessagePack bytes to DataStream or Event.

        Args:
            wire_bytes: MessagePack-encoded bytes

        Returns:
            Deserialized DataStream or Event

        Raises:
            VersionMismatchError: If version is incompatible (older than N-1)
        """
        # Deserialize envelope
        envelope = msgpack.unpackb(wire_bytes, timestamp=3)

        # Extract and validate version
        msg_version = Version(
            major=envelope["version"]["major"],
            minor=envelope["version"]["minor"]
        )

        if not MessagePackSerializer.CURRENT_VERSION.is_compatible(msg_version):
            raise VersionMismatchError(
                f"Version {msg_version.major}.{msg_version.minor} incompatible with "
                f"{MessagePackSerializer.CURRENT_VERSION.major}.{MessagePackSerializer.CURRENT_VERSION.minor}"
            )

        # Reconstruct trace context
        trace_ctx_data = envelope["trace_context"]
        trace_context = TraceContext(
            trace_id=UUID(trace_ctx_data["trace_id"]),
            span_id=UUID(trace_ctx_data["span_id"]),
            parent_span=UUID(trace_ctx_data["parent_span"]) if trace_ctx_data["parent_span"] else None
        )

        # Deserialize payload based on type
        data_type = envelope["type"]
        payload = envelope["payload"]

        if data_type == "datastream":
            return MessagePackSerializer._deserialize_datastream(payload, msg_version, trace_context)
        else:
            return MessagePackSerializer._deserialize_event(payload, msg_version, trace_context)

    @staticmethod
    def _serialize_payload(data: Union[DataStream, Event]) -> dict:
        """Serialize payload (DataStream or Event specific fields)."""
        if isinstance(data, DataStream):
            return {
                "stream_id": data.stream_id,
                "source": data.source,
                "timestamp_start": data.timestamp_start.isoformat(),
                "timestamp_end": data.timestamp_end.isoformat(),
                "data_points": data.data_points,
                "interval": data.interval,
                "metadata": data.metadata or {}
            }
        else:  # Event
            return {
                "event_id": str(data.event_id),
                "timestamp": data.timestamp.isoformat(),
                "source": data.source,
                "event_type": data.event_type,
                "data": data.data,
                "stream_id": data.stream_id,
                "metadata": data.metadata or {}
            }

    @staticmethod
    def _deserialize_datastream(payload: dict, version: Version, trace_context: TraceContext) -> DataStream:
        """Deserialize DataStream payload."""
        return DataStream(
            stream_id=payload["stream_id"],
            source=payload["source"],
            timestamp_start=datetime.fromisoformat(payload["timestamp_start"]),
            timestamp_end=datetime.fromisoformat(payload["timestamp_end"]),
            data_points=payload["data_points"],
            trace_context=trace_context,
            version=version,
            interval=payload.get("interval"),
            metadata=payload.get("metadata")
        )

    @staticmethod
    def _deserialize_event(payload: dict, version: Version, trace_context: TraceContext) -> Event:
        """Deserialize Event payload."""
        return Event(
            event_id=UUID(payload["event_id"]),
            timestamp=datetime.fromisoformat(payload["timestamp"]),
            source=payload["source"],
            event_type=payload["event_type"],
            data=payload["data"],
            trace_context=trace_context,
            version=version,
            stream_id=payload.get("stream_id"),
            metadata=payload.get("metadata")
        )


__all__ = ['MessagePackSerializer', 'VersionMismatchError']
