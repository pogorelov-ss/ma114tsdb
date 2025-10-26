"""
Unit tests for MessagePack serialization.

Tests serialization/deserialization with version compatibility checks.
"""

import pytest
import msgpack
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
from components.serialization.msgpack import MessagePackSerializer, VersionMismatchError
from components.core.models import DataStream, Event, TraceContext, Version


class TestMessagePackSerializer:
    """Test suite for MessagePackSerializer."""

    def test_serialize_datastream(self):
        """Test DataStream serialization to MessagePack."""
        trace_context = TraceContext(
            trace_id=uuid4(),
            span_id=uuid4(),
            parent_span=None
        )

        datastream = DataStream(
            stream_id="sensor-123",
            source="temp-sensor",
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc) + timedelta(seconds=60),
            data_points=[1.0, 2.0, 3.0, 4.0],
            trace_context=trace_context,
            version=Version(major=1, minor=0),
            interval=1.0,
            metadata={"location": "room1"}
        )

        wire_bytes = MessagePackSerializer.serialize(datastream)

        # Verify it's bytes
        assert isinstance(wire_bytes, bytes)

        # Verify it can be unpacked
        envelope = msgpack.unpackb(wire_bytes, timestamp=3)
        assert envelope["type"] == "datastream"
        assert envelope["version"]["major"] == 1
        assert envelope["version"]["minor"] == 0

    def test_serialize_event(self):
        """Test Event serialization to MessagePack."""
        trace_context = TraceContext(
            trace_id=uuid4(),
            span_id=uuid4(),
            parent_span=uuid4()
        )

        event = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="device-monitor",
            event_type="health-check",
            data={"status": "healthy", "cpu": 45.2},
            trace_context=trace_context,
            version=Version(major=1, minor=0),
            stream_id="stream-456",
            metadata={"priority": "high"}
        )

        wire_bytes = MessagePackSerializer.serialize(event)

        assert isinstance(wire_bytes, bytes)

        envelope = msgpack.unpackb(wire_bytes, timestamp=3)
        assert envelope["type"] == "event"
        assert envelope["payload"]["event_type"] == "health-check"

    def test_deserialize_datastream(self):
        """Test DataStream deserialization from MessagePack."""
        original = DataStream(
            stream_id="sensor-123",
            source="temp-sensor",
            timestamp_start=datetime(2025, 10, 4, 12, 0, 0, tzinfo=timezone.utc),
            timestamp_end=datetime(2025, 10, 4, 12, 1, 0, tzinfo=timezone.utc),
            data_points=[21.5, 21.6, 21.7],
            trace_context=TraceContext(
                trace_id=UUID('12345678-1234-5678-1234-567812345678'),
                span_id=UUID('87654321-4321-8765-4321-876543218765'),
                parent_span=None
            ),
            version=Version(major=1, minor=0)
        )

        wire_bytes = MessagePackSerializer.serialize(original)
        deserialized = MessagePackSerializer.deserialize(wire_bytes)

        assert isinstance(deserialized, DataStream)
        assert deserialized.stream_id == original.stream_id
        assert deserialized.source == original.source
        assert deserialized.data_points == original.data_points
        assert deserialized.trace_context.trace_id == original.trace_context.trace_id

    def test_deserialize_event(self):
        """Test Event deserialization from MessagePack."""
        event_id = uuid4()
        trace_id = uuid4()
        span_id = uuid4()

        original = Event(
            event_id=event_id,
            timestamp=datetime(2025, 10, 4, 12, 0, 0, tzinfo=timezone.utc),
            source="device-monitor",
            event_type="alarm",
            data={"level": "critical", "message": "Temperature exceeded"},
            trace_context=TraceContext(
                trace_id=trace_id,
                span_id=span_id,
                parent_span=None
            ),
            version=Version(major=1, minor=0)
        )

        wire_bytes = MessagePackSerializer.serialize(original)
        deserialized = MessagePackSerializer.deserialize(wire_bytes)

        assert isinstance(deserialized, Event)
        assert deserialized.event_id == event_id
        assert deserialized.event_type == "alarm"
        assert deserialized.data["level"] == "critical"
        assert deserialized.trace_context.trace_id == trace_id

    def test_roundtrip_preserves_trace_context(self):
        """Test serialization-deserialization preserves trace context."""
        parent_span = uuid4()
        trace_context = TraceContext(
            trace_id=uuid4(),
            span_id=uuid4(),
            parent_span=parent_span
        )

        event = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="test",
            data={"test": "value"},
            trace_context=trace_context,
            version=Version(major=1, minor=0)
        )

        deserialized = MessagePackSerializer.deserialize(
            MessagePackSerializer.serialize(event)
        )

        assert deserialized.trace_context.trace_id == trace_context.trace_id
        assert deserialized.trace_context.span_id == trace_context.span_id
        assert deserialized.trace_context.parent_span == parent_span

    def test_version_compatibility_same_major(self):
        """Test same major version is compatible."""
        event = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="test",
            data={"value": 123},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(major=1, minor=5)  # Different minor
        )

        # Should deserialize without error
        wire_bytes = MessagePackSerializer.serialize(event)
        deserialized = MessagePackSerializer.deserialize(wire_bytes)
        assert deserialized.version.major == 1

    def test_version_compatibility_n_minus_1(self):
        """Test N-1 major version is compatible."""
        # Simulate receiving data from v0.x when current is v1.0
        trace_context = TraceContext(uuid4(), uuid4())

        # Manually create envelope with older version
        envelope = {
            "type": "event",
            "version": {"major": 0, "minor": 9},  # N-1 version
            "trace_context": {
                "trace_id": str(trace_context.trace_id),
                "span_id": str(trace_context.span_id),
                "parent_span": None
            },
            "payload": {
                "event_id": str(uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "old-adapter",
                "event_type": "test",
                "data": {"old": "data"},
                "stream_id": None,
                "metadata": {}
            }
        }

        wire_bytes = msgpack.packb(envelope, datetime=True, use_bin_type=True)

        # Assuming current version is 1.0, version 0.9 should be accepted
        # Note: This test assumes CURRENT_VERSION is 1.0
        # If CURRENT_VERSION changes, adjust test accordingly
        if MessagePackSerializer.CURRENT_VERSION.major == 1:
            deserialized = MessagePackSerializer.deserialize(wire_bytes)
            assert deserialized.version.major == 0

    def test_version_incompatible_too_old(self):
        """Test version older than N-1 is rejected."""
        # Assuming current version is 1.0, version 0.0 when current is 2.0+ would fail
        if MessagePackSerializer.CURRENT_VERSION.major >= 2:
            trace_context = TraceContext(uuid4(), uuid4())

            envelope = {
                "type": "event",
                "version": {"major": 0, "minor": 0},  # Too old
                "trace_context": {
                    "trace_id": str(trace_context.trace_id),
                    "span_id": str(trace_context.span_id),
                    "parent_span": None
                },
                "payload": {
                    "event_id": str(uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "ancient-adapter",
                    "event_type": "test",
                    "data": {},
                    "stream_id": None,
                    "metadata": {}
                }
            }

            wire_bytes = msgpack.packb(envelope, datetime=True, use_bin_type=True)

            with pytest.raises(VersionMismatchError) as exc_info:
                MessagePackSerializer.deserialize(wire_bytes)

            assert "incompatible" in str(exc_info.value).lower()

    def test_timestamp_precision_preserved(self):
        """Test sub-second timestamp precision is preserved."""
        timestamp = datetime(2025, 10, 4, 12, 30, 45, 123456, tzinfo=timezone.utc)

        event = Event(
            event_id=uuid4(),
            timestamp=timestamp,
            source="test",
            event_type="test",
            data={"precision": "test"},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(major=1, minor=0)
        )

        deserialized = MessagePackSerializer.deserialize(
            MessagePackSerializer.serialize(event)
        )

        assert deserialized.timestamp.microsecond == 123456

    def test_uuid_integrity_maintained(self):
        """Test UUID values maintain integrity through serialization."""
        event_id = UUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
        trace_id = UUID('11111111-2222-3333-4444-555555555555')

        event = Event(
            event_id=event_id,
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="test",
            data={"uuid_test": "data"},
            trace_context=TraceContext(
                trace_id=trace_id,
                span_id=uuid4()
            ),
            version=Version(major=1, minor=0)
        )

        deserialized = MessagePackSerializer.deserialize(
            MessagePackSerializer.serialize(event)
        )

        assert deserialized.event_id == event_id
        assert deserialized.trace_context.trace_id == trace_id

    def test_optional_fields_handled(self):
        """Test optional fields (metadata, interval, stream_id) handled correctly."""
        # DataStream without metadata or interval
        ds = DataStream(
            stream_id="test",
            source="test",
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc),
            data_points=[1, 2, 3],
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(major=1, minor=0)
        )

        deserialized_ds = MessagePackSerializer.deserialize(
            MessagePackSerializer.serialize(ds)
        )
        assert deserialized_ds.metadata in (None, {})
        assert deserialized_ds.interval is None

        # Event without stream_id or metadata
        evt = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test",
            event_type="test",
            data={"optional": "test"},
            trace_context=TraceContext(uuid4(), uuid4()),
            version=Version(major=1, minor=0)
        )

        deserialized_evt = MessagePackSerializer.deserialize(
            MessagePackSerializer.serialize(evt)
        )
        assert deserialized_evt.stream_id is None
        assert deserialized_evt.metadata in (None, {})
