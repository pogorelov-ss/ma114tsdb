"""
Contract tests for IConsumer interface.

These tests validate that any consumer implementation conforms to IConsumer contract.
Per TDD approach: These tests MUST FAIL initially (no implementations exist yet).

Constitutional requirement: FR-070 (contract tests for all adapters)
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.core.interfaces import IConsumer
from components.core.models import (
    DataStream, Event, TraceContext, Version,
    AdapterType, DataType, Result
)


class TestIConsumerContract:
    """
    Contract tests for IConsumer interface.

    All consumer implementations must pass these tests.
    Per FR-011: Consumers MUST receive and process DataStreams or Events.
    """

    @pytest.fixture
    def consumer(self) -> IConsumer:
        """
        Provide consumer instance for testing.

        Override this fixture in consumer-specific test files.
        """
        pytest.skip("No consumer implementation provided - expected to FAIL")

    @pytest.fixture
    def valid_config(self) -> dict:
        """Provide valid configuration for consumer."""
        return {}

    @pytest.fixture
    def sample_datastream(self) -> DataStream:
        """Create sample DataStream for testing."""
        now = datetime.now(timezone.utc)
        return DataStream(
            stream_id="test-stream-01",
            source="test-producer",
            timestamp_start=now,
            timestamp_end=now,
            data_points=[{"value": 42.5}],
            trace_context=TraceContext.new(),
            version=Version(1, 0)
        )

    @pytest.fixture
    def sample_event(self) -> Event:
        """Create sample Event for testing."""
        return Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test-producer",
            event_type="test-event",
            data={"status": "ok"},
            trace_context=TraceContext.new(),
            version=Version(1, 0)
        )

    @pytest.mark.asyncio
    async def test_consume_returns_result(self, consumer: IConsumer, valid_config: dict, sample_datastream: DataStream):
        """
        Test consume() returns Result instance.

        Per FR-011: Consumers must process data and return result.
        """
        await consumer.init(valid_config)
        await consumer.start()

        result = await consumer.consume(sample_datastream)

        assert isinstance(result, Result), f"consume() must return Result, got {type(result)}"

        await consumer.stop()

    @pytest.mark.asyncio
    async def test_consume_datastream(self, consumer: IConsumer, valid_config: dict, sample_datastream: DataStream):
        """
        Test consumer can process DataStream.

        Per FR-011: Consumers must handle DataStreams.
        """
        await consumer.init(valid_config)
        await consumer.start()

        result = await consumer.consume(sample_datastream)

        # Consumer should process without crashing
        assert isinstance(result, Result), "consume() must return Result"
        # Success or failure both acceptable, but must handle gracefully

        await consumer.stop()

    @pytest.mark.asyncio
    async def test_consume_event(self, consumer: IConsumer, valid_config: dict, sample_event: Event):
        """
        Test consumer can process Event.

        Per FR-011: Consumers must handle Events.
        """
        await consumer.init(valid_config)
        await consumer.start()

        result = await consumer.consume(sample_event)

        assert isinstance(result, Result), "consume() must return Result"

        await consumer.stop()

    @pytest.mark.asyncio
    async def test_consume_never_blocks_on_bad_data(self, consumer: IConsumer, valid_config: dict):
        """
        Test consume() never blocks pipeline on invalid data.

        Per FR-040: System MUST never block pipeline on bad data.
        """
        await consumer.init(valid_config)
        await consumer.start()

        # Create intentionally malformed data (missing required fields)
        bad_datastream = DataStream(
            stream_id="",  # Invalid: empty stream_id
            source="test",
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc),
            data_points=[1, 2, 3],
            trace_context=TraceContext.new(),
            version=Version(1, 0)
        )

        # Consumer should handle gracefully without raising exception
        try:
            result = await consumer.consume(bad_datastream)
            assert isinstance(result, Result), "consume() must return Result even for bad data"
            # Failure is acceptable, but must not crash
        except Exception as e:
            pytest.fail(f"consume() must not raise exception for bad data, got: {e}")

        await consumer.stop()

    @pytest.mark.asyncio
    async def test_consume_handles_old_version(self, consumer: IConsumer, valid_config: dict, sample_event: Event):
        """
        Test consumer handles N-1 version compatibility.

        Per FR-047: Consumers MUST handle older data format versions.
        """
        await consumer.init(valid_config)
        await consumer.start()

        # Create event with older version (0.9 instead of 1.0)
        old_version_event = Event(
            event_id=uuid4(),
            timestamp=datetime.now(timezone.utc),
            source="test-producer",
            event_type="legacy-event",
            data={"status": "ok"},
            trace_context=TraceContext.new(),
            version=Version(0, 9)  # Older version
        )

        result = await consumer.consume(old_version_event)

        # Should handle gracefully (accept or reject with clear message)
        assert isinstance(result, Result), "consume() must return Result for old versions"

        await consumer.stop()

    @pytest.mark.asyncio
    async def test_metadata_declares_consumer_type(self, consumer: IConsumer):
        """
        Test metadata declares CONSUMER adapter type.

        Per FR-014: Consumers must identify as CONSUMER type.
        """
        metadata = consumer.metadata

        assert metadata.type == AdapterType.CONSUMER, \
            f"Consumer metadata.type must be CONSUMER, got {metadata.type}"

    @pytest.mark.asyncio
    async def test_metadata_capabilities_include_data_types(self, consumer: IConsumer):
        """
        Test capabilities declare supported data types.

        Per FR-014: Consumers must declare DataStream and/or Event support.
        """
        capabilities = consumer.metadata.capabilities

        assert len(capabilities.data_types) > 0, "capabilities.data_types must be non-empty"
        valid_types = {DataType.DATASTREAM, DataType.EVENT}
        for dt in capabilities.data_types:
            assert dt in valid_types, f"Invalid data type: {dt}"

    @pytest.mark.asyncio
    async def test_consume_provides_error_message_on_failure(self, consumer: IConsumer, valid_config: dict, sample_event: Event):
        """
        Test failed consume() provides descriptive error message.

        Per FR-042: Failed data skipped with error logging.
        """
        await consumer.init(valid_config)
        await consumer.start()

        result = await consumer.consume(sample_event)

        if not result.success:
            assert result.message is not None, "Failed consume() should provide error message"
            assert len(result.message) > 0, "Error message should be non-empty"

        await consumer.stop()
