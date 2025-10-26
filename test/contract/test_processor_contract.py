"""
Contract tests for IProcessor interface.

These tests validate that any processor implementation conforms to IProcessor contract.
Per TDD approach: These tests MUST FAIL initially (no implementations exist yet).

Constitutional requirement: FR-070 (contract tests for all adapters)
"""

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from components.core.interfaces import IProcessor
from components.core.models import (
    DataStream, Event, TraceContext, Version,
    AdapterType, DataType
)


class TestIProcessorContract:
    """
    Contract tests for IProcessor interface.

    All processor implementations must pass these tests.
    Per FR-012: Processors MUST transform data and MAY filter by returning None.
    """

    @pytest.fixture
    def processor(self) -> IProcessor:
        """
        Provide processor instance for testing.

        Override this fixture in processor-specific test files.
        """
        pytest.skip("No processor implementation provided - expected to FAIL")

    @pytest.fixture
    def valid_config(self) -> dict:
        """Provide valid configuration for processor."""
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
    async def test_process_returns_data_or_none(self, processor: IProcessor, valid_config: dict, sample_datastream: DataStream):
        """
        Test process() returns DataStream, Event, or None.

        Per FR-012: Processors transform or filter data.
        """
        await processor.init(valid_config)
        await processor.start()

        result = await processor.process(sample_datastream)

        assert result is None or isinstance(result, (DataStream, Event)), \
            f"process() must return DataStream, Event, or None, got {type(result)}"

        await processor.stop()

    @pytest.mark.asyncio
    async def test_process_datastream(self, processor: IProcessor, valid_config: dict, sample_datastream: DataStream):
        """
        Test processor can handle DataStream.

        Per FR-012: Processors must process DataStreams.
        """
        await processor.init(valid_config)
        await processor.start()

        result = await processor.process(sample_datastream)

        # Result can be transformed DataStream, Event, or None (filtered)
        assert result is None or isinstance(result, (DataStream, Event))

        await processor.stop()

    @pytest.mark.asyncio
    async def test_process_event(self, processor: IProcessor, valid_config: dict, sample_event: Event):
        """
        Test processor can handle Event.

        Per FR-012: Processors must process Events.
        """
        await processor.init(valid_config)
        await processor.start()

        result = await processor.process(sample_event)

        assert result is None or isinstance(result, (DataStream, Event))

        await processor.stop()

    @pytest.mark.asyncio
    async def test_process_preserves_trace_id(self, processor: IProcessor, valid_config: dict, sample_datastream: DataStream):
        """
        Test process() preserves original trace_id.

        Per FR-032: Trace IDs must be propagated through pipeline.
        """
        await processor.init(valid_config)
        await processor.start()

        original_trace_id = sample_datastream.trace_context.trace_id

        result = await processor.process(sample_datastream)

        if result is not None:
            assert result.trace_context.trace_id == original_trace_id, \
                "process() must preserve original trace_id"

        await processor.stop()

    @pytest.mark.asyncio
    async def test_process_creates_child_span(self, processor: IProcessor, valid_config: dict, sample_datastream: DataStream):
        """
        Test process() creates new child span for transformed data.

        Per FR-032: Processors should create child spans for tracing.
        """
        await processor.init(valid_config)
        await processor.start()

        original_span_id = sample_datastream.trace_context.span_id

        result = await processor.process(sample_datastream)

        if result is not None:
            # New span_id should be different
            assert result.trace_context.span_id != original_span_id, \
                "process() should create new span_id"
            # Parent span should reference original span
            assert result.trace_context.parent_span == original_span_id, \
                "process() should set parent_span to original span_id"

        await processor.stop()

    @pytest.mark.asyncio
    async def test_process_filtering_returns_none(self, processor: IProcessor, valid_config: dict, sample_datastream: DataStream):
        """
        Test process() can filter by returning None.

        Per FR-012: Processors MAY filter data by returning None.
        """
        await processor.init(valid_config)
        await processor.start()

        result = await processor.process(sample_datastream)

        # None is valid (filtering), or transformed data
        assert result is None or isinstance(result, (DataStream, Event))

        await processor.stop()

    @pytest.mark.asyncio
    async def test_metadata_declares_processor_type(self, processor: IProcessor):
        """
        Test metadata declares PROCESSOR adapter type.

        Per FR-014: Processors must identify as PROCESSOR type.
        """
        metadata = processor.metadata

        assert metadata.type == AdapterType.PROCESSOR, \
            f"Processor metadata.type must be PROCESSOR, got {metadata.type}"

    @pytest.mark.asyncio
    async def test_metadata_capabilities_include_data_types(self, processor: IProcessor):
        """
        Test capabilities declare supported data types.

        Per FR-014: Processors must declare DataStream and/or Event support.
        """
        capabilities = processor.metadata.capabilities

        assert len(capabilities.data_types) > 0, "capabilities.data_types must be non-empty"
        valid_types = {DataType.DATASTREAM, DataType.EVENT}
        for dt in capabilities.data_types:
            assert dt in valid_types, f"Invalid data type: {dt}"

    @pytest.mark.asyncio
    async def test_process_handles_bad_data_gracefully(self, processor: IProcessor, valid_config: dict):
        """
        Test process() handles invalid data without crashing.

        Per FR-040: System must never block pipeline on bad data.
        """
        await processor.init(valid_config)
        await processor.start()

        # Create malformed DataStream
        bad_datastream = DataStream(
            stream_id="",  # Invalid
            source="test",
            timestamp_start=datetime.now(timezone.utc),
            timestamp_end=datetime.now(timezone.utc),
            data_points=[1],
            trace_context=TraceContext.new(),
            version=Version(1, 0)
        )

        # Should handle gracefully (return None or raise no exception)
        try:
            result = await processor.process(bad_datastream)
            # None (filtered) or transformed data both acceptable
            assert result is None or isinstance(result, (DataStream, Event))
        except Exception as e:
            pytest.fail(f"process() must handle bad data gracefully, got exception: {e}")

        await processor.stop()
