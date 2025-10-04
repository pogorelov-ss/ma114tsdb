"""
Contract tests for IProducer interface.

These tests validate that any producer implementation conforms to IProducer contract.
Per TDD approach: These tests MUST FAIL initially (no implementations exist yet).

Constitutional requirement: FR-070 (contract tests for all adapters)
"""

import pytest
from datetime import datetime, timezone

from components.core.interfaces import IProducer
from components.core.models import DataStream, Event, AdapterType, DataType


class TestIProducerContract:
    """
    Contract tests for IProducer interface.

    All producer implementations must pass these tests.
    Per FR-010: Producers MUST generate and publish DataStreams or Events.
    """

    @pytest.fixture
    def producer(self) -> IProducer:
        """
        Provide producer instance for testing.

        Override this fixture in producer-specific test files.
        """
        pytest.skip("No producer implementation provided - expected to FAIL")

    @pytest.fixture
    def valid_config(self) -> dict:
        """Provide valid configuration for producer."""
        return {}

    @pytest.mark.asyncio
    async def test_produce_yields_data(self, producer: IProducer, valid_config: dict):
        """
        Test produce() yields DataStream or Event instances.

        Per FR-010: Producers must generate data.
        """
        await producer.init(valid_config)
        await producer.start()

        # Collect first item from producer
        async for data in producer.produce():
            assert isinstance(data, (DataStream, Event)), \
                f"produce() must yield DataStream or Event, got {type(data)}"
            break  # Test at least one item
        else:
            pytest.fail("produce() yielded no data")

        await producer.stop()

    @pytest.mark.asyncio
    async def test_produced_data_has_trace_context(self, producer: IProducer, valid_config: dict):
        """
        Test all produced data carries TraceContext.

        Per FR-004: All data MUST carry TraceContext.
        """
        await producer.init(valid_config)
        await producer.start()

        async for data in producer.produce():
            assert hasattr(data, 'trace_context'), "Data must have trace_context"
            assert data.trace_context is not None, "trace_context must not be None"
            assert data.trace_context.trace_id is not None, "trace_id must be set"
            assert data.trace_context.span_id is not None, "span_id must be set"
            break

        await producer.stop()

    @pytest.mark.asyncio
    async def test_produced_data_has_version(self, producer: IProducer, valid_config: dict):
        """
        Test all produced data carries version information.

        Per FR-005: All data MUST carry version information.
        """
        await producer.init(valid_config)
        await producer.start()

        async for data in producer.produce():
            assert hasattr(data, 'version'), "Data must have version"
            assert data.version is not None, "version must not be None"
            assert data.version.major >= 0, "version.major must be non-negative"
            assert data.version.minor >= 0, "version.minor must be non-negative"
            break

        await producer.stop()

    @pytest.mark.asyncio
    async def test_produced_data_has_valid_timestamps(self, producer: IProducer, valid_config: dict):
        """
        Test DataStreams have valid timestamp ranges, Events have valid timestamps.

        Per FR-002, FR-003: Sub-second precision, timezone-aware timestamps.
        """
        await producer.init(valid_config)
        await producer.start()

        async for data in producer.produce():
            if isinstance(data, DataStream):
                assert data.timestamp_start is not None, "timestamp_start must be set"
                assert data.timestamp_end is not None, "timestamp_end must be set"
                assert data.timestamp_start.tzinfo is not None, "timestamp_start must be timezone-aware"
                assert data.timestamp_end.tzinfo is not None, "timestamp_end must be timezone-aware"
                assert data.timestamp_end >= data.timestamp_start, \
                    "timestamp_end must be >= timestamp_start"
            elif isinstance(data, Event):
                assert data.timestamp is not None, "timestamp must be set"
                assert data.timestamp.tzinfo is not None, "timestamp must be timezone-aware"
            break

        await producer.stop()

    @pytest.mark.asyncio
    async def test_produced_data_source_matches_adapter_id(self, producer: IProducer, valid_config: dict):
        """
        Test source field matches adapter metadata ID.

        Per FR-010: Source must be set to adapter ID.
        """
        await producer.init(valid_config)
        await producer.start()

        adapter_id = producer.metadata.id

        async for data in producer.produce():
            assert data.source == adapter_id, \
                f"source '{data.source}' must match adapter ID '{adapter_id}'"
            break

        await producer.stop()

    @pytest.mark.asyncio
    async def test_metadata_declares_producer_type(self, producer: IProducer):
        """
        Test metadata declares PRODUCER adapter type.

        Per FR-014: Producers must identify as PRODUCER type.
        """
        metadata = producer.metadata

        assert metadata.type == AdapterType.PRODUCER, \
            f"Producer metadata.type must be PRODUCER, got {metadata.type}"

    @pytest.mark.asyncio
    async def test_metadata_capabilities_include_data_types(self, producer: IProducer):
        """
        Test capabilities declare supported data types.

        Per FR-014: Producers must declare DataStream and/or Event support.
        """
        capabilities = producer.metadata.capabilities

        assert len(capabilities.data_types) > 0, "capabilities.data_types must be non-empty"
        valid_types = {DataType.DATASTREAM, DataType.EVENT}
        for dt in capabilities.data_types:
            assert dt in valid_types, f"Invalid data type: {dt}"

    @pytest.mark.asyncio
    async def test_produce_stops_when_adapter_stopped(self, producer: IProducer, valid_config: dict):
        """
        Test produce() stops yielding when adapter is stopped.

        Producers should respect adapter state.
        """
        await producer.init(valid_config)
        await producer.start()

        # Start consuming
        count = 0
        async for data in producer.produce():
            count += 1
            if count >= 3:
                # Stop adapter mid-stream
                await producer.stop()
                break

        # After stop, produce should not yield more data
        # (Implementation-specific behavior, but should be graceful)
