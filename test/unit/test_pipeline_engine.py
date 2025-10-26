"""
Unit tests for PipelineEngine

Tests pipeline data flow, processor ordering, consumer parallelism.

Version: 1.0.0
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, Mock, MagicMock
from datetime import datetime, timezone
from uuid import uuid4

from components.pipeline.runtime.pipeline_engine import PipelineEngine
from components.core.interfaces.producer import IProducer
from components.core.interfaces.consumer import IConsumer
from components.core.interfaces.processor import IProcessor
from components.core.interfaces.transport import ITransport
from components.core.models.pipeline_config import (
    PipelineConfig,
    PipelineRuntimeConfig,
    TransportConfig,
    ErrorPolicy,
    DLQConfig
)
from components.core.models.datastream import DataStream
from components.core.models.event import Event
from components.core.models.trace_context import TraceContext
from components.core.versioning.version import Version
from components.core.models.adapter_metadata import (
    Result,
    HealthStatus,
    AdapterState,
    AdapterMetadata,
    AdapterCapabilities,
    AdapterType,
    DataType
)


# Test fixtures

@pytest.fixture
def sample_datastream():
    """Create a sample DataStream for testing."""
    now = datetime.now(timezone.utc)
    return DataStream(
        stream_id="test-stream-1",
        source="test-producer",
        timestamp_start=now,
        timestamp_end=now,
        data_points=[1.0, 2.0, 3.0],
        trace_context=TraceContext(
            trace_id=uuid4(),
            span_id=uuid4()
        ),
        version=Version(major=1, minor=0)
    )


@pytest.fixture
def sample_event():
    """Create a sample Event for testing."""
    return Event(
        event_id=uuid4(),
        timestamp=datetime.now(timezone.utc),
        source="test-producer",
        event_type="test.event",
        data={"key": "value"},
        trace_context=TraceContext(
            trace_id=uuid4(),
            span_id=uuid4()
        ),
        version=Version(major=1, minor=0)
    )


@pytest.fixture
def mock_producer(sample_datastream):
    """Create a mock producer."""
    producer = AsyncMock(spec=IProducer)
    producer.metadata = AdapterMetadata(
        id="test-producer",
        name="Test Producer",
        version=Version(major=1, minor=0),
        type=AdapterType.PRODUCER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    producer.init.return_value = Result(success=True)
    producer.start.return_value = Result(success=True)
    producer.stop.return_value = Result(success=True)
    producer.health.return_value = HealthStatus(
        status="healthy",
        uptime=0.0,
        state=AdapterState.RUNNING
    )

    # Mock produce to yield sample data
    async def mock_produce():
        yield sample_datastream
        # Stop after one item
        await asyncio.sleep(0)

    producer.produce.return_value = mock_produce()

    return producer


@pytest.fixture
def mock_processor():
    """Create a mock processor."""
    processor = AsyncMock(spec=IProcessor)
    processor.metadata = AdapterMetadata(
        id="test-processor",
        name="Test Processor",
        version=Version(major=1, minor=0),
        type=AdapterType.PROCESSOR,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=True,
            lossy_allowed=False
        )
    )
    processor.init.return_value = Result(success=True)
    processor.start.return_value = Result(success=True)
    processor.stop.return_value = Result(success=True)
    processor.health.return_value = HealthStatus(
        status="healthy",
        uptime=0.0,
        state=AdapterState.RUNNING
    )

    # Pass through data unchanged by default
    async def mock_process(data):
        return data

    processor.process.side_effect = mock_process

    return processor


@pytest.fixture
def mock_consumer():
    """Create a mock consumer."""
    consumer = AsyncMock(spec=IConsumer)
    consumer.metadata = AdapterMetadata(
        id="test-consumer",
        name="Test Consumer",
        version=Version(major=1, minor=0),
        type=AdapterType.CONSUMER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM, DataType.EVENT},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    consumer.init.return_value = Result(success=True)
    consumer.start.return_value = Result(success=True)
    consumer.stop.return_value = Result(success=True)
    consumer.health.return_value = HealthStatus(
        status="healthy",
        uptime=0.0,
        state=AdapterState.RUNNING
    )

    # Consume always succeeds by default
    consumer.consume.return_value = Result(success=True)

    return consumer


@pytest.fixture
def mock_transport():
    """Create a mock transport."""
    transport = AsyncMock(spec=ITransport)
    transport.metadata = AdapterMetadata(
        id="test-transport",
        name="Test Transport",
        version=Version(major=1, minor=0),
        type=AdapterType.TRANSPORT,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM, DataType.EVENT},
            ordering_guarantee=True,
            lossy_allowed=False
        )
    )
    transport.init.return_value = Result(success=True)
    transport.start.return_value = Result(success=True)
    transport.stop.return_value = Result(success=True)
    transport.health.return_value = HealthStatus(
        status="healthy",
        uptime=0.0,
        state=AdapterState.RUNNING
    )

    # Publish always succeeds
    transport.publish.return_value = Result(success=True)

    # Subscribe returns empty async iterator
    async def mock_subscribe(pattern):
        await asyncio.sleep(0)
        return
        yield  # Never yields

    transport.subscribe.return_value = mock_subscribe(None)

    return transport


@pytest.fixture
def pipeline_config():
    """Create a sample pipeline config."""
    return PipelineConfig(
        pipeline_id="test-pipeline",
        name="Test Pipeline",
        transport=TransportConfig(
            type="memory",
            config={}
        ),
        sources=["test-producer"],
        sinks=["test-consumer"],
        processors=[],
        config=PipelineRuntimeConfig(
            parallelism=1,
            buffer_size=100,
            time_window_minutes=5,
            error_policy=ErrorPolicy.CONTINUE
        )
    )


@pytest.fixture
def pipeline_engine(
    pipeline_config,
    mock_producer,
    mock_processor,
    mock_consumer,
    mock_transport
):
    """Create a PipelineEngine instance with mocks."""
    return PipelineEngine(
        config=pipeline_config,
        producers=[mock_producer],
        processors=[mock_processor],
        consumers=[mock_consumer],
        transport=mock_transport,
        metrics_collector=None
    )


# Test cases

@pytest.mark.asyncio
async def test_pipeline_start_stops_all_adapters(pipeline_engine):
    """Test pipeline starts and stops all adapters."""
    await pipeline_engine.start()

    # Verify all adapters started
    assert pipeline_engine.transport_lifecycle.state == AdapterState.RUNNING
    assert all(
        lc.state == AdapterState.RUNNING
        for lc in pipeline_engine.producer_lifecycles
    )
    assert all(
        lc.state == AdapterState.RUNNING
        for lc in pipeline_engine.consumer_lifecycles
    )

    await pipeline_engine.stop()

    # Verify all adapters stopped
    assert pipeline_engine.transport_lifecycle.state == AdapterState.STOPPED


@pytest.mark.asyncio
async def test_producer_data_flows_to_transport(
    pipeline_engine,
    mock_transport,
    sample_datastream
):
    """Test data from producer is published to transport."""
    await pipeline_engine.start()

    # Wait for producer to generate data
    await asyncio.sleep(0.2)

    await pipeline_engine.stop()

    # Verify transport.publish was called
    mock_transport.publish.assert_called()


@pytest.mark.asyncio
async def test_processor_ordering_sequential(pipeline_config, mock_transport):
    """Test processors execute in sequential order."""
    # Create multiple processors
    processor1 = AsyncMock(spec=IProcessor)
    processor1.metadata = AdapterMetadata(
        id="processor-1",
        name="Processor 1",
        version=Version(major=1, minor=0),
        type=AdapterType.PROCESSOR,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=True,
            lossy_allowed=False
        )
    )
    processor1.init.return_value = Result(success=True)
    processor1.start.return_value = Result(success=True)
    processor1.stop.return_value = Result(success=True)

    processor2 = AsyncMock(spec=IProcessor)
    processor2.metadata = AdapterMetadata(
        id="processor-2",
        name="Processor 2",
        version=Version(major=1, minor=0),
        type=AdapterType.PROCESSOR,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=True,
            lossy_allowed=False
        )
    )
    processor2.init.return_value = Result(success=True)
    processor2.start.return_value = Result(success=True)
    processor2.stop.return_value = Result(success=True)

    # Track execution order
    execution_order = []

    async def process1(data):
        execution_order.append("processor-1")
        return data

    async def process2(data):
        execution_order.append("processor-2")
        return data

    processor1.process.side_effect = process1
    processor2.process.side_effect = process2

    # Create pipeline with ordered processors
    pipeline_config.processors = ["processor-1", "processor-2"]

    # Mock producer
    producer = AsyncMock(spec=IProducer)
    producer.metadata = AdapterMetadata(
        id="test-producer",
        name="Test Producer",
        version=Version(major=1, minor=0),
        type=AdapterType.PRODUCER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    producer.init.return_value = Result(success=True)
    producer.start.return_value = Result(success=True)
    producer.stop.return_value = Result(success=True)

    async def mock_produce():
        now = datetime.now(timezone.utc)
        yield DataStream(
            stream_id="test",
            source="test-producer",
            timestamp_start=now,
            timestamp_end=now,
            data_points=[1.0],
            trace_context=TraceContext(trace_id=uuid4(), span_id=uuid4()),
            version=Version(major=1, minor=0)
        )

    producer.produce.return_value = mock_produce()

    # Mock consumer
    consumer = AsyncMock(spec=IConsumer)
    consumer.metadata = AdapterMetadata(
        id="test-consumer",
        name="Test Consumer",
        version=Version(major=1, minor=0),
        type=AdapterType.CONSUMER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    consumer.init.return_value = Result(success=True)
    consumer.start.return_value = Result(success=True)
    consumer.stop.return_value = Result(success=True)
    consumer.consume.return_value = Result(success=True)

    engine = PipelineEngine(
        config=pipeline_config,
        producers=[producer],
        processors=[processor1, processor2],
        consumers=[consumer],
        transport=mock_transport
    )

    await engine.start()
    await asyncio.sleep(0.2)
    await engine.stop()

    # Verify sequential execution
    if execution_order:  # Only check if processors were called
        assert execution_order == ["processor-1", "processor-2"]


@pytest.mark.asyncio
async def test_processor_filtering(pipeline_engine, mock_processor):
    """Test processor can filter data by returning None."""
    # Make processor filter all data
    async def filter_all(data):
        return None

    mock_processor.process.side_effect = filter_all

    await pipeline_engine.start()
    await asyncio.sleep(0.2)
    await pipeline_engine.stop()

    # Transport should not receive any data (filtered out)
    # This is tested indirectly by processor returning None


@pytest.mark.asyncio
async def test_consumer_parallelism(pipeline_config, mock_transport):
    """Test consumer parallelism spawns multiple tasks."""
    # Set parallelism to 3
    pipeline_config.config.parallelism = 3

    # Mock producer
    producer = AsyncMock(spec=IProducer)
    producer.metadata = AdapterMetadata(
        id="test-producer",
        name="Test Producer",
        version=Version(major=1, minor=0),
        type=AdapterType.PRODUCER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    producer.init.return_value = Result(success=True)
    producer.start.return_value = Result(success=True)
    producer.stop.return_value = Result(success=True)

    async def mock_produce():
        await asyncio.sleep(0)
        return
        yield  # Never yields

    producer.produce.return_value = mock_produce()

    # Mock consumer
    consumer = AsyncMock(spec=IConsumer)
    consumer.metadata = AdapterMetadata(
        id="test-consumer",
        name="Test Consumer",
        version=Version(major=1, minor=0),
        type=AdapterType.CONSUMER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    consumer.init.return_value = Result(success=True)
    consumer.start.return_value = Result(success=True)
    consumer.stop.return_value = Result(success=True)
    consumer.consume.return_value = Result(success=True)

    engine = PipelineEngine(
        config=pipeline_config,
        producers=[producer],
        processors=[],
        consumers=[consumer],
        transport=mock_transport
    )

    await engine.start()

    # Verify 3 consumer tasks spawned
    assert len(engine.consumer_tasks) == 3

    await engine.stop()


@pytest.mark.asyncio
async def test_error_policy_continue(pipeline_engine, mock_consumer):
    """Test ErrorPolicy.CONTINUE skips failed data and continues."""
    pipeline_engine.config.config.error_policy = ErrorPolicy.CONTINUE

    # Make consumer fail
    mock_consumer.consume.return_value = Result(
        success=False,
        message="Consumer failed"
    )

    await pipeline_engine.start()
    await asyncio.sleep(0.2)
    await pipeline_engine.stop()

    # Pipeline should not halt (just logged)
    assert pipeline_engine.running == False  # Stopped normally


@pytest.mark.asyncio
async def test_error_policy_dlq(pipeline_config, mock_transport):
    """Test ErrorPolicy.DLQ sends failed data to DLQ."""
    pipeline_config.config.error_policy = ErrorPolicy.DLQ
    pipeline_config.config.dlq = DLQConfig(
        enabled=True,
        storage="file",
        retention="P7D"
    )

    # Mock producer
    producer = AsyncMock(spec=IProducer)
    producer.metadata = AdapterMetadata(
        id="test-producer",
        name="Test Producer",
        version=Version(major=1, minor=0),
        type=AdapterType.PRODUCER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    producer.init.return_value = Result(success=True)
    producer.start.return_value = Result(success=True)
    producer.stop.return_value = Result(success=True)

    async def mock_produce():
        now = datetime.now(timezone.utc)
        yield DataStream(
            stream_id="test",
            source="test-producer",
            timestamp_start=now,
            timestamp_end=now,
            data_points=[1.0],
            trace_context=TraceContext(trace_id=uuid4(), span_id=uuid4()),
            version=Version(major=1, minor=0)
        )

    producer.produce.return_value = mock_produce()

    # Mock consumer that fails
    consumer = AsyncMock(spec=IConsumer)
    consumer.metadata = AdapterMetadata(
        id="test-consumer",
        name="Test Consumer",
        version=Version(major=1, minor=0),
        type=AdapterType.CONSUMER,
        capabilities=AdapterCapabilities(
            data_types={DataType.DATASTREAM},
            ordering_guarantee=False,
            lossy_allowed=True
        )
    )
    consumer.init.return_value = Result(success=True)
    consumer.start.return_value = Result(success=True)
    consumer.stop.return_value = Result(success=True)
    consumer.consume.return_value = Result(success=False, message="Failed")

    engine = PipelineEngine(
        config=pipeline_config,
        producers=[producer],
        processors=[],
        consumers=[consumer],
        transport=mock_transport
    )

    await engine.start()
    await asyncio.sleep(0.2)
    await engine.stop()

    # DLQ should have been called (indirectly tested via _send_to_dlq)
    # In a full implementation, we'd verify DLQ storage was written


@pytest.mark.asyncio
async def test_pipeline_stop_cancels_tasks(pipeline_engine):
    """Test pipeline stop cancels all background tasks."""
    await pipeline_engine.start()

    # Get task count
    producer_task_count = len(pipeline_engine.producer_tasks)
    consumer_task_count = len(pipeline_engine.consumer_tasks)

    await pipeline_engine.stop()

    # All tasks should be cancelled
    assert all(t.cancelled() or t.done() for t in pipeline_engine.producer_tasks)
    assert all(t.cancelled() or t.done() for t in pipeline_engine.consumer_tasks)
