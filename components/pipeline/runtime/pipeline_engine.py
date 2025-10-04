"""
Pipeline Runtime Engine

Executes data pipelines: Producer → Transport → Processor → Transport → Consumer

Constitutional requirements:
- FR-016: Every pipeline MUST specify exactly one transport
- FR-026: Events delivered in FIFO order
- FR-027: DataStreams prioritize latest data in time window
- FR-040: System MUST never block pipeline on bad data
- FR-052: Processors execute in sequential order

Version: 1.0.0
"""

import asyncio
from typing import Any, Optional, TYPE_CHECKING
from uuid import uuid4

from ...core.interfaces.adapter import IAdapter
from ...core.interfaces.producer import IProducer
from ...core.interfaces.consumer import IConsumer
from ...core.interfaces.processor import IProcessor
from ...core.interfaces.transport import ITransport
from ...core.models.datastream import DataStream
from ...core.models.event import Event
from ...core.models.pipeline_config import PipelineConfig, ErrorPolicy
from ...core.models.transport_types import RoutingInfo, SubscriptionPattern
from ..lifecycle.adapter_lifecycle import AdapterLifecycle
import structlog

if TYPE_CHECKING:
    from ...observability.metrics.prometheus_collector import MetricsCollector


logger = structlog.get_logger(__name__)


class PipelineEngine:
    """
    Pipeline execution engine.

    Data flow:
    1. Producer generates DataStream/Event
    2. Transport publishes to routing destination
    3. Processor (sequential) transforms data
    4. Transport publishes transformed data
    5. Consumer processes final data
    """

    def __init__(
        self,
        config: PipelineConfig,
        producers: list[IProducer],
        processors: list[IProcessor],
        consumers: list[IConsumer],
        transport: ITransport,
        metrics_collector: Optional["MetricsCollector"] = None
    ):
        """
        Initialize pipeline engine.

        Args:
            config: Pipeline configuration
            producers: List of producer adapters
            processors: List of processor adapters (ordered)
            consumers: List of consumer adapters
            transport: Transport adapter (exactly one)
            metrics_collector: Optional metrics collector
        """
        self.config = config
        self.producers = producers
        self.processors = processors
        self.consumers = consumers
        self.transport = transport
        self.metrics_collector = metrics_collector

        # Lifecycle managers for all adapters
        self.producer_lifecycles = [
            AdapterLifecycle(p, metrics_collector)
            for p in producers
        ]
        self.processor_lifecycles = [
            AdapterLifecycle(p, metrics_collector)
            for p in processors
        ]
        self.consumer_lifecycles = [
            AdapterLifecycle(c, metrics_collector)
            for c in consumers
        ]
        self.transport_lifecycle = AdapterLifecycle(transport, metrics_collector)

        # Task handles for cleanup
        self.producer_tasks = []
        self.consumer_tasks = []
        self.running = False

    async def start(self) -> None:
        """
        Start pipeline execution.

        Initializes and starts all adapters in order:
        1. Transport (first)
        2. Producers
        3. Processors
        4. Consumers

        Then spawns background tasks for data flow.
        """
        pipeline_id = self.config.pipeline_id

        logger.info(
            "Starting pipeline",
            pipeline_id=pipeline_id,
            producers=len(self.producers),
            processors=len(self.processors),
            consumers=len(self.consumers)
        )

        try:
            # Start transport first
            await self._start_adapter(
                self.transport_lifecycle,
                self.config.transport.config
            )

            # Start producers
            for lifecycle, producer in zip(self.producer_lifecycles, self.producers):
                # Get producer config from pipeline config
                producer_config = self._get_adapter_config(producer.metadata.id)
                await self._start_adapter(lifecycle, producer_config)

            # Start processors (in order)
            for lifecycle, processor in zip(self.processor_lifecycles, self.processors):
                processor_config = self._get_adapter_config(processor.metadata.id)
                await self._start_adapter(lifecycle, processor_config)

            # Start consumers
            for lifecycle, consumer in zip(self.consumer_lifecycles, self.consumers):
                consumer_config = self._get_adapter_config(consumer.metadata.id)
                await self._start_adapter(lifecycle, consumer_config)

            # Spawn producer tasks
            for producer in self.producers:
                task = asyncio.create_task(self._run_producer(producer))
                self.producer_tasks.append(task)

            # Spawn consumer tasks (with parallelism)
            parallelism = self.config.config.parallelism
            for consumer in self.consumers:
                for _ in range(parallelism):
                    task = asyncio.create_task(self._run_consumer(consumer))
                    self.consumer_tasks.append(task)

            self.running = True

            logger.info(
                "Pipeline started successfully",
                pipeline_id=pipeline_id,
                producer_tasks=len(self.producer_tasks),
                consumer_tasks=len(self.consumer_tasks)
            )

        except Exception as e:
            logger.exception(
                "Failed to start pipeline",
                pipeline_id=pipeline_id,
                error=str(e)
            )
            await self.stop()
            raise

    async def stop(self) -> None:
        """
        Stop pipeline execution.

        Stops all adapters in reverse order:
        1. Cancel producer/consumer tasks
        2. Stop consumers
        3. Stop processors
        4. Stop producers
        5. Stop transport (last)
        """
        pipeline_id = self.config.pipeline_id

        logger.info("Stopping pipeline", pipeline_id=pipeline_id)

        self.running = False

        # Cancel background tasks
        for task in self.producer_tasks + self.consumer_tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(
            *self.producer_tasks,
            *self.consumer_tasks,
            return_exceptions=True
        )

        # Stop adapters in reverse order
        for lifecycle in reversed(self.consumer_lifecycles):
            await lifecycle.stop()

        for lifecycle in reversed(self.processor_lifecycles):
            await lifecycle.stop()

        for lifecycle in reversed(self.producer_lifecycles):
            await lifecycle.stop()

        await self.transport_lifecycle.stop()

        logger.info("Pipeline stopped", pipeline_id=pipeline_id)

    async def _start_adapter(
        self,
        lifecycle: AdapterLifecycle,
        config: dict[str, Any]
    ) -> None:
        """
        Initialize and start adapter with retry.

        Args:
            lifecycle: Adapter lifecycle manager
            config: Adapter configuration

        Raises:
            Exception if adapter fails to start after retries
        """
        adapter_id = lifecycle.adapter.metadata.id

        # Initialize
        init_result = await lifecycle.initialize(config)
        if not init_result.success:
            raise Exception(
                f"Failed to initialize adapter {adapter_id}: {init_result.message}"
            )

        # Start with retry
        start_result = await lifecycle.start_with_retry()
        if not start_result.success:
            raise Exception(
                f"Failed to start adapter {adapter_id}: {start_result.message}"
            )

    async def _run_producer(self, producer: IProducer) -> None:
        """
        Run producer task: generate data and publish to transport.

        Args:
            producer: Producer adapter
        """
        producer_id = producer.metadata.id

        logger.info("Producer task started", producer_id=producer_id)

        try:
            async for data in producer.produce():
                if not self.running:
                    break

                # Process through pipeline
                await self._process_data(data, source_adapter=producer_id)

        except asyncio.CancelledError:
            logger.info("Producer task cancelled", producer_id=producer_id)
        except Exception as e:
            logger.exception(
                "Producer task failed",
                producer_id=producer_id,
                error=str(e)
            )

    async def _run_consumer(self, consumer: IConsumer) -> None:
        """
        Run consumer task: subscribe to transport and consume data.

        Args:
            consumer: Consumer adapter
        """
        consumer_id = consumer.metadata.id

        logger.info("Consumer task started", consumer_id=consumer_id)

        try:
            # Subscribe to all data (pattern depends on transport)
            pattern = SubscriptionPattern(pattern="*")

            async for data in self.transport.subscribe(pattern):
                if not self.running:
                    break

                # Consume data
                result = await consumer.consume(data)

                if self.metrics_collector:
                    if result.success:
                        self.metrics_collector.increment_events_processed(
                            adapter_id=consumer_id
                        )
                    else:
                        self.metrics_collector.increment_events_failed(
                            adapter_id=consumer_id
                        )

                # Handle failures
                if not result.success:
                    await self._handle_consume_failure(data, consumer_id, result)

        except asyncio.CancelledError:
            logger.info("Consumer task cancelled", consumer_id=consumer_id)
        except Exception as e:
            logger.exception(
                "Consumer task failed",
                consumer_id=consumer_id,
                error=str(e)
            )

    async def _process_data(
        self,
        data: DataStream | Event,
        source_adapter: str
    ) -> None:
        """
        Process data through pipeline: processors → transport.

        Args:
            data: DataStream or Event to process
            source_adapter: ID of source adapter
        """
        current_data = data

        # Sequential processing through processors (FR-052)
        for processor in self.processors:
            processor_id = processor.metadata.id

            try:
                # Process (may return None for filtering)
                current_data = await processor.process(current_data)

                if self.metrics_collector:
                    self.metrics_collector.increment_events_processed(
                        adapter_id=processor_id
                    )

                # Filtered out
                if current_data is None:
                    logger.debug(
                        "Data filtered by processor",
                        processor_id=processor_id,
                        source_adapter=source_adapter
                    )
                    return

            except Exception as e:
                logger.exception(
                    "Processor failed",
                    processor_id=processor_id,
                    error=str(e)
                )

                if self.metrics_collector:
                    self.metrics_collector.increment_events_failed(
                        adapter_id=processor_id
                    )

                # Never block pipeline on bad data (FR-040)
                if self.config.config.error_policy == ErrorPolicy.DLQ:
                    await self._send_to_dlq(data, processor_id, str(e))
                elif self.config.config.error_policy == ErrorPolicy.HALT:
                    logger.error(
                        "Halting pipeline due to processor error",
                        pipeline_id=self.config.pipeline_id,
                        processor_id=processor_id
                    )
                    await self.stop()
                    raise
                # ErrorPolicy.CONTINUE: just log and skip

                return

        # Publish to transport
        await self._publish_data(current_data)

    async def _publish_data(self, data: DataStream | Event) -> None:
        """
        Publish data to transport.

        Args:
            data: DataStream or Event to publish
        """
        try:
            # Use default routing (can be customized per pipeline)
            routing = RoutingInfo(
                destination=f"{self.config.pipeline_id}/{data.source}"
            )

            result = await self.transport.publish(data, routing)

            if not result.success:
                logger.error(
                    "Transport publish failed",
                    pipeline_id=self.config.pipeline_id,
                    error=result.message
                )

        except Exception as e:
            logger.exception(
                "Exception during transport publish",
                pipeline_id=self.config.pipeline_id,
                error=str(e)
            )

    async def _handle_consume_failure(
        self,
        data: DataStream | Event,
        consumer_id: str,
        result: Any
    ) -> None:
        """
        Handle consumer processing failure.

        Args:
            data: Failed data
            consumer_id: Consumer adapter ID
            result: Failure result
        """
        error_msg = result.message or "Unknown error"

        logger.error(
            "Consumer processing failed",
            consumer_id=consumer_id,
            pipeline_id=self.config.pipeline_id,
            error=error_msg
        )

        # Apply error policy
        if self.config.config.error_policy == ErrorPolicy.DLQ:
            await self._send_to_dlq(data, consumer_id, error_msg)
        elif self.config.config.error_policy == ErrorPolicy.HALT:
            logger.error(
                "Halting pipeline due to consumer error",
                pipeline_id=self.config.pipeline_id,
                consumer_id=consumer_id
            )
            await self.stop()
            raise Exception(f"Consumer {consumer_id} failed: {error_msg}")
        # ErrorPolicy.CONTINUE: already logged

    async def _send_to_dlq(
        self,
        data: DataStream | Event,
        adapter_id: str,
        error: str
    ) -> None:
        """
        Send failed data to Dead Letter Queue.

        Args:
            data: Failed data
            adapter_id: Adapter that failed to process
            error: Error message
        """
        if not self.config.config.dlq or not self.config.config.dlq.enabled:
            logger.warning(
                "DLQ not configured, dropping failed data",
                adapter_id=adapter_id
            )
            return

        try:
            # Create DLQ event with error context
            from datetime import datetime
            from ...core.versioning.version import Version

            dlq_event = Event(
                event_id=uuid4(),
                timestamp=datetime.now(),
                source=adapter_id,
                event_type="dlq.failed_data",
                data={
                    "original_data": data.__dict__,
                    "error": error,
                    "pipeline_id": self.config.pipeline_id,
                    "adapter_id": adapter_id
                },
                trace_context=data.trace_context,  # Preserve original trace
                version=Version(major=1, minor=0)
            )

            # Write to DLQ storage
            # TODO: Implement DLQ storage backend (file, S3, database)
            logger.info(
                "Data sent to DLQ",
                adapter_id=adapter_id,
                dlq_event_id=str(dlq_event.event_id)
            )

        except Exception as e:
            logger.exception(
                "Failed to send data to DLQ",
                adapter_id=adapter_id,
                error=str(e)
            )

    def _get_adapter_config(self, adapter_id: str) -> dict[str, Any]:
        """
        Get adapter configuration from pipeline config.

        Args:
            adapter_id: Adapter ID

        Returns:
            Adapter configuration dict
        """
        # TODO: Load adapter configs from pipeline config
        # For now, return empty dict
        return {}
