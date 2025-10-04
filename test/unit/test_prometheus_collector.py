"""
Unit tests for Prometheus metrics collector.

Tests metric collection (counter increment, histogram recording, gauge set).
"""

import pytest
from prometheus_client import CollectorRegistry
from components.observability.metrics import PrometheusCollector


class TestPrometheusCollector:
    """Test suite for PrometheusCollector."""

    def test_collector_initialization(self):
        """Test collector initializes with adapter_id."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-adapter", registry=registry)
        assert collector.adapter_id == "test-adapter"
        assert collector.registry == registry

    def test_record_event_processed_increments_counter(self):
        """Test events processed counter increments."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-adapter", registry=registry)

        # Record events
        collector.record_event_processed()
        collector.record_event_processed()

        # Get metric value
        metrics = registry.get_sample_value(
            'ma114tsdb_events_processed_total',
            {'adapter_id': 'test-adapter'}
        )
        assert metrics == 2.0

    def test_record_event_failed_increments_counter(self):
        """Test events failed counter increments."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-adapter", registry=registry)

        collector.record_event_failed()
        collector.record_event_failed()
        collector.record_event_failed()

        metrics = registry.get_sample_value(
            'ma114tsdb_events_failed_total',
            {'adapter_id': 'test-adapter'}
        )
        assert metrics == 3.0

    def test_record_event_filtered_increments_counter(self):
        """Test events filtered counter increments (for processors)."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-processor", registry=registry)

        collector.record_event_filtered()

        metrics = registry.get_sample_value(
            'ma114tsdb_events_filtered_total',
            {'adapter_id': 'test-processor'}
        )
        assert metrics == 1.0

    def test_record_processing_time_histogram(self):
        """Test processing time histogram recording."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-adapter", registry=registry)

        # Record various durations
        collector.record_processing_time(0.001)  # 1ms
        collector.record_processing_time(0.05)   # 50ms
        collector.record_processing_time(0.5)    # 500ms

        # Check count
        count = registry.get_sample_value(
            'ma114tsdb_processing_time_seconds_count',
            {'adapter_id': 'test-adapter'}
        )
        assert count == 3.0

        # Check sum
        sum_val = registry.get_sample_value(
            'ma114tsdb_processing_time_seconds_sum',
            {'adapter_id': 'test-adapter'}
        )
        assert sum_val == pytest.approx(0.551, rel=1e-3)

    def test_set_queue_depth_gauge(self):
        """Test queue depth gauge set."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-adapter", registry=registry)

        collector.set_queue_depth(10)
        metrics = registry.get_sample_value(
            'ma114tsdb_queue_depth',
            {'adapter_id': 'test-adapter'}
        )
        assert metrics == 10.0

        collector.set_queue_depth(5)
        metrics = registry.get_sample_value(
            'ma114tsdb_queue_depth',
            {'adapter_id': 'test-adapter'}
        )
        assert metrics == 5.0

    def test_set_adapter_state_gauge(self):
        """Test adapter state gauge mapping."""
        registry = CollectorRegistry()
        collector = PrometheusCollector("test-adapter", registry=registry)

        # Test state transitions
        states = {
            'init': 0,
            'running': 1,
            'stopped': 2,
            'error': 3,
            'failed': 4
        }

        for state, expected_value in states.items():
            collector.set_adapter_state(state)
            metrics = registry.get_sample_value(
                'ma114tsdb_adapter_state',
                {'adapter_id': 'test-adapter'}
            )
            assert metrics == expected_value

    def test_multiple_adapters_independent_metrics(self):
        """Test multiple adapters have independent metrics."""
        registry = CollectorRegistry()
        collector1 = PrometheusCollector("adapter-1", registry=registry)
        collector2 = PrometheusCollector("adapter-2", registry=registry)

        collector1.record_event_processed()
        collector1.record_event_processed()
        collector2.record_event_processed()

        metrics1 = registry.get_sample_value(
            'ma114tsdb_events_processed_total',
            {'adapter_id': 'adapter-1'}
        )
        metrics2 = registry.get_sample_value(
            'ma114tsdb_events_processed_total',
            {'adapter_id': 'adapter-2'}
        )

        assert metrics1 == 2.0
        assert metrics2 == 1.0
