"""
Prometheus Metrics Collector for ma114tsdb Adapters

Provides metrics collection for:
- events_processed (Counter)
- events_failed (Counter)
- processing_time (Histogram)
- queue_depth (Gauge)
- adapter_state (Gauge)

Based on FR-030: Prometheus metrics requirement
"""

from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, REGISTRY
from typing import Dict, Optional


class PrometheusCollector:
    """Collects and exposes Prometheus metrics for adapters."""

    def __init__(self, adapter_id: str, registry: Optional[CollectorRegistry] = None):
        """
        Initialize Prometheus metrics collector.

        Args:
            adapter_id: Unique adapter identifier for metric labels
            registry: Optional Prometheus registry (defaults to global REGISTRY)
        """
        self.adapter_id = adapter_id
        self.registry = registry or REGISTRY

        # Get or create metrics (handles multiple instances with same registry)
        # Counter: Total events processed
        try:
            self.events_processed = Counter(
                'ma114tsdb_events_processed',
                'Total number of events processed',
                ['adapter_id'],
                registry=self.registry
            )
        except ValueError:
            # Metric already exists, get it from registry
            self.events_processed = next(
                c for c in self.registry._collector_to_names
                if isinstance(c, Counter) and 'ma114tsdb_events_processed' in self.registry._collector_to_names[c]
            )

        # Counter: Total events failed
        try:
            self.events_failed = Counter(
                'ma114tsdb_events_failed',
                'Total number of events that failed processing',
                ['adapter_id'],
                registry=self.registry
            )
        except ValueError:
            self.events_failed = next(
                c for c in self.registry._collector_to_names
                if isinstance(c, Counter) and 'ma114tsdb_events_failed' in self.registry._collector_to_names[c]
            )

        # Counter: Total events filtered (for processors)
        try:
            self.events_filtered = Counter(
                'ma114tsdb_events_filtered',
                'Total number of events filtered out by processors',
                ['adapter_id'],
                registry=self.registry
            )
        except ValueError:
            self.events_filtered = next(
                c for c in self.registry._collector_to_names
                if isinstance(c, Counter) and 'ma114tsdb_events_filtered' in self.registry._collector_to_names[c]
            )

        # Histogram: Processing time in seconds
        try:
            self.processing_time = Histogram(
                'ma114tsdb_processing_time_seconds',
                'Time spent processing events',
                ['adapter_id'],
                buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0),
                registry=self.registry
            )
        except ValueError:
            self.processing_time = next(
                c for c in self.registry._collector_to_names
                if isinstance(c, Histogram) and 'ma114tsdb_processing_time_seconds' in self.registry._collector_to_names[c]
            )

        # Gauge: Current queue depth
        try:
            self.queue_depth = Gauge(
                'ma114tsdb_queue_depth',
                'Current queue depth',
                ['adapter_id'],
                registry=self.registry
            )
        except ValueError:
            self.queue_depth = next(
                c for c in self.registry._collector_to_names
                if isinstance(c, Gauge) and 'ma114tsdb_queue_depth' in self.registry._collector_to_names[c]
            )

        # Gauge: Adapter state (0=init, 1=running, 2=stopped, 3=error, 4=failed)
        try:
            self.adapter_state = Gauge(
                'ma114tsdb_adapter_state',
                'Current adapter state',
                ['adapter_id'],
                registry=self.registry
            )
        except ValueError:
            self.adapter_state = next(
                c for c in self.registry._collector_to_names
                if isinstance(c, Gauge) and 'ma114tsdb_adapter_state' in self.registry._collector_to_names[c]
            )

    def record_event_processed(self) -> None:
        """Increment events processed counter."""
        self.events_processed.labels(adapter_id=self.adapter_id).inc()

    def record_event_failed(self) -> None:
        """Increment events failed counter."""
        self.events_failed.labels(adapter_id=self.adapter_id).inc()

    def record_event_filtered(self) -> None:
        """Increment events filtered counter (for processors)."""
        self.events_filtered.labels(adapter_id=self.adapter_id).inc()

    def record_processing_time(self, duration_seconds: float) -> None:
        """
        Record processing time.

        Args:
            duration_seconds: Duration in seconds
        """
        self.processing_time.labels(adapter_id=self.adapter_id).observe(duration_seconds)

    def set_queue_depth(self, depth: int) -> None:
        """
        Set current queue depth.

        Args:
            depth: Current queue depth
        """
        self.queue_depth.labels(adapter_id=self.adapter_id).set(depth)

    def set_adapter_state(self, state: str) -> None:
        """
        Set adapter state.

        Args:
            state: Adapter state (init, running, stopped, error, failed)
        """
        state_mapping = {
            'init': 0,
            'running': 1,
            'stopped': 2,
            'error': 3,
            'failed': 4
        }
        value = state_mapping.get(state.lower(), -1)
        self.adapter_state.labels(adapter_id=self.adapter_id).set(value)


__all__ = ['PrometheusCollector']
