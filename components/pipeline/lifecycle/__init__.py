"""
Adapter lifecycle management.

Provides exponential backoff retry and circuit breaker pattern.
"""

from .adapter_lifecycle import AdapterLifecycle

__all__ = ["AdapterLifecycle"]
