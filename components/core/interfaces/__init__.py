"""Adapter interface contracts for ma114tsdb."""

from .adapter import IAdapter
from .producer import IProducer
from .consumer import IConsumer
from .processor import IProcessor
from .transport import ITransport

__all__ = ["IAdapter", "IProducer", "IConsumer", "IProcessor", "ITransport"]
