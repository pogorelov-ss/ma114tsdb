"""
Pipeline runtime execution engine.

Orchestrates data flow: Producer → Transport → Processor → Consumer
"""

from .pipeline_engine import PipelineEngine

__all__ = ["PipelineEngine"]
