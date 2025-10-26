"""
Runtime Base - Pipeline Execution Entry Point

Main application entry point for running pipelines.
"""

from bases.runtime.main import main, run_pipeline, setup_signal_handlers

__all__ = ["main", "run_pipeline", "setup_signal_handlers"]
