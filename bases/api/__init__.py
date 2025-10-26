"""
API Base - Web API Entry Point

FastStream-based web API for observability and future runtime control.
"""

from bases.api.main import app, health_check, list_adapters, list_pipelines, main, metrics_summary

__all__ = ["app", "main", "health_check", "metrics_summary", "list_adapters", "list_pipelines"]
