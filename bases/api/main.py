"""
API Base - FastStream API Entry Point

Web API for observability queries and future runtime control per T081.
Constitutional principle: III. Observable by Default

Note: This is a minimal v1.0 implementation. Full runtime API is v2.0 scope.
"""

import argparse
import sys
from typing import Any

from faststream import FastStream
from faststream.nats import NatsBroker


# Placeholder for v1.0 - minimal observability API
# v2.0 will add:
# - Pipeline management (create, update, delete)
# - Adapter lifecycle control
# - Configuration hot-reload
# - GraphQL/REST endpoints


app = FastStream()


@app.get("/health")
async def health_check() -> dict[str, str]:
    """
    Health check endpoint.

    Returns:
        Health status dictionary
    """
    return {
        "status": "healthy",
        "service": "ma114tsdb-api",
        "version": "1.0.0",
    }


@app.get("/metrics/summary")
async def metrics_summary() -> dict[str, Any]:
    """
    Metrics summary endpoint.

    Returns aggregate metrics across all adapters.
    Future: Integrate with Prometheus for real-time metrics.
    """
    return {
        "total_adapters": 0,
        "total_pipelines": 0,
        "events_processed": 0,
        "events_failed": 0,
        "message": "v1.0: Stub implementation. See Prometheus endpoint for metrics.",
    }


@app.get("/adapters")
async def list_adapters() -> dict[str, Any]:
    """
    List all registered adapters.

    Returns:
        List of adapter metadata.
    """
    return {
        "adapters": [],
        "count": 0,
        "message": "v1.0: Adapter registry not implemented. See v2.0 runtime API.",
    }


@app.get("/pipelines")
async def list_pipelines() -> dict[str, Any]:
    """
    List all configured pipelines.

    Returns:
        List of pipeline configurations.
    """
    return {
        "pipelines": [],
        "count": 0,
        "message": "v1.0: Pipeline listing not implemented. See v2.0 runtime API.",
    }


def main() -> None:
    """CLI entry point for API server."""
    parser = argparse.ArgumentParser(
        description="ma114tsdb API Server (v1.0 - Observability Only)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
v1.0 Endpoints:
  GET /health              - Health check
  GET /metrics/summary     - Metrics summary (stub)
  GET /adapters            - List adapters (stub)
  GET /pipelines           - List pipelines (stub)

v2.0 (Future):
  POST /pipelines          - Create pipeline
  PUT /pipelines/:id       - Update pipeline
  DELETE /pipelines/:id    - Delete pipeline
  POST /adapters/:id/start - Start adapter
  POST /adapters/:id/stop  - Stop adapter

Examples:
  %(prog)s                 # Start API server on default port
  %(prog)s --port 8080     # Start on custom port
        """,
    )

    parser.add_argument(
        "--host",
        type=str,
        default="0.0.0.0",
        help="API server host (default: 0.0.0.0)",
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="API server port (default: 8000)",
    )

    args = parser.parse_args()

    print(f"[STARTUP] Starting ma114tsdb API server (v1.0)")
    print(f"[STARTUP] Host: {args.host}, Port: {args.port}")
    print(f"[INFO] v1.0: Observability endpoints only")
    print(f"[INFO] v2.0: Full runtime API (pipeline management, hot-reload)")
    print()

    try:
        # Run FastStream app
        # Note: FastStream will handle async event loop
        app.run(host=args.host, port=args.port)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] API server stopped")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] Failed to start API server: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
