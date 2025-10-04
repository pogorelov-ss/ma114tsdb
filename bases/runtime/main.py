"""
Runtime Base - Main Entry Point for Pipeline Execution

Loads configuration and starts pipeline runtime per T080.
Constitutional principle: II. Everything is an Adapter
"""

import argparse
import asyncio
import signal
import sys
from pathlib import Path
from typing import Any

from components.observability.logging.structured_logger import configure_logging
from components.pipeline.config import ConfigLoadError, ConfigValidationError, load_pipeline_configs
from components.pipeline.runtime.pipeline_engine import PipelineEngine


# Global engine reference for signal handling
_engine: PipelineEngine | None = None


def setup_signal_handlers(engine: PipelineEngine) -> None:
    """
    Setup signal handlers for graceful shutdown.

    Handles SIGINT (Ctrl+C) and SIGTERM for clean adapter shutdown.
    """

    def signal_handler(signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name
        print(f"\n[SHUTDOWN] Received {signal_name}, stopping gracefully...")

        # Schedule engine shutdown
        asyncio.create_task(engine.stop())

    # Register handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def run_pipeline(config_path: Path) -> int:
    """
    Main pipeline execution function.

    Args:
        config_path: Path to TOML configuration file

    Returns:
        Exit code (0=success, 1=error)
    """
    global _engine

    try:
        # Load pipeline configurations
        print(f"[STARTUP] Loading configuration from {config_path}")
        pipelines = load_pipeline_configs(config_path)
        print(f"[STARTUP] Loaded {len(pipelines)} pipeline(s)")

        # Create pipeline engine
        _engine = PipelineEngine(pipelines)

        # Setup signal handlers
        setup_signal_handlers(_engine)

        # Start engine
        print("[STARTUP] Starting pipeline engine...")
        result = await _engine.start()

        if not result.success:
            print(f"[ERROR] Failed to start engine: {result.message}")
            return 1

        print("[STARTUP] Pipeline engine started successfully")
        print("[RUNTIME] Press Ctrl+C to stop")

        # Run until stopped
        await _engine.run()

        print("[SHUTDOWN] Pipeline engine stopped")
        return 0

    except ConfigLoadError as e:
        print(f"[ERROR] Configuration load error: {e}")
        return 1
    except ConfigValidationError as e:
        print(f"[ERROR] Configuration validation error: {e}")
        return 1
    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="ma114tsdb Pipeline Runtime",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s pipeline.toml              # Run pipeline from config
  %(prog)s --log-level debug app.toml # Run with debug logging
        """,
    )

    parser.add_argument(
        "config",
        type=str,
        help="Path to TOML configuration file",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["debug", "info", "warning", "error"],
        default="info",
        help="Logging level (default: info)",
    )

    parser.add_argument(
        "--log-format",
        type=str,
        choices=["json", "console"],
        default="console",
        help="Log output format (default: console)",
    )

    args = parser.parse_args()

    # Configure logging
    configure_logging(level=args.log_level.upper(), format_type=args.log_format)

    # Validate config file exists
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"[ERROR] Configuration file not found: {config_path}")
        sys.exit(1)

    # Run pipeline
    try:
        exit_code = asyncio.run(run_pipeline(config_path))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
