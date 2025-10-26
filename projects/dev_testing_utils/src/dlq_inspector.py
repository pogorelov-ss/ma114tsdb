#!/usr/bin/env python3
"""
Dead Letter Queue Inspector Tool

CLI tool to inspect failed data preserved in DLQ per T071.

Usage:
    python -m projects.dev_testing_utils.src.dlq_inspector <dlq_path>
    python -m projects.dev_testing_utils.src.dlq_inspector dlq/ --format json
    python -m projects.dev_testing_utils.src.dlq_inspector dlq/ --filter event
"""

import argparse
import json
from pathlib import Path
from typing import Any


def inspect_dlq_file(file_path: Path, format_type: str = "summary") -> dict[str, Any]:
    """
    Inspect a single DLQ file.

    Args:
        file_path: Path to DLQ file
        format_type: Output format ("summary" | "json" | "verbose")

    Returns:
        Dictionary with file metadata and content
    """
    try:
        with open(file_path, "r") as f:
            content = json.load(f)

        # Extract metadata
        data_type = content.get("type", "unknown")
        timestamp = content.get("timestamp", "unknown")
        error_context = content.get("error_context", {})

        result = {
            "file": str(file_path),
            "type": data_type,
            "timestamp": timestamp,
            "error": error_context.get("message", "No error message"),
            "trace_id": None,
            "content": content,
        }

        # Extract trace ID if present
        if "trace_context" in content:
            result["trace_id"] = content["trace_context"].get("trace_id")

        return result

    except json.JSONDecodeError as e:
        return {
            "file": str(file_path),
            "error": f"Invalid JSON: {e}",
            "content": None,
        }
    except Exception as e:
        return {
            "file": str(file_path),
            "error": f"Failed to read: {e}",
            "content": None,
        }


def print_summary(entry: dict[str, Any]) -> None:
    """Print summary of DLQ entry."""
    print(f"File: {entry['file']}")
    print(f"  Type:      {entry.get('type', 'N/A')}")
    print(f"  Timestamp: {entry.get('timestamp', 'N/A')}")
    if entry.get("trace_id"):
        print(f"  Trace ID:  {entry['trace_id']}")
    print(f"  Error:     {entry.get('error', 'N/A')}")
    print()


def print_json(entry: dict[str, Any]) -> None:
    """Print JSON representation of DLQ entry."""
    print(json.dumps(entry["content"], indent=2))
    print()


def print_verbose(entry: dict[str, Any]) -> None:
    """Print verbose details of DLQ entry."""
    print("=" * 80)
    print(f"File:       {entry['file']}")
    print(f"Type:       {entry.get('type', 'N/A')}")
    print(f"Timestamp:  {entry.get('timestamp', 'N/A')}")

    if entry.get("trace_id"):
        print(f"Trace ID:   {entry['trace_id']}")

    content = entry.get("content", {})
    if "error_context" in content:
        error_ctx = content["error_context"]
        print(f"\nError Context:")
        print(f"  Message:    {error_ctx.get('message', 'N/A')}")
        print(f"  Exception:  {error_ctx.get('exception', 'N/A')}")
        print(f"  Adapter ID: {error_ctx.get('adapter_id', 'N/A')}")

    if content.get("payload"):
        print(f"\nPayload:")
        print(json.dumps(content["payload"], indent=2))

    print("=" * 80)
    print()


def main() -> None:
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Inspect Dead Letter Queue entries",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s dlq/                         # Summary of all DLQ entries
  %(prog)s dlq/ --format json           # JSON output
  %(prog)s dlq/ --format verbose        # Verbose output
  %(prog)s dlq/ --filter event          # Show only Events
  %(prog)s dlq/ --trace-id <uuid>       # Filter by trace ID
        """,
    )

    parser.add_argument(
        "dlq_path",
        type=str,
        help="Path to DLQ directory",
    )

    parser.add_argument(
        "--format",
        type=str,
        choices=["summary", "json", "verbose"],
        default="summary",
        help="Output format (default: summary)",
    )

    parser.add_argument(
        "--filter",
        type=str,
        choices=["datastream", "event"],
        help="Filter by data type",
    )

    parser.add_argument(
        "--trace-id",
        type=str,
        help="Filter by trace ID",
    )

    args = parser.parse_args()

    dlq_path = Path(args.dlq_path)

    # Validate DLQ path
    if not dlq_path.exists():
        print(f"Error: DLQ path does not exist: {dlq_path}")
        exit(1)

    if not dlq_path.is_dir():
        print(f"Error: DLQ path is not a directory: {dlq_path}")
        exit(1)

    # Find all DLQ files (JSON files)
    dlq_files = sorted(dlq_path.glob("*.json"))

    if not dlq_files:
        print(f"No DLQ entries found in {dlq_path}")
        exit(0)

    print(f"Found {len(dlq_files)} DLQ entries in {dlq_path}")
    print()

    # Inspect each file
    matched_count = 0
    for file_path in dlq_files:
        entry = inspect_dlq_file(file_path, args.format)

        # Apply filters
        if args.filter and entry.get("type") != args.filter:
            continue

        if args.trace_id and entry.get("trace_id") != args.trace_id:
            continue

        matched_count += 1

        # Print based on format
        if args.format == "summary":
            print_summary(entry)
        elif args.format == "json":
            print_json(entry)
        else:  # verbose
            print_verbose(entry)

    # Summary
    if args.filter or args.trace_id:
        print(f"Matched {matched_count} of {len(dlq_files)} entries")


if __name__ == "__main__":
    main()
