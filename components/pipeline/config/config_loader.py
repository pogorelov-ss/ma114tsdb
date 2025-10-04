"""
TOML Configuration Loader for ma114tsdb

Constitutional principle: IV. Programmed with Strict Contracts
File-based configuration with pydantic validation per FR-056, FR-057.
"""

import tomllib
from pathlib import Path
from typing import Any

from components.core.models.pipeline_config import (
    DLQConfig,
    ErrorPolicy,
    PipelineConfig,
    PipelineRuntimeConfig,
    TransportConfig,
)


class ConfigLoadError(Exception):
    """Raised when configuration file cannot be loaded or parsed."""

    pass


class ConfigValidationError(Exception):
    """Raised when configuration validation fails."""

    pass


def load_config(config_path: str | Path) -> dict[str, Any]:
    """
    Load and parse TOML configuration file.

    Args:
        config_path: Path to TOML configuration file

    Returns:
        Dictionary with parsed configuration sections:
        - system: System configuration
        - transport: Transport configuration
        - adapters: List of adapter configurations
        - pipelines: List of pipeline configurations

    Raises:
        ConfigLoadError: If file cannot be read or parsed
        ConfigValidationError: If configuration is invalid

    Example:
        >>> config = load_config("pipeline.toml")
        >>> config["system"]["id"]
        'ma114tsdb-prod'
        >>> len(config["pipelines"])
        2
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise ConfigLoadError(f"Configuration file not found: {config_path}")

    if not config_path.is_file():
        raise ConfigLoadError(f"Configuration path is not a file: {config_path}")

    try:
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
    except tomllib.TOMLDecodeError as e:
        raise ConfigLoadError(f"Failed to parse TOML: {e}") from e
    except OSError as e:
        raise ConfigLoadError(f"Failed to read file: {e}") from e

    # Validate required sections
    if "system" not in config:
        raise ConfigValidationError("Missing required section: [system]")
    if "transport" not in config:
        raise ConfigValidationError("Missing required section: [transport]")
    if "pipelines" not in config:
        raise ConfigValidationError(
            "Missing required section: [[pipelines]] (at least one pipeline required)"
        )

    # Validate system section
    if "id" not in config["system"]:
        raise ConfigValidationError("Missing required field: system.id")
    if "environment" not in config["system"]:
        raise ConfigValidationError("Missing required field: system.environment")

    # Validate adapters section (optional but validate if present)
    if "adapters" in config and not isinstance(config["adapters"], list):
        raise ConfigValidationError("adapters must be an array: [[adapters]]")

    # Validate pipelines section
    if not isinstance(config["pipelines"], list):
        raise ConfigValidationError("pipelines must be an array: [[pipelines]]")
    if len(config["pipelines"]) == 0:
        raise ConfigValidationError(
            "At least one pipeline required in [[pipelines]] section"
        )

    return config


def parse_transport_config(transport_dict: dict[str, Any]) -> TransportConfig:
    """
    Parse transport configuration from TOML dict.

    Args:
        transport_dict: Transport configuration dictionary from TOML

    Returns:
        TransportConfig instance

    Raises:
        ConfigValidationError: If transport config is invalid

    Example:
        >>> transport_dict = {
        ...     "type": "mqtt",
        ...     "broker": "mqtt://localhost:1883",
        ...     "qos_datastream": 0,
        ...     "qos_event": 1
        ... }
        >>> transport = parse_transport_config(transport_dict)
        >>> transport.type
        'mqtt'
    """
    if "type" not in transport_dict:
        raise ConfigValidationError("transport.type is required")

    transport_type = transport_dict["type"]

    # Extract all fields except 'type' as transport-specific config
    config_dict = {k: v for k, v in transport_dict.items() if k != "type"}

    try:
        return TransportConfig(type=transport_type, config=config_dict)
    except ValueError as e:
        raise ConfigValidationError(f"Invalid transport configuration: {e}") from e


def parse_dlq_config(dlq_dict: dict[str, Any]) -> DLQConfig:
    """
    Parse DLQ configuration from TOML dict.

    Args:
        dlq_dict: DLQ configuration dictionary from TOML

    Returns:
        DLQConfig instance

    Raises:
        ConfigValidationError: If DLQ config is invalid

    Example:
        >>> dlq_dict = {
        ...     "enabled": True,
        ...     "storage": "file",
        ...     "retention": "P7D",
        ...     "storage_config": {"path": "dlq/"}
        ... }
        >>> dlq = parse_dlq_config(dlq_dict)
        >>> dlq.enabled
        True
    """
    required_fields = ["enabled", "storage", "retention"]
    for field in required_fields:
        if field not in dlq_dict:
            raise ConfigValidationError(f"DLQ config missing required field: {field}")

    try:
        return DLQConfig(
            enabled=dlq_dict["enabled"],
            storage=dlq_dict["storage"],
            retention=dlq_dict["retention"],
            retry_policy=dlq_dict.get("retry_policy"),
        )
    except ValueError as e:
        raise ConfigValidationError(f"Invalid DLQ configuration: {e}") from e


def parse_runtime_config(config_dict: dict[str, Any]) -> PipelineRuntimeConfig:
    """
    Parse pipeline runtime configuration from TOML dict.

    Args:
        config_dict: Runtime configuration dictionary from TOML

    Returns:
        PipelineRuntimeConfig instance

    Raises:
        ConfigValidationError: If runtime config is invalid

    Example:
        >>> config_dict = {
        ...     "parallelism": 4,
        ...     "buffer_size": 5000,
        ...     "time_window_minutes": 5,
        ...     "error_policy": "dlq",
        ...     "dlq": {"enabled": True, "storage": "file", "retention": "P7D"}
        ... }
        >>> runtime_config = parse_runtime_config(config_dict)
        >>> runtime_config.parallelism
        4
    """
    # Parse error policy
    error_policy_str = config_dict.get("error_policy", "continue")
    try:
        error_policy = ErrorPolicy(error_policy_str)
    except ValueError:
        raise ConfigValidationError(
            f"Invalid error_policy: {error_policy_str}. "
            f"Must be one of: continue, dlq, halt"
        )

    # Parse DLQ config if present
    dlq = None
    if "dlq" in config_dict:
        dlq = parse_dlq_config(config_dict["dlq"])

    try:
        return PipelineRuntimeConfig(
            parallelism=config_dict.get("parallelism", 1),
            buffer_size=config_dict.get("buffer_size", 1000),
            time_window_minutes=config_dict.get("time_window_minutes", 5),
            error_policy=error_policy,
            dlq=dlq,
        )
    except ValueError as e:
        raise ConfigValidationError(f"Invalid runtime configuration: {e}") from e


def parse_pipeline_config(
    pipeline_dict: dict[str, Any], transport_config: TransportConfig
) -> PipelineConfig:
    """
    Parse pipeline configuration from TOML dict.

    Args:
        pipeline_dict: Pipeline configuration dictionary from TOML
        transport_config: Transport configuration (shared across pipelines)

    Returns:
        PipelineConfig instance

    Raises:
        ConfigValidationError: If pipeline config is invalid

    Example:
        >>> pipeline_dict = {
        ...     "pipeline_id": "sensor-pipeline",
        ...     "name": "Temperature Sensor Pipeline",
        ...     "sources": ["temp-sensor"],
        ...     "sinks": ["console-logger"],
        ...     "processors": ["anomaly-filter"],
        ...     "config": {"parallelism": 4}
        ... }
        >>> transport = TransportConfig(type="memory", config={})
        >>> pipeline = parse_pipeline_config(pipeline_dict, transport)
        >>> pipeline.pipeline_id
        'sensor-pipeline'
    """
    required_fields = ["pipeline_id", "name", "sources", "sinks"]
    for field in required_fields:
        if field not in pipeline_dict:
            raise ConfigValidationError(
                f"Pipeline config missing required field: {field}"
            )

    # Parse runtime config (required)
    if "config" not in pipeline_dict:
        raise ConfigValidationError("Pipeline config missing required section: config")

    runtime_config = parse_runtime_config(pipeline_dict["config"])

    try:
        return PipelineConfig(
            pipeline_id=pipeline_dict["pipeline_id"],
            name=pipeline_dict["name"],
            transport=transport_config,
            sources=pipeline_dict["sources"],
            sinks=pipeline_dict["sinks"],
            processors=pipeline_dict.get("processors", []),
            config=runtime_config,
        )
    except ValueError as e:
        raise ConfigValidationError(f"Invalid pipeline configuration: {e}") from e


def load_pipeline_configs(config_path: str | Path) -> list[PipelineConfig]:
    """
    Load and validate all pipeline configurations from TOML file.

    This is the main entry point for loading pipeline configurations.

    Args:
        config_path: Path to TOML configuration file

    Returns:
        List of validated PipelineConfig instances

    Raises:
        ConfigLoadError: If file cannot be read or parsed
        ConfigValidationError: If configuration is invalid

    Example:
        >>> pipelines = load_pipeline_configs("pipeline.toml")
        >>> len(pipelines)
        2
        >>> pipelines[0].pipeline_id
        'sensor-pipeline'
    """
    # Load raw TOML config
    config = load_config(config_path)

    # Parse transport config (shared across all pipelines in v1.0)
    transport = parse_transport_config(config["transport"])

    # Parse all pipeline configs
    pipelines = []
    for i, pipeline_dict in enumerate(config["pipelines"]):
        try:
            pipeline = parse_pipeline_config(pipeline_dict, transport)
            pipelines.append(pipeline)
        except ConfigValidationError as e:
            raise ConfigValidationError(
                f"Failed to parse pipeline {i + 1}: {e}"
            ) from e

    return pipelines
