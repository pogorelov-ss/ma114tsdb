"""
Unit tests for TOML configuration loader

Tests configuration parsing, validation, and error handling per T067.
"""

import tempfile
from pathlib import Path

import pytest

from components.core.models.pipeline_config import (
    DLQConfig,
    ErrorPolicy,
    PipelineConfig,
    PipelineRuntimeConfig,
    TransportConfig,
)
from components.pipeline.config import (
    ConfigLoadError,
    ConfigValidationError,
    load_config,
    load_pipeline_configs,
    parse_dlq_config,
    parse_pipeline_config,
    parse_runtime_config,
    parse_transport_config,
)


class TestLoadConfig:
    """Test raw TOML configuration loading."""

    def test_load_valid_config(self, tmp_path: Path) -> None:
        """Test loading valid TOML configuration."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test-system"
environment = "test"

[transport]
type = "memory"

[[pipelines]]
pipeline_id = "test-pipeline"
name = "Test Pipeline"
sources = ["producer1"]
sinks = ["consumer1"]

[pipelines.config]
parallelism = 1
"""
        )

        config = load_config(config_path)
        assert config["system"]["id"] == "test-system"
        assert config["system"]["environment"] == "test"
        assert config["transport"]["type"] == "memory"
        assert len(config["pipelines"]) == 1

    def test_load_missing_file(self) -> None:
        """Test loading non-existent file raises ConfigLoadError."""
        with pytest.raises(ConfigLoadError, match="Configuration file not found"):
            load_config("/nonexistent/config.toml")

    def test_load_directory_instead_of_file(self, tmp_path: Path) -> None:
        """Test loading directory instead of file raises ConfigLoadError."""
        with pytest.raises(ConfigLoadError, match="not a file"):
            load_config(tmp_path)

    def test_load_invalid_toml(self, tmp_path: Path) -> None:
        """Test loading invalid TOML raises ConfigLoadError."""
        config_path = tmp_path / "invalid.toml"
        config_path.write_text("invalid toml [[[")

        with pytest.raises(ConfigLoadError, match="Failed to parse TOML"):
            load_config(config_path)

    def test_load_missing_system_section(self, tmp_path: Path) -> None:
        """Test missing [system] section raises ConfigValidationError."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[transport]
type = "memory"

[[pipelines]]
pipeline_id = "test"
"""
        )

        with pytest.raises(ConfigValidationError, match="Missing required section: \\[system\\]"):
            load_config(config_path)

    def test_load_missing_transport_section(self, tmp_path: Path) -> None:
        """Test missing [transport] section raises ConfigValidationError."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test"
environment = "test"

[[pipelines]]
pipeline_id = "test"
"""
        )

        with pytest.raises(ConfigValidationError, match="Missing required section: \\[transport\\]"):
            load_config(config_path)

    def test_load_missing_pipelines_section(self, tmp_path: Path) -> None:
        """Test missing [[pipelines]] section raises ConfigValidationError."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test"
environment = "test"

[transport]
type = "memory"
"""
        )

        with pytest.raises(ConfigValidationError, match="Missing required section: \\[\\[pipelines\\]\\]"):
            load_config(config_path)

    def test_load_empty_pipelines(self, tmp_path: Path) -> None:
        """Test empty pipelines array raises ConfigValidationError."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test"
environment = "test"

[transport]
type = "memory"

[[pipelines]]
"""
        )

        # Empty pipelines table is treated as missing required fields in parse_pipeline_config
        with pytest.raises(ConfigValidationError):
            load_pipeline_configs(config_path)


class TestParseTransportConfig:
    """Test transport configuration parsing."""

    def test_parse_memory_transport(self) -> None:
        """Test parsing memory transport configuration."""
        transport_dict = {"type": "memory", "buffer_size": 1000}
        transport = parse_transport_config(transport_dict)

        assert transport.type == "memory"
        assert transport.config == {"buffer_size": 1000}

    def test_parse_mqtt_transport(self) -> None:
        """Test parsing MQTT transport configuration."""
        transport_dict = {
            "type": "mqtt",
            "broker": "mqtt://localhost:1883",
            "qos_datastream": 0,
            "qos_event": 1,
        }
        transport = parse_transport_config(transport_dict)

        assert transport.type == "mqtt"
        assert transport.config["broker"] == "mqtt://localhost:1883"
        assert transport.config["qos_datastream"] == 0
        assert transport.config["qos_event"] == 1

    def test_parse_kombu_transport(self) -> None:
        """Test parsing Kombu transport configuration."""
        transport_dict = {
            "type": "kombu",
            "url": "amqp://guest:guest@localhost:5672//",
            "exchange": "ma114tsdb",
        }
        transport = parse_transport_config(transport_dict)

        assert transport.type == "kombu"
        assert transport.config["url"] == "amqp://guest:guest@localhost:5672//"

    def test_parse_missing_type(self) -> None:
        """Test missing type field raises ConfigValidationError."""
        transport_dict = {"broker": "mqtt://localhost"}

        with pytest.raises(ConfigValidationError, match="transport.type is required"):
            parse_transport_config(transport_dict)

    def test_parse_invalid_type(self) -> None:
        """Test invalid transport type raises ConfigValidationError."""
        transport_dict = {"type": "invalid-transport"}

        with pytest.raises(ConfigValidationError, match="Invalid transport configuration"):
            parse_transport_config(transport_dict)


class TestParseDLQConfig:
    """Test DLQ configuration parsing."""

    def test_parse_valid_dlq(self) -> None:
        """Test parsing valid DLQ configuration."""
        dlq_dict = {
            "enabled": True,
            "storage": "file",
            "retention": "P7D",
            "storage_config": {"path": "dlq/"},
        }
        dlq = parse_dlq_config(dlq_dict)

        assert dlq.enabled is True
        assert dlq.storage == "file"
        assert dlq.retention == "P7D"

    def test_parse_dlq_missing_enabled(self) -> None:
        """Test missing enabled field raises ConfigValidationError."""
        dlq_dict = {"storage": "file", "retention": "P7D"}

        with pytest.raises(ConfigValidationError, match="DLQ config missing required field: enabled"):
            parse_dlq_config(dlq_dict)

    def test_parse_dlq_invalid_storage(self) -> None:
        """Test invalid storage type raises ConfigValidationError."""
        dlq_dict = {
            "enabled": True,
            "storage": "invalid-storage",
            "retention": "P7D",
        }

        with pytest.raises(ConfigValidationError, match="Invalid DLQ configuration"):
            parse_dlq_config(dlq_dict)


class TestParseRuntimeConfig:
    """Test pipeline runtime configuration parsing."""

    def test_parse_runtime_config_defaults(self) -> None:
        """Test parsing runtime config with all defaults."""
        config_dict = {}
        runtime_config = parse_runtime_config(config_dict)

        assert runtime_config.parallelism == 1
        assert runtime_config.buffer_size == 1000
        assert runtime_config.time_window_minutes == 5
        assert runtime_config.error_policy == ErrorPolicy.CONTINUE
        assert runtime_config.dlq is None

    def test_parse_runtime_config_custom_values(self) -> None:
        """Test parsing runtime config with custom values."""
        config_dict = {
            "parallelism": 4,
            "buffer_size": 5000,
            "time_window_minutes": 10,
            "error_policy": "continue",
        }
        runtime_config = parse_runtime_config(config_dict)

        assert runtime_config.parallelism == 4
        assert runtime_config.buffer_size == 5000
        assert runtime_config.time_window_minutes == 10
        assert runtime_config.error_policy == ErrorPolicy.CONTINUE

    def test_parse_runtime_config_with_dlq(self) -> None:
        """Test parsing runtime config with DLQ."""
        config_dict = {
            "error_policy": "dlq",
            "dlq": {
                "enabled": True,
                "storage": "file",
                "retention": "P7D",
            },
        }
        runtime_config = parse_runtime_config(config_dict)

        assert runtime_config.error_policy == ErrorPolicy.DLQ
        assert runtime_config.dlq is not None
        assert runtime_config.dlq.enabled is True

    def test_parse_invalid_error_policy(self) -> None:
        """Test invalid error policy raises ConfigValidationError."""
        config_dict = {"error_policy": "invalid-policy"}

        with pytest.raises(ConfigValidationError, match="Invalid error_policy"):
            parse_runtime_config(config_dict)

    def test_parse_dlq_policy_without_dlq_config(self) -> None:
        """Test DLQ error policy without DLQ config raises ConfigValidationError."""
        config_dict = {"error_policy": "dlq"}

        with pytest.raises(ConfigValidationError, match="DLQ error_policy requires dlq configuration"):
            parse_runtime_config(config_dict)


class TestParsePipelineConfig:
    """Test pipeline configuration parsing."""

    def test_parse_valid_pipeline(self) -> None:
        """Test parsing valid pipeline configuration."""
        pipeline_dict = {
            "pipeline_id": "test-pipeline",
            "name": "Test Pipeline",
            "sources": ["producer1"],
            "sinks": ["consumer1"],
            "processors": ["processor1"],
            "config": {
                "parallelism": 2,
            },
        }
        transport = TransportConfig(type="memory", config={})
        pipeline = parse_pipeline_config(pipeline_dict, transport)

        assert pipeline.pipeline_id == "test-pipeline"
        assert pipeline.name == "Test Pipeline"
        assert pipeline.sources == ["producer1"]
        assert pipeline.sinks == ["consumer1"]
        assert pipeline.processors == ["processor1"]
        assert pipeline.config.parallelism == 2
        assert pipeline.transport.type == "memory"

    def test_parse_pipeline_missing_pipeline_id(self) -> None:
        """Test missing pipeline_id raises ConfigValidationError."""
        pipeline_dict = {
            "name": "Test Pipeline",
            "sources": ["producer1"],
            "sinks": ["consumer1"],
            "config": {},
        }
        transport = TransportConfig(type="memory", config={})

        with pytest.raises(ConfigValidationError, match="missing required field: pipeline_id"):
            parse_pipeline_config(pipeline_dict, transport)

    def test_parse_pipeline_missing_config(self) -> None:
        """Test missing config section raises ConfigValidationError."""
        pipeline_dict = {
            "pipeline_id": "test",
            "name": "Test",
            "sources": ["producer1"],
            "sinks": ["consumer1"],
        }
        transport = TransportConfig(type="memory", config={})

        with pytest.raises(ConfigValidationError, match="missing required section: config"):
            parse_pipeline_config(pipeline_dict, transport)


class TestLoadPipelineConfigs:
    """Test end-to-end pipeline configuration loading."""

    def test_load_single_pipeline(self, tmp_path: Path) -> None:
        """Test loading single pipeline configuration."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test-system"
environment = "test"

[transport]
type = "memory"

[[pipelines]]
pipeline_id = "pipeline1"
name = "Pipeline 1"
sources = ["producer1"]
sinks = ["consumer1"]

[pipelines.config]
parallelism = 1
"""
        )

        pipelines = load_pipeline_configs(config_path)
        assert len(pipelines) == 1
        assert pipelines[0].pipeline_id == "pipeline1"

    def test_load_multiple_pipelines(self, tmp_path: Path) -> None:
        """Test loading multiple pipeline configurations."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test-system"
environment = "test"

[transport]
type = "mqtt"
broker = "mqtt://localhost:1883"

[[pipelines]]
pipeline_id = "pipeline1"
name = "Pipeline 1"
sources = ["producer1"]
sinks = ["consumer1"]

[pipelines.config]
parallelism = 1

[[pipelines]]
pipeline_id = "pipeline2"
name = "Pipeline 2"
sources = ["producer2"]
sinks = ["consumer2"]
processors = ["filter1"]

[pipelines.config]
parallelism = 2
"""
        )

        pipelines = load_pipeline_configs(config_path)
        assert len(pipelines) == 2
        assert pipelines[0].pipeline_id == "pipeline1"
        assert pipelines[1].pipeline_id == "pipeline2"
        assert pipelines[1].processors == ["filter1"]

    def test_load_pipeline_with_dlq(self, tmp_path: Path) -> None:
        """Test loading pipeline with DLQ configuration."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test-system"
environment = "test"

[transport]
type = "memory"

[[pipelines]]
pipeline_id = "pipeline1"
name = "Pipeline 1"
sources = ["producer1"]
sinks = ["consumer1"]

[pipelines.config]
parallelism = 1
error_policy = "dlq"

[pipelines.config.dlq]
enabled = true
storage = "file"
retention = "P7D"
"""
        )

        pipelines = load_pipeline_configs(config_path)
        assert len(pipelines) == 1
        assert pipelines[0].config.error_policy == ErrorPolicy.DLQ
        assert pipelines[0].config.dlq is not None
        assert pipelines[0].config.dlq.enabled is True

    def test_load_invalid_pipeline_provides_context(self, tmp_path: Path) -> None:
        """Test invalid pipeline config includes pipeline number in error."""
        config_path = tmp_path / "config.toml"
        config_path.write_text(
            """
[system]
id = "test"
environment = "test"

[transport]
type = "memory"

[[pipelines]]
pipeline_id = "valid"
name = "Valid"
sources = ["p1"]
sinks = ["c1"]

[pipelines.config]
parallelism = 1

[[pipelines]]
# Missing pipeline_id
name = "Invalid"
sources = ["p2"]
sinks = ["c2"]

[pipelines.config]
parallelism = 1
"""
        )

        with pytest.raises(ConfigValidationError, match="Failed to parse pipeline 2"):
            load_pipeline_configs(config_path)
