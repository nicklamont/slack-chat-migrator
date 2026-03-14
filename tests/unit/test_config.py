"""Unit tests for the config module."""

import tempfile
from pathlib import Path

import pytest
import yaml

from slack_chat_migrator.core.config import (
    ImportCompletionStrategy,
    MigrationConfig,
    create_default_config,
    load_config,
    should_process_channel,
)


def test_load_config_with_empty_file():
    """Test loading config from an empty file."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as temp_file:
        config = load_config(Path(temp_file.name))

        # Check default values
        assert config.exclude_channels == []
        assert config.include_channels == []
        assert config.user_mapping_overrides == {}
        assert config.email_domain_override == ""

        # Check error handling defaults
        assert config.abort_on_error is False
        assert config.max_failure_percentage == 10
        assert (
            config.import_completion_strategy == ImportCompletionStrategy.SKIP_ON_ERROR
        )
        assert config.cleanup_on_error is False

        # Check retry defaults
        assert config.max_retries == 3
        assert config.retry_delay == 2


def test_load_config_with_values():
    """Test loading config with specific values."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as temp_file:
        # Write test config
        config_data = {
            "shared_drive": {"name": "Custom Drive"},
            "email_domain_override": "example.com",
            "exclude_channels": ["random", "general"],
            "include_channels": ["important"],
            "user_mapping_overrides": {"U123": "user@example.com"},
        }

        with open(temp_file.name, "w") as f:
            yaml.dump(config_data, f)

        # Load the config
        config = load_config(Path(temp_file.name))

        # Check values
        assert config.shared_drive.name == "Custom Drive"
        assert config.email_domain_override == "example.com"
        assert "random" in config.exclude_channels
        assert "general" in config.exclude_channels
        assert "important" in config.include_channels
        assert config.user_mapping_overrides["U123"] == "user@example.com"


def test_invalid_import_completion_strategy_raises():
    """Invalid import_completion_strategy in YAML raises ValueError at load time."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as temp_file:
        config_data = {"import_completion_strategy": "skip_on_eror"}  # typo
        with open(temp_file.name, "w") as f:
            yaml.dump(config_data, f)

        with pytest.raises(ValueError, match="Invalid import_completion_strategy"):
            load_config(Path(temp_file.name))


def test_valid_import_completion_strategies():
    """Both valid strategy values load correctly from YAML."""
    for strategy in ("skip_on_error", "force_complete"):
        with tempfile.NamedTemporaryFile(suffix=".yaml") as temp_file:
            config_data = {"import_completion_strategy": strategy}
            with open(temp_file.name, "w") as f:
                yaml.dump(config_data, f)

            config = load_config(Path(temp_file.name))
            assert config.import_completion_strategy == ImportCompletionStrategy(
                strategy
            )


def test_should_process_channel():
    """Test channel processing logic."""
    # Test with include list
    config = MigrationConfig(include_channels=["channel1", "channel2"])
    assert should_process_channel("channel1", config) is True
    assert should_process_channel("channel3", config) is False

    # Test with exclude list
    config = MigrationConfig(exclude_channels=["channel1", "channel2"])
    assert should_process_channel("channel1", config) is False
    assert should_process_channel("channel3", config) is True

    # Test with both include and exclude (include takes precedence)
    config = MigrationConfig(
        include_channels=["channel1", "channel2"],
        exclude_channels=["channel1", "channel3"],
    )
    assert should_process_channel("channel1", config) is True  # In include list
    assert should_process_channel("channel2", config) is True  # In include list
    assert should_process_channel("channel3", config) is False  # Not in include list
    assert should_process_channel("channel4", config) is False  # Not in include list


def test_create_default_config(tmp_path):
    """Test creating a default config file."""
    config_path = tmp_path / "config.yaml"
    result = create_default_config(config_path)

    assert result is True
    assert config_path.exists()

    # Verify the file is valid YAML with expected keys
    with open(config_path) as f:
        config = yaml.safe_load(f)
    assert "exclude_channels" in config
    assert "user_mapping_overrides" in config
    assert "max_retries" in config


def test_create_default_config_no_overwrite(tmp_path):
    """Test that create_default_config won't overwrite an existing file."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text("existing: true\n")

    result = create_default_config(config_path)

    assert result is False
    # Verify original content is preserved
    with open(config_path) as f:
        config = yaml.safe_load(f)
    assert config == {"existing": True}


def test_should_process_channel_empty_lists():
    """Test should_process_channel with empty include/exclude lists."""
    config = MigrationConfig()
    assert should_process_channel("anything", config) is True


def test_should_process_channel_no_lists():
    """Test should_process_channel with default MigrationConfig."""
    config = MigrationConfig()
    assert should_process_channel("anything", config) is True


def test_should_process_channel_normalizes_hash_prefix():
    """should_process_channel handles '#' prefix in config channel names."""
    config = MigrationConfig(exclude_channels=["#general", "random"])
    assert should_process_channel("general", config) is False
    assert should_process_channel("random", config) is False
    assert should_process_channel("other", config) is True

    config = MigrationConfig(include_channels=["#general"])
    assert should_process_channel("general", config) is True
    assert should_process_channel("random", config) is False


def test_load_config_nonexistent_path():
    """Test loading config from a nonexistent path returns defaults."""
    config = load_config(Path("/nonexistent/path/config.yaml"))

    assert config.max_retries == 3
    assert config.exclude_channels == []
    assert config.email_domain_override == ""


def test_load_config_invalid_yaml(tmp_path):
    """Test loading config from a file with invalid YAML."""
    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text("{{invalid yaml: [")

    config = load_config(bad_file)

    # Should fall back to defaults
    assert config.max_retries == 3
    assert config.exclude_channels == []


# ---------------------------------------------------------------------------
# MigrationConfig __post_init__ validation
# ---------------------------------------------------------------------------


class TestMigrationConfigValidation:
    """Tests for MigrationConfig.__post_init__ validation."""

    def test_valid_defaults_accepted(self):
        """Default values pass validation without error."""
        config = MigrationConfig()
        assert config.max_retries == 3
        assert config.max_failure_percentage == 10
        assert config.retry_delay == 2

    def test_zero_values_accepted(self):
        """Zero is a valid value for all validated fields."""
        config = MigrationConfig(max_retries=0, max_failure_percentage=0, retry_delay=0)
        assert config.max_retries == 0
        assert config.max_failure_percentage == 0
        assert config.retry_delay == 0

    def test_max_failure_percentage_100_accepted(self):
        """100 is a valid max_failure_percentage."""
        config = MigrationConfig(max_failure_percentage=100)
        assert config.max_failure_percentage == 100

    def test_negative_max_retries_rejected(self):
        """Negative max_retries raises ValueError."""
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            MigrationConfig(max_retries=-1)

    def test_negative_retry_delay_rejected(self):
        """Negative retry_delay raises ValueError."""
        with pytest.raises(ValueError, match="retry_delay must be >= 0"):
            MigrationConfig(retry_delay=-1)

    def test_max_failure_percentage_below_zero_rejected(self):
        """Negative max_failure_percentage raises ValueError."""
        with pytest.raises(ValueError, match="max_failure_percentage must be between"):
            MigrationConfig(max_failure_percentage=-1)

    def test_max_failure_percentage_above_100_rejected(self):
        """max_failure_percentage > 100 raises ValueError."""
        with pytest.raises(ValueError, match="max_failure_percentage must be between"):
            MigrationConfig(max_failure_percentage=101)

    def test_invalid_config_from_yaml(self, tmp_path):
        """Invalid values in YAML are rejected at load time."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump({"max_retries": -5}))

        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            load_config(config_path)
