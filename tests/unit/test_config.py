"""Unit tests for the config module."""

import logging
import tempfile
from pathlib import Path

import pytest
import yaml

from slack_chat_migrator.core.config import (
    ImportCompletionStrategy,
    MigrationConfig,
    load_config,
    load_space_mapping,
    should_process_channel,
)
from slack_chat_migrator.exceptions import ConfigError


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
    """Invalid YAML is an error, never a silent fallback to defaults."""
    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text("{{invalid yaml: [")

    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad_file)


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


class TestNewConfigOptions:
    """Options added for the October 2026 audit fixes."""

    def test_defaults(self):
        cfg = MigrationConfig.from_dict({})
        assert cfg.workspace_domains == []
        assert cfg.conversation_types == ["public", "private", "mpim", "dm"]
        assert cfg.max_parallel_channels == 1
        assert cfg.file_workers == 4
        assert cfg.space_manager == "admin"
        assert cfg.migrate_pins is True
        assert cfg.convert_canvases is True
        assert cfg.quote_shared_messages is False

    def test_slack_token_falls_back_to_env(self, monkeypatch):
        env_value, config_value = "xoxb-from-env", "xoxb-from-config"
        monkeypatch.setenv("SLACK_TOKEN", env_value)
        assert MigrationConfig.from_dict({}).slack_token == env_value
        cfg = MigrationConfig.from_dict({"slack_token": config_value})
        assert cfg.slack_token == config_value

    @pytest.mark.parametrize(
        "data",
        [
            {"max_parallel_channels": 0},
            {"file_workers": 0},
            {"space_manager": "owner"},
            {"conversation_types": ["public", "threads"]},
        ],
    )
    def test_invalid_values_rejected(self, data):
        with pytest.raises(ValueError):
            MigrationConfig.from_dict(data)


# ---------------------------------------------------------------------------
# Loading and validation (issue #87)
# ---------------------------------------------------------------------------


def _load(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return load_config(path)


class TestLoadConfigFailures:
    """Bad or missing config files fail loudly instead of using defaults."""

    def test_unclosed_list_is_an_error(self, tmp_path):
        with pytest.raises(ConfigError, match="not valid YAML"):
            _load(
                tmp_path,
                "exclude_channels: [general\nuser_mapping_overrides: {U002: b@x.com}\n",
            )

    def test_missing_required_file_is_an_error(self, tmp_path):
        with pytest.raises(ConfigError, match="Config file not found"):
            load_config(tmp_path / "missing.yaml", required=True)

    def test_missing_optional_file_warns_and_uses_defaults(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            config = load_config(tmp_path / "config.yaml", required=False)
        assert config == MigrationConfig()
        assert "not found, using default settings" in caplog.text

    @pytest.mark.parametrize("text", ["- general\n- random\n", "just a string\n"])
    def test_top_level_must_be_a_mapping(self, tmp_path, text):
        with pytest.raises(ConfigError, match="mapping"):
            _load(tmp_path, text)

    def test_unreadable_file_is_an_error(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_bytes(b"\xff\xfe\x00 not utf-8")
        with pytest.raises(ConfigError, match="Could not read"):
            load_config(path)

    def test_error_names_the_file(self, tmp_path):
        with pytest.raises(ConfigError, match=r"config\.yaml.*ignore_bots"):
            _load(tmp_path, "ignore_bots: 'false'\n")

    def test_config_error_is_a_value_error(self):
        """Callers that caught ValueError for bad config keep working."""
        assert issubclass(ConfigError, ValueError)


class TestListOptions:
    """include/exclude/announcement channel lists and other list options."""

    def test_single_string_becomes_a_list(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            config = _load(tmp_path, "include_channels: general\n")
        assert config.include_channels == ["general"]
        assert should_process_channel("general", config) is True
        assert should_process_channel("g", config) is False
        assert "should be a list" in caplog.text

    def test_string_exclude_excludes_the_channel(self, tmp_path):
        config = _load(tmp_path, "exclude_channels: random\n")
        assert should_process_channel("random", config) is False
        assert should_process_channel("general", config) is True

    def test_numeric_channel_names_are_strings(self, tmp_path):
        config = _load(tmp_path, "include_channels: [2024, general]\n")
        assert config.include_channels == ["2024", "general"]
        assert should_process_channel("2024", config) is True

    def test_null_list_is_empty(self, tmp_path):
        config = _load(tmp_path, "include_channels:\n  # - general\n")
        assert config.include_channels == []
        assert should_process_channel("anything", config) is True

    def test_hash_prefix_and_whitespace_are_stripped(self, tmp_path):
        config = _load(
            tmp_path,
            "exclude_channels: ['#random', ' #ops ']\nannouncement_channels: '#news'\n",
        )
        assert config.exclude_channels == ["random", "ops"]
        assert config.announcement_channels == ["news"]

    def test_null_items_are_skipped(self, tmp_path):
        config = _load(tmp_path, "exclude_channels:\n  - random\n  -\n")
        assert config.exclude_channels == ["random"]

    @pytest.mark.parametrize(
        "text",
        [
            "include_channels: {general: true}\n",
            "include_channels: 5\n",
            "include_channels: [[general]]\n",
            "workspace_domains: true\n",
        ],
    )
    def test_wrong_types_rejected(self, tmp_path, text):
        with pytest.raises(ConfigError):
            _load(tmp_path, text)

    def test_conversation_types_null_means_all(self, tmp_path):
        config = _load(tmp_path, "conversation_types:\n")
        assert config.conversation_types == ["public", "private", "mpim", "dm"]

    def test_conversation_types_string(self, tmp_path):
        config = _load(tmp_path, "conversation_types: public\n")
        assert config.conversation_types == ["public"]


class TestScalarOptions:
    """Booleans, integers, numbers and strings are type-checked."""

    @pytest.mark.parametrize(
        "text",
        [
            "ignore_bots: 'false'\n",
            "abort_on_error: 'yes'\n",
            "migrate_pins: 1\n",
            "use_chat_custom_emoji:\n",
        ],
    )
    def test_booleans_must_be_real_booleans(self, tmp_path, text):
        with pytest.raises(ConfigError, match="must be true or false"):
            _load(tmp_path, text)

    def test_yaml_booleans_accepted(self, tmp_path):
        config = _load(tmp_path, "ignore_bots: yes\nmigrate_pins: false\n")
        assert config.ignore_bots is True
        assert config.migrate_pins is False

    @pytest.mark.parametrize(
        "text",
        [
            "max_retries: '3'\n",
            "max_retries:\n",
            "max_retries: ~\n",
            "max_failure_percentage: 10.5\n",
            "file_workers: true\n",
        ],
    )
    def test_integers_must_be_integers(self, tmp_path, text):
        with pytest.raises(ConfigError, match="must be a whole number"):
            _load(tmp_path, text)

    def test_retry_delay_accepts_floats(self, tmp_path):
        assert _load(tmp_path, "retry_delay: 1.5\n").retry_delay == 1.5

    def test_retry_delay_rejects_strings(self, tmp_path):
        with pytest.raises(ConfigError, match="must be a number"):
            _load(tmp_path, "retry_delay: '2'\n")

    def test_strings_reject_other_types(self, tmp_path):
        with pytest.raises(ConfigError, match="must be a string"):
            _load(tmp_path, "email_domain_override: [example.com]\n")

    def test_empty_string_option_uses_default(self, tmp_path):
        config = _load(tmp_path, "email_domain_override:\nspace_manager:\n")
        assert config.email_domain_override == ""
        assert config.space_manager == "admin"

    def test_range_errors_are_config_errors(self, tmp_path):
        with pytest.raises(ConfigError, match="max_retries must be >= 0"):
            _load(tmp_path, "max_retries: -1\n")


class TestMappingOptions:
    """Mapping options, shared_drive and space_mapping."""

    def test_user_mapping_overrides_null_is_empty(self, tmp_path):
        assert _load(tmp_path, "user_mapping_overrides:\n").user_mapping_overrides == {}

    def test_user_mapping_overrides_must_be_a_mapping(self, tmp_path):
        with pytest.raises(ConfigError, match="must be a mapping"):
            _load(tmp_path, "user_mapping_overrides: [U1]\n")

    def test_user_mapping_override_values_must_be_strings(self, tmp_path):
        with pytest.raises(ConfigError, match="U1"):
            _load(tmp_path, "user_mapping_overrides: {U1: [a@b.com]}\n")

    def test_custom_emoji_map_keys_coerced(self, tmp_path):
        config = _load(tmp_path, "custom_emoji_map: {100: ':hundred:'}\n")
        assert config.custom_emoji_map == {"100": ":hundred:"}

    def test_shared_drive_must_be_a_mapping(self, tmp_path):
        with pytest.raises(ConfigError, match="shared_drive"):
            _load(tmp_path, "shared_drive: My Drive\n")

    def test_shared_drive_values(self, tmp_path):
        config = _load(tmp_path, "shared_drive: {name: Files, id: 0AB}\n")
        assert config.shared_drive.name == "Files"
        assert config.shared_drive.id == "0AB"

    def test_shared_drive_null_name_uses_default(self, tmp_path):
        config = _load(tmp_path, "shared_drive:\n  name:\n")
        assert config.shared_drive.name == "Imported Slack Attachments"
        assert config.shared_drive.id is None

    def test_space_mapping_must_be_a_mapping(self, tmp_path):
        with pytest.raises(ConfigError, match="space_mapping"):
            _load(tmp_path, "space_mapping: [general]\n")

    def test_space_mapping_keys_are_strings(self, tmp_path):
        path = tmp_path / "config.yaml"
        path.write_text("space_mapping:\n  123: spaces/X\n  '#general': AAA\n")
        load_config(path)  # valid
        assert load_space_mapping(path) == {"123": "spaces/X", "general": "AAA"}


class TestUnknownKeys:
    """Misspelled options fail; other unknown keys are reported."""

    @pytest.mark.parametrize(
        ("text", "suggestion"),
        [
            ("include_channel: [general]\n", "include_channels"),
            ("max_retry: 5\n", "max_retries"),
            ("ignore_bot: true\n", "ignore_bots"),
        ],
    )
    def test_typo_is_an_error_with_suggestion(self, tmp_path, text, suggestion):
        with pytest.raises(ConfigError, match=f"did you mean '{suggestion}'"):
            _load(tmp_path, text)

    def test_unrelated_unknown_key_warns(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            config = _load(tmp_path, "attachments_folder: Slack\nmax_retries: 4\n")
        assert config.max_retries == 4
        assert "Ignoring unknown config option(s): attachments_folder" in caplog.text

    def test_space_mapping_is_known(self, tmp_path, caplog):
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            _load(tmp_path, "space_mapping: {general: AAA}\n")
        assert "unknown" not in caplog.text.lower()

    def test_example_config_is_valid(self):
        example = Path(__file__).resolve().parents[2] / "config.yaml.example"
        config = load_config(example, required=True)
        assert (
            config.import_completion_strategy is ImportCompletionStrategy.SKIP_ON_ERROR
        )
