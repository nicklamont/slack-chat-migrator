"""CLI-level config handling: bad or missing config files fail loudly.

These run the real ``migrate --dry_run`` command on a generated export, so
they need no Google credentials.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from slack_chat_migrator.cli.commands import cli
from tests.integration.conftest import (
    GENERAL_CHANNEL,
    RANDOM_CHANNEL,
    USERS,
    build_export,
    make_messages,
)

pytestmark = pytest.mark.integration


def _export(tmp_path: Path) -> Path:
    export = tmp_path / "export"
    export.mkdir()
    return build_export(
        export,
        users=USERS,
        channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
        messages_by_channel={
            "general": make_messages(2),
            "random": make_messages(1),
        },
    )


def _dry_run(export: Path, config: Path | None = None) -> Result:
    args = ["migrate", "--dry_run", "--export_path", str(export)]
    if config is not None:
        args += ["--config", str(config)]
    return CliRunner().invoke(cli, args)


class TestBadConfigFails:
    """Invalid configs stop the run before anything is migrated."""

    def test_invalid_yaml_exits_non_zero(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text(
            "exclude_channels: [general\nuser_mapping_overrides: {U002: b@x.com}\n"
        )
        result = _dry_run(_export(tmp_path), config)
        assert result.exit_code == 1, result.output
        assert "not valid YAML" in result.output
        assert "Migrating channels" not in result.output

    def test_explicit_missing_config_exits_non_zero(self, tmp_path: Path) -> None:
        result = _dry_run(_export(tmp_path), tmp_path / "missing.yaml")
        assert result.exit_code == 1, result.output
        assert "Config file not found" in result.output

    def test_typo_key_exits_non_zero(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("exclude_channel: [general]\n")
        result = _dry_run(_export(tmp_path), config)
        assert result.exit_code == 1, result.output
        assert "did you mean 'exclude_channels'" in result.output

    def test_string_boolean_exits_non_zero(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("ignore_bots: 'false'\n")
        result = _dry_run(_export(tmp_path), config)
        assert result.exit_code == 1, result.output
        assert "ignore_bots" in result.output


class TestConfigWarningsVisible:
    """Warnings about the config reach the console, not just the log file."""

    def test_default_config_missing_warns_on_console(self, tmp_path: Path) -> None:
        # No --config and no ./config.yaml (tests run in an empty cwd).
        result = _dry_run(_export(tmp_path))
        assert "not found, using default settings" in result.output

    def test_string_list_is_treated_as_one_item(self, tmp_path: Path) -> None:
        config = tmp_path / "config.yaml"
        config.write_text("include_channels: general\n")
        result = _dry_run(_export(tmp_path), config)
        assert "should be a list" in result.output
        assert "Skipping channel random based on configuration" in result.output
        assert "Skipping channel general based on configuration" not in result.output
