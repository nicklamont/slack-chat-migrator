"""Config interaction tests for the dry-run migration pipeline.

Exercises channel filtering, failure thresholds, and import completion
strategies through the full pipeline.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    RANDOM_CHANNEL,
    USERS,
    build_export,
    make_messages,
    make_migrator,
)

pytestmark = pytest.mark.integration


class TestIncludeChannelsFilter:
    """``include_channels`` restricts processing to the named channels."""

    def test_include_channels_filters(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
            messages_by_channel={
                "general": make_messages(2),
                "random": make_messages(2),
            },
        )
        m = make_migrator(
            tmp_path,
            config_text="include_channels: [general]\nexclude_channels: []\n",
        )
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 2
        assert summary["spaces_created"] == 1


class TestExcludeChannelsFilter:
    """``exclude_channels`` skips the named channels."""

    def test_exclude_channels_filters(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
            messages_by_channel={
                "general": make_messages(2),
                "random": make_messages(2),
            },
        )
        m = make_migrator(
            tmp_path,
            config_text="exclude_channels: [random]\ninclude_channels: []\n",
        )
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 2
        assert summary["spaces_created"] == 1


class TestIncludeOverridesExclude:
    """When both are set, ``include_channels`` takes precedence."""

    def test_include_overrides_exclude(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
            messages_by_channel={
                "general": make_messages(2),
                "random": make_messages(2),
            },
        )
        m = make_migrator(
            tmp_path,
            config_text="include_channels: [general]\nexclude_channels: [general]\n",
        )
        m.migrate()

        summary = m.state.progress.migration_summary
        # include_channels wins — general is processed
        assert summary["messages_created"] == 2
        assert summary["spaces_created"] == 1


class TestMaxFailurePercentageZero:
    """``max_failure_percentage: 0`` flags a channel on the first failure."""

    def test_max_failure_percentage_zero(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(3)},
        )
        m = make_migrator(
            tmp_path,
            config_text="exclude_channels: []\ninclude_channels: []\nmax_failure_percentage: 0\n",
            message_error_schedule={2: 400},
        )
        m.migrate()

        assert "general" in m.state.errors.high_failure_rate_channels


class TestForceCompleteWithErrors:
    """``import_completion_strategy: force_complete`` completes import even with errors."""

    def test_force_complete_with_errors(self, tmp_path: Path) -> None:
        config = (
            "exclude_channels: []\n"
            "include_channels: []\n"
            "import_completion_strategy: force_complete\n"
        )
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(3)},
        )
        m = make_migrator(
            tmp_path,
            config_text=config,
            message_error_schedule={1: 400},
        )
        m.migrate()

        # force_complete means the space should NOT be in incomplete_import_spaces
        incomplete_channels = [ch for _, ch in m.state.errors.incomplete_import_spaces]
        assert "general" not in incomplete_channels


class TestSkipOnErrorWithErrors:
    """``import_completion_strategy: skip_on_error`` (default) leaves incomplete on errors."""

    def test_skip_on_error_with_errors(self, tmp_path: Path) -> None:
        config = (
            "exclude_channels: []\n"
            "include_channels: []\n"
            "import_completion_strategy: skip_on_error\n"
            "cleanup_on_error: false\n"
        )
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(3)},
        )
        m = make_migrator(
            tmp_path,
            config_text=config,
            message_error_schedule={1: 400},
        )
        m.migrate()

        incomplete_channels = [ch for _, ch in m.state.errors.incomplete_import_spaces]
        assert "general" in incomplete_channels
