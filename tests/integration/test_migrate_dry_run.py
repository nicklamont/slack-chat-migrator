"""Full-pipeline dry-run migration tests.

Each test builds a Slack export in ``tmp_path``, creates a
``SlackToChatMigrator(dry_run=True)`` and calls ``migrate()``.
Because dry-run mode injects no-op Chat / Drive services, these
tests exercise the complete pipeline without any Google credentials.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import make_migrator

pytestmark = pytest.mark.integration


class TestSingleChannelMigration:
    """One channel, 3 messages → migrate() succeeds."""

    def test_returns_true(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        assert m.migrate() is True

    def test_summary_counts(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["spaces_created"] == 1
        assert summary["messages_created"] == 3
        assert len(summary["channels_processed"]) == 1
        assert "general" in summary["channels_processed"]


class TestMultiChannelMigration:
    """Two channels → both spaces created, all messages sent."""

    def test_both_channels_processed(self, multi_channel_export: Path) -> None:
        m = make_migrator(multi_channel_export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["spaces_created"] == 2
        assert summary["messages_created"] == 5  # 3 + 2
        processed = set(summary["channels_processed"])
        assert processed == {"general", "random"}


class TestEmptyChannel:
    """Channel with 0 messages → space created, 0 messages in summary."""

    def test_empty_channel_handled(self, empty_channel_export: Path) -> None:
        m = make_migrator(empty_channel_export)
        m.migrate()

        summary = m.state.progress.migration_summary
        # Space is still created even with no messages
        assert summary["spaces_created"] == 1
        assert summary["messages_created"] == 0


class TestBotMessages:
    """Messages from bots → no crash, messages counted."""

    def test_bot_messages_processed(self, bot_messages_export: Path) -> None:
        m = make_migrator(bot_messages_export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 2


class TestDryRunNoSideEffects:
    """After migrate(), no artefacts outside the pytest tmp area."""

    def test_no_files_outside_tmp(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        m.migrate()

        # Output dir is a sibling of the export dir, both under pytest's
        # tmp_path_factory root — verify it stays within the same parent.
        assert m.state.context.output_dir is not None
        output_dir = Path(m.state.context.output_dir)
        assert output_dir.parent == single_channel_export.parent


class TestMigrationSummaryStructure:
    """Summary dict has expected keys and types."""

    def test_required_keys(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert "channels_processed" in summary
        assert "spaces_created" in summary
        assert "messages_created" in summary
        assert "reactions_created" in summary
        assert "files_created" in summary

    def test_value_types(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert isinstance(summary["channels_processed"], list)
        assert isinstance(summary["spaces_created"], int)
        assert isinstance(summary["messages_created"], int)
        assert isinstance(summary["reactions_created"], int)
        assert isinstance(summary["files_created"], int)
