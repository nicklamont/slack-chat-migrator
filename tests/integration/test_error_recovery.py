"""Error handling and edge-case integration tests.

These tests verify that the migration pipeline produces clear errors
for invalid inputs and handles gracefully-recoverable situations.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slack_chat_migrator.core.migrator import SlackToChatMigrator
from slack_chat_migrator.exceptions import ExportError
from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_migrator,
)

pytestmark = pytest.mark.integration


class TestInvalidExportPath:
    """Non-existent export path → clear error."""

    def test_raises_value_error(self, tmp_path: Path) -> None:
        fake_path = tmp_path / "does_not_exist"
        config_path = tmp_path / "config.yaml"
        config_path.write_text("")

        with pytest.raises(ValueError, match="not a valid directory"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(fake_path),
                workspace_admin="admin@example.com",
                config_path=str(config_path),
                dry_run=True,
            )


class TestMalformedUsersJson:
    """Invalid JSON in users.json → clear error."""

    def test_raises_on_bad_json(self, tmp_path: Path) -> None:
        (tmp_path / "users.json").write_text("NOT VALID JSON {{{")
        (tmp_path / "channels.json").write_text(json.dumps([GENERAL_CHANNEL]))
        (tmp_path / "general").mkdir()

        with pytest.raises(ExportError, match=r"users\.json"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="admin@example.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )


class TestMalformedChannelMessages:
    """Bad message format → migration continues, errors logged."""

    def test_migration_continues(self, tmp_path: Path) -> None:
        # Messages with missing 'ts' or weird structure
        bad_messages = [
            {
                "type": "message",
                "user": "U001",
                "text": "good",
                "ts": "1609459200.000000",
            },
            {"type": "message"},  # Missing user, text, ts
            {
                "type": "message",
                "user": "U001",
                "text": "also good",
                "ts": "1609459202.000000",
            },
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": bad_messages},
        )
        m = make_migrator(export)

        # Should not crash — the pipeline handles malformed messages
        result = m.migrate()
        assert result is True

        # The 2 valid messages should still be processed
        summary = m.state.progress.migration_summary
        assert summary["messages_created"] >= 2


class TestMissingChannelDirectory:
    """channels.json lists a channel but no directory exists → handled gracefully."""

    def test_missing_dir_handled(self, tmp_path: Path) -> None:
        ghost_channel = {
            "id": "C999",
            "name": "ghost",
            "members": ["U001"],
            "purpose": {"value": ""},
            "topic": {"value": ""},
        }
        # Build export with both channels in channels.json but only create
        # the "general" directory (not "ghost")
        (tmp_path / "users.json").write_text(json.dumps(USERS))
        (tmp_path / "channels.json").write_text(
            json.dumps([GENERAL_CHANNEL, ghost_channel])
        )
        general_dir = tmp_path / "general"
        general_dir.mkdir()
        (general_dir / "2021-01-01.json").write_text(
            json.dumps(
                [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "hi",
                        "ts": "1609459200.000000",
                    }
                ]
            )
        )
        # Deliberately do NOT create tmp_path / "ghost"

        m = make_migrator(tmp_path)
        result = m.migrate()
        assert result is True

        # Only the channel with a directory should be processed
        summary = m.state.progress.migration_summary
        assert "general" in summary["channels_processed"]
        assert "ghost" not in summary["channels_processed"]
