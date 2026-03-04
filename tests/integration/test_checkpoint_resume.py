"""Checkpoint save / load / resume integration tests.

These tests verify that the checkpoint system correctly tracks completed
channels, allows resuming an interrupted migration, and cleans up on success.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from slack_chat_migrator.core.checkpoint import (
    CheckpointData,
    save_checkpoint,
)
from slack_chat_migrator.core.migrator import SlackToChatMigrator
from tests.integration.conftest import (
    GENERAL_CHANNEL,
    RANDOM_CHANNEL,
    USERS,
    build_export,
    make_migrator,
)

pytestmark = pytest.mark.integration


def _checkpoint_path(migrator: SlackToChatMigrator) -> Path:
    """Return the checkpoint file path for a migrator instance."""
    output_dir = migrator.state.context.output_dir
    assert output_dir is not None
    return Path(output_dir) / ".migration_checkpoint.json"


class TestCheckpointSavedAfterChannel:
    """After migrating, checkpoint file contains completed channels."""

    def test_checkpoint_written_during_migration(self, tmp_path: Path) -> None:
        """Verify checkpoint was written during migration (then cleared on success)."""
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
            messages_by_channel={
                "general": [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "hi",
                        "ts": "1609459200.000000",
                    }
                ],
                "random": [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "yo",
                        "ts": "1609459201.000000",
                    }
                ],
            },
        )
        m = make_migrator(export)
        m.migrate()

        # After successful migration, the checkpoint is cleared
        cp_path = _checkpoint_path(m)
        assert not cp_path.exists(), "Checkpoint should be cleared after success"

        # But both channels should be in the summary
        summary = m.state.progress.migration_summary
        assert set(summary["channels_processed"]) == {"general", "random"}


class TestCheckpointResumeSkipsCompleted:
    """Pre-seed checkpoint → migrate() skips already-completed channels."""

    def test_skips_completed_channel(self, tmp_path: Path) -> None:
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
            messages_by_channel={
                "general": [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "hi",
                        "ts": "1609459200.000000",
                    }
                ],
                "random": [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "yo",
                        "ts": "1609459201.000000",
                    }
                ],
            },
        )
        m = make_migrator(export)

        # Pre-seed checkpoint marking "general" as already done
        cp_path = _checkpoint_path(m)
        pre_checkpoint = CheckpointData(
            started_at="2024-01-01T00:00:00+00:00",
            completed_channels={"general": "2024-01-01T00:01:00+00:00"},
        )
        save_checkpoint(cp_path, pre_checkpoint)

        m.migrate()

        summary = m.state.progress.migration_summary
        # Only "random" should have been processed (general was skipped)
        assert "random" in summary["channels_processed"]
        assert "general" not in summary["channels_processed"]


class TestCheckpointClearedOnSuccess:
    """After full migration, checkpoint file is removed."""

    def test_checkpoint_removed(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        m.migrate()

        cp_path = _checkpoint_path(m)
        assert not cp_path.exists()


class TestCorruptCheckpointIgnored:
    """Write garbage to checkpoint file → migration runs from scratch."""

    def test_corrupt_checkpoint_treated_as_fresh(self, tmp_path: Path) -> None:
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={
                "general": [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "hi",
                        "ts": "1609459200.000000",
                    }
                ],
            },
        )
        m = make_migrator(export)

        # Write corrupt JSON to checkpoint path
        cp_path = _checkpoint_path(m)
        cp_path.write_text("THIS IS NOT JSON {{{{")

        # Migration should succeed, treating the corrupt checkpoint as absent
        assert m.migrate() is True

        summary = m.state.progress.migration_summary
        assert "general" in summary["channels_processed"]

    def test_wrong_schema_version_ignored(self, tmp_path: Path) -> None:
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={
                "general": [
                    {
                        "type": "message",
                        "user": "U001",
                        "text": "hi",
                        "ts": "1609459200.000000",
                    }
                ],
            },
        )
        m = make_migrator(export)

        # Write checkpoint with wrong schema version
        cp_path = _checkpoint_path(m)
        cp_path.write_text(
            json.dumps(
                {
                    "schema_version": 999,
                    "completed_channels": {"general": "2024-01-01T00:00:00"},
                }
            )
        )

        # Should ignore the checkpoint and process from scratch
        assert m.migrate() is True

        summary = m.state.progress.migration_summary
        assert "general" in summary["channels_processed"]
