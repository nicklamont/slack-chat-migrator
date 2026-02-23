"""Unit tests for the message processing module."""

from unittest.mock import MagicMock

from slack_migrator.services.message import track_message_stats


def _make_migrator(dry_run=False, channel="general", ignore_bots=False):
    """Create a mock migrator for message testing."""
    migrator = MagicMock()
    migrator.dry_run = dry_run
    migrator.current_channel = channel
    migrator.config = {"ignore_bots": ignore_bots}
    migrator.migration_summary = {
        "messages_created": 0,
        "reactions_created": 0,
        "files_created": 0,
        "channels_processed": [],
        "spaces_created": 0,
    }
    # Remove mock auto-creation for hasattr checks
    del migrator.channel_stats
    del migrator.sent_messages
    migrator.update_mode = False

    # Set up attachment processor
    migrator.attachment_processor = MagicMock()
    migrator.attachment_processor.count_message_files.return_value = 0

    return migrator


class TestTrackMessageStats:
    """Tests for track_message_stats()."""

    def test_basic_message_counting(self):
        migrator = _make_migrator()
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        assert migrator.channel_stats["general"]["message_count"] == 1

    def test_reaction_counting(self):
        migrator = _make_migrator()
        msg = {
            "ts": "1234.5",
            "user": "U001",
            "reactions": [{"name": "thumbsup", "users": ["U001", "U002"]}],
        }
        migrator._get_user_data.return_value = None

        track_message_stats(migrator, msg)

        assert migrator.channel_stats["general"]["reaction_count"] == 2

    def test_file_counting(self):
        migrator = _make_migrator()
        migrator.attachment_processor.count_message_files.return_value = 3
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        assert migrator.channel_stats["general"]["file_count"] == 3
        assert migrator.migration_summary["files_created"] == 3

    def test_dry_run_counts_reactions(self):
        migrator = _make_migrator(dry_run=True)
        msg = {
            "ts": "1234.5",
            "user": "U001",
            "reactions": [{"name": "wave", "users": ["U001"]}],
        }
        migrator._get_user_data.return_value = None

        track_message_stats(migrator, msg)

        assert migrator.migration_summary["reactions_created"] == 1

    def test_skips_bot_messages_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        msg = {"ts": "1234.5", "user": "U001", "subtype": "bot_message"}

        track_message_stats(migrator, msg)

        # channel_stats should not be created since the message was skipped
        assert not hasattr(migrator, "channel_stats") or (
            "general" not in migrator.channel_stats
        )

    def test_skips_bot_user_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        migrator._get_user_data.return_value = {"is_bot": True, "real_name": "Bot"}
        msg = {"ts": "1234.5", "user": "B001"}

        track_message_stats(migrator, msg)

        assert not hasattr(migrator, "channel_stats") or (
            "general" not in migrator.channel_stats
        )

    def test_processes_non_bot_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        migrator._get_user_data.return_value = {"is_bot": False}
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        assert migrator.channel_stats["general"]["message_count"] == 1

    def test_multiple_messages_increment(self):
        migrator = _make_migrator()
        for i in range(5):
            track_message_stats(migrator, {"ts": f"{i}.0", "user": "U001"})

        assert migrator.channel_stats["general"]["message_count"] == 5
