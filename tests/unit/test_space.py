"""Unit tests for the space management module."""

from unittest.mock import MagicMock

from slack_migrator.services.space import (
    DEFAULT_FALLBACK_JOIN_TIME,
    EARLIEST_MESSAGE_OFFSET_MINUTES,
    FIRST_MESSAGE_OFFSET_MINUTES,
    HISTORICAL_DELETE_TIME_OFFSET_SECONDS,
    IMPORT_MODE_DAYS_LIMIT,
    channel_has_external_users,
)


def _make_migrator(
    user_map=None, workspace_domain="example.com", channels_meta=None, export_root=None
):
    """Create a mock migrator with common attributes."""
    migrator = MagicMock()
    migrator.user_map = user_map or {}
    migrator.workspace_domain = workspace_domain
    migrator.channels_meta = channels_meta or {}
    migrator.users_without_email = []
    if export_root:
        migrator.export_root = export_root
    return migrator


class TestChannelHasExternalUsers:
    """Tests for channel_has_external_users()."""

    def test_no_external_users(self):
        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        migrator._is_external_user.return_value = False

        result = channel_has_external_users(migrator, "general")
        assert result is False

    def test_has_external_user(self):
        migrator = _make_migrator(
            user_map={"U001": "alice@example.com", "U002": "ext@other.com"},
            channels_meta={"general": {"members": ["U001", "U002"]}},
        )
        migrator._is_external_user.side_effect = lambda email: email == "ext@other.com"

        result = channel_has_external_users(migrator, "general")
        assert result is True

    def test_no_members_in_metadata(self, tmp_path):
        """When metadata has no members, scans message files."""
        # Create channel directory with no message files
        ch_dir = tmp_path / "empty-channel"
        ch_dir.mkdir()

        migrator = _make_migrator(
            channels_meta={"empty-channel": {}},
            export_root=tmp_path,
        )

        result = channel_has_external_users(migrator, "empty-channel")
        assert result is False

    def test_unmapped_user_skipped(self):
        migrator = _make_migrator(
            user_map={},  # No mappings
            channels_meta={"general": {"members": ["U001"]}},
        )

        result = channel_has_external_users(migrator, "general")
        assert result is False


class TestNamedConstants:
    """Verify the named constants have correct values."""

    def test_import_mode_days_limit(self):
        assert IMPORT_MODE_DAYS_LIMIT == 90

    def test_default_fallback_join_time(self):
        assert DEFAULT_FALLBACK_JOIN_TIME == "2020-01-01T00:00:00Z"

    def test_historical_delete_offset(self):
        assert HISTORICAL_DELETE_TIME_OFFSET_SECONDS == 5

    def test_earliest_message_offset(self):
        assert EARLIEST_MESSAGE_OFFSET_MINUTES == 2

    def test_first_message_offset(self):
        assert FIRST_MESSAGE_OFFSET_MINUTES == 1
