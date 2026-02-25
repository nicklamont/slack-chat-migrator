"""Unit tests for the unmapped user validation module."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

from slack_migrator.core.config import MigrationConfig
from slack_migrator.utils.user_validation import (
    UnmappedUserTracker,
    UserType,
    analyze_unmapped_users,
    categorize_user_analysis,
    initialize_unmapped_user_tracking,
    log_unmapped_user_summary_for_dry_run,
    scan_channel_members_for_unmapped_users,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_migrator(
    *,
    export_root: str = "/fake/export",
    user_map: dict | None = None,
    config: MigrationConfig | None = None,
    has_tracker: bool = True,
) -> MagicMock:
    """Build a lightweight MagicMock standing in for SlackToChatMigrator."""
    migrator = MagicMock()
    migrator.export_root = export_root
    migrator.user_map = user_map or {}
    migrator.config = config or MigrationConfig()

    if has_tracker:
        migrator.unmapped_user_tracker = UnmappedUserTracker()
    else:
        # Remove the attribute so hasattr checks return False
        del migrator.unmapped_user_tracker

    return migrator


def _write_json(path: Path, data: list | dict) -> None:
    path.write_text(json.dumps(data))


# ===========================================================================
# UnmappedUserTracker
# ===========================================================================


class TestUnmappedUserTracker:
    """Tests for the UnmappedUserTracker class."""

    def test_initial_state_empty(self):
        tracker = UnmappedUserTracker()

        assert not tracker.has_unmapped_users()
        assert tracker.get_unmapped_count() == 0
        assert tracker.get_unmapped_users_list() == []

    # -- add_unmapped_user --------------------------------------------------

    def test_add_unmapped_user_without_context(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001")

        assert tracker.has_unmapped_users()
        assert tracker.get_unmapped_count() == 1
        assert "U001" in tracker.unmapped_users
        assert (
            tracker.user_contexts.get("U001") is None
            or len(tracker.user_contexts["U001"]) == 0
        )

    def test_add_unmapped_user_with_context(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001", "mention in #general")

        assert "U001" in tracker.unmapped_users
        assert "mention in #general" in tracker.user_contexts["U001"]

    def test_add_same_user_multiple_contexts(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001", "ctx-a")
        tracker.add_unmapped_user("U001", "ctx-b")

        assert tracker.get_unmapped_count() == 1
        assert tracker.user_contexts["U001"] == {"ctx-a", "ctx-b"}

    def test_add_multiple_distinct_users(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001")
        tracker.add_unmapped_user("U002")
        tracker.add_unmapped_user("U003")

        assert tracker.get_unmapped_count() == 3

    def test_add_unmapped_user_empty_context_not_stored(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001", "")

        assert "U001" in tracker.unmapped_users
        # Empty context string should not be added
        assert len(tracker.user_contexts.get("U001", set())) == 0

    # -- track_unmapped_mention ---------------------------------------------

    def test_track_unmapped_mention_full_context(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_mention(
            "U001", channel="general", message_ts="1234567890.000100", text="hello"
        )

        assert "U001" in tracker.unmapped_users
        contexts = tracker.user_contexts["U001"]
        assert len(contexts) == 1
        ctx = next(iter(contexts))
        assert "mention" in ctx
        assert "channel:general" in ctx
        assert "ts:1234567890.000100" in ctx

    def test_track_unmapped_mention_channel_only(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_mention("U001", channel="random")

        contexts = tracker.user_contexts["U001"]
        ctx = next(iter(contexts))
        assert "channel:random" in ctx
        # No ts should be present (empty string default)
        assert "ts:" not in ctx

    def test_track_unmapped_mention_no_optional_args(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_mention("U001")

        contexts = tracker.user_contexts["U001"]
        ctx = next(iter(contexts))
        assert ctx == "mention"

    def test_track_unmapped_mention_unknown_channel_excluded(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_mention("U001", channel="unknown")

        contexts = tracker.user_contexts["U001"]
        ctx = next(iter(contexts))
        assert "channel:" not in ctx

    def test_track_unmapped_mention_unknown_ts_excluded(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_mention("U001", channel="general", message_ts="unknown")

        contexts = tracker.user_contexts["U001"]
        ctx = next(iter(contexts))
        assert "ts:" not in ctx

    # -- track_unmapped_channel_member --------------------------------------

    def test_track_unmapped_channel_member(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_channel_member("U001", "general")

        assert "U001" in tracker.unmapped_users
        contexts = tracker.user_contexts["U001"]
        assert "channel_member:#general" in contexts

    def test_track_unmapped_channel_member_multiple_channels(self):
        tracker = UnmappedUserTracker()
        tracker.track_unmapped_channel_member("U001", "general")
        tracker.track_unmapped_channel_member("U001", "random")

        assert tracker.get_unmapped_count() == 1
        contexts = tracker.user_contexts["U001"]
        assert "channel_member:#general" in contexts
        assert "channel_member:#random" in contexts

    # -- get_unmapped_users_list --------------------------------------------

    def test_get_unmapped_users_list_sorted(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U003")
        tracker.add_unmapped_user("U001")
        tracker.add_unmapped_user("U002")

        assert tracker.get_unmapped_users_list() == ["U001", "U002", "U003"]

    # -- has_unmapped_users / get_unmapped_count ----------------------------

    def test_has_unmapped_users_true(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001")
        assert tracker.has_unmapped_users() is True

    def test_has_unmapped_users_false(self):
        tracker = UnmappedUserTracker()
        assert tracker.has_unmapped_users() is False

    def test_get_unmapped_count_zero(self):
        tracker = UnmappedUserTracker()
        assert tracker.get_unmapped_count() == 0

    def test_get_unmapped_count_deduplicates(self):
        tracker = UnmappedUserTracker()
        tracker.add_unmapped_user("U001")
        tracker.add_unmapped_user("U001")
        assert tracker.get_unmapped_count() == 1


# ===========================================================================
# categorize_user_analysis
# ===========================================================================


class TestCategorizeUserAnalysis:
    """Tests for categorize_user_analysis()."""

    def test_empty_analysis(self):
        result = categorize_user_analysis({})
        assert result["Bots and workflow automations"] == 0
        assert result["Deleted users"] == 0
        assert result["Other"] == 0

    def test_bot_category(self):
        analysis = {"U001": {"type": UserType.BOT}}
        result = categorize_user_analysis(analysis)
        assert result["Bots and workflow automations"] == 1

    def test_workflow_bot_category(self):
        analysis = {"U001": {"type": UserType.WORKFLOW_BOT}}
        result = categorize_user_analysis(analysis)
        assert result["Bots and workflow automations"] == 1

    def test_deleted_user_category(self):
        analysis = {"U001": {"type": UserType.DELETED_USER}}
        result = categorize_user_analysis(analysis)
        assert result["Deleted users"] == 1

    def test_no_email_category(self):
        analysis = {"U001": {"type": UserType.NO_EMAIL}}
        result = categorize_user_analysis(analysis)
        assert result["Users without email addresses"] == 1

    def test_restricted_user_category(self):
        analysis = {"U001": {"type": UserType.RESTRICTED_USER}}
        result = categorize_user_analysis(analysis)
        assert result["Restricted/guest users"] == 1

    def test_missing_from_export_category(self):
        analysis = {"U001": {"type": UserType.MISSING_FROM_EXPORT}}
        result = categorize_user_analysis(analysis)
        assert result["Missing from export"] == 1

    def test_unknown_type_falls_to_other(self):
        analysis = {"U001": {"type": UserType.REGULAR_USER}}
        result = categorize_user_analysis(analysis)
        assert result["Other"] == 1

    def test_mixed_categories(self):
        analysis = {
            "U001": {"type": UserType.BOT},
            "U002": {"type": UserType.DELETED_USER},
            "U003": {"type": UserType.NO_EMAIL},
            "U004": {"type": UserType.WORKFLOW_BOT},
            "U005": {"type": UserType.RESTRICTED_USER},
            "U006": {"type": UserType.MISSING_FROM_EXPORT},
            "U007": {"type": UserType.REGULAR_USER},
        }
        result = categorize_user_analysis(analysis)
        assert result["Bots and workflow automations"] == 2
        assert result["Deleted users"] == 1
        assert result["Users without email addresses"] == 1
        assert result["Restricted/guest users"] == 1
        assert result["Missing from export"] == 1
        assert result["Other"] == 1

    def test_entry_without_type_key(self):
        analysis = {"U001": {"name": "someone"}}
        result = categorize_user_analysis(analysis)
        assert result["Other"] == 1


# ===========================================================================
# analyze_unmapped_users
# ===========================================================================


class TestAnalyzeUnmappedUsers:
    """Tests for analyze_unmapped_users()."""

    def test_users_json_missing(self, tmp_path):
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U001"])

        assert result == {}

    def test_regular_user(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "real_name": "Alice A.",
                "profile": {"email": "alice@example.com"},
            }
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U001"])

        assert result["U001"]["type"] == UserType.REGULAR_USER
        assert result["U001"]["name"] == "Alice A."

    def test_bot_user(self, tmp_path):
        users = [
            {
                "id": "B001",
                "name": "slackbot",
                "real_name": "Slack Bot",
                "is_bot": True,
                "profile": {},
            }
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["B001"])

        assert result["B001"]["type"] == UserType.BOT
        assert "Bot/app integration" in result["B001"]["details"]

    def test_workflow_bot(self, tmp_path):
        users = [
            {
                "id": "W001",
                "name": "workflow",
                "real_name": "My Workflow",
                "is_bot": True,
                "is_workflow_bot": True,
                "profile": {},
            }
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["W001"])

        assert result["W001"]["type"] == UserType.WORKFLOW_BOT
        assert "Slack workflow automation" in result["W001"]["details"]

    def test_deleted_user(self, tmp_path):
        users = [
            {
                "id": "U002",
                "name": "gone",
                "real_name": "Gone User",
                "deleted": True,
                "profile": {"email": "gone@example.com"},
            }
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U002"])

        assert result["U002"]["type"] == UserType.DELETED_USER

    def test_restricted_user(self, tmp_path):
        users = [
            {
                "id": "U003",
                "name": "guest",
                "real_name": "Guest User",
                "is_restricted": True,
                "profile": {"email": "guest@partner.com"},
            }
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U003"])

        assert result["U003"]["type"] == UserType.RESTRICTED_USER

    def test_user_without_email(self, tmp_path):
        users = [
            {"id": "U004", "name": "noemail", "real_name": "No Email", "profile": {}}
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U004"])

        assert result["U004"]["type"] == UserType.NO_EMAIL

    def test_user_missing_from_export(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            }
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U999"])

        assert result["U999"]["type"] == UserType.MISSING_FROM_EXPORT
        assert result["U999"]["name"] == "Unknown"

    def test_multiple_unmapped_users(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "real_name": "Alice",
                "profile": {"email": "alice@example.com"},
            },
            {
                "id": "B001",
                "name": "bot",
                "real_name": "Bot",
                "is_bot": True,
                "profile": {},
            },
            {
                "id": "U002",
                "name": "gone",
                "real_name": "Gone",
                "deleted": True,
                "profile": {},
            },
        ]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U001", "B001", "U002"])

        assert len(result) == 3
        assert result["U001"]["type"] == UserType.REGULAR_USER
        assert result["B001"]["type"] == UserType.BOT
        assert result["U002"]["type"] == UserType.DELETED_USER

    def test_fallback_name_uses_name_field(self, tmp_path):
        users = [{"id": "U001", "name": "fallback_name", "profile": {}}]
        _write_json(tmp_path / "users.json", users)
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U001"])

        assert result["U001"]["name"] == "fallback_name"

    def test_exception_returns_empty_dict(self, tmp_path):
        """When users.json cannot be parsed, the function catches the error."""
        (tmp_path / "users.json").write_text("{bad json")
        migrator = _make_migrator(export_root=str(tmp_path))

        result = analyze_unmapped_users(migrator, ["U001"])

        assert result == {}


# ===========================================================================
# initialize_unmapped_user_tracking
# ===========================================================================


class TestInitializeUnmappedUserTracking:
    """Tests for initialize_unmapped_user_tracking()."""

    def test_creates_tracker_when_absent(self):
        migrator = _make_migrator(has_tracker=False)
        tracker = initialize_unmapped_user_tracking(migrator)

        assert isinstance(tracker, UnmappedUserTracker)
        assert migrator.unmapped_user_tracker is tracker

    def test_returns_existing_tracker(self):
        migrator = _make_migrator(has_tracker=True)
        existing = migrator.unmapped_user_tracker
        tracker = initialize_unmapped_user_tracking(migrator)

        assert tracker is existing


# ===========================================================================
# log_unmapped_user_summary_for_dry_run
# ===========================================================================


class TestLogUnmappedUserSummaryForDryRun:
    """Tests for log_unmapped_user_summary_for_dry_run()."""

    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_no_tracker_logs_success(self, mock_log):
        migrator = _make_migrator(has_tracker=False)

        log_unmapped_user_summary_for_dry_run(migrator)

        mock_log.assert_called_once_with(
            logging.INFO, "✅ No unmapped users detected during dry run"
        )

    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_empty_tracker_logs_success(self, mock_log):
        migrator = _make_migrator(has_tracker=True)
        # Tracker has no unmapped users

        log_unmapped_user_summary_for_dry_run(migrator)

        mock_log.assert_called_once_with(
            logging.INFO, "✅ No unmapped users detected during dry run"
        )

    @patch("slack_migrator.utils.user_validation.analyze_unmapped_users")
    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_unmapped_users_triggers_error_logging(self, mock_log, mock_analyze):
        migrator = _make_migrator(has_tracker=True)
        migrator.unmapped_user_tracker.add_unmapped_user("U001", "mention")

        mock_analyze.return_value = {
            "U001": {"type": UserType.REGULAR_USER, "name": "Alice"},
        }

        log_unmapped_user_summary_for_dry_run(migrator)

        # Should have been called many times (the function logs a lot)
        assert mock_log.call_count > 5
        # First error call should contain "UNMAPPED USERS"
        error_calls = [c for c in mock_log.call_args_list if c[0][0] == logging.ERROR]
        assert any("UNMAPPED USERS" in str(c) for c in error_calls)

    @patch("slack_migrator.utils.user_validation.analyze_unmapped_users")
    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_bot_users_show_bot_recommendations(self, mock_log, mock_analyze):
        migrator = _make_migrator(has_tracker=True)
        migrator.unmapped_user_tracker.add_unmapped_user("B001")

        mock_analyze.return_value = {
            "B001": {"type": UserType.BOT, "name": "TestBot"},
        }

        log_unmapped_user_summary_for_dry_run(migrator)

        all_log_text = " ".join(str(c) for c in mock_log.call_args_list)
        assert "bot" in all_log_text.lower() or "Bot" in all_log_text
        assert "ignore_bots" in all_log_text

    @patch("slack_migrator.utils.user_validation.analyze_unmapped_users")
    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_deleted_users_show_deleted_recommendations(self, mock_log, mock_analyze):
        migrator = _make_migrator(has_tracker=True)
        migrator.unmapped_user_tracker.add_unmapped_user("U001")

        mock_analyze.return_value = {
            "U001": {"type": UserType.DELETED_USER, "name": "GoneUser"},
        }

        log_unmapped_user_summary_for_dry_run(migrator)

        all_log_text = " ".join(str(c) for c in mock_log.call_args_list)
        assert "deleted" in all_log_text.lower()

    @patch("slack_migrator.utils.user_validation.analyze_unmapped_users")
    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_no_bots_skips_ignore_bots_option(self, mock_log, mock_analyze):
        migrator = _make_migrator(has_tracker=True)
        migrator.unmapped_user_tracker.add_unmapped_user("U001")

        mock_analyze.return_value = {
            "U001": {"type": UserType.REGULAR_USER, "name": "Alice"},
        }

        log_unmapped_user_summary_for_dry_run(migrator)

        # "Add user mappings:" is shown when no bots present (instead of "EASIEST SOLUTION")
        error_calls = [c for c in mock_log.call_args_list if c[0][0] == logging.ERROR]
        messages = [str(c) for c in error_calls]
        assert any("Add user mappings" in m for m in messages)

    @patch("slack_migrator.utils.user_validation.analyze_unmapped_users")
    @patch("slack_migrator.utils.user_validation.log_with_context")
    def test_context_info_appended_to_log(self, mock_log, mock_analyze):
        migrator = _make_migrator(has_tracker=True)
        migrator.unmapped_user_tracker.add_unmapped_user(
            "U001", "channel_member:#general"
        )

        mock_analyze.return_value = {
            "U001": {"type": UserType.REGULAR_USER, "name": "Alice"},
        }

        log_unmapped_user_summary_for_dry_run(migrator)

        all_log_text = " ".join(str(c) for c in mock_log.call_args_list)
        assert "channel_member:#general" in all_log_text


# ===========================================================================
# scan_channel_members_for_unmapped_users
# ===========================================================================


class TestScanChannelMembersForUnmappedUsers:
    """Tests for scan_channel_members_for_unmapped_users()."""

    def test_no_channels_json(self, tmp_path):
        """When channels.json is absent, a warning is logged and function returns."""
        migrator = _make_migrator(export_root=str(tmp_path), has_tracker=True)

        with patch("slack_migrator.utils.user_validation.log_with_context") as mock_log:
            scan_channel_members_for_unmapped_users(migrator)

        warning_calls = [
            c for c in mock_log.call_args_list if c[0][0] == logging.WARNING
        ]
        assert any("channels.json not found" in str(c) for c in warning_calls)

    def test_all_members_mapped(self, tmp_path):
        channels = [
            {"name": "general", "members": ["U001", "U002"]},
        ]
        _write_json(tmp_path / "channels.json", channels)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={"U001": "a@x.com", "U002": "b@x.com"},
        )

        with patch("slack_migrator.utils.user_validation.log_with_context") as mock_log:
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        assert not tracker.has_unmapped_users()
        # Verify success log
        info_calls = [c for c in mock_log.call_args_list if c[0][0] == logging.INFO]
        assert any("All" in str(c) and "user mappings" in str(c) for c in info_calls)

    def test_unmapped_members_detected(self, tmp_path):
        channels = [
            {"name": "general", "members": ["U001", "U002", "U003"]},
        ]
        _write_json(tmp_path / "channels.json", channels)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={"U001": "a@x.com"},
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        assert tracker.get_unmapped_count() == 2
        assert set(tracker.get_unmapped_users_list()) == {"U002", "U003"}

    def test_include_channels_filter(self, tmp_path):
        channels = [
            {"name": "general", "members": ["U001"]},
            {"name": "random", "members": ["U002"]},
        ]
        _write_json(tmp_path / "channels.json", channels)
        config = MigrationConfig(include_channels=["general"])
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
            config=config,
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        # Only U001 (from included "general") should be tracked
        assert tracker.get_unmapped_users_list() == ["U001"]

    def test_exclude_channels_filter(self, tmp_path):
        channels = [
            {"name": "general", "members": ["U001"]},
            {"name": "random", "members": ["U002"]},
        ]
        _write_json(tmp_path / "channels.json", channels)
        config = MigrationConfig(exclude_channels=["random"])
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
            config=config,
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        # Only U001 (from non-excluded "general") should be tracked
        assert tracker.get_unmapped_users_list() == ["U001"]

    def test_ignore_bots_skips_bot_members(self, tmp_path):
        channels = [{"name": "general", "members": ["U001", "B001"]}]
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
            {
                "id": "B001",
                "name": "testbot",
                "is_bot": True,
                "real_name": "Test Bot",
                "profile": {},
            },
        ]
        _write_json(tmp_path / "channels.json", channels)
        _write_json(tmp_path / "users.json", users)
        config = MigrationConfig(ignore_bots=True)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
            config=config,
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        # Bot B001 should be skipped; only U001 should be tracked
        assert tracker.get_unmapped_users_list() == ["U001"]

    def test_ignore_bots_without_users_json(self, tmp_path):
        """When ignore_bots is True but users.json is missing, all unmapped members tracked."""
        channels = [{"name": "general", "members": ["U001", "B001"]}]
        _write_json(tmp_path / "channels.json", channels)
        config = MigrationConfig(ignore_bots=True)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
            config=config,
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        # Both should be tracked because we can't determine bot status
        assert tracker.get_unmapped_count() == 2

    def test_initializes_tracker_if_absent(self, tmp_path):
        channels = [{"name": "general", "members": ["U001"]}]
        _write_json(tmp_path / "channels.json", channels)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
            has_tracker=False,
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        # Tracker should have been created
        assert hasattr(migrator, "unmapped_user_tracker")
        assert isinstance(migrator.unmapped_user_tracker, UnmappedUserTracker)
        assert migrator.unmapped_user_tracker.get_unmapped_count() == 1

    def test_multiple_channels_with_overlapping_members(self, tmp_path):
        channels = [
            {"name": "general", "members": ["U001", "U002"]},
            {"name": "random", "members": ["U002", "U003"]},
        ]
        _write_json(tmp_path / "channels.json", channels)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        # U002 appears in both channels but should only be counted once
        assert tracker.get_unmapped_count() == 3
        # U002 should have contexts from both channels
        assert "channel_member:#general" in tracker.user_contexts["U002"]
        assert "channel_member:#random" in tracker.user_contexts["U002"]

    def test_exception_is_caught_gracefully(self, tmp_path):
        """When an unexpected error occurs, it's caught and logged."""
        # Write invalid JSON to channels.json to trigger an exception
        (tmp_path / "channels.json").write_text("{invalid")
        migrator = _make_migrator(export_root=str(tmp_path))

        with patch("slack_migrator.utils.user_validation.log_with_context") as mock_log:
            # Should not raise
            scan_channel_members_for_unmapped_users(migrator)

        error_calls = [c for c in mock_log.call_args_list if c[0][0] == logging.ERROR]
        assert any("Error scanning" in str(c) for c in error_calls)

    def test_channel_without_members_key(self, tmp_path):
        """Channels missing the 'members' key default to empty list."""
        channels = [{"name": "general"}]
        _write_json(tmp_path / "channels.json", channels)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        assert not tracker.has_unmapped_users()

    def test_ignore_bots_with_invalid_users_json(self, tmp_path):
        """When ignore_bots is True but users.json is corrupt, tracks all members."""
        channels = [{"name": "general", "members": ["U001", "B001"]}]
        _write_json(tmp_path / "channels.json", channels)
        (tmp_path / "users.json").write_text("{bad json")
        config = MigrationConfig(ignore_bots=True)
        migrator = _make_migrator(
            export_root=str(tmp_path),
            user_map={},
            config=config,
        )

        with patch("slack_migrator.utils.user_validation.log_with_context"):
            scan_channel_members_for_unmapped_users(migrator)

        tracker = migrator.unmapped_user_tracker
        # Both should be tracked since we couldn't load user data
        assert tracker.get_unmapped_count() == 2
