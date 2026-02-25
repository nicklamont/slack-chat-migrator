"""Unit tests for the migration_logging module."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

from slack_migrator.core.migration_logging import (
    _collect_statistics,
    log_migration_failure,
    log_migration_success,
)
from slack_migrator.core.state import MigrationState
from slack_migrator.types import MigrationSummary

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_migrator(
    *,
    dry_run: bool = False,
    channels_processed: list | None = None,
    spaces_created: int = 0,
    messages_created: int = 0,
    reactions_created: int = 0,
    files_created: int = 0,
    channels_with_errors: list | None = None,
    incomplete_import_spaces: list | None = None,
    unmapped_count: int = 0,
) -> MagicMock:
    """Build a mock migrator with realistic state attributes."""
    migrator = MagicMock()
    migrator.dry_run = dry_run

    state = MigrationState()
    state.migration_summary = MigrationSummary(
        channels_processed=channels_processed or [],
        spaces_created=spaces_created,
        messages_created=messages_created,
        reactions_created=reactions_created,
        files_created=files_created,
    )
    state.channels_with_errors = channels_with_errors or []
    state.incomplete_import_spaces = incomplete_import_spaces or []
    migrator.state = state

    # Unmapped user tracker
    if unmapped_count > 0:
        migrator.unmapped_user_tracker.has_unmapped_users.return_value = True
        migrator.unmapped_user_tracker.get_unmapped_count.return_value = unmapped_count
    else:
        migrator.unmapped_user_tracker.has_unmapped_users.return_value = False

    return migrator


# ---------------------------------------------------------------------------
# _collect_statistics
# ---------------------------------------------------------------------------


class TestCollectStatistics:
    """Tests for the _collect_statistics helper."""

    def test_returns_all_expected_keys(self):
        migrator = _make_mock_migrator()
        stats = _collect_statistics(migrator)
        expected_keys = {
            "channels_processed",
            "spaces_created",
            "messages_created",
            "reactions_created",
            "files_created",
            "channels_with_errors",
            "incomplete_imports",
            "unmapped_users",
        }
        assert set(stats.keys()) == expected_keys

    def test_basic_counts(self):
        migrator = _make_mock_migrator(
            channels_processed=["general", "random"],
            spaces_created=2,
            messages_created=42,
            reactions_created=7,
            files_created=3,
        )
        stats = _collect_statistics(migrator)
        assert stats["channels_processed"] == 2
        assert stats["spaces_created"] == 2
        assert stats["messages_created"] == 42
        assert stats["reactions_created"] == 7
        assert stats["files_created"] == 3
        assert stats["channels_with_errors"] == 0
        assert stats["incomplete_imports"] == 0
        assert stats["unmapped_users"] == 0

    def test_unmapped_users_counted(self):
        migrator = _make_mock_migrator(unmapped_count=5)
        stats = _collect_statistics(migrator)
        assert stats["unmapped_users"] == 5

    def test_error_channels_and_incomplete_imports(self):
        migrator = _make_mock_migrator(
            channels_with_errors=["ch1", "ch2"],
            incomplete_import_spaces=[("space1", "ch1")],
        )
        stats = _collect_statistics(migrator)
        assert stats["channels_with_errors"] == 2
        assert stats["incomplete_imports"] == 1

    def test_handles_missing_unmapped_user_tracker(self):
        """If the migrator lacks the tracker attribute, unmapped_users is 0."""
        migrator = _make_mock_migrator()
        del migrator.unmapped_user_tracker
        stats = _collect_statistics(migrator)
        assert stats["unmapped_users"] == 0


# ---------------------------------------------------------------------------
# log_migration_success
# ---------------------------------------------------------------------------


class TestLogMigrationSuccess:
    """Tests for log_migration_success."""

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_dry_run_success(self, mock_log):
        migrator = _make_mock_migrator(dry_run=True, channels_processed=["general"])
        log_migration_success(migrator, duration=60.0)

        # First call should be the dry-run header
        first_call = mock_log.call_args_list[0]
        assert first_call[0][0] == logging.INFO
        assert "DRY RUN VALIDATION COMPLETED" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_normal_success(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["general"],
            spaces_created=1,
            messages_created=10,
        )
        log_migration_success(migrator, duration=120.0)

        first_call = mock_log.call_args_list[0]
        assert first_call[0][0] == logging.INFO
        assert "COMPLETED SUCCESSFULLY" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_no_work_interrupted_early(self, mock_log):
        """No spaces created AND no channels processed => interrupted early."""
        migrator = _make_mock_migrator()
        log_migration_success(migrator, duration=5.0)

        first_call = mock_log.call_args_list[0]
        assert first_call[0][0] == logging.WARNING
        assert "INTERRUPTED DURING INITIALIZATION" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_no_work_but_channels_processed(self, mock_log):
        """Channels processed but no spaces/messages => interrupted before import."""
        migrator = _make_mock_migrator(channels_processed=["general"])
        log_migration_success(migrator, duration=10.0)

        first_call = mock_log.call_args_list[0]
        assert first_call[0][0] == logging.WARNING
        assert "INTERRUPTED BEFORE ANY SPACES" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_duration_logged(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["general"],
            spaces_created=1,
            messages_created=5,
        )
        log_migration_success(migrator, duration=90.0)

        # The second call logs the duration
        duration_call = mock_log.call_args_list[1]
        assert "1.5 minutes" in duration_call[0][1]
        assert "90.0 seconds" in duration_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_zero_duration(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["ch"],
            spaces_created=1,
            messages_created=1,
        )
        log_migration_success(migrator, duration=0.0)

        duration_call = mock_log.call_args_list[1]
        assert "0.0 minutes" in duration_call[0][1]
        assert "0.0 seconds" in duration_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_very_long_duration(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["ch"],
            spaces_created=1,
            messages_created=1,
        )
        log_migration_success(migrator, duration=7200.0)

        duration_call = mock_log.call_args_list[1]
        assert "120.0 minutes" in duration_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_issues_logged_as_warnings(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["ch"],
            spaces_created=1,
            messages_created=5,
            channels_with_errors=["ch"],
            incomplete_import_spaces=[("sp", "ch")],
            unmapped_count=3,
        )
        log_migration_success(migrator, duration=60.0)

        # Gather all WARNING-level messages
        warning_messages = [
            call[0][1]
            for call in mock_log.call_args_list
            if call[0][0] == logging.WARNING
        ]
        assert any("Unmapped users: 3" in m for m in warning_messages)
        assert any("Channels with errors: 1" in m for m in warning_messages)
        assert any("Incomplete imports: 1" in m for m in warning_messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_no_issues_logged_when_clean(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["ch"],
            spaces_created=1,
            messages_created=5,
        )
        log_migration_success(migrator, duration=60.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert any("No issues detected" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_dry_run_skips_space_message_stats(self, mock_log):
        """In dry-run mode, spaces/messages/reactions/files stats are not logged."""
        migrator = _make_mock_migrator(dry_run=True, channels_processed=["ch"])
        log_migration_success(migrator, duration=10.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert not any("Spaces created" in m for m in messages)
        assert not any("Messages migrated" in m for m in messages)
        assert not any("Reactions migrated" in m for m in messages)
        assert not any("Files migrated" in m for m in messages)


# ---------------------------------------------------------------------------
# log_migration_failure
# ---------------------------------------------------------------------------


class TestLogMigrationFailure:
    """Tests for log_migration_failure."""

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_keyboard_interrupt(self, mock_log):
        migrator = _make_mock_migrator(channels_processed=["ch"])
        log_migration_failure(migrator, KeyboardInterrupt(), duration=30.0)

        first_call = mock_log.call_args_list[0]
        assert first_call[0][0] == logging.WARNING
        assert "INTERRUPTED BY USER" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_keyboard_interrupt_dry_run(self, mock_log):
        migrator = _make_mock_migrator(dry_run=True, channels_processed=["ch"])
        log_migration_failure(migrator, KeyboardInterrupt(), duration=10.0)

        first_call = mock_log.call_args_list[0]
        assert "DRY RUN VALIDATION INTERRUPTED" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_runtime_error(self, mock_log):
        migrator = _make_mock_migrator(channels_processed=["ch"])
        exc = RuntimeError("something went wrong")
        log_migration_failure(migrator, exc, duration=45.0)

        first_call = mock_log.call_args_list[0]
        assert first_call[0][0] == logging.ERROR
        assert "MIGRATION FAILED" in first_call[0][1]
        assert first_call[1]["exception_type"] == "RuntimeError"

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_dry_run_failure(self, mock_log):
        migrator = _make_mock_migrator(dry_run=True)
        exc = ValueError("bad config")
        log_migration_failure(migrator, exc, duration=2.0)

        first_call = mock_log.call_args_list[0]
        assert "DRY RUN VALIDATION FAILED" in first_call[0][1]

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_duration_logged_on_failure(self, mock_log):
        migrator = _make_mock_migrator()
        log_migration_failure(migrator, RuntimeError("fail"), duration=300.0)

        all_messages = " ".join(call[0][1] for call in mock_log.call_args_list)
        assert "5.0 minutes" in all_messages
        assert "300.0 seconds" in all_messages

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_zero_duration_on_failure(self, mock_log):
        migrator = _make_mock_migrator()
        log_migration_failure(migrator, RuntimeError("instant"), duration=0.0)

        all_messages = " ".join(call[0][1] for call in mock_log.call_args_list)
        assert "0.0 minutes" in all_messages

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_progress_before_failure(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["a", "b"],
            spaces_created=2,
            messages_created=50,
        )
        log_migration_failure(migrator, RuntimeError("oops"), duration=60.0)

        all_messages = " ".join(call[0][1] for call in mock_log.call_args_list)
        assert "Channels processed: 2" in all_messages
        assert "Spaces created: 2" in all_messages
        assert "Messages migrated: 50" in all_messages

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_progress_before_interruption(self, mock_log):
        migrator = _make_mock_migrator(
            channels_processed=["a"],
            spaces_created=1,
            messages_created=10,
        )
        log_migration_failure(migrator, KeyboardInterrupt(), duration=15.0)

        all_messages = " ".join(call[0][1] for call in mock_log.call_args_list)
        assert "PROGRESS BEFORE INTERRUPTION" in all_messages

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_dry_run_skips_space_message_progress(self, mock_log):
        migrator = _make_mock_migrator(dry_run=True, channels_processed=["ch"])
        log_migration_failure(migrator, RuntimeError("fail"), duration=5.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert not any("Spaces created" in m for m in messages)
        assert not any("Messages migrated" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_recovery_guidance_interrupt(self, mock_log):
        migrator = _make_mock_migrator()
        log_migration_failure(migrator, KeyboardInterrupt(), duration=5.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert any("--update_mode" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_recovery_guidance_interrupt_dry_run(self, mock_log):
        migrator = _make_mock_migrator(dry_run=True)
        log_migration_failure(migrator, KeyboardInterrupt(), duration=5.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert any("restart the validation" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_recovery_guidance_error(self, mock_log):
        migrator = _make_mock_migrator()
        log_migration_failure(migrator, RuntimeError("boom"), duration=5.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert any("--update_mode" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_recovery_guidance_error_dry_run(self, mock_log):
        migrator = _make_mock_migrator(dry_run=True)
        log_migration_failure(migrator, ValueError("bad"), duration=5.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert any("Fix the validation issues" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_different_exception_types(self, mock_log):
        """Verify various exception types are handled and reported correctly."""
        exceptions = [
            (TypeError("type err"), "TypeError"),
            (OSError("os err"), "OSError"),
            (ConnectionError("conn err"), "ConnectionError"),
        ]
        for exc, expected_type in exceptions:
            mock_log.reset_mock()
            migrator = _make_mock_migrator()
            log_migration_failure(migrator, exc, duration=1.0)
            first_call = mock_log.call_args_list[0]
            assert first_call[1]["exception_type"] == expected_type

    @patch("slack_migrator.core.migration_logging.traceback.format_exc")
    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_traceback_logged_on_error(self, mock_log, mock_format_exc):
        mock_format_exc.return_value = "Traceback (most recent call last):\n  ..."
        migrator = _make_mock_migrator()
        log_migration_failure(migrator, RuntimeError("fail"), duration=1.0)

        messages = [call[0][1] for call in mock_log.call_args_list]
        assert any("Traceback" in m for m in messages)

    @patch("slack_migrator.core.migration_logging.traceback.format_exc")
    @patch("slack_migrator.core.migration_logging.log_with_context")
    def test_traceback_not_logged_on_interrupt(self, mock_log, mock_format_exc):
        """KeyboardInterrupt should not produce a traceback log."""
        migrator = _make_mock_migrator()
        log_migration_failure(migrator, KeyboardInterrupt(), duration=1.0)

        mock_format_exc.assert_not_called()
