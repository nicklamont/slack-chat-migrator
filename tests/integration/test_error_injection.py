"""Error injection tests for the dry-run migration pipeline.

Uses ``message_error_schedule`` to inject ``HttpError`` at specific
message positions and verifies that the pipeline's error handling
(failure tracking, abort logic, cleanup) works correctly.
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


class TestSingleFailureContinues:
    """One failure out of 3 messages → 2 created, 1 failed."""

    def test_single_failure_continues(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(3)},
        )
        m = make_migrator(tmp_path, message_error_schedule={2: 400})
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 2


class TestFailureTrackedInState:
    """A failed message is recorded in ``failed_messages``."""

    def test_failure_tracked_in_state(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(3)},
        )
        m = make_migrator(tmp_path, message_error_schedule={2: 400})
        m.migrate()

        assert len(m.state.messages.failed_messages) == 1
        failed = m.state.messages.failed_messages[0]
        assert failed["channel"] == "general"


class TestFailureThresholdFlagsChannel:
    """Many failures with ``max_failure_percentage: 10`` flags the channel."""

    def test_failure_threshold_flags_channel(self, tmp_path: Path) -> None:
        # 5 messages, 3 fail → 60% failure rate, threshold at 10%
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(5)},
        )
        m = make_migrator(
            tmp_path,
            config_text="exclude_channels: []\ninclude_channels: []\nmax_failure_percentage: 10\n",
            message_error_schedule={1: 400, 3: 400, 4: 400},
        )
        m.migrate()

        assert "general" in m.state.errors.high_failure_rate_channels


class TestAbortOnErrorStopsMigration:
    """``abort_on_error: true`` + error in first channel → second channel skipped."""

    def test_abort_on_error_stops_migration(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
            messages_by_channel={
                "general": make_messages(3),
                "random": make_messages(2),
            },
        )
        config = (
            "exclude_channels: []\n"
            "include_channels: []\n"
            "abort_on_error: true\n"
            "cleanup_on_error: false\n"
        )
        m = make_migrator(
            tmp_path,
            config_text=config,
            message_error_schedule={1: 400},
        )
        m.migrate()

        assert len(m.state.messages.failed_messages) == 1

        # general: 3 messages, error at position 1 → 2 succeed, 1 fails.
        # Abort prevents random from being processed → 0 from random.
        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 2


class TestRateLimitError:
    """A 429 error is recorded with the correct status code."""

    def test_rate_limit_error_is_429(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(2)},
        )
        m = make_migrator(tmp_path, message_error_schedule={1: 429})
        m.migrate()

        assert len(m.state.messages.failed_messages) == 1
        failed = m.state.messages.failed_messages[0]
        assert "429" in failed["error"]


class TestClientError:
    """A 400 error is recorded with the correct status code."""

    def test_client_error_is_400(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(2)},
        )
        m = make_migrator(tmp_path, message_error_schedule={1: 400})
        m.migrate()

        assert len(m.state.messages.failed_messages) == 1
        failed = m.state.messages.failed_messages[0]
        assert "400" in failed["error"]


class TestCleanupOnError:
    """``cleanup_on_error: true`` + error → space removed from ``created_spaces``."""

    def test_cleanup_on_error_removes_space(self, tmp_path: Path) -> None:
        config = "exclude_channels: []\ninclude_channels: []\ncleanup_on_error: true\n"
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

        # cleanup_on_error deletes the space and removes it from created_spaces
        assert "general" not in m.state.spaces.created_spaces


class TestNoCleanupByDefault:
    """Default ``cleanup_on_error: false`` keeps the space despite errors."""

    def test_space_retained_when_cleanup_disabled(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(3)},
        )
        m = make_migrator(tmp_path, message_error_schedule={1: 400})
        m.migrate()

        # Default cleanup_on_error is false — space stays in created_spaces
        assert "general" in m.state.spaces.created_spaces


class TestErroredCallsStillCaptured:
    """Error-injected calls are still recorded in ``captured_messages``."""

    def test_errored_call_is_captured(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(2)},
        )
        m = make_migrator(tmp_path, message_error_schedule={1: 400})
        m.migrate()

        assert m._dry_run_chat_service is not None
        # Both calls (the errored one and the successful one) are captured
        assert len(m._dry_run_chat_service.captured_messages) == 2
