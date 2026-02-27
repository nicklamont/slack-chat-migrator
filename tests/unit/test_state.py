"""Unit tests for the MigrationState dataclass and sub-states."""

from __future__ import annotations

import pytest

from slack_migrator.core.state import (
    ContextState,
    ErrorState,
    MessageState,
    MigrationState,
    ProgressState,
    SpaceState,
    UserState,
    _default_migration_summary,
)
from slack_migrator.types import FailedMessage, MigrationSummary


def _make_summary(**overrides: object) -> MigrationSummary:
    """Return a MigrationSummary with defaults, applying any overrides."""
    summary = _default_migration_summary()
    summary.update(overrides)  # type: ignore[typeddict-item]
    return summary


def _make_failed(ts: str = "1", channel: str = "general") -> FailedMessage:
    """Return a minimal FailedMessage for testing."""
    return FailedMessage(
        channel=channel,
        ts=ts,
        error="test error",
        error_details="details",
        payload={},
    )


# ---------------------------------------------------------------------------
# Sub-state construction
# ---------------------------------------------------------------------------


class TestSubStateConstruction:
    """Tests that sub-state dataclasses can be independently created."""

    def test_space_state_defaults(self):
        s = SpaceState()
        assert s.space_mapping == {}
        assert s.channel_handlers == {}

    def test_message_state_defaults(self):
        m = MessageState()
        assert m.thread_map == {}
        assert m.sent_messages == set()
        assert m.failed_messages == []

    def test_user_state_defaults(self):
        u = UserState()
        assert u.external_users == set()
        assert u.skipped_reactions == []

    def test_progress_state_defaults(self):
        p = ProgressState()
        assert p.migration_summary == _default_migration_summary()

    def test_error_state_defaults(self):
        e = ErrorState()
        assert e.channel_error_count == 0
        assert e.migration_errors == []

    def test_context_state_defaults(self):
        c = ContextState()
        assert c.current_channel is None
        assert c.first_channel_processed is False


# ---------------------------------------------------------------------------
# MigrationState construction and backward compatibility
# ---------------------------------------------------------------------------


class TestMigrationStateConstruction:
    """Tests that MigrationState composes sub-states correctly."""

    def test_default_construction(self):
        state = MigrationState()
        assert state.errors.channel_error_count == 0
        assert state.context.first_channel_processed is False
        assert state.context.current_channel is None
        assert state.spaces.space_mapping == {}
        assert state.messages.thread_map == {}
        assert state.progress.migration_summary == _default_migration_summary()

    def test_sub_state_construction(self):
        """Construct with explicit sub-states."""
        state = MigrationState(
            spaces=SpaceState(channel_handlers={"ch1": "handler"}),
            errors=ErrorState(channel_error_count=5),
        )
        assert state.spaces.channel_handlers == {"ch1": "handler"}
        assert state.errors.channel_error_count == 5

    def test_sub_state_reads(self):
        """Reading through sub-state attributes returns correct values."""
        state = MigrationState(
            spaces=SpaceState(space_cache={"s1": "v1"}),
            messages=MessageState(sent_messages={"msg1"}),
            users=UserState(external_users={"ext@ex.com"}),
        )
        assert state.spaces.space_cache == {"s1": "v1"}
        assert state.messages.sent_messages == {"msg1"}
        assert state.users.external_users == {"ext@ex.com"}

    def test_sub_state_writes(self):
        """Setting sub-state attributes updates correctly."""
        state = MigrationState()
        state.spaces.channel_handlers = {"ch1": "handler"}
        assert state.spaces.channel_handlers == {"ch1": "handler"}

        state.messages.thread_map = {"ts1": "thread1"}
        assert state.messages.thread_map == {"ts1": "thread1"}

        state.context.current_channel = "general"
        assert state.context.current_channel == "general"

    def test_drive_files_cache_is_top_level(self):
        """drive_files_cache remains a direct field on MigrationState."""
        state = MigrationState()
        state.drive_files_cache["f1"] = {"id": "f1"}
        assert state.drive_files_cache == {"f1": {"id": "f1"}}


# ---------------------------------------------------------------------------
# __post_init__ validation
# ---------------------------------------------------------------------------


class TestPostInitValidation:
    """Tests for __post_init__ validation."""

    def test_negative_channel_error_count_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            MigrationState(errors=ErrorState(channel_error_count=-1))

    def test_zero_channel_error_count_ok(self):
        state = MigrationState(errors=ErrorState(channel_error_count=0))
        assert state.errors.channel_error_count == 0

    def test_positive_channel_error_count_ok(self):
        state = MigrationState(errors=ErrorState(channel_error_count=5))
        assert state.errors.channel_error_count == 5


# ---------------------------------------------------------------------------
# reset_for_run
# ---------------------------------------------------------------------------


class TestResetForRun:
    """Tests for MigrationState.reset_for_run."""

    def test_resets_channel_handlers(self):
        state = MigrationState(spaces=SpaceState(channel_handlers={"ch1": "handler"}))
        state.reset_for_run()
        assert state.spaces.channel_handlers == {}

    def test_resets_thread_map(self):
        state = MigrationState(messages=MessageState(thread_map={"ts1": "thread1"}))
        state.reset_for_run()
        assert state.messages.thread_map == {}

    def test_resets_migration_summary_to_initial_structure(self):
        state = MigrationState(
            progress=ProgressState(
                migration_summary=_make_summary(
                    channels_processed=["a", "b"],
                    spaces_created=5,
                    messages_created=100,
                    reactions_created=20,
                    files_created=10,
                )
            )
        )
        state.reset_for_run()
        assert state.progress.migration_summary == _default_migration_summary()

    def test_resets_migration_errors(self):
        state = MigrationState(errors=ErrorState(migration_errors=["error1"]))
        state.reset_for_run()
        assert state.errors.migration_errors == []

    def test_resets_channels_with_errors(self):
        state = MigrationState(errors=ErrorState(channels_with_errors=["ch1"]))
        state.reset_for_run()
        assert state.errors.channels_with_errors == []

    def test_resets_channel_error_count(self):
        state = MigrationState(errors=ErrorState(channel_error_count=3))
        state.reset_for_run()
        assert state.errors.channel_error_count == 0

    def test_resets_first_channel_processed(self):
        state = MigrationState(context=ContextState(first_channel_processed=True))
        state.reset_for_run()
        assert state.context.first_channel_processed is False

    def test_does_not_reset_space_cache(self):
        """Space cache should persist across runs (it is not per-run state)."""
        state = MigrationState(spaces=SpaceState(space_cache={"s1": "v1"}))
        state.reset_for_run()
        assert state.spaces.space_cache == {"s1": "v1"}

    def test_does_not_reset_sent_messages(self):
        """Sent messages should persist across runs for deduplication."""
        state = MigrationState(messages=MessageState(sent_messages={"msg1"}))
        state.reset_for_run()
        assert state.messages.sent_messages == {"msg1"}

    def test_complete_reset(self):
        """Verify all expected fields are reset when starting from dirty state."""
        state = MigrationState(
            spaces=SpaceState(channel_handlers={"h": "v"}),
            messages=MessageState(thread_map={"t": "v"}),
            progress=ProgressState(
                migration_summary=_make_summary(
                    channels_processed=["x"], spaces_created=1
                )
            ),
            errors=ErrorState(
                migration_errors=["err"],
                channels_with_errors=["ch"],
                channel_error_count=2,
            ),
            context=ContextState(first_channel_processed=True),
        )
        state.reset_for_run()
        assert state.spaces.channel_handlers == {}
        assert state.messages.thread_map == {}
        assert state.progress.migration_summary["channels_processed"] == []
        assert state.progress.migration_summary["spaces_created"] == 0
        assert state.errors.migration_errors == []
        assert state.errors.channels_with_errors == []
        assert state.errors.channel_error_count == 0
        assert state.context.first_channel_processed is False


# ---------------------------------------------------------------------------
# has_errors property
# ---------------------------------------------------------------------------


class TestHasErrors:
    """Tests for MigrationState.has_errors."""

    def test_no_errors(self):
        state = MigrationState()
        assert state.has_errors is False

    def test_migration_errors_present(self):
        state = MigrationState(errors=ErrorState(migration_errors=["some error"]))
        assert state.has_errors is True

    def test_channels_with_errors_present(self):
        state = MigrationState(errors=ErrorState(channels_with_errors=["ch1"]))
        assert state.has_errors is True

    def test_both_error_types_present(self):
        state = MigrationState(
            errors=ErrorState(
                migration_errors=["err"],
                channels_with_errors=["ch"],
            )
        )
        assert state.has_errors is True

    def test_empty_error_lists(self):
        state = MigrationState()
        assert state.has_errors is False


# ---------------------------------------------------------------------------
# success_rate property
# ---------------------------------------------------------------------------


class TestSuccessRate:
    """Tests for MigrationState.success_rate."""

    def test_no_messages_attempted_returns_100(self):
        state = MigrationState()
        assert state.success_rate == 100.0

    def test_all_messages_successful(self):
        state = MigrationState(
            progress=ProgressState(
                migration_summary=_make_summary(messages_created=10)
            ),
        )
        assert state.success_rate == 100.0

    def test_all_messages_failed(self):
        state = MigrationState(
            messages=MessageState(
                failed_messages=[_make_failed("1"), _make_failed("2")]
            ),
        )
        assert state.success_rate == 0.0

    def test_partial_failure(self):
        state = MigrationState(
            progress=ProgressState(migration_summary=_make_summary(messages_created=7)),
            messages=MessageState(
                failed_messages=[
                    _make_failed("1"),
                    _make_failed("2"),
                    _make_failed("3"),
                ]
            ),
        )
        # 7 / 10 = 70%
        assert state.success_rate == pytest.approx(70.0)

    def test_one_success_one_failure(self):
        state = MigrationState(
            progress=ProgressState(migration_summary=_make_summary(messages_created=1)),
            messages=MessageState(failed_messages=[_make_failed("x")]),
        )
        assert state.success_rate == pytest.approx(50.0)

    def test_default_summary_zero_messages(self):
        """Default MigrationSummary has messages_created=0."""
        state = MigrationState(
            messages=MessageState(failed_messages=[_make_failed("1")]),
        )
        assert state.success_rate == 0.0


# ---------------------------------------------------------------------------
# total_messages_attempted property
# ---------------------------------------------------------------------------


class TestTotalMessagesAttempted:
    """Tests for MigrationState.total_messages_attempted."""

    def test_no_messages(self):
        state = MigrationState()
        assert state.total_messages_attempted == 0

    def test_only_created(self):
        state = MigrationState(
            progress=ProgressState(migration_summary=_make_summary(messages_created=5))
        )
        assert state.total_messages_attempted == 5

    def test_only_failed(self):
        state = MigrationState(
            messages=MessageState(
                failed_messages=[_make_failed("1"), _make_failed("2")]
            ),
        )
        assert state.total_messages_attempted == 2

    def test_created_plus_failed(self):
        state = MigrationState(
            progress=ProgressState(migration_summary=_make_summary(messages_created=8)),
            messages=MessageState(
                failed_messages=[_make_failed("1"), _make_failed("2")]
            ),
        )
        assert state.total_messages_attempted == 10

    def test_default_summary_zero_created(self):
        """Default MigrationSummary has messages_created=0."""
        state = MigrationState()
        assert state.total_messages_attempted == 0
