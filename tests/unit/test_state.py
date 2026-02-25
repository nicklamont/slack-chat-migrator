"""Unit tests for the MigrationState dataclass."""

import pytest

from slack_migrator.core.state import MigrationState

# ---------------------------------------------------------------------------
# __post_init__ validation
# ---------------------------------------------------------------------------


class TestPostInitValidation:
    """Tests for __post_init__ type and value validation."""

    def test_default_construction(self):
        state = MigrationState()
        assert state.channel_error_count == 0
        assert state.first_channel_processed is False
        assert state.current_channel is None

    def test_negative_channel_error_count_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            MigrationState(channel_error_count=-1)

    def test_zero_channel_error_count_ok(self):
        state = MigrationState(channel_error_count=0)
        assert state.channel_error_count == 0

    def test_positive_channel_error_count_ok(self):
        state = MigrationState(channel_error_count=5)
        assert state.channel_error_count == 5

    def test_dict_field_wrong_type_raises(self):
        with pytest.raises(TypeError, match="space_cache must be a dict"):
            MigrationState(space_cache=[])  # type: ignore[arg-type]

    def test_list_field_wrong_type_raises(self):
        with pytest.raises(TypeError, match="failed_messages must be a list"):
            MigrationState(failed_messages={})  # type: ignore[arg-type]

    def test_set_field_wrong_type_raises(self):
        with pytest.raises(TypeError, match="sent_messages must be a set"):
            MigrationState(sent_messages=[])  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "field_name",
        [
            "created_spaces",
            "channel_to_space",
            "channel_id_to_space_id",
            "channel_handlers",
            "thread_map",
            "message_id_map",
            "failed_messages_by_channel",
            "drive_files_cache",
            "chat_delegates",
            "valid_users",
            "migration_summary",
            "last_processed_timestamps",
            "channel_stats",
            "spaces_with_external_users",
            "active_users_by_channel",
            "high_failure_rate_channels",
            "migration_issues",
        ],
    )
    def test_all_dict_fields_reject_non_dict(self, field_name):
        with pytest.raises(TypeError, match=f"{field_name} must be a dict"):
            MigrationState(**{field_name: "not a dict"})  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "field_name",
        [
            "failed_messages",
            "skipped_reactions",
            "incomplete_import_spaces",
            "migration_errors",
            "channels_with_errors",
        ],
    )
    def test_all_list_fields_reject_non_list(self, field_name):
        with pytest.raises(TypeError, match=f"{field_name} must be a list"):
            MigrationState(**{field_name: "not a list"})  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "field_name",
        ["sent_messages", "external_users", "channel_conflicts"],
    )
    def test_all_set_fields_reject_non_set(self, field_name):
        with pytest.raises(TypeError, match=f"{field_name} must be a set"):
            MigrationState(**{field_name: "not a set"})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# reset_for_run
# ---------------------------------------------------------------------------


class TestResetForRun:
    """Tests for MigrationState.reset_for_run."""

    def test_resets_channel_handlers(self):
        state = MigrationState(channel_handlers={"ch1": "handler"})
        state.reset_for_run()
        assert state.channel_handlers == {}

    def test_resets_thread_map(self):
        state = MigrationState(thread_map={"ts1": "thread1"})
        state.reset_for_run()
        assert state.thread_map == {}

    def test_resets_migration_summary_to_initial_structure(self):
        state = MigrationState(
            migration_summary={
                "channels_processed": ["a", "b"],
                "spaces_created": 5,
                "messages_created": 100,
                "reactions_created": 20,
                "files_created": 10,
            }
        )
        state.reset_for_run()
        assert state.migration_summary == {
            "channels_processed": [],
            "spaces_created": 0,
            "messages_created": 0,
            "reactions_created": 0,
            "files_created": 0,
        }

    def test_resets_migration_errors(self):
        state = MigrationState(migration_errors=["error1"])
        state.reset_for_run()
        assert state.migration_errors == []

    def test_resets_channels_with_errors(self):
        state = MigrationState(channels_with_errors=["ch1"])
        state.reset_for_run()
        assert state.channels_with_errors == []

    def test_resets_channel_error_count(self):
        state = MigrationState(channel_error_count=3)
        state.reset_for_run()
        assert state.channel_error_count == 0

    def test_resets_first_channel_processed(self):
        state = MigrationState(first_channel_processed=True)
        state.reset_for_run()
        assert state.first_channel_processed is False

    def test_does_not_reset_space_cache(self):
        """Space cache should persist across runs (it is not per-run state)."""
        state = MigrationState(space_cache={"s1": "v1"})
        state.reset_for_run()
        assert state.space_cache == {"s1": "v1"}

    def test_does_not_reset_sent_messages(self):
        """Sent messages should persist across runs for deduplication."""
        state = MigrationState(sent_messages={"msg1"})
        state.reset_for_run()
        assert state.sent_messages == {"msg1"}

    def test_complete_reset(self):
        """Verify all expected fields are reset when starting from dirty state."""
        state = MigrationState(
            channel_handlers={"h": "v"},
            thread_map={"t": "v"},
            migration_summary={"channels_processed": ["x"], "spaces_created": 1},
            migration_errors=["err"],
            channels_with_errors=["ch"],
            channel_error_count=2,
            first_channel_processed=True,
        )
        state.reset_for_run()
        assert state.channel_handlers == {}
        assert state.thread_map == {}
        assert state.migration_summary["channels_processed"] == []
        assert state.migration_summary["spaces_created"] == 0
        assert state.migration_errors == []
        assert state.channels_with_errors == []
        assert state.channel_error_count == 0
        assert state.first_channel_processed is False


# ---------------------------------------------------------------------------
# has_errors property
# ---------------------------------------------------------------------------


class TestHasErrors:
    """Tests for MigrationState.has_errors."""

    def test_no_errors(self):
        state = MigrationState()
        assert state.has_errors is False

    def test_migration_errors_present(self):
        state = MigrationState(migration_errors=["some error"])
        assert state.has_errors is True

    def test_channels_with_errors_present(self):
        state = MigrationState(channels_with_errors=["ch1"])
        assert state.has_errors is True

    def test_both_error_types_present(self):
        state = MigrationState(
            migration_errors=["err"],
            channels_with_errors=["ch"],
        )
        assert state.has_errors is True

    def test_empty_error_lists(self):
        state = MigrationState(migration_errors=[], channels_with_errors=[])
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
            migration_summary={"messages_created": 10},
            failed_messages=[],
        )
        assert state.success_rate == 100.0

    def test_all_messages_failed(self):
        state = MigrationState(
            migration_summary={"messages_created": 0},
            failed_messages=[{"ts": "1"}, {"ts": "2"}],
        )
        assert state.success_rate == 0.0

    def test_partial_failure(self):
        state = MigrationState(
            migration_summary={"messages_created": 7},
            failed_messages=[{"ts": "1"}, {"ts": "2"}, {"ts": "3"}],
        )
        # 7 / 10 = 70%
        assert state.success_rate == pytest.approx(70.0)

    def test_one_success_one_failure(self):
        state = MigrationState(
            migration_summary={"messages_created": 1},
            failed_messages=[{"ts": "x"}],
        )
        assert state.success_rate == pytest.approx(50.0)

    def test_missing_messages_created_key(self):
        """If messages_created is not in the summary, it defaults to 0."""
        state = MigrationState(
            migration_summary={},
            failed_messages=[{"ts": "1"}],
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
        state = MigrationState(migration_summary={"messages_created": 5})
        assert state.total_messages_attempted == 5

    def test_only_failed(self):
        state = MigrationState(
            failed_messages=[{"ts": "1"}, {"ts": "2"}],
        )
        assert state.total_messages_attempted == 2

    def test_created_plus_failed(self):
        state = MigrationState(
            migration_summary={"messages_created": 8},
            failed_messages=[{"ts": "1"}, {"ts": "2"}],
        )
        assert state.total_messages_attempted == 10

    def test_missing_messages_created_key(self):
        state = MigrationState(migration_summary={})
        assert state.total_messages_attempted == 0
