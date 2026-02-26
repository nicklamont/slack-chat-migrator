"""Unit tests for the channel processor module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from slack_migrator.core.channel_processor import ChannelProcessor
from slack_migrator.core.config import ImportCompletionStrategy, MigrationConfig
from slack_migrator.core.context import MigrationContext
from slack_migrator.core.state import MigrationState, _default_migration_summary

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ctx(
    *,
    dry_run: bool = False,
    update_mode: bool = False,
    export_root: Path | None = None,
    config: MigrationConfig | None = None,
) -> MigrationContext:
    """Build a MigrationContext with sensible test defaults."""
    return MigrationContext(
        export_root=export_root or Path("/tmp/test_export"),
        creds_path="/fake/creds.json",
        workspace_admin="admin@example.com",
        workspace_domain="example.com",
        dry_run=dry_run,
        update_mode=update_mode,
        verbose=False,
        debug_api=False,
        config=config or MigrationConfig(),
        user_map={},
        users_without_email=[],
        channels_meta={},
        channel_id_to_name={},
        channel_name_to_id={},
    )


def _make_processor(
    dry_run: bool = False,
    update_mode: bool = False,
    abort_on_error: bool = False,
    cleanup_on_error: bool = False,
    import_completion_strategy: ImportCompletionStrategy = ImportCompletionStrategy.SKIP_ON_ERROR,
    max_failure_percentage: int = 10,
    export_root: Path | None = None,
) -> ChannelProcessor:
    """Create a ChannelProcessor with sensible test defaults."""
    config = MigrationConfig(
        abort_on_error=abort_on_error,
        cleanup_on_error=cleanup_on_error,
        import_completion_strategy=import_completion_strategy,
        max_failure_percentage=max_failure_percentage,
    )
    ctx = _make_ctx(
        dry_run=dry_run,
        update_mode=update_mode,
        export_root=export_root,
        config=config,
    )

    state = MigrationState()
    state.current_channel = "general"
    state.current_space = None
    state.migration_summary = _default_migration_summary()
    state.output_dir = Path("/tmp/test_output")

    return ChannelProcessor(
        ctx=ctx,
        state=state,
        chat=MagicMock(),
        user_resolver=MagicMock(),
        file_handler=MagicMock(),
        attachment_processor=MagicMock(),
    )


# ---------------------------------------------------------------------------
# process_channel
# ---------------------------------------------------------------------------
class TestProcessChannel:
    """Tests for ChannelProcessor.process_channel()."""

    @patch("slack_migrator.core.channel_processor.track_message_stats")
    @patch(
        "slack_migrator.core.channel_processor.send_message",
        return_value="spaces/SPACE1/messages/MSG1",
    )
    @patch("slack_migrator.core.channel_processor.add_users_to_space")
    @patch("slack_migrator.core.channel_processor.add_regular_members")
    @patch(
        "slack_migrator.core.channel_processor.create_space",
        return_value="spaces/SPACE1",
    )
    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=True,
    )
    def test_happy_path_returns_false(
        self,
        mock_should,
        mock_create,
        mock_add_reg,
        mock_add_hist,
        mock_send,
        mock_track,
        tmp_path,
    ):
        """A successful channel process returns False (do not abort)."""
        processor = _make_processor(export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        # Write a simple message file
        (ch_dir / "2024-01-01.json").write_text(
            json.dumps([{"type": "message", "ts": "1000.0", "text": "hello"}])
        )

        with (
            patch.object(processor, "_setup_channel_logging"),
            patch.object(processor, "_discover_channel_resources"),
        ):
            result = processor.process_channel(ch_dir)

        assert result is False
        assert "general" in processor.state.migration_summary["channels_processed"]
        assert processor.state.channel_to_space["general"] == "spaces/SPACE1"

    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=False,
    )
    def test_channel_skipped_by_config(self, mock_should, tmp_path):
        """Channel filtered by config returns False without processing."""
        processor = _make_processor()
        ch_dir = tmp_path / "random"
        ch_dir.mkdir()

        result = processor.process_channel(ch_dir)

        assert result is False
        mock_should.assert_called_once_with("random", processor.ctx.config)

    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=True,
    )
    def test_channel_with_space_conflict(self, mock_should, tmp_path):
        """Channel with unresolved space conflict is skipped."""
        processor = _make_processor()
        processor.state.channel_conflicts = {"general": ["spaces/A", "spaces/B"]}

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        result = processor.process_channel(ch_dir)

        assert result is False
        assert "general" in processor.state.migration_issues

    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=True,
    )
    @patch(
        "slack_migrator.core.channel_processor.create_space",
        return_value="ERROR_NO_PERMISSION_general",
    )
    def test_permission_error_on_space_creation(
        self, mock_create, mock_should, tmp_path
    ):
        """Permission error on space creation skips the channel."""
        processor = _make_processor(export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        with patch.object(processor, "_setup_channel_logging"):
            result = processor.process_channel(ch_dir)

        assert result is False

    @patch("slack_migrator.core.channel_processor.add_users_to_space")
    @patch("slack_migrator.core.channel_processor.add_regular_members")
    @patch(
        "slack_migrator.core.channel_processor.create_space",
        return_value="spaces/SPACE1",
    )
    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=True,
    )
    def test_abort_on_error_returns_true(
        self, mock_should, mock_create, mock_add_reg, mock_add_hist, tmp_path
    ):
        """When abort_on_error is True and there are failures, returns True."""
        processor = _make_processor(abort_on_error=True, export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(
            json.dumps([{"type": "message", "ts": "1000.0", "text": "hello"}])
        )

        # Simulate message failure by making _process_messages return failures
        with (
            patch.object(processor, "_setup_channel_logging"),
            patch.object(processor, "_process_messages", return_value=(1, 5, True)),
            patch.object(processor, "_complete_import_mode", return_value=True),
            patch.object(processor, "_add_members", return_value=True),
            patch.object(processor, "_should_abort_import", return_value=True),
        ):
            result = processor.process_channel(ch_dir)

        assert result is True

    @patch("slack_migrator.core.channel_processor.add_users_to_space")
    @patch("slack_migrator.core.channel_processor.add_regular_members")
    @patch(
        "slack_migrator.core.channel_processor.create_space",
        return_value="spaces/SPACE1",
    )
    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=True,
    )
    def test_delete_space_if_errors_and_cleanup_enabled(
        self, mock_should, mock_create, mock_add_reg, mock_add_hist, tmp_path
    ):
        """When channel_had_errors and cleanup_on_error, _delete_space_if_errors is called."""
        processor = _make_processor(cleanup_on_error=True, export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(json.dumps([]))

        with (
            patch.object(processor, "_setup_channel_logging"),
            patch.object(processor, "_process_messages", return_value=(5, 2, True)),
            patch.object(processor, "_complete_import_mode", return_value=True),
            patch.object(processor, "_add_members", return_value=True),
            patch.object(processor, "_should_abort_import", return_value=False),
            patch.object(processor, "_delete_space_if_errors") as mock_delete,
        ):
            processor.process_channel(ch_dir)

        mock_delete.assert_called_once_with("spaces/SPACE1", "general")

    @patch(
        "slack_migrator.core.channel_processor.should_process_channel",
        return_value=True,
    )
    @patch(
        "slack_migrator.core.channel_processor.create_space",
        return_value="spaces/DRY",
    )
    def test_dry_run_mode(self, mock_create, mock_should, tmp_path):
        """Dry run sets mode_prefix and does not delete space on errors."""
        processor = _make_processor(dry_run=True, export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(
            json.dumps([{"type": "message", "ts": "1.0", "text": "hi"}])
        )

        with (
            patch.object(processor, "_setup_channel_logging"),
            patch.object(processor, "_process_messages", return_value=(0, 0, True)),
            patch.object(processor, "_complete_import_mode", return_value=True),
            patch.object(processor, "_add_members", return_value=True),
            patch.object(processor, "_should_abort_import", return_value=False),
            patch.object(processor, "_delete_space_if_errors") as mock_delete,
        ):
            result = processor.process_channel(ch_dir)

        assert result is False
        # In dry run mode, _delete_space_if_errors should NOT be called
        mock_delete.assert_not_called()


# ---------------------------------------------------------------------------
# _setup_channel_logging
# ---------------------------------------------------------------------------
class TestSetupChannelLogging:
    """Tests for ChannelProcessor._setup_channel_logging()."""

    def test_creates_handler_and_stores_it(self):
        """Should create a channel handler via setup_channel_logger and store it."""
        processor = _make_processor()

        with patch(
            "slack_migrator.core.channel_processor.setup_channel_logger",
            return_value=MagicMock(),
        ) as mock_setup:
            with patch(
                "slack_migrator.core.channel_processor.is_debug_api_enabled",
                return_value=False,
            ):
                processor._setup_channel_logging("general")

        mock_setup.assert_called_once_with(
            processor.state.output_dir, "general", False, False
        )
        assert "general" in processor.state.channel_handlers


# ---------------------------------------------------------------------------
# _create_or_reuse_space
# ---------------------------------------------------------------------------
class TestCreateOrReuseSpace:
    """Tests for ChannelProcessor._create_or_reuse_space()."""

    @patch(
        "slack_migrator.core.channel_processor.create_space",
        return_value="spaces/NEW1",
    )
    def test_new_space_creation(self, mock_create, tmp_path):
        """Creates a new space when not in update mode."""
        processor = _make_processor()
        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        space, is_new = processor._create_or_reuse_space(ch_dir)

        assert space == "spaces/NEW1"
        assert is_new is True
        mock_create.assert_called_once_with(
            processor.ctx,
            processor.state,
            processor.chat,
            processor.user_resolver,
            "general",
        )
        assert processor.state.space_cache["general"] == "spaces/NEW1"

    def test_update_mode_reuses_existing_space(self, tmp_path):
        """In update mode with an existing space, reuses it."""
        processor = _make_processor(update_mode=True)
        processor.state.created_spaces["general"] = "spaces/EXISTING"

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        space, is_new = processor._create_or_reuse_space(ch_dir)

        assert space == "spaces/EXISTING"
        assert is_new is False
        assert processor.state.space_cache["general"] == "spaces/EXISTING"

    @patch("slack_migrator.core.channel_processor.create_space")
    def test_space_from_cache(self, mock_create, tmp_path):
        """Uses cached space if available instead of creating a new one."""
        processor = _make_processor()
        processor.state.space_cache["general"] = "spaces/CACHED"

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        space, is_new = processor._create_or_reuse_space(ch_dir)

        assert space == "spaces/CACHED"
        assert is_new is True
        mock_create.assert_not_called()


# ---------------------------------------------------------------------------
# _process_messages
# ---------------------------------------------------------------------------
class TestProcessMessages:
    """Tests for ChannelProcessor._process_messages()."""

    @patch(
        "slack_migrator.core.channel_processor.send_message",
        return_value="spaces/S/messages/M1",
    )
    @patch("slack_migrator.core.channel_processor.track_message_stats")
    def test_happy_path_with_messages(self, mock_track, mock_send, tmp_path):
        """Processes messages successfully and returns counts."""
        processor = _make_processor(export_root=tmp_path)
        processor.state.channel_to_space = {"general": "spaces/S1"}

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(
            json.dumps(
                [
                    {"type": "message", "ts": "100.0", "text": "hello"},
                    {"type": "message", "ts": "200.0", "text": "world"},
                ]
            )
        )

        with patch.object(processor, "_discover_channel_resources"):
            processed, failed, had_errors = processor._process_messages(
                ch_dir, "spaces/S1", False
            )

        assert processed == 2
        assert failed == 0
        assert had_errors is False
        assert mock_send.call_count == 2

    @patch("slack_migrator.core.channel_processor.track_message_stats")
    def test_message_loading_failure_bad_json(self, mock_track, tmp_path):
        """Bad JSON files are skipped with a warning; valid files still process."""
        processor = _make_processor(dry_run=True, export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "bad.json").write_text("{not valid json")
        (ch_dir / "good.json").write_text(
            json.dumps([{"type": "message", "ts": "1.0", "text": "ok"}])
        )

        _processed, _failed, _had_errors = processor._process_messages(
            ch_dir, "spaces/S1", False
        )

        # In dry run, we just count messages
        assert processor.state.migration_summary["messages_created"] == 1

    @patch(
        "slack_migrator.core.channel_processor.send_message",
        return_value="spaces/S/messages/M1",
    )
    @patch("slack_migrator.core.channel_processor.track_message_stats")
    def test_duplicate_message_deduplication(self, mock_track, mock_send, tmp_path):
        """Duplicate timestamps are deduplicated, only unique messages sent."""
        processor = _make_processor(export_root=tmp_path)
        processor.state.channel_to_space = {"general": "spaces/S1"}

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(
            json.dumps(
                [
                    {"type": "message", "ts": "100.0", "text": "first"},
                    {"type": "message", "ts": "100.0", "text": "duplicate"},
                    {"type": "message", "ts": "200.0", "text": "second"},
                ]
            )
        )

        with patch.object(processor, "_discover_channel_resources"):
            processed, _failed, _had_errors = processor._process_messages(
                ch_dir, "spaces/S1", False
            )

        assert processed == 2
        assert mock_send.call_count == 2

    @patch("slack_migrator.core.channel_processor.track_message_stats")
    def test_dry_run_counts_only(self, mock_track, tmp_path):
        """Dry run mode counts messages but does not send them."""
        processor = _make_processor(dry_run=True, export_root=tmp_path)

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(
            json.dumps(
                [
                    {"type": "message", "ts": "100.0", "text": "a"},
                    {"type": "message", "ts": "200.0", "text": "b"},
                    {"type": "message", "ts": "300.0", "text": "c"},
                ]
            )
        )

        processed, failed, _had_errors = processor._process_messages(
            ch_dir, "spaces/S1", False
        )

        assert processor.state.migration_summary["messages_created"] == 3
        assert processed == 0
        assert failed == 0

    @patch("slack_migrator.core.channel_processor.send_message")
    @patch("slack_migrator.core.channel_processor.track_message_stats")
    def test_failure_threshold_exceeded(self, mock_track, mock_send, tmp_path):
        """When failure rate exceeds threshold, channel is flagged."""
        processor = _make_processor(max_failure_percentage=10, export_root=tmp_path)
        processor.state.channel_to_space = {"general": "spaces/S1"}

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        # Create enough messages to trigger the threshold check.
        # We need at least 1 success so processed_count > 0.
        messages = []
        # 1 success + many failures => high failure rate
        messages.append({"type": "message", "ts": "1.0", "text": "ok"})
        for i in range(10):
            messages.append({"type": "message", "ts": f"{i + 2}.0", "text": "fail"})

        (ch_dir / "2024-01-01.json").write_text(json.dumps(messages))

        # First call succeeds, rest fail (return None)
        mock_send.side_effect = ["spaces/S/messages/M1"] + [None] * 10

        with patch.object(processor, "_discover_channel_resources"):
            _processed, _failed, had_errors = processor._process_messages(
                ch_dir, "spaces/S1", False
            )

        assert had_errors is True
        assert "general" in processor.state.high_failure_rate_channels


# ---------------------------------------------------------------------------
# _complete_import_mode
# ---------------------------------------------------------------------------
class TestCompleteImportMode:
    """Tests for ChannelProcessor._complete_import_mode()."""

    def test_success(self):
        """Successfully completes import mode."""
        processor = _make_processor()
        (
            processor.chat.spaces.return_value.completeImport.return_value.execute.return_value
        ) = {}

        result = processor._complete_import_mode("spaces/S1", "general", False)

        assert result is False
        processor.chat.spaces.return_value.completeImport.assert_called_once_with(
            name="spaces/S1"
        )

    def test_api_error(self):
        """API error during completion sets channel_had_errors to True."""
        processor = _make_processor()
        processor.chat.spaces.return_value.completeImport.return_value.execute.side_effect = RefreshError(
            "token expired"
        )

        result = processor._complete_import_mode("spaces/S1", "general", False)

        assert result is True
        assert ("spaces/S1", "general") in processor.state.incomplete_import_spaces

    def test_http_error(self):
        """HttpError during completion sets channel_had_errors to True."""
        processor = _make_processor()
        http_error = HttpError(resp=MagicMock(status=403), content=b"Forbidden")
        processor.chat.spaces.return_value.completeImport.return_value.execute.side_effect = http_error

        result = processor._complete_import_mode("spaces/S1", "general", False)

        assert result is True
        assert ("spaces/S1", "general") in processor.state.incomplete_import_spaces

    def test_skip_on_error_strategy(self):
        """With skip_on_error strategy and errors, skips completion."""
        processor = _make_processor(
            import_completion_strategy=ImportCompletionStrategy.SKIP_ON_ERROR,
        )

        result = processor._complete_import_mode("spaces/S1", "general", True)

        # Should not attempt to call completeImport
        processor.chat.spaces.return_value.completeImport.assert_not_called()
        assert result is True
        assert ("spaces/S1", "general") in processor.state.incomplete_import_spaces

    def test_force_complete_despite_errors(self):
        """With force_complete strategy, completes even when there are errors."""
        processor = _make_processor(
            import_completion_strategy=ImportCompletionStrategy.FORCE_COMPLETE,
        )
        (
            processor.chat.spaces.return_value.completeImport.return_value.execute.return_value
        ) = {}

        result = processor._complete_import_mode("spaces/S1", "general", True)

        processor.chat.spaces.return_value.completeImport.assert_called_once_with(
            name="spaces/S1"
        )
        # channel_had_errors was True going in, but completion succeeded
        # The method returns the (potentially unchanged) value
        assert result is True  # still True because it was True going in

    def test_dry_run_skips_completion(self):
        """Dry run mode does not call completeImport."""
        processor = _make_processor(dry_run=True)

        result = processor._complete_import_mode("spaces/S1", "general", False)

        processor.chat.spaces.return_value.completeImport.assert_not_called()
        assert result is False


# ---------------------------------------------------------------------------
# _add_members
# ---------------------------------------------------------------------------
class TestAddMembers:
    """Tests for ChannelProcessor._add_members()."""

    @patch("slack_migrator.core.channel_processor.add_regular_members")
    def test_success_for_new_space(self, mock_add):
        """Adds members to a newly created space without errors."""
        processor = _make_processor()

        result = processor._add_members("spaces/S1", "general", True, False)

        assert result is False
        mock_add.assert_called_once_with(
            processor.ctx,
            processor.state,
            processor.chat,
            processor.user_resolver,
            processor.file_handler,
            "spaces/S1",
            "general",
        )

    @patch("slack_migrator.core.channel_processor.add_regular_members")
    def test_success_for_existing_space(self, mock_add):
        """Updates members in an existing space (not newly created)."""
        processor = _make_processor()

        result = processor._add_members("spaces/S1", "general", False, False)

        assert result is False
        mock_add.assert_called_once_with(
            processor.ctx,
            processor.state,
            processor.chat,
            processor.user_resolver,
            processor.file_handler,
            "spaces/S1",
            "general",
        )

    @patch("slack_migrator.core.channel_processor.add_regular_members")
    def test_existing_space_adds_members_even_with_errors(self, mock_add):
        """For existing spaces, members are updated even if channel had errors."""
        processor = _make_processor()

        result = processor._add_members("spaces/S1", "general", False, True)

        # is_newly_created=False, so condition `not is_newly_created` is True
        assert result is True  # channel_had_errors passed through
        mock_add.assert_called_once()

    @patch("slack_migrator.core.channel_processor.add_regular_members")
    def test_api_error_http_error(self, mock_add):
        """HttpError during member addition sets channel_had_errors."""
        processor = _make_processor()
        mock_add.side_effect = HttpError(
            resp=MagicMock(status=403), content=b"Forbidden"
        )

        result = processor._add_members("spaces/S1", "general", True, False)

        assert result is True

    @patch("slack_migrator.core.channel_processor.add_regular_members")
    def test_unexpected_error_broad_catch(self, mock_add):
        """Unexpected errors are caught by the broad except clause."""
        processor = _make_processor()
        mock_add.side_effect = RuntimeError("unexpected failure")

        result = processor._add_members("spaces/S1", "general", True, False)

        assert result is True

    @patch("slack_migrator.core.channel_processor.add_regular_members")
    def test_skip_for_new_space_with_errors(self, mock_add):
        """Skips member addition for a newly created space that had import errors."""
        processor = _make_processor()

        result = processor._add_members("spaces/S1", "general", True, True)

        # channel_had_errors=True AND is_newly_created=True => skip
        mock_add.assert_not_called()
        assert result is True


# ---------------------------------------------------------------------------
# _should_abort_import
# ---------------------------------------------------------------------------
class TestShouldAbortImport:
    """Tests for ChannelProcessor._should_abort_import()."""

    def test_no_failures_returns_false(self):
        """No failures means no abort."""
        processor = _make_processor()

        assert processor._should_abort_import("general", 10, 0) is False

    def test_failures_with_abort_on_error_true(self):
        """With failures and abort_on_error=True, returns True."""
        processor = _make_processor(abort_on_error=True)

        assert processor._should_abort_import("general", 5, 3) is True

    def test_failures_with_abort_on_error_false(self):
        """With failures but abort_on_error=False, returns False."""
        processor = _make_processor(abort_on_error=False)

        assert processor._should_abort_import("general", 5, 3) is False

    def test_dry_run_always_returns_false(self):
        """Dry run mode never aborts, even with failures and abort_on_error."""
        processor = _make_processor(dry_run=True, abort_on_error=True)

        assert processor._should_abort_import("general", 5, 3) is False


# ---------------------------------------------------------------------------
# _delete_space_if_errors
# ---------------------------------------------------------------------------
class TestDeleteSpaceIfErrors:
    """Tests for ChannelProcessor._delete_space_if_errors()."""

    def test_cleanup_enabled_deletes_space(self):
        """When cleanup_on_error is True, deletes the space and updates tracking."""
        processor = _make_processor(cleanup_on_error=True)
        processor.state.created_spaces["general"] = "spaces/S1"
        processor.state.migration_summary["spaces_created"] = 1
        processor.chat.spaces.return_value.delete.return_value.execute.return_value = {}

        processor._delete_space_if_errors("spaces/S1", "general")

        processor.chat.spaces.return_value.delete.assert_called_once_with(
            name="spaces/S1"
        )
        assert "general" not in processor.state.created_spaces
        assert processor.state.migration_summary["spaces_created"] == 0

    def test_cleanup_disabled_skips(self):
        """When cleanup_on_error is False, does not delete the space."""
        processor = _make_processor(cleanup_on_error=False)

        processor._delete_space_if_errors("spaces/S1", "general")

        processor.chat.spaces.return_value.delete.assert_not_called()

    def test_api_error_during_delete(self):
        """API error during space deletion is caught and logged."""
        processor = _make_processor(cleanup_on_error=True)
        processor.state.created_spaces["general"] = "spaces/S1"
        processor.state.migration_summary["spaces_created"] = 1
        processor.chat.spaces.return_value.delete.return_value.execute.side_effect = (
            HttpError(resp=MagicMock(status=404), content=b"Not found")
        )

        # Should not raise
        processor._delete_space_if_errors("spaces/S1", "general")

        # Space was NOT removed from created_spaces because delete failed
        assert "general" in processor.state.created_spaces
        assert processor.state.migration_summary["spaces_created"] == 1


# ---------------------------------------------------------------------------
# _discover_channel_resources
# ---------------------------------------------------------------------------
class TestDiscoverChannelResources:
    """Tests for ChannelProcessor._discover_channel_resources()."""

    @patch(
        "slack_migrator.core.channel_processor.get_last_message_timestamp",
        return_value=12345.0,
    )
    def test_found_last_timestamp(self, mock_get_ts):
        """When a last timestamp is found, stores it and initializes thread_map."""
        processor = _make_processor()
        processor.state.channel_to_space = {"general": "spaces/S1"}

        processor._discover_channel_resources("general")

        mock_get_ts.assert_called_once_with(processor.chat, "general", "spaces/S1")
        assert processor.state.last_processed_timestamps["general"] == 12345.0

    def test_no_space_found_for_channel(self):
        """When no space mapping exists, returns early without calling API."""
        processor = _make_processor()
        processor.state.channel_to_space = {}

        # Should not raise
        processor._discover_channel_resources("general")

        assert "general" not in processor.state.last_processed_timestamps

    @patch(
        "slack_migrator.core.channel_processor.get_last_message_timestamp",
        return_value=0,
    )
    def test_no_messages_found_timestamp_zero(self, mock_get_ts):
        """When no messages found (timestamp=0), does not store a timestamp."""
        processor = _make_processor()
        processor.state.channel_to_space = {"general": "spaces/S1"}

        processor._discover_channel_resources("general")

        assert "general" not in processor.state.last_processed_timestamps

    @patch(
        "slack_migrator.core.channel_processor.get_last_message_timestamp",
        return_value=999.0,
    )
    def test_initializes_thread_map_when_missing(self, mock_get_ts):
        """When thread_map doesn't exist, it gets initialized to an empty dict."""
        processor = _make_processor()
        processor.state.channel_to_space = {"general": "spaces/S1"}
        # thread_map defaults to empty dict in MigrationState

        processor._discover_channel_resources("general")

        assert processor.state.thread_map == {}
