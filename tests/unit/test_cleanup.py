"""Unit tests for slack_chat_migrator.core.cleanup module."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import httplib2
import pytest
from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError

from slack_chat_migrator.core.cleanup import (
    _complete_single_space,
    _resolve_channel_name,
    cleanup_channel_handlers,
    run_cleanup,
)
from slack_chat_migrator.core.config import ImportCompletionStrategy, MigrationConfig
from slack_chat_migrator.core.state import MigrationState

from .conftest import _make_ctx

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _http_error(status: int, reason: str = "error") -> HttpError:
    """Build an HttpError with the given status code."""
    resp = httplib2.Response({"status": status})
    resp.reason = reason
    return HttpError(resp, b"error")


def _mock_chat(
    *,
    spaces_list: list[dict] | None = None,
    space_get: dict | None = None,
    list_side_effect: BaseException | None = None,
    get_side_effect: BaseException | None = None,
    complete_import_side_effect: BaseException | None = None,
    patch_side_effect: BaseException | None = None,
) -> MagicMock:
    """Build a mock ChatAdapter.

    By default, returns empty spaces list and a non-import-mode space.
    """
    chat = MagicMock()

    # chat.list_spaces()
    if list_side_effect:
        chat.list_spaces.side_effect = list_side_effect
    else:
        chat.list_spaces.return_value = {"spaces": spaces_list or []}

    # chat.get_space(name)
    if get_side_effect:
        chat.get_space.side_effect = get_side_effect
    else:
        chat.get_space.return_value = space_get or {
            "name": "spaces/abc",
            "importMode": False,
        }

    # chat.complete_import(name)
    if complete_import_side_effect:
        chat.complete_import.side_effect = complete_import_side_effect
    else:
        chat.complete_import.return_value = {}

    # chat.patch_space(name=..., update_mask=..., body=...)
    if patch_side_effect:
        chat.patch_space.side_effect = patch_side_effect
    else:
        chat.patch_space.return_value = {}

    return chat


# ===================================================================
# TestCleanupChannelHandlers
# ===================================================================


class TestCleanupChannelHandlers:
    """Tests for cleanup_channel_handlers."""

    def test_empty_handlers_is_noop(self, fresh_state: MigrationState) -> None:
        """An empty handlers dict does nothing."""
        assert fresh_state.spaces.channel_handlers == {}
        cleanup_channel_handlers(fresh_state)
        assert fresh_state.spaces.channel_handlers == {}

    def test_multiple_handlers_cleaned_up(self, fresh_state: MigrationState) -> None:
        """All handlers are flushed, closed, and removed from the logger."""
        h1 = MagicMock()
        h2 = MagicMock()
        fresh_state.spaces.channel_handlers = {"general": h1, "random": h2}

        cleanup_channel_handlers(fresh_state)

        h1.flush.assert_called_once()
        h1.close.assert_called_once()
        h2.flush.assert_called_once()
        h2.close.assert_called_once()

    def test_oserror_on_close_prints_warning_and_continues(
        self, fresh_state: MigrationState, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """An OSError during handler cleanup prints a warning but doesn't stop."""
        bad_handler = MagicMock()
        bad_handler.close.side_effect = OSError("disk full")
        good_handler = MagicMock()
        fresh_state.spaces.channel_handlers = {
            "broken": bad_handler,
            "ok": good_handler,
        }

        cleanup_channel_handlers(fresh_state)

        captured = capsys.readouterr()
        assert (
            "Warning: Failed to clean up log handler for channel broken" in captured.out
        )
        assert "disk full" in captured.out
        good_handler.flush.assert_called_once()
        good_handler.close.assert_called_once()

    def test_handlers_dict_cleared_after_cleanup(
        self, fresh_state: MigrationState
    ) -> None:
        """The handlers dict is empty after cleanup even if errors occur."""
        h1 = MagicMock()
        h1.flush.side_effect = OSError("flush fail")
        fresh_state.spaces.channel_handlers = {"ch": h1}

        cleanup_channel_handlers(fresh_state)

        assert fresh_state.spaces.channel_handlers == {}

    def test_handler_removed_from_logger(self, fresh_state: MigrationState) -> None:
        """Each handler is removed from the slack_chat_migrator logger."""
        handler = MagicMock()
        fresh_state.spaces.channel_handlers = {"general": handler}
        logger = logging.getLogger("slack_chat_migrator")

        # Ensure the handler is "in" the logger so removeHandler is meaningful
        logger.addHandler(handler)

        cleanup_channel_handlers(fresh_state)

        assert handler not in logger.handlers


# ===================================================================
# TestRunCleanup
# ===================================================================


def _state_with_spaces(**spaces: str) -> MigrationState:
    """A MigrationState whose run created the given channel -> space pairs."""
    state = MigrationState()
    state.spaces.created_spaces = dict(spaces)
    return state


def _import_mode(name: str, **extra: object) -> dict:
    return {"name": name, "displayName": "Slack #x", "importMode": True, **extra}


@patch("slack_chat_migrator.core.cleanup.add_regular_members")
class TestRunCleanup:
    """run_cleanup only completes this run's spaces (issue #85)."""

    def test_dry_run_skips_cleanup(self, mock_members: MagicMock) -> None:
        """In dry_run mode, cleanup returns immediately without API calls."""
        ctx = _make_ctx(dry_run=True)
        state = _state_with_spaces(general="spaces/a")
        state.context.current_channel = "leftover"
        chat = MagicMock()

        result = run_cleanup(
            ctx, state, chat=chat, user_resolver=None, file_handler=None
        )

        assert state.context.current_channel is None
        assert result.ok and not result.completed
        chat.get_space.assert_not_called()
        mock_members.assert_not_called()

    def test_no_created_spaces_makes_no_api_calls(
        self, mock_members: MagicMock
    ) -> None:
        chat = MagicMock()
        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            result = run_cleanup(
                _make_ctx(),
                MigrationState(),
                chat,
                user_resolver=None,
                file_handler=None,
            )
        assert chat.method_calls == []
        assert result.ok
        log_messages = [c.args[1] for c in mock_log.call_args_list]
        assert any("No spaces found in import mode" in msg for msg in log_messages)

    def test_never_lists_or_touches_other_spaces(self, mock_members: MagicMock) -> None:
        """Spaces the listing would return (other runs, other tools) are ignored."""
        chat = MagicMock()
        chat.list_spaces.return_value = {
            "spaces": [_import_mode("spaces/other"), _import_mode("spaces/perm")]
        }
        chat.get_space.side_effect = lambda name: _import_mode(name)
        state = _state_with_spaces(general="spaces/mine")

        run_cleanup(_make_ctx(), state, chat, MagicMock(), None)

        chat.list_spaces.assert_not_called()
        chat.complete_import.assert_called_once_with("spaces/mine")

    def test_completes_space_still_in_import_mode(
        self, mock_members: MagicMock
    ) -> None:
        ctx = _make_ctx()
        state = _state_with_spaces(general="spaces/a")
        chat = _mock_chat(space_get=_import_mode("spaces/a"))
        resolver = MagicMock()

        result = run_cleanup(ctx, state, chat, resolver, None)

        chat.complete_import.assert_called_once_with("spaces/a")
        mock_members.assert_called_once_with(
            ctx, state, chat, resolver, None, "spaces/a", "general"
        )
        assert result.completed == ["spaces/a"]
        assert result.ok

    def test_already_completed_space_left_alone(self, mock_members: MagicMock) -> None:
        state = _state_with_spaces(general="spaces/a")
        chat = _mock_chat(space_get={"name": "spaces/a", "importMode": False})

        result = run_cleanup(_make_ctx(), state, chat, MagicMock(), None)

        chat.complete_import.assert_not_called()
        assert result.completed == []

    def test_skip_on_error_spaces_stay_in_import_mode(
        self, mock_members: MagicMock
    ) -> None:
        """Spaces left open on purpose (incomplete_import_spaces) are not touched."""
        state = _state_with_spaces(general="spaces/a", random="spaces/b")
        state.errors.incomplete_import_spaces = [("spaces/b", "random")]
        chat = MagicMock()
        chat.get_space.side_effect = lambda name: _import_mode(name)

        result = run_cleanup(_make_ctx(), state, chat, MagicMock(), None)

        chat.complete_import.assert_called_once_with("spaces/a")
        assert [c.args[0] for c in chat.get_space.call_args_list] == ["spaces/a"]
        assert result.completed == ["spaces/a"]

    def test_errored_channels_skipped_under_skip_on_error(
        self, mock_members: MagicMock
    ) -> None:
        state = _state_with_spaces(general="spaces/a", random="spaces/b")
        state.errors.channels_with_errors = ["random"]
        chat = MagicMock()
        chat.get_space.side_effect = lambda name: _import_mode(name)

        run_cleanup(_make_ctx(), state, chat, MagicMock(), None)

        chat.complete_import.assert_called_once_with("spaces/a")

    def test_errored_channels_completed_under_force_complete(
        self, mock_members: MagicMock
    ) -> None:
        config = MigrationConfig(
            import_completion_strategy=ImportCompletionStrategy.FORCE_COMPLETE
        )
        state = _state_with_spaces(general="spaces/a", random="spaces/b")
        state.errors.channels_with_errors = ["random"]
        chat = MagicMock()
        chat.get_space.side_effect = lambda name: _import_mode(name)

        run_cleanup(_make_ctx(config=config), state, chat, MagicMock(), None)

        assert sorted(c.args[0] for c in chat.complete_import.call_args_list) == [
            "spaces/a",
            "spaces/b",
        ]

    @pytest.mark.parametrize(
        "error",
        [_http_error(403, "Forbidden"), RefreshError("expired"), TransportError("x")],
    )
    def test_status_check_failure_leaves_space_unchanged(
        self, mock_members: MagicMock, error: BaseException
    ) -> None:
        state = _state_with_spaces(general="spaces/a")
        chat = _mock_chat(get_side_effect=error)

        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            result = run_cleanup(_make_ctx(), state, chat, MagicMock(), None)

        chat.complete_import.assert_not_called()
        assert result.failed == {}
        assert len(result.errors) == 1
        warnings = [
            c.args[1] for c in mock_log.call_args_list if c.args[0] == logging.WARNING
        ]
        assert any("leaving it unchanged" in msg for msg in warnings)

    def test_completion_failure_reported(self, mock_members: MagicMock) -> None:
        state = _state_with_spaces(general="spaces/a")
        chat = _mock_chat(
            space_get=_import_mode("spaces/a"),
            complete_import_side_effect=_http_error(500, "boom"),
        )

        result = run_cleanup(_make_ctx(), state, chat, MagicMock(), None)

        assert "spaces/a" in result.failed
        assert not result.ok
        mock_members.assert_not_called()

    def test_current_channel_cleared(self, mock_members: MagicMock) -> None:
        """run_cleanup always clears state.context.current_channel."""
        state = MigrationState()
        state.context.current_channel = "leftover-channel"
        run_cleanup(
            _make_ctx(), state, _mock_chat(), user_resolver=None, file_handler=None
        )
        assert state.context.current_channel is None

    def test_unexpected_exception_caught(self, mock_members: MagicMock) -> None:
        """An unexpected exception is caught, logged and reported."""
        state = _state_with_spaces(general="spaces/a")
        chat = MagicMock()
        chat.get_space.side_effect = ValueError("unexpected")

        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            result = run_cleanup(_make_ctx(), state, chat, None, None)

        log_messages = [c.args[1] for c in mock_log.call_args_list]
        assert any("Unexpected error during cleanup" in msg for msg in log_messages)
        assert result.errors

    def test_chat_is_none_logs_error(self, mock_members: MagicMock) -> None:
        """When chat is None, the error is caught and logged."""
        state = _state_with_spaces(general="spaces/a")
        result = run_cleanup(_make_ctx(), state, None, None, None)  # type: ignore[arg-type]
        assert result.errors
        mock_members.assert_not_called()


# ===================================================================
# TestCompleteSingleSpace
# ===================================================================


@patch("slack_chat_migrator.core.cleanup.add_regular_members")
class TestCompleteSingleSpace:
    """Tests for _complete_single_space."""

    def test_success_no_external_users(self, mock_members: MagicMock) -> None:
        """Successful import completion, no external users, resolves channel, adds members."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        chat = _mock_chat()
        user_resolver = MagicMock()
        file_handler = MagicMock()
        space_info = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
            "externalUserAllowed": False,
        }

        completed = _complete_single_space(
            ctx, state, chat, user_resolver, file_handler, "spaces/abc", space_info
        )

        assert completed is True
        chat.complete_import.assert_called_once()
        # patch_space() should NOT be called since no external users
        chat.patch_space.assert_not_called()
        mock_members.assert_called_once_with(
            ctx, state, chat, user_resolver, file_handler, "spaces/abc", "general"
        )

    def test_complete_import_http_error_5xx_returns_early(
        self, mock_members: MagicMock
    ) -> None:
        """A 5xx HttpError during completeImport returns early."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(
            complete_import_side_effect=_http_error(500, "Internal Server Error")
        )
        space_info = {"name": "spaces/abc", "displayName": "Slack #general"}

        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            _complete_single_space(
                ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
            )

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("HTTP error completing import" in msg for msg in log_messages)
            assert any("Server error completing import" in msg for msg in log_messages)

        mock_members.assert_not_called()

    def test_complete_import_http_error_4xx_returns_early(
        self, mock_members: MagicMock
    ) -> None:
        """A 4xx HttpError during completeImport returns early (no server warning)."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(complete_import_side_effect=_http_error(400, "Bad Request"))
        space_info = {"name": "spaces/abc"}

        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            _complete_single_space(
                ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
            )

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("HTTP error completing import" in msg for msg in log_messages)
            # 400 is below 500, so no "Server error" message
            assert not any(
                "Server error completing import" in msg for msg in log_messages
            )

        mock_members.assert_not_called()

    def test_complete_import_refresh_error_returns_early(
        self, mock_members: MagicMock
    ) -> None:
        """A RefreshError during completeImport returns early."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(complete_import_side_effect=RefreshError("token expired"))
        space_info = {"name": "spaces/abc"}

        completed = _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        assert completed is False
        mock_members.assert_not_called()

    def test_complete_import_transport_error_returns_early(
        self, mock_members: MagicMock
    ) -> None:
        """A TransportError during completeImport returns early."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(
            complete_import_side_effect=TransportError("connection reset")
        )
        space_info = {"name": "spaces/abc"}

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        mock_members.assert_not_called()

    def test_external_users_from_space_info(self, mock_members: MagicMock) -> None:
        """When externalUserAllowed is True in space_info, patch is called."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        chat = _mock_chat()
        space_info = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
            "externalUserAllowed": True,
        }

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        chat.patch_space.assert_called_once_with(
            name="spaces/abc",
            update_mask="externalUserAllowed",
            body={"externalUserAllowed": True},
        )

    def test_external_users_from_state_tracking(self, mock_members: MagicMock) -> None:
        """External users flag from state.progress.spaces_with_external_users is used."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        state.progress.spaces_with_external_users = {"spaces/abc": True}
        chat = _mock_chat()
        space_info = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
            "externalUserAllowed": False,
        }

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        chat.patch_space.assert_called_once_with(
            name="spaces/abc",
            update_mask="externalUserAllowed",
            body={"externalUserAllowed": True},
        )

    def test_external_users_patch_http_error_continues(
        self, mock_members: MagicMock
    ) -> None:
        """HttpError patching external user access doesn't stop member addition."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        chat = _mock_chat(patch_side_effect=_http_error(500, "Internal Server Error"))
        space_info = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
            "externalUserAllowed": True,
        }

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        # Members should still be added despite the patch error
        mock_members.assert_called_once()

    def test_external_users_patch_refresh_error_continues(
        self, mock_members: MagicMock
    ) -> None:
        """RefreshError patching external user access continues to add members."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        chat = _mock_chat(patch_side_effect=RefreshError("expired"))
        space_info = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
            "externalUserAllowed": True,
        }

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        mock_members.assert_called_once()

    def test_channel_name_not_found_skips_members(
        self, mock_members: MagicMock
    ) -> None:
        """When channel name can't be resolved, logs warning and skips members."""
        ctx = _make_ctx()
        state = MigrationState()
        # No channel_to_space mapping and export_root has no matching dirs
        chat = _mock_chat()
        space_info = {"name": "spaces/abc", "displayName": "Unknown Space"}

        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            with patch(
                "slack_chat_migrator.core.cleanup._resolve_channel_name",
                return_value=None,
            ):
                _complete_single_space(
                    ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
                )

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any(
                "Could not determine channel name" in msg for msg in log_messages
            )

        mock_members.assert_not_called()

    def test_add_regular_members_exception_caught(
        self, mock_members: MagicMock
    ) -> None:
        """An exception from add_regular_members is caught and logged."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        chat = _mock_chat()
        space_info = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
        }
        mock_members.side_effect = RuntimeError("membership explosion")

        with patch("slack_chat_migrator.core.cleanup.log_with_context") as mock_log:
            _complete_single_space(
                ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
            )

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("Error adding regular members" in msg for msg in log_messages)

    def test_chat_none_during_complete_import(self, mock_members: MagicMock) -> None:
        """AttributeError is raised when chat is None during complete_import."""
        ctx = _make_ctx()
        state = MigrationState()
        space_info = {"name": "spaces/abc"}

        # chat=None will raise AttributeError on None.complete_import()
        # which is NOT caught by the HttpError/RefreshError/TransportError
        # handlers, so it propagates to the caller (_complete_import_mode_spaces)
        with pytest.raises(AttributeError):
            _complete_single_space(
                ctx, state, None, MagicMock(), None, "spaces/abc", space_info
            )

    def test_no_external_user_flag_at_all(self, mock_members: MagicMock) -> None:
        """When externalUserAllowed is absent from both space_info and state, patch is not called."""
        ctx = _make_ctx()
        state = MigrationState()
        state.spaces.channel_to_space = {"general": "spaces/abc"}
        chat = _mock_chat()
        # No externalUserAllowed key at all
        space_info = {"name": "spaces/abc", "displayName": "Slack #general"}

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

        chat.patch_space.assert_not_called()
        mock_members.assert_called_once()


# ===================================================================
# TestResolveChannelName
# ===================================================================


class TestResolveChannelName:
    """Tests for _resolve_channel_name."""

    def test_exact_match_in_channel_to_space(self) -> None:
        """Finds channel via channel_to_space mapping."""
        state = MigrationState()
        state.spaces.channel_to_space = {
            "general": "spaces/abc",
            "random": "spaces/def",
        }
        export_root = Path("/fake/export")

        result = _resolve_channel_name(
            state,
            export_root,
            "spaces/abc",
            {"displayName": "irrelevant"},
        )

        assert result == "general"

    def test_display_name_fallback(self, tmp_path: Path) -> None:
        """Falls back to display name matching against export directory names."""
        state = MigrationState()
        # No channel_to_space mapping
        # Create channel directories
        (tmp_path / "general").mkdir()
        (tmp_path / "random").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Slack #general"},
        )

        assert result == "general"

    def test_no_match_returns_none(self, tmp_path: Path) -> None:
        """Returns None when no match in mapping or display name."""
        state = MigrationState()
        (tmp_path / "general").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Some Unrelated Space"},
        )

        assert result is None

    def test_empty_mapping_and_no_display_name_match(self, tmp_path: Path) -> None:
        """Returns None with empty channel_to_space and no matching dirs."""
        state = MigrationState()
        # Empty export dir -- no channel subdirectories

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Slack #nonexistent"},
        )

        assert result is None

    def test_channel_to_space_checked_first(self, tmp_path: Path) -> None:
        """channel_to_space mapping takes priority over display name matching."""
        state = MigrationState()
        state.spaces.channel_to_space = {"mapped-channel": "spaces/abc"}
        # Also create a dir that would match display name
        (tmp_path / "general").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/abc",
            {"displayName": "Slack #general"},
        )

        # Should return from mapping, not display name
        assert result == "mapped-channel"

    def test_display_name_without_prefix_no_match(self, tmp_path: Path) -> None:
        """A display name that doesn't contain the SPACE_NAME_PREFIX won't match."""
        state = MigrationState()
        (tmp_path / "general").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "general"},  # Missing "Slack #" prefix
        )

        assert result is None

    def test_display_name_empty(self, tmp_path: Path) -> None:
        """Empty display name returns None."""
        state = MigrationState()
        (tmp_path / "general").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": ""},
        )

        assert result is None

    def test_display_name_missing_key(self, tmp_path: Path) -> None:
        """Missing displayName key in space_info returns None."""
        state = MigrationState()
        (tmp_path / "general").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {},  # No displayName at all
        )

        assert result is None

    def test_only_the_exact_channel_matches(self, tmp_path: Path) -> None:
        """Similar channel names never match each other."""
        state = MigrationState()
        (tmp_path / "general").mkdir()
        (tmp_path / "general-old").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Slack #general-old"},
        )

        assert result == "general-old"

    def test_prefix_of_another_channel_does_not_match(self, tmp_path: Path) -> None:
        """'Slack #eng-leadership' must never resolve to 'eng'."""
        state = MigrationState()
        (tmp_path / "eng").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Slack #eng-leadership"},
        )

        assert result is None

    def test_general_suffix_matches(self, tmp_path: Path) -> None:
        state = MigrationState()
        (tmp_path / "general").mkdir()

        result = _resolve_channel_name(
            state, tmp_path, "spaces/xyz", {"displayName": "Slack #general (General)"}
        )

        assert result == "general"

    def test_display_name_substring_does_not_match(self, tmp_path: Path) -> None:
        """A display name merely containing the prefix doesn't match."""
        state = MigrationState()
        (tmp_path / "engineering").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Migrated: Slack #engineering channel"},
        )

        assert result is None
