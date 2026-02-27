"""Unit tests for slack_migrator.core.cleanup module."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import httplib2
import pytest
from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError

from slack_migrator.core.cleanup import (
    _complete_import_mode_spaces,
    _complete_single_space,
    _resolve_channel_name,
    cleanup_channel_handlers,
    run_cleanup,
)
from slack_migrator.core.state import MigrationState

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
    """Build a mock Google Chat API service.

    By default, returns empty spaces list and a non-import-mode space.
    """
    chat = MagicMock()

    # spaces().list().execute()
    if list_side_effect:
        chat.spaces().list().execute.side_effect = list_side_effect
    else:
        chat.spaces().list().execute.return_value = {"spaces": spaces_list or []}

    # spaces().get(name=...).execute()
    if get_side_effect:
        chat.spaces().get.return_value.execute.side_effect = get_side_effect
    else:
        chat.spaces().get.return_value.execute.return_value = space_get or {
            "name": "spaces/abc",
            "importMode": False,
        }

    # spaces().completeImport(name=...).execute()
    if complete_import_side_effect:
        (
            chat.spaces().completeImport.return_value.execute.side_effect
        ) = complete_import_side_effect
    else:
        chat.spaces().completeImport.return_value.execute.return_value = {}

    # spaces().patch(name=..., ...).execute()
    if patch_side_effect:
        chat.spaces().patch.return_value.execute.side_effect = patch_side_effect
    else:
        chat.spaces().patch.return_value.execute.return_value = {}

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
        """Each handler is removed from the slack_migrator logger."""
        handler = MagicMock()
        fresh_state.spaces.channel_handlers = {"general": handler}
        logger = logging.getLogger("slack_migrator")

        # Ensure the handler is "in" the logger so removeHandler is meaningful
        logger.addHandler(handler)

        cleanup_channel_handlers(fresh_state)

        assert handler not in logger.handlers


# ===================================================================
# TestRunCleanup
# ===================================================================


@patch("slack_migrator.core.cleanup.tqdm", side_effect=lambda x, **kw: x)
@patch("slack_migrator.core.cleanup.add_regular_members")
class TestRunCleanup:
    """Tests for run_cleanup."""

    def test_dry_run_skips_cleanup(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """In dry_run mode, cleanup returns immediately without API calls."""
        ctx = _make_ctx(dry_run=True)
        state = MigrationState()
        state.context.current_channel = "leftover"

        run_cleanup(ctx, state, chat=MagicMock(), user_resolver=None, file_handler=None)

        assert state.context.current_channel is None
        mock_members.assert_not_called()

    def test_chat_is_none_logs_error(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """When chat is None, the RuntimeError is caught and logged."""
        ctx = _make_ctx()
        state = MigrationState()

        # Should not raise -- error is caught internally
        run_cleanup(ctx, state, chat=None, user_resolver=None, file_handler=None)

        mock_members.assert_not_called()

    def test_empty_spaces_list_logs_no_import_mode(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """When no spaces exist, logs 'no spaces in import mode'."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(spaces_list=[])

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("No spaces found in import mode" in msg for msg in log_messages)

    def test_http_error_listing_spaces_5xx(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """A 5xx HttpError listing spaces logs a server error warning and returns."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(list_side_effect=_http_error(500, "Internal Server Error"))

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("Server error listing spaces" in msg for msg in log_messages)

    def test_refresh_error_listing_spaces(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """A RefreshError listing spaces logs an error and returns."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(list_side_effect=RefreshError("token expired"))

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("Failed to list spaces" in msg for msg in log_messages)

    def test_transport_error_listing_spaces(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """A TransportError listing spaces logs an error and returns."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat(list_side_effect=TransportError("connection reset"))

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("Failed to list spaces" in msg for msg in log_messages)

    def test_space_in_import_mode_delegates(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """A space in import mode is passed to _complete_import_mode_spaces."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {"spaces": [{"name": "spaces/abc"}]}
        chat.spaces().get.return_value.execute.return_value = {
            "name": "spaces/abc",
            "displayName": "Slack #general",
            "importMode": True,
        }
        chat.spaces().completeImport.return_value.execute.return_value = {}
        chat.spaces().patch.return_value.execute.return_value = {}

        with patch(
            "slack_migrator.core.cleanup._complete_import_mode_spaces"
        ) as mock_complete:
            run_cleanup(ctx, state, chat, user_resolver=MagicMock(), file_handler=None)

            mock_complete.assert_called_once()
            args = mock_complete.call_args
            import_mode_spaces = args[0][5]  # 6th positional arg
            assert len(import_mode_spaces) == 1
            assert import_mode_spaces[0][0] == "spaces/abc"

    def test_http_error_checking_individual_space_continues(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """HttpError on one space check doesn't stop checking other spaces."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {
            "spaces": [{"name": "spaces/a"}, {"name": "spaces/b"}]
        }
        # First get() raises, second succeeds with no importMode
        chat.spaces().get.return_value.execute.side_effect = [
            _http_error(404, "Not Found"),
            {"name": "spaces/b", "importMode": False},
        ]

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any(
                "HTTP error checking space status" in msg for msg in log_messages
            )
            # Should still reach "no spaces in import mode"
            assert any("No spaces found in import mode" in msg for msg in log_messages)

    def test_outer_http_error_403_logs_permission(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """An outer 403 HttpError logs a permission warning."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {"spaces": []}

        # Force the outer try to raise
        with patch(
            "slack_migrator.core.cleanup._complete_import_mode_spaces",
            side_effect=_http_error(403, "Forbidden"),
        ):
            # Need import_mode_spaces to be non-empty so _complete is called
            chat2 = MagicMock()
            chat2.spaces().list().execute.return_value = {
                "spaces": [{"name": "spaces/x"}]
            }
            chat2.spaces().get.return_value.execute.return_value = {
                "name": "spaces/x",
                "importMode": True,
            }

            with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
                run_cleanup(ctx, state, chat2, user_resolver=None, file_handler=None)

                log_messages = [c.args[1] for c in mock_log.call_args_list]
                assert any(
                    "Permission error during cleanup" in msg for msg in log_messages
                )

    def test_outer_http_error_429_logs_rate_limit(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """An outer 429 HttpError logs a rate limit warning."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {"spaces": [{"name": "spaces/x"}]}
        chat.spaces().get.return_value.execute.return_value = {
            "name": "spaces/x",
            "importMode": True,
        }

        with patch(
            "slack_migrator.core.cleanup._complete_import_mode_spaces",
            side_effect=_http_error(429, "Too Many Requests"),
        ):
            with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
                run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

                log_messages = [c.args[1] for c in mock_log.call_args_list]
                assert any(
                    "Rate limit exceeded during cleanup" in msg for msg in log_messages
                )

    def test_outer_http_error_5xx_logs_server_error(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """An outer 5xx HttpError logs a server error warning."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {"spaces": [{"name": "spaces/x"}]}
        chat.spaces().get.return_value.execute.return_value = {
            "name": "spaces/x",
            "importMode": True,
        }

        with patch(
            "slack_migrator.core.cleanup._complete_import_mode_spaces",
            side_effect=_http_error(503, "Service Unavailable"),
        ):
            with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
                run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

                log_messages = [c.args[1] for c in mock_log.call_args_list]
                assert any("Server error during cleanup" in msg for msg in log_messages)

    def test_current_channel_cleared(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """run_cleanup always clears state.context.current_channel."""
        ctx = _make_ctx()
        state = MigrationState()
        state.context.current_channel = "leftover-channel"

        chat = _mock_chat(spaces_list=[])
        run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

        assert state.context.current_channel is None

    def test_space_without_name_skipped(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """A space dict with empty/missing name is skipped."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {"spaces": [{"name": ""}, {}]}

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("No spaces found in import mode" in msg for msg in log_messages)

    def test_unexpected_exception_caught(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """An unexpected exception in the outer try block is caught and logged."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.side_effect = ValueError("unexpected")

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("Unexpected error during cleanup" in msg for msg in log_messages)

    def test_refresh_error_checking_individual_space(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """RefreshError on individual space check logs warning and continues."""
        ctx = _make_ctx()
        state = MigrationState()

        chat = MagicMock()
        chat.spaces().list().execute.return_value = {"spaces": [{"name": "spaces/a"}]}
        chat.spaces().get.return_value.execute.side_effect = RefreshError(
            "token expired"
        )

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            run_cleanup(ctx, state, chat, user_resolver=None, file_handler=None)

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any(
                "Failed to get space info during cleanup" in msg for msg in log_messages
            )


# ===================================================================
# TestCompleteImportModeSpaces
# ===================================================================


@patch("slack_migrator.core.cleanup.tqdm", side_effect=lambda x, **kw: x)
@patch("slack_migrator.core.cleanup.add_regular_members")
class TestCompleteImportModeSpaces:
    """Tests for _complete_import_mode_spaces."""

    def test_single_space_completed(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """A single space is passed through to _complete_single_space."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat()

        import_mode_spaces = [
            ("spaces/abc", {"name": "spaces/abc", "displayName": "Slack #general"}),
        ]

        with patch("slack_migrator.core.cleanup._complete_single_space") as mock_single:
            _complete_import_mode_spaces(
                ctx, state, chat, MagicMock(), None, import_mode_spaces
            )
            mock_single.assert_called_once()

    def test_http_error_one_space_continues_to_next(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """HttpError for one space does not stop processing the next."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat()

        import_mode_spaces = [
            ("spaces/a", {"name": "spaces/a"}),
            ("spaces/b", {"name": "spaces/b"}),
        ]

        call_count = 0

        def fake_complete(*args: object, **kwargs: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise _http_error(500, "Server Error")

        with patch(
            "slack_migrator.core.cleanup._complete_single_space",
            side_effect=fake_complete,
        ):
            _complete_import_mode_spaces(
                ctx, state, chat, MagicMock(), None, import_mode_spaces
            )

        assert call_count == 2

    def test_refresh_error_one_space_continues_to_next(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """RefreshError for one space does not stop processing the next."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat()

        import_mode_spaces = [
            ("spaces/a", {"name": "spaces/a"}),
            ("spaces/b", {"name": "spaces/b"}),
        ]

        call_count = 0

        def fake_complete(*args: object, **kwargs: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RefreshError("token expired")

        with patch(
            "slack_migrator.core.cleanup._complete_single_space",
            side_effect=fake_complete,
        ):
            _complete_import_mode_spaces(
                ctx, state, chat, MagicMock(), None, import_mode_spaces
            )

        assert call_count == 2

    def test_transport_error_one_space_continues_to_next(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """TransportError for one space does not stop processing the next."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat()

        import_mode_spaces = [
            ("spaces/a", {"name": "spaces/a"}),
            ("spaces/b", {"name": "spaces/b"}),
        ]

        call_count = 0

        def fake_complete(*args: object, **kwargs: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TransportError("connection reset")

        with patch(
            "slack_migrator.core.cleanup._complete_single_space",
            side_effect=fake_complete,
        ):
            _complete_import_mode_spaces(
                ctx, state, chat, MagicMock(), None, import_mode_spaces
            )

        assert call_count == 2

    def test_logs_count_of_import_mode_spaces(
        self, mock_members: MagicMock, mock_tqdm: MagicMock
    ) -> None:
        """Logs how many spaces were found in import mode."""
        ctx = _make_ctx()
        state = MigrationState()
        chat = _mock_chat()

        import_mode_spaces = [
            ("spaces/a", {}),
            ("spaces/b", {}),
            ("spaces/c", {}),
        ]

        with (
            patch("slack_migrator.core.cleanup._complete_single_space"),
            patch("slack_migrator.core.cleanup.log_with_context") as mock_log,
        ):
            _complete_import_mode_spaces(
                ctx, state, chat, MagicMock(), None, import_mode_spaces
            )

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("3 spaces still in import mode" in msg for msg in log_messages)


# ===================================================================
# TestCompleteSingleSpace
# ===================================================================


@patch("slack_migrator.core.cleanup.add_regular_members")
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

        _complete_single_space(
            ctx, state, chat, user_resolver, file_handler, "spaces/abc", space_info
        )

        chat.spaces().completeImport.assert_called_once()
        # patch() should NOT be called since no external users
        chat.spaces().patch.assert_not_called()
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

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
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

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
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

        _complete_single_space(
            ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
        )

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

        chat.spaces().patch.assert_called_once_with(
            name="spaces/abc",
            updateMask="externalUserAllowed",
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

        chat.spaces().patch.assert_called_once_with(
            name="spaces/abc",
            updateMask="externalUserAllowed",
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

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            with patch(
                "slack_migrator.core.cleanup._resolve_channel_name",
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

        with patch("slack_migrator.core.cleanup.log_with_context") as mock_log:
            _complete_single_space(
                ctx, state, chat, MagicMock(), None, "spaces/abc", space_info
            )

            log_messages = [c.args[1] for c in mock_log.call_args_list]
            assert any("Error adding regular members" in msg for msg in log_messages)

    def test_chat_none_during_complete_import(self, mock_members: MagicMock) -> None:
        """RuntimeError is raised when chat is None during completeImport."""
        ctx = _make_ctx()
        state = MigrationState()
        space_info = {"name": "spaces/abc"}

        # chat=None should trigger RuntimeError which is NOT caught by
        # the HttpError/RefreshError/TransportError handlers, so it will
        # propagate up to the caller (_complete_import_mode_spaces)
        with pytest.raises(RuntimeError, match="Chat API service not initialized"):
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

        chat.spaces().patch.assert_not_called()
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

    def test_multiple_channels_match_returns_first(self, tmp_path: Path) -> None:
        """If display name matches multiple dirs, returns the first found."""
        state = MigrationState()
        # Create directories -- one will match
        (tmp_path / "general").mkdir()
        (tmp_path / "general-old").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Slack #general"},
        )

        # "Slack #general" is contained in display name, so "general" matches
        assert result == "general"

    def test_display_name_partial_match(self, tmp_path: Path) -> None:
        """Display name with the prefix as substring still matches."""
        state = MigrationState()
        (tmp_path / "engineering").mkdir()

        result = _resolve_channel_name(
            state,
            tmp_path,
            "spaces/xyz",
            {"displayName": "Migrated: Slack #engineering channel"},
        )

        assert result == "engineering"
