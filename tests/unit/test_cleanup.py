"""Tests for slack_migrator.core.cleanup module."""

from unittest.mock import MagicMock, patch

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError

from slack_migrator.core.cleanup import (
    _complete_import_mode_spaces,
    _complete_single_space,
    _resolve_channel_name,
    cleanup_channel_handlers,
    run_cleanup,
)
from tests.unit.conftest import _build_mock_migrator

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http_error(status, reason="error", content=b"{}"):
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=content)


def _make_cleanup_migrator(**kwargs):
    """Build a migrator mock with sensible defaults for cleanup tests."""
    m = _build_mock_migrator(**kwargs)
    # Ensure chat chain works:
    # chat.spaces().list().execute()
    # chat.spaces().get(name=...).execute()
    # chat.spaces().completeImport(name=...).execute()
    # chat.spaces().patch(...).execute()
    m.chat.spaces.return_value.list.return_value.execute.return_value = {"spaces": []}
    return m


# ===========================================================================
# cleanup_channel_handlers
# ===========================================================================


class TestCleanupChannelHandlers:
    """Tests for cleanup_channel_handlers()."""

    def test_empty_handlers(self):
        """No-op when no channel handlers exist."""
        m = _make_cleanup_migrator()
        m.state.channel_handlers = {}
        cleanup_channel_handlers(m)
        assert m.state.channel_handlers == {}

    def test_multiple_handlers_flushed_and_closed(self):
        handler1 = MagicMock()
        handler2 = MagicMock()
        m = _make_cleanup_migrator()
        m.state.channel_handlers = {"general": handler1, "random": handler2}

        cleanup_channel_handlers(m)

        handler1.flush.assert_called_once()
        handler1.close.assert_called_once()
        handler2.flush.assert_called_once()
        handler2.close.assert_called_once()
        assert m.state.channel_handlers == {}

    def test_oserror_on_close_prints_warning(self, capsys):
        handler = MagicMock()
        handler.close.side_effect = OSError("disk full")
        m = _make_cleanup_migrator()
        m.state.channel_handlers = {"broken": handler}

        cleanup_channel_handlers(m)

        out = capsys.readouterr().out
        assert "Failed to clean up log handler" in out
        assert "broken" in out
        assert m.state.channel_handlers == {}


# ===========================================================================
# run_cleanup
# ===========================================================================


class TestRunCleanup:
    """Tests for run_cleanup()."""

    def test_dry_run_skips(self):
        m = _make_cleanup_migrator(dry_run=True)
        run_cleanup(m)
        # Should not try to list spaces
        m.chat.spaces.return_value.list.assert_not_called()

    def test_clears_current_channel(self):
        m = _make_cleanup_migrator(current_channel="general")
        run_cleanup(m)
        assert m.state.current_channel is None

    def test_no_import_mode_spaces(self):
        """When no spaces are in import mode, logs and returns."""
        m = _make_cleanup_migrator()
        space = {"name": "spaces/AAA"}
        m.chat.spaces.return_value.list.return_value.execute.return_value = {
            "spaces": [space]
        }
        m.chat.spaces.return_value.get.return_value.execute.return_value = {
            "name": "spaces/AAA",
            "importMode": False,
        }
        run_cleanup(m)
        # completeImport should not be called
        m.chat.spaces.return_value.completeImport.assert_not_called()

    def test_import_mode_space_triggers_completion(self):
        m = _make_cleanup_migrator()
        space = {"name": "spaces/AAA"}
        m.chat.spaces.return_value.list.return_value.execute.return_value = {
            "spaces": [space]
        }
        m.chat.spaces.return_value.get.return_value.execute.return_value = {
            "name": "spaces/AAA",
            "importMode": True,
        }
        with patch(
            "slack_migrator.core.cleanup._complete_import_mode_spaces"
        ) as mock_complete:
            run_cleanup(m)
            mock_complete.assert_called_once()

    def test_http_error_listing_spaces(self):
        """HttpError during spaces list is caught and logged."""
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.side_effect = (
            _make_http_error(500, "Server Error")
        )
        # Should not raise
        run_cleanup(m)

    def test_refresh_error_listing_spaces(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.side_effect = RefreshError(
            "token expired"
        )
        run_cleanup(m)

    def test_transport_error_listing_spaces(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.side_effect = (
            TransportError("network down")
        )
        run_cleanup(m)

    def test_http_error_getting_space_info(self):
        """HttpError checking individual space status is caught, continues."""
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.return_value = {
            "spaces": [{"name": "spaces/AAA"}, {"name": "spaces/BBB"}]
        }
        m.chat.spaces.return_value.get.return_value.execute.side_effect = [
            _make_http_error(403, "Forbidden"),
            {"name": "spaces/BBB", "importMode": False},
        ]
        run_cleanup(m)

    def test_space_with_empty_name_skipped(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.return_value = {
            "spaces": [{"name": ""}, {"name": "spaces/BBB"}]
        }
        m.chat.spaces.return_value.get.return_value.execute.return_value = {
            "name": "spaces/BBB",
            "importMode": False,
        }
        run_cleanup(m)
        # get should only be called for spaces/BBB, not for empty name
        m.chat.spaces.return_value.get.assert_called_once()

    def test_outer_http_error_caught(self):
        """General HttpError wrapping the whole cleanup is caught."""
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.return_value = {
            "spaces": [{"name": "spaces/AAA"}]
        }
        m.chat.spaces.return_value.get.return_value.execute.return_value = {
            "name": "spaces/AAA",
            "importMode": True,
        }
        with patch(
            "slack_migrator.core.cleanup._complete_import_mode_spaces",
            side_effect=_make_http_error(429, "Rate Limit"),
        ):
            run_cleanup(m)

    def test_unexpected_error_caught(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.list.return_value.execute.return_value = {
            "spaces": [{"name": "spaces/AAA"}]
        }
        m.chat.spaces.return_value.get.return_value.execute.side_effect = RuntimeError(
            "unexpected"
        )
        # Should not propagate (caught by outer except Exception)
        run_cleanup(m)


# ===========================================================================
# _complete_import_mode_spaces
# ===========================================================================


class TestCompleteImportModeSpaces:
    """Tests for _complete_import_mode_spaces()."""

    @patch("slack_migrator.core.cleanup.tqdm", side_effect=lambda x, **kw: x)
    def test_empty_list(self, _tqdm):
        m = _make_cleanup_migrator()
        _complete_import_mode_spaces(m, [])

    @patch("slack_migrator.core.cleanup.tqdm", side_effect=lambda x, **kw: x)
    @patch("slack_migrator.core.cleanup._complete_single_space")
    def test_single_space(self, mock_complete, _tqdm):
        m = _make_cleanup_migrator()
        spaces = [("spaces/AAA", {"name": "spaces/AAA", "importMode": True})]
        _complete_import_mode_spaces(m, spaces)
        mock_complete.assert_called_once_with(m, "spaces/AAA", spaces[0][1])

    @patch("slack_migrator.core.cleanup.tqdm", side_effect=lambda x, **kw: x)
    @patch(
        "slack_migrator.core.cleanup._complete_single_space",
        side_effect=_make_http_error(500),
    )
    def test_http_error_per_space_continues(self, mock_complete, _tqdm):
        """HttpError on one space doesn't prevent processing others."""
        m = _make_cleanup_migrator()
        spaces = [
            ("spaces/AAA", {"importMode": True}),
            ("spaces/BBB", {"importMode": True}),
        ]
        _complete_import_mode_spaces(m, spaces)
        assert mock_complete.call_count == 2

    @patch("slack_migrator.core.cleanup.tqdm", side_effect=lambda x, **kw: x)
    @patch(
        "slack_migrator.core.cleanup._complete_single_space",
        side_effect=RefreshError("expired"),
    )
    def test_refresh_error_per_space_continues(self, mock_complete, _tqdm):
        m = _make_cleanup_migrator()
        spaces = [
            ("spaces/AAA", {"importMode": True}),
            ("spaces/BBB", {"importMode": True}),
        ]
        _complete_import_mode_spaces(m, spaces)
        assert mock_complete.call_count == 2


# ===========================================================================
# _complete_single_space
# ===========================================================================


class TestCompleteSingleSpace:
    """Tests for _complete_single_space()."""

    def test_success_no_external_users(self):
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {"general": "spaces/AAA"}
        m._get_all_channel_names.return_value = []
        with patch("slack_migrator.core.cleanup.add_regular_members"):
            _complete_single_space(
                m,
                "spaces/AAA",
                {"name": "spaces/AAA", "importMode": True},
            )
        m.chat.spaces.return_value.completeImport.assert_called_once()

    def test_external_user_flag_preserved(self):
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {}
        m._get_all_channel_names.return_value = []
        with patch("slack_migrator.core.cleanup.add_regular_members"):
            _complete_single_space(
                m,
                "spaces/AAA",
                {"name": "spaces/AAA", "externalUserAllowed": True},
            )
        m.chat.spaces.return_value.patch.assert_called_once()

    def test_external_user_from_state(self):
        """External user flag from state.spaces_with_external_users."""
        m = _make_cleanup_migrator()
        m.state.spaces_with_external_users = {"spaces/AAA": True}
        m.state.channel_to_space = {}
        m._get_all_channel_names.return_value = []
        with patch("slack_migrator.core.cleanup.add_regular_members"):
            _complete_single_space(
                m,
                "spaces/AAA",
                {"name": "spaces/AAA", "externalUserAllowed": False},
            )
        m.chat.spaces.return_value.patch.assert_called_once()

    def test_complete_import_http_error_returns(self):
        """HttpError on completeImport causes early return."""
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.completeImport.return_value.execute.side_effect = (
            _make_http_error(403, "Forbidden")
        )
        _complete_single_space(m, "spaces/AAA", {})
        # patch should NOT be called since completeImport failed
        m.chat.spaces.return_value.patch.assert_not_called()

    def test_complete_import_server_error(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.completeImport.return_value.execute.side_effect = (
            _make_http_error(500, "Internal Error")
        )
        _complete_single_space(m, "spaces/AAA", {})

    def test_complete_import_refresh_error_returns(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.completeImport.return_value.execute.side_effect = (
            RefreshError("expired")
        )
        _complete_single_space(m, "spaces/AAA", {})

    def test_complete_import_transport_error_returns(self):
        m = _make_cleanup_migrator()
        m.chat.spaces.return_value.completeImport.return_value.execute.side_effect = (
            TransportError("no network")
        )
        _complete_single_space(m, "spaces/AAA", {})

    def test_patch_http_error_does_not_prevent_member_add(self):
        """HttpError on patch (external users) still allows member addition."""
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {"general": "spaces/AAA"}
        m.chat.spaces.return_value.patch.return_value.execute.side_effect = (
            _make_http_error(403, "Forbidden")
        )
        with patch("slack_migrator.core.cleanup.add_regular_members") as mock_add:
            _complete_single_space(
                m,
                "spaces/AAA",
                {"externalUserAllowed": True},
            )
            mock_add.assert_called_once()

    def test_add_members_error_caught(self):
        """Error adding members is caught and logged, not re-raised."""
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {"general": "spaces/AAA"}
        with patch(
            "slack_migrator.core.cleanup.add_regular_members",
            side_effect=RuntimeError("member add failed"),
        ):
            # Should not raise
            _complete_single_space(
                m,
                "spaces/AAA",
                {"name": "spaces/AAA"},
            )

    def test_no_channel_name_skips_member_add(self):
        """If channel name can't be resolved, members are not added."""
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {}
        m._get_all_channel_names.return_value = []
        with patch("slack_migrator.core.cleanup.add_regular_members") as mock_add:
            _complete_single_space(m, "spaces/AAA", {"displayName": "unknown"})
            mock_add.assert_not_called()


# ===========================================================================
# _resolve_channel_name
# ===========================================================================


class TestResolveChannelName:
    """Tests for _resolve_channel_name()."""

    def test_exact_mapping_match(self):
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {"general": "spaces/AAA"}
        result = _resolve_channel_name(m, "spaces/AAA", {})
        assert result == "general"

    def test_display_name_parse(self):
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {}
        m._get_all_channel_names.return_value = ["general"]
        m._get_space_name.return_value = "general"
        result = _resolve_channel_name(
            m, "spaces/AAA", {"displayName": "[Slack] general"}
        )
        assert result == "general"

    def test_no_match_returns_none(self):
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {}
        m._get_all_channel_names.return_value = ["random"]
        m._get_space_name.return_value = "random"
        result = _resolve_channel_name(
            m, "spaces/AAA", {"displayName": "unknown-space"}
        )
        assert result is None

    def test_mapping_checked_before_display_name(self):
        """channel_to_space mapping takes priority over display name."""
        m = _make_cleanup_migrator()
        m.state.channel_to_space = {"general": "spaces/AAA"}
        m._get_all_channel_names.return_value = ["other"]
        m._get_space_name.return_value = "other"
        result = _resolve_channel_name(m, "spaces/AAA", {"displayName": "other"})
        assert result == "general"
