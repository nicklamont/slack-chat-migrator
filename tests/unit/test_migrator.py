"""Unit tests for the migrator module."""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.migrator import (
    SlackToChatMigrator,
    _list_all_spaces,
    cleanup_import_mode_spaces,
)


def _setup_export(tmp_path, users=None, channels=None):
    """Set up a minimal export directory structure for testing."""
    users = users or [
        {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}}
    ]
    channels = channels or [{"id": "C001", "name": "general", "members": ["U001"]}]

    (tmp_path / "users.json").write_text(json.dumps(users))
    (tmp_path / "channels.json").write_text(json.dumps(channels))

    # Create at least one channel directory
    for ch in channels:
        ch_dir = tmp_path / ch["name"]
        ch_dir.mkdir(exist_ok=True)


def _make_migrator(tmp_path, domain="example.com", users=None, channels=None, **kwargs):
    """Create a migrator instance with a minimal export directory."""
    _setup_export(tmp_path, users=users, channels=channels)
    return SlackToChatMigrator(
        creds_path="fake_creds.json",
        export_path=str(tmp_path),
        workspace_admin=f"admin@{domain}",
        config_path=str(tmp_path / "config.yaml"),
        dry_run=kwargs.get("dry_run", True),
        verbose=kwargs.get("verbose", False),
        update_mode=kwargs.get("update_mode", False),
        debug_api=kwargs.get("debug_api", False),
    )


# ---------------------------------------------------------------------------
# __init__ tests
# ---------------------------------------------------------------------------


class TestEmailValidation:
    """Tests for workspace_admin email validation in __init__."""

    def test_valid_email(self, tmp_path):
        _setup_export(tmp_path)
        # Should not raise
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@example.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.workspace_admin == "admin@example.com"

    def test_invalid_email_no_at(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="not-an-email",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_invalid_email_empty(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_invalid_email_at_only(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="@",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_whitespace_stripped(self, tmp_path):
        _setup_export(tmp_path)
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="  admin@example.com  ",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.workspace_admin == "admin@example.com"

    def test_invalid_email_multiple_at(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="user@@example.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )


class TestInitParams:
    """Tests for __init__ parameter storage and default state."""

    def test_dry_run_flag(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=True)
        assert m.dry_run is True

    def test_dry_run_default_false(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=False)
        assert m.dry_run is False

    def test_verbose_flag(self, tmp_path):
        m = _make_migrator(tmp_path, verbose=True)
        assert m.verbose is True

    def test_update_mode_sets_import_mode_false(self, tmp_path):
        m = _make_migrator(tmp_path, update_mode=True)
        assert m.update_mode is True
        assert m.import_mode is False

    def test_import_mode_true_when_not_update(self, tmp_path):
        m = _make_migrator(tmp_path, update_mode=False)
        assert m.update_mode is False
        assert m.import_mode is True

    def test_debug_api_flag(self, tmp_path):
        m = _make_migrator(tmp_path, debug_api=True)
        assert m.debug_api is True

    def test_creds_path_stored(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.creds_path == "fake_creds.json"

    def test_config_path_stored(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.config_path == tmp_path / "config.yaml"

    def test_export_root_is_path(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert isinstance(m.export_root, Path)
        assert m.export_root == tmp_path


class TestInitCaches:
    """Tests that caches and state tracking dicts are initialized."""

    def test_space_cache_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.space_cache == {}

    def test_created_spaces_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.created_spaces == {}

    def test_user_map_populated(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert "U001" in m.user_map
        assert m.user_map["U001"] == "alice@example.com"

    def test_thread_map_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.thread_map == {}

    def test_external_users_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.external_users == set()

    def test_failed_messages_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.failed_messages == []

    def test_channel_handlers_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.channel_handlers == {}

    def test_channel_to_space_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.channel_to_space == {}

    def test_current_space_none(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.current_space is None

    def test_migration_summary_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.migration_summary == {}

    def test_api_services_not_initialized(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m._api_services_initialized is False
        assert m.chat is None
        assert m.drive is None

    def test_chat_delegates_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.chat_delegates == {}

    def test_valid_users_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.valid_users == {}

    def test_channel_id_to_space_id_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.channel_id_to_space_id == {}


class TestInitConfig:
    """Tests that config loading works during __init__."""

    def test_config_is_migration_config(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert isinstance(m.config, MigrationConfig)

    def test_config_from_yaml(self, tmp_path):
        _setup_export(tmp_path)
        config_content = "abort_on_error: true\nmax_retries: 5\n"
        (tmp_path / "config.yaml").write_text(config_content)
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@example.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.config.abort_on_error is True
        assert m.config.max_retries == 5

    def test_config_missing_file_uses_defaults(self, tmp_path):
        m = _make_migrator(tmp_path)
        # Non-existent config should load defaults
        assert m.config.abort_on_error is False
        assert m.config.max_retries == 3

    def test_progress_file_path(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.progress_file == tmp_path / ".migration_progress.json"


class TestInitUnmappedUserTracker:
    """Tests that unmapped user tracking is initialized."""

    def test_tracker_created(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert hasattr(m, "unmapped_user_tracker")

    def test_users_without_email_tracked(self, tmp_path):
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}},
            {"id": "U002", "name": "nomail", "profile": {}},
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001", "U002"]}]
        m = _make_migrator(tmp_path, users=users, channels=channels)
        # U002 has no email, so it should appear in users_without_email
        assert any(u.get("id") == "U002" for u in m.users_without_email)


# ---------------------------------------------------------------------------
# _is_external_user tests
# ---------------------------------------------------------------------------


class TestIsExternalUser:
    """Tests for _is_external_user()."""

    def test_internal_user(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@example.com") is False

    def test_external_user(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@other.com") is True

    def test_case_insensitive(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@EXAMPLE.COM") is False

    def test_none_email(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m._is_external_user(None) is False

    def test_empty_email(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m._is_external_user("") is False

    def test_subdomain_is_external(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@sub.example.com") is True


# ---------------------------------------------------------------------------
# _validate_export_format tests
# ---------------------------------------------------------------------------


class TestExportPathValidation:
    """Tests for export path validation."""

    def test_valid_export_path(self, tmp_path):
        _setup_export(tmp_path)
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@example.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.export_root == tmp_path

    def test_workspace_domain_extraction(self, tmp_path):
        _setup_export(tmp_path)
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@mycompany.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.workspace_domain == "mycompany.com"

    def test_invalid_export_path_not_a_dir(self, tmp_path):
        """Non-existent directory should raise ValueError."""
        fake_path = tmp_path / "nonexistent"
        with pytest.raises(ValueError, match="not a valid directory"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(fake_path),
                workspace_admin="admin@example.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_missing_users_json_raises(self, tmp_path):
        """Directory without users.json should raise ValueError."""
        channels = [{"id": "C001", "name": "general", "members": []}]
        (tmp_path / "channels.json").write_text(json.dumps(channels))
        (tmp_path / "general").mkdir()
        with pytest.raises(ValueError, match=r"users\.json not found"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="admin@example.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_no_channel_dirs_raises(self, tmp_path):
        """Directory with required files but no channel subdirs should raise."""
        users = [{"id": "U001", "name": "alice", "profile": {"email": "a@b.com"}}]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "channels.json").write_text(json.dumps(channels))
        # Don't create subdirectory
        with pytest.raises(ValueError, match="No channel directories found"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="admin@b.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_missing_channels_json_warns(self, tmp_path, caplog):
        """Missing channels.json should log a warning but not crash."""
        users = [{"id": "U001", "name": "alice", "profile": {"email": "a@b.com"}}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "general").mkdir()
        with caplog.at_level(logging.WARNING, logger="slack_migrator"):
            m = SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="admin@b.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )
        assert any("channels.json not found" in r.message for r in caplog.records)
        assert m is not None

    def test_channel_dir_without_json_warns(self, tmp_path, caplog):
        """Channel directory with no JSON files should log a warning."""
        users = [{"id": "U001", "name": "alice", "profile": {"email": "a@b.com"}}]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "channels.json").write_text(json.dumps(channels))
        (tmp_path / "general").mkdir()
        # Create dir but no .json files inside it

        with caplog.at_level(logging.WARNING, logger="slack_migrator"):
            _m = SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="admin@b.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )
        assert any(
            "No JSON files found in channel directory" in r.message
            for r in caplog.records
        )


# ---------------------------------------------------------------------------
# _load_channels_meta tests
# ---------------------------------------------------------------------------


class TestLoadChannelsMeta:
    """Tests for _load_channels_meta()."""

    def test_loads_channel_data(self, tmp_path):
        channels = [
            {"id": "C001", "name": "general", "members": ["U001"]},
            {"id": "C002", "name": "random", "members": ["U001", "U002"]},
        ]
        m = _make_migrator(tmp_path, channels=channels)
        assert "general" in m.channels_meta
        assert "random" in m.channels_meta
        assert m.channels_meta["general"]["id"] == "C001"

    def test_id_to_name_mapping(self, tmp_path):
        channels = [
            {"id": "C001", "name": "general", "members": ["U001"]},
            {"id": "C002", "name": "random", "members": ["U001"]},
        ]
        m = _make_migrator(tmp_path, channels=channels)
        assert m.channel_id_to_name["C001"] == "general"
        assert m.channel_id_to_name["C002"] == "random"

    def test_name_to_id_mapping(self, tmp_path):
        channels = [
            {"id": "C001", "name": "general", "members": ["U001"]},
        ]
        m = _make_migrator(tmp_path, channels=channels)
        assert m.channel_name_to_id["general"] == "C001"

    def test_empty_channels_json(self, tmp_path):
        """Empty channels list should produce empty dicts."""
        users = [{"id": "U001", "name": "a", "profile": {"email": "a@b.com"}}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "channels.json").write_text(json.dumps([]))
        (tmp_path / "somedir").mkdir()
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@b.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.channels_meta == {}
        assert m.channel_id_to_name == {}

    def test_no_channels_json_file(self, tmp_path):
        """If channels.json doesn't exist, meta should be empty dicts."""
        users = [{"id": "U001", "name": "a", "profile": {"email": "a@b.com"}}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "somedir").mkdir()
        # No channels.json
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@b.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.channels_meta == {}
        assert m.channel_id_to_name == {}


# ---------------------------------------------------------------------------
# _get_space_name / _get_all_channel_names tests
# ---------------------------------------------------------------------------


class TestSpaceNameHelpers:
    """Tests for _get_space_name() and _get_all_channel_names()."""

    def test_get_space_name(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m._get_space_name("general") == "Slack #general"
        assert m._get_space_name("random") == "Slack #random"

    def test_get_all_channel_names(self, tmp_path):
        channels = [
            {"id": "C001", "name": "general", "members": ["U001"]},
            {"id": "C002", "name": "random", "members": ["U001"]},
        ]
        m = _make_migrator(tmp_path, channels=channels)
        names = m._get_all_channel_names()
        assert "general" in names
        assert "random" in names


# ---------------------------------------------------------------------------
# _should_abort_import tests
# ---------------------------------------------------------------------------


class TestShouldAbortImport:
    """Tests for _should_abort_import()."""

    def test_no_abort_in_dry_run(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=True)
        m.config = MigrationConfig(abort_on_error=True)
        assert m._should_abort_import("general", 10, 5) is False

    def test_no_abort_when_no_failures(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=False)
        m.config = MigrationConfig(abort_on_error=True)
        assert m._should_abort_import("general", 10, 0) is False

    def test_abort_when_failures_and_abort_enabled(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=False)
        m.config = MigrationConfig(abort_on_error=True)
        assert m._should_abort_import("general", 10, 1) is True

    def test_no_abort_when_failures_but_abort_disabled(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=False)
        m.config = MigrationConfig(abort_on_error=False)
        assert m._should_abort_import("general", 10, 3) is False

    def test_abort_with_zero_processed(self, tmp_path):
        """Failures with zero processed should still trigger abort if enabled."""
        m = _make_migrator(tmp_path, dry_run=False)
        m.config = MigrationConfig(abort_on_error=True)
        assert m._should_abort_import("general", 0, 1) is True


# ---------------------------------------------------------------------------
# _delete_space_if_errors tests
# ---------------------------------------------------------------------------


class TestDeleteSpaceIfErrors:
    """Tests for _delete_space_if_errors()."""

    def test_no_delete_when_cleanup_disabled(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.config = MigrationConfig(cleanup_on_error=False)
        m.chat = MagicMock()
        m._delete_space_if_errors("spaces/abc123", "general")
        m.chat.spaces().delete.assert_not_called()

    def test_delete_when_cleanup_enabled(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.config = MigrationConfig(cleanup_on_error=True)
        m.chat = MagicMock()
        m.state.created_spaces = {"general": "spaces/abc123"}
        m.state.migration_summary = {"spaces_created": 1}
        m._delete_space_if_errors("spaces/abc123", "general")
        m.chat.spaces().delete.assert_called_once_with(name="spaces/abc123")
        assert "general" not in m.state.created_spaces
        assert m.state.migration_summary["spaces_created"] == 0

    def test_delete_handles_api_error(self, tmp_path):
        from google.auth.exceptions import RefreshError

        m = _make_migrator(tmp_path)
        m.config = MigrationConfig(cleanup_on_error=True)
        m.chat = MagicMock()
        m.chat.spaces().delete().execute.side_effect = RefreshError("API error")
        m.state.created_spaces = {"general": "spaces/abc123"}
        m.state.migration_summary = {"spaces_created": 1}
        # Should not raise
        m._delete_space_if_errors("spaces/abc123", "general")

    def test_delete_handles_transport_error(self, tmp_path):
        from google.auth.exceptions import TransportError

        m = _make_migrator(tmp_path)
        m.config = MigrationConfig(cleanup_on_error=True)
        m.chat = MagicMock()
        m.chat.spaces().delete().execute.side_effect = TransportError("network down")
        m.state.created_spaces = {"general": "spaces/abc123"}
        m.state.migration_summary = {"spaces_created": 1}
        # Should not raise — TransportError is a recoverable network issue
        m._delete_space_if_errors("spaces/abc123", "general")

    def test_delete_handles_http_error(self, tmp_path):
        from googleapiclient.errors import HttpError

        m = _make_migrator(tmp_path)
        m.config = MigrationConfig(cleanup_on_error=True)
        m.chat = MagicMock()
        resp = MagicMock()
        resp.status = 500
        m.chat.spaces().delete().execute.side_effect = HttpError(
            resp=resp, content=b"Server Error"
        )
        m.state.created_spaces = {"general": "spaces/abc123"}
        m.state.migration_summary = {"spaces_created": 1}
        # Should not raise — HttpError is caught and logged
        m._delete_space_if_errors("spaces/abc123", "general")
        # Space should NOT be removed from created_spaces (delete failed)
        assert "general" in m.state.created_spaces

    def test_delete_propagates_unexpected_error(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.config = MigrationConfig(cleanup_on_error=True)
        m.chat = MagicMock()
        m.chat.spaces().delete().execute.side_effect = RuntimeError("truly unexpected")
        m.state.created_spaces = {"general": "spaces/abc123"}
        m.state.migration_summary = {"spaces_created": 1}
        # Should raise — RuntimeError is not a known API/transport error
        with pytest.raises(RuntimeError):
            m._delete_space_if_errors("spaces/abc123", "general")


# ---------------------------------------------------------------------------
# _initialize_api_services tests
# ---------------------------------------------------------------------------


class TestInitializeApiServices:
    """Tests for _initialize_api_services()."""

    @patch("slack_migrator.core.migrator.get_gcp_service")
    def test_services_initialized(self, mock_gcp_service, tmp_path):
        m = _make_migrator(tmp_path)
        mock_chat = MagicMock()
        mock_drive = MagicMock()
        mock_gcp_service.side_effect = [mock_chat, mock_drive]

        # Mock dependent service initialization to avoid side effects
        with patch.object(m, "_initialize_dependent_services"):
            m._initialize_api_services()

        assert m.chat is mock_chat
        assert m.drive is mock_drive
        assert m._api_services_initialized is True

    @patch("slack_migrator.core.migrator.get_gcp_service")
    def test_no_double_initialization(self, mock_gcp_service, tmp_path):
        m = _make_migrator(tmp_path)
        m._api_services_initialized = True
        m._initialize_api_services()
        mock_gcp_service.assert_not_called()


# ---------------------------------------------------------------------------
# _cleanup_channel_handlers tests
# ---------------------------------------------------------------------------


class TestCleanupChannelHandlers:
    """Tests for _cleanup_channel_handlers()."""

    def test_cleanup_flushes_and_closes(self, tmp_path):
        m = _make_migrator(tmp_path)
        handler = MagicMock()
        m.state.channel_handlers = {"general": handler}
        m._cleanup_channel_handlers()
        handler.flush.assert_called_once()
        handler.close.assert_called_once()
        assert m.state.channel_handlers == {}

    def test_cleanup_with_no_handlers(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.channel_handlers = {}
        # Should not raise
        m._cleanup_channel_handlers()

    def test_cleanup_with_empty_handlers(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.channel_handlers = {}
        # Should not raise — channel_handlers always exists on MigrationState
        m._cleanup_channel_handlers()

    def test_cleanup_handles_handler_error(self, tmp_path):
        m = _make_migrator(tmp_path)
        handler = MagicMock()
        handler.flush.side_effect = Exception("flush failed")
        m.state.channel_handlers = {"general": handler}
        # Should not raise despite handler error
        m._cleanup_channel_handlers()
        assert m.state.channel_handlers == {}

    def test_cleanup_multiple_handlers(self, tmp_path):
        m = _make_migrator(tmp_path)
        handler1 = MagicMock()
        handler2 = MagicMock()
        m.state.channel_handlers = {"general": handler1, "random": handler2}
        m._cleanup_channel_handlers()
        handler1.flush.assert_called_once()
        handler1.close.assert_called_once()
        handler2.flush.assert_called_once()
        handler2.close.assert_called_once()
        assert m.state.channel_handlers == {}


# ---------------------------------------------------------------------------
# _get_internal_email tests
# ---------------------------------------------------------------------------


class TestGetInternalEmail:
    """Tests for _get_internal_email()."""

    def test_mapped_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        result = m._get_internal_email("U001")
        assert result == "alice@example.com"

    def test_unmapped_user_returns_none(self, tmp_path):
        m = _make_migrator(tmp_path)
        result = m._get_internal_email("U_UNKNOWN")
        assert result is None

    def test_explicit_email_arg(self, tmp_path):
        m = _make_migrator(tmp_path)
        result = m._get_internal_email("U001", "override@example.com")
        assert result == "override@example.com"

    def test_bot_user_ignored_when_config_enabled(self, tmp_path):
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}},
            {
                "id": "B001",
                "name": "testbot",
                "profile": {},
                "is_bot": True,
            },
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        m = _make_migrator(tmp_path, users=users, channels=channels)
        m.config.ignore_bots = True
        result = m._get_internal_email("B001")
        assert result is None

    def test_bot_user_not_ignored_when_config_disabled(self, tmp_path):
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}},
            {
                "id": "B001",
                "name": "testbot",
                "profile": {"email": "bot@example.com"},
                "is_bot": True,
            },
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        m = _make_migrator(tmp_path, users=users, channels=channels)
        m.config.ignore_bots = False
        # Bot has email mapping
        m.user_map["B001"] = "bot@example.com"
        result = m._get_internal_email("B001")
        assert result == "bot@example.com"


# ---------------------------------------------------------------------------
# _get_user_data tests
# ---------------------------------------------------------------------------


class TestGetUserData:
    """Tests for _get_user_data()."""

    def test_existing_user(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
                "is_bot": False,
            },
        ]
        m = _make_migrator(tmp_path, users=users)
        data = m._get_user_data("U001")
        assert data is not None
        assert data["name"] == "alice"

    def test_nonexistent_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        data = m._get_user_data("U_NONEXISTENT")
        assert data is None

    def test_caches_after_first_call(self, tmp_path):
        m = _make_migrator(tmp_path)
        m._get_user_data("U001")
        assert m.user_resolver._users_data is not None
        # Second call should use cache
        m._get_user_data("U001")


# ---------------------------------------------------------------------------
# _handle_unmapped_user_message tests
# ---------------------------------------------------------------------------


class TestHandleUnmappedUserMessage:
    """Tests for _handle_unmapped_user_message()."""

    def test_returns_admin_email_and_attributed_text(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.current_channel = "general"
        email, text = m._handle_unmapped_user_message("U999", "Hello world")
        assert email == "admin@example.com"
        assert "Hello world" in text
        assert "*[From:" in text

    def test_attribution_includes_user_info_when_available(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com", "real_name": "Alice Smith"},
            },
            {
                "id": "U002",
                "name": "bob",
                "profile": {"email": "bob@other.com", "real_name": "Bob Jones"},
            },
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        m = _make_migrator(tmp_path, users=users, channels=channels)
        m.state.current_channel = "general"
        _email, text = m._handle_unmapped_user_message("U002", "Hi")
        assert "Bob Jones" in text
        assert "bob@other.com" in text

    def test_attribution_with_user_id_only(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.current_channel = "general"
        _email, text = m._handle_unmapped_user_message("U_UNKNOWN", "Hi")
        assert "U_UNKNOWN" in text

    def test_attribution_with_user_mapping_override(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.config.user_mapping_overrides = {"U999": "mapped@example.com"}
        m.state.current_channel = "general"
        _email, text = m._handle_unmapped_user_message("U999", "Hi")
        assert "mapped@example.com" in text

    def test_tracks_unmapped_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.current_channel = "general"
        m._handle_unmapped_user_message("U999", "text")
        assert "U999" in m.unmapped_user_tracker.unmapped_users


# ---------------------------------------------------------------------------
# _handle_unmapped_user_reaction tests
# ---------------------------------------------------------------------------


class TestHandleUnmappedUserReaction:
    """Tests for _handle_unmapped_user_reaction()."""

    def test_returns_false(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.current_channel = "general"
        result = m._handle_unmapped_user_reaction("U999", "thumbsup", "123.456")
        assert result is False

    def test_tracks_unmapped_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.current_channel = "general"
        m._handle_unmapped_user_reaction("U999", "thumbsup", "123.456")
        assert "U999" in m.unmapped_user_tracker.unmapped_users

    def test_records_skipped_reaction(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.current_channel = "general"
        m._handle_unmapped_user_reaction("U999", "heart", "123.456")
        assert hasattr(m.state, "skipped_reactions")
        assert len(m.state.skipped_reactions) == 1
        assert m.state.skipped_reactions[0]["reaction"] == "heart"
        assert m.state.skipped_reactions[0]["user_id"] == "U999"


# ---------------------------------------------------------------------------
# _log_migration_success tests
# ---------------------------------------------------------------------------


class TestLogMigrationSuccess:
    """Tests for _log_migration_success()."""

    def test_logs_success_dry_run(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=True)
        m.state.migration_summary = {
            "channels_processed": ["general"],
            "spaces_created": 0,
            "messages_created": 10,
            "reactions_created": 0,
            "files_created": 0,
        }
        with caplog.at_level(logging.INFO, logger="slack_migrator"):
            m._log_migration_success(60.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "DRY RUN" in messages
        assert "1.0 minutes" in messages

    def test_logs_success_real_run(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.migration_summary = {
            "channels_processed": ["general", "random"],
            "spaces_created": 2,
            "messages_created": 50,
            "reactions_created": 10,
            "files_created": 5,
        }
        with caplog.at_level(logging.INFO, logger="slack_migrator"):
            m._log_migration_success(120.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "Channels processed: 2" in messages
        assert "Spaces created/updated: 2" in messages
        assert "Messages migrated: 50" in messages

    def test_logs_issues_when_present(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.migration_summary = {
            "channels_processed": ["general"],
            "spaces_created": 1,
            "messages_created": 10,
            "reactions_created": 0,
            "files_created": 0,
        }
        m.state.channels_with_errors = ["general"]
        m.state.incomplete_import_spaces = [("spaces/abc", "general")]
        with caplog.at_level(logging.WARNING, logger="slack_migrator"):
            m._log_migration_success(30.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "Channels with errors: 1" in messages
        assert "Incomplete imports: 1" in messages

    def test_logs_no_work_done(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.migration_summary = {
            "channels_processed": [],
            "spaces_created": 0,
            "messages_created": 0,
            "reactions_created": 0,
            "files_created": 0,
        }
        with caplog.at_level(logging.WARNING, logger="slack_migrator"):
            m._log_migration_success(5.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "INTERRUPTED" in messages


# ---------------------------------------------------------------------------
# _log_migration_failure tests
# ---------------------------------------------------------------------------


class TestLogMigrationFailure:
    """Tests for _log_migration_failure()."""

    def test_logs_generic_exception(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.migration_summary = {
            "channels_processed": ["general"],
            "spaces_created": 1,
            "messages_created": 5,
        }
        exc = RuntimeError("something broke")
        with caplog.at_level(logging.ERROR, logger="slack_migrator"):
            m._log_migration_failure(exc, 30.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "RuntimeError" in messages
        assert "something broke" in messages
        assert "FAILED" in messages

    def test_logs_keyboard_interrupt(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.migration_summary = {
            "channels_processed": ["general"],
            "spaces_created": 1,
            "messages_created": 5,
        }
        exc = KeyboardInterrupt()
        with caplog.at_level(logging.WARNING, logger="slack_migrator"):
            m._log_migration_failure(exc, 10.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "INTERRUPTED" in messages
        assert "User interruption" in messages

    def test_logs_dry_run_failure(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=True)
        m.state.migration_summary = {
            "channels_processed": [],
            "spaces_created": 0,
            "messages_created": 0,
        }
        exc = ValueError("bad config")
        with caplog.at_level(logging.ERROR, logger="slack_migrator"):
            m._log_migration_failure(exc, 2.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "DRY RUN VALIDATION FAILED" in messages

    def test_logs_progress_before_failure(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.migration_summary = {
            "channels_processed": ["general", "random"],
            "spaces_created": 2,
            "messages_created": 100,
        }
        exc = Exception("api error")
        with caplog.at_level(logging.ERROR, logger="slack_migrator"):
            m._log_migration_failure(exc, 60.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "Channels processed: 2" in messages
        assert "Spaces created: 2" in messages
        assert "Messages migrated: 100" in messages


# ---------------------------------------------------------------------------
# _list_all_spaces standalone function tests
# ---------------------------------------------------------------------------


class TestListAllSpaces:
    """Tests for the _list_all_spaces() standalone function."""

    def test_single_page(self):
        chat_service = MagicMock()
        chat_service.spaces().list().execute.return_value = {
            "spaces": [{"name": "spaces/abc"}, {"name": "spaces/def"}],
        }
        result = _list_all_spaces(chat_service)
        assert len(result) == 2
        assert result[0]["name"] == "spaces/abc"

    def test_multiple_pages(self):
        chat_service = MagicMock()
        # First call returns page with nextPageToken
        # Second call returns last page
        chat_service.spaces().list().execute.side_effect = [
            {
                "spaces": [{"name": "spaces/abc"}],
                "nextPageToken": "page2",
            },
            {
                "spaces": [{"name": "spaces/def"}],
            },
        ]
        result = _list_all_spaces(chat_service)
        assert len(result) == 2

    def test_empty_response(self):
        chat_service = MagicMock()
        chat_service.spaces().list().execute.return_value = {}
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_http_error_returns_partial(self):
        from googleapiclient.errors import HttpError

        resp = MagicMock()
        resp.status = 500

        chat_service = MagicMock()
        chat_service.spaces().list().execute.side_effect = HttpError(
            resp=resp, content=b"Server Error"
        )
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_refresh_error_returns_empty(self):
        from google.auth.exceptions import RefreshError

        chat_service = MagicMock()
        chat_service.spaces().list().execute.side_effect = RefreshError("network error")
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_transport_error_returns_empty(self):
        from google.auth.exceptions import TransportError

        chat_service = MagicMock()
        chat_service.spaces().list().execute.side_effect = TransportError("DNS failure")
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_connection_error_propagates(self):
        chat_service = MagicMock()
        chat_service.spaces().list().execute.side_effect = ConnectionError("refused")
        with pytest.raises(ConnectionError):
            _list_all_spaces(chat_service)


# ---------------------------------------------------------------------------
# cleanup_import_mode_spaces standalone function tests
# ---------------------------------------------------------------------------


class TestCleanupImportModeSpaces:
    """Tests for the cleanup_import_mode_spaces() standalone function."""

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_no_spaces_found(self, mock_list):
        mock_list.return_value = []
        chat_service = MagicMock()
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_no_spaces_in_import_mode(self, mock_list):
        mock_list.return_value = [{"name": "spaces/abc"}]
        chat_service = MagicMock()
        # get() returns space NOT in import mode
        chat_service.spaces().get().execute.return_value = {"importMode": False}
        cleanup_import_mode_spaces(chat_service)
        chat_service.spaces().completeImport.assert_not_called()

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_completes_import_mode_spaces(self, mock_list):
        mock_list.return_value = [{"name": "spaces/abc"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        cleanup_import_mode_spaces(chat_service)
        chat_service.spaces().completeImport.assert_called_once_with(name="spaces/abc")

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_preserves_external_user_access(self, mock_list):
        mock_list.return_value = [{"name": "spaces/xyz"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": True,
        }
        cleanup_import_mode_spaces(chat_service)
        chat_service.spaces().completeImport.assert_called_once()
        chat_service.spaces().patch.assert_called_once_with(
            name="spaces/xyz",
            updateMask="externalUserAllowed",
            body={"externalUserAllowed": True},
        )

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_complete_import_http_error(self, mock_list):
        from googleapiclient.errors import HttpError

        mock_list.return_value = [{"name": "spaces/fail"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        resp = MagicMock()
        resp.status = 500
        chat_service.spaces().completeImport().execute.side_effect = HttpError(
            resp=resp, content=b"Server Error"
        )
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_complete_import_refresh_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/fail"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        from google.auth.exceptions import RefreshError

        chat_service.spaces().completeImport().execute.side_effect = RefreshError(
            "boom"
        )
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_complete_import_transport_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/fail"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        from google.auth.exceptions import TransportError

        chat_service.spaces().completeImport().execute.side_effect = TransportError(
            "network timeout"
        )
        # Should not raise — TransportError is caught
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_skips_spaces_with_empty_name(self, mock_list):
        mock_list.return_value = [{"name": ""}, {"name": "spaces/ok"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        cleanup_import_mode_spaces(chat_service)
        # Only the "spaces/ok" space should be checked
        chat_service.spaces().get.assert_called()

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_get_exception(self, mock_list):
        mock_list.return_value = [{"name": "spaces/err"}]
        chat_service = MagicMock()
        from google.auth.exceptions import RefreshError

        chat_service.spaces().get().execute.side_effect = RefreshError("get failed")
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_get_transport_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/err"}]
        chat_service = MagicMock()
        from google.auth.exceptions import TransportError

        chat_service.spaces().get().execute.side_effect = TransportError("DNS timeout")
        # Should not raise — TransportError is caught
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_patch_exception(self, mock_list):
        mock_list.return_value = [{"name": "spaces/ok"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": True,
        }
        from google.auth.exceptions import RefreshError

        chat_service.spaces().patch().execute.side_effect = RefreshError("patch failed")
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_migrator.core.migrator._list_all_spaces")
    def test_handles_patch_transport_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/ok"}]
        chat_service = MagicMock()
        chat_service.spaces().get().execute.return_value = {
            "importMode": True,
            "externalUserAllowed": True,
        }
        from google.auth.exceptions import TransportError

        chat_service.spaces().patch().execute.side_effect = TransportError(
            "connection reset"
        )
        # Should not raise — TransportError is caught
        cleanup_import_mode_spaces(chat_service)


# ---------------------------------------------------------------------------
# cleanup() instance method tests
# ---------------------------------------------------------------------------


class TestCleanup:
    """Tests for the cleanup() instance method."""

    def test_dry_run_skips_cleanup(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=True)
        with caplog.at_level(logging.INFO, logger="slack_migrator"):
            m.cleanup()
        messages = " ".join(r.message for r in caplog.records)
        assert "DRY RUN" in messages

    def test_clears_current_channel(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=True)
        m.state.current_channel = "general"
        m.cleanup()
        assert m.state.current_channel is None


# ---------------------------------------------------------------------------
# User map generation during __init__ tests
# ---------------------------------------------------------------------------


class TestUserMapGeneration:
    """Tests for user map generation during __init__."""

    def test_single_user_mapped(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.user_map.get("U001") == "alice@example.com"

    def test_multiple_users_mapped(self, tmp_path):
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}},
            {"id": "U002", "name": "bob", "profile": {"email": "bob@example.com"}},
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001", "U002"]}]
        m = _make_migrator(tmp_path, users=users, channels=channels)
        assert m.user_map["U001"] == "alice@example.com"
        assert m.user_map["U002"] == "bob@example.com"

    def test_user_without_email(self, tmp_path):
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}},
            {"id": "U002", "name": "nomail", "profile": {}},
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        m = _make_migrator(tmp_path, users=users, channels=channels)
        assert "U002" not in m.user_map
        assert len(m.users_without_email) > 0
