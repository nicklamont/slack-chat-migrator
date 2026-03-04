"""Unit tests for the migrator module."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from slack_chat_migrator.core.channel_processor import ChannelProcessor
from slack_chat_migrator.core.checkpoint import CheckpointData
from slack_chat_migrator.core.cleanup import cleanup_channel_handlers, run_cleanup
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.core.context import MigrationContext
from slack_chat_migrator.core.migration_logging import (
    log_migration_failure,
    log_migration_success,
)
from slack_chat_migrator.core.migrator import SlackToChatMigrator
from slack_chat_migrator.core.state import MigrationState, _default_migration_summary
from slack_chat_migrator.services.spaces.space_creator import (
    _list_all_spaces,
    cleanup_import_mode_spaces,
)
from slack_chat_migrator.services.user_resolver import UserResolver
from slack_chat_migrator.types import MigrationSummary


def _make_summary(**overrides: object) -> MigrationSummary:
    """Return a MigrationSummary with defaults, applying any overrides."""
    summary = _default_migration_summary()
    summary.update(overrides)  # type: ignore[typeddict-item]
    return summary


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
    """Create a migrator instance with a minimal export directory.

    Also creates a UserResolver (normally done in _initialize_api_services)
    so tests can exercise user_resolver methods without needing real API creds.
    """
    _setup_export(tmp_path, users=users, channels=channels)
    m = SlackToChatMigrator(
        creds_path="fake_creds.json",
        export_path=str(tmp_path),
        workspace_admin=f"admin@{domain}",
        config_path=str(tmp_path / "config.yaml"),
        dry_run=kwargs.get("dry_run", True),
        verbose=kwargs.get("verbose", False),
        update_mode=kwargs.get("update_mode", False),
        debug_api=kwargs.get("debug_api", False),
    )
    # Create UserResolver eagerly (mirrors _initialize_api_services but
    # with chat=None — tests here don't exercise impersonation/API calls).
    m.user_resolver = UserResolver(
        config=m.config,
        state=m.state,
        chat=None,
        creds_path=m.creds_path,
        user_map=m.user_map,
        unmapped_user_tracker=m.unmapped_user_tracker,
        export_root=m.export_root,
        workspace_admin=m.workspace_admin,
        workspace_domain=m.workspace_domain,
    )
    return m


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

    def test_update_mode_flag(self, tmp_path):
        m = _make_migrator(tmp_path, update_mode=True)
        assert m.update_mode is True

    def test_update_mode_defaults_false(self, tmp_path):
        m = _make_migrator(tmp_path, update_mode=False)
        assert m.update_mode is False

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
        assert m.state.spaces.space_cache == {}

    def test_created_spaces_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.spaces.created_spaces == {}

    def test_user_map_populated(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert "U001" in m.user_map
        assert m.user_map["U001"] == "alice@example.com"

    def test_thread_map_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.messages.thread_map == {}

    def test_external_users_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.users.external_users == set()

    def test_failed_messages_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.messages.failed_messages == []

    def test_channel_handlers_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.spaces.channel_handlers == {}

    def test_channel_to_space_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.spaces.channel_to_space == {}

    def test_current_space_none(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.context.current_space is None

    def test_migration_summary_default(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.progress.migration_summary == _default_migration_summary()

    def test_api_services_not_initialized(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m._api_services_initialized is False
        assert m.chat is None
        assert m.drive is None

    def test_chat_delegates_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.users.chat_delegates == {}

    def test_valid_users_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.users.valid_users == {}

    def test_channel_id_to_space_id_empty(self, tmp_path):
        m = _make_migrator(tmp_path)
        assert m.state.spaces.channel_id_to_space_id == {}


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
# user_resolver.is_external_user tests
# ---------------------------------------------------------------------------


class TestIsExternalUser:
    """Tests for user_resolver.is_external_user()."""

    def test_internal_user(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m.user_resolver.is_external_user("user@example.com") is False

    def test_external_user(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m.user_resolver.is_external_user("user@other.com") is True

    def test_case_insensitive(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m.user_resolver.is_external_user("user@EXAMPLE.COM") is False

    def test_none_email(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m.user_resolver.is_external_user(None) is False

    def test_empty_email(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m.user_resolver.is_external_user("") is False

    def test_subdomain_is_external(self, tmp_path):
        m = _make_migrator(tmp_path, "example.com")
        assert m.user_resolver.is_external_user("user@sub.example.com") is True


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
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            m = SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="admin@b.com",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )
        assert any("channels.json not found" in r.message for r in caplog.records)
        assert m is not None
        assert m.export_root == tmp_path
        assert m.workspace_admin == "admin@b.com"

    def test_channel_dir_without_json_warns(self, tmp_path, caplog):
        """Channel directory with no JSON files should log a warning."""
        users = [{"id": "U001", "name": "alice", "profile": {"email": "a@b.com"}}]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "channels.json").write_text(json.dumps(channels))
        (tmp_path / "general").mkdir()
        # Create dir but no .json files inside it

        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
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
# ChannelProcessor._should_abort_import / _delete_space_if_errors helpers
# ---------------------------------------------------------------------------


def _make_channel_processor(
    dry_run: bool = False,
    config: MigrationConfig | None = None,
    chat: MagicMock | None = None,
) -> ChannelProcessor:
    """Build a lightweight ChannelProcessor with explicit deps for unit tests."""
    if config is None:
        config = MigrationConfig()
    ctx = MigrationContext(
        export_root=Path("/tmp/fake"),
        creds_path="/tmp/fake_creds.json",
        workspace_admin="admin@example.com",
        workspace_domain="example.com",
        dry_run=dry_run,
        update_mode=False,
        verbose=False,
        debug_api=False,
        config=config,
        user_map={"U001": "user1@example.com"},
        users_without_email=[],
        bot_user_ids=frozenset(),
        channels_meta={},
        channel_id_to_name={},
        channel_name_to_id={},
    )
    state = MigrationState()
    return ChannelProcessor(
        ctx=ctx,
        state=state,
        chat=chat or MagicMock(),
        user_resolver=MagicMock(),
        file_handler=None,
        attachment_processor=MagicMock(),
    )


# ---------------------------------------------------------------------------
# ChannelProcessor._should_abort_import tests
# ---------------------------------------------------------------------------


class TestShouldAbortImport:
    """Tests for ChannelProcessor._should_abort_import()."""

    def test_dry_run_respects_abort_on_error(self):
        p = _make_channel_processor(
            dry_run=True, config=MigrationConfig(abort_on_error=True)
        )
        assert p._should_abort_import("general", 10, 5) is True

    def test_no_abort_when_no_failures(self):
        p = _make_channel_processor(config=MigrationConfig(abort_on_error=True))
        assert p._should_abort_import("general", 10, 0) is False

    def test_abort_when_failures_and_abort_enabled(self):
        p = _make_channel_processor(config=MigrationConfig(abort_on_error=True))
        assert p._should_abort_import("general", 10, 1) is True

    def test_no_abort_when_failures_but_abort_disabled(self):
        p = _make_channel_processor(config=MigrationConfig(abort_on_error=False))
        assert p._should_abort_import("general", 10, 3) is False

    def test_abort_with_zero_processed(self):
        """Failures with zero processed should still trigger abort if enabled."""
        p = _make_channel_processor(config=MigrationConfig(abort_on_error=True))
        assert p._should_abort_import("general", 0, 1) is True


# ---------------------------------------------------------------------------
# ChannelProcessor._delete_space_if_errors tests
# ---------------------------------------------------------------------------


class TestDeleteSpaceIfErrors:
    """Tests for ChannelProcessor._delete_space_if_errors()."""

    def test_no_delete_when_cleanup_disabled(self):
        p = _make_channel_processor(config=MigrationConfig(cleanup_on_error=False))
        p._delete_space_if_errors("spaces/abc123", "general")
        p.chat.delete_space.assert_not_called()

    def test_delete_when_cleanup_enabled(self):
        mock_chat = MagicMock()
        mock_chat.delete_space.return_value = {}
        p = _make_channel_processor(
            config=MigrationConfig(cleanup_on_error=True), chat=mock_chat
        )
        p.state.spaces.created_spaces = {"general": "spaces/abc123"}
        p.state.progress.migration_summary = _make_summary(spaces_created=1)
        p._delete_space_if_errors("spaces/abc123", "general")
        mock_chat.delete_space.assert_called_once_with("spaces/abc123")
        assert "general" not in p.state.spaces.created_spaces
        assert p.state.progress.migration_summary["spaces_created"] == 0

    def test_delete_handles_api_error(self):
        from google.auth.exceptions import RefreshError

        mock_chat = MagicMock()
        mock_chat.delete_space.side_effect = RefreshError("API error")
        p = _make_channel_processor(
            config=MigrationConfig(cleanup_on_error=True), chat=mock_chat
        )
        p.state.spaces.created_spaces = {"general": "spaces/abc123"}
        p.state.progress.migration_summary = _make_summary(spaces_created=1)
        # Should not raise
        p._delete_space_if_errors("spaces/abc123", "general")

    def test_delete_handles_transport_error(self):
        from google.auth.exceptions import TransportError

        mock_chat = MagicMock()
        mock_chat.delete_space.side_effect = TransportError("network down")
        p = _make_channel_processor(
            config=MigrationConfig(cleanup_on_error=True), chat=mock_chat
        )
        p.state.spaces.created_spaces = {"general": "spaces/abc123"}
        p.state.progress.migration_summary = _make_summary(spaces_created=1)
        # Should not raise — TransportError is a recoverable network issue
        p._delete_space_if_errors("spaces/abc123", "general")

    def test_delete_handles_http_error(self):
        from googleapiclient.errors import HttpError

        mock_chat = MagicMock()
        resp = MagicMock()
        resp.status = 500
        mock_chat.delete_space.side_effect = HttpError(
            resp=resp, content=b"Server Error"
        )
        p = _make_channel_processor(
            config=MigrationConfig(cleanup_on_error=True), chat=mock_chat
        )
        p.state.spaces.created_spaces = {"general": "spaces/abc123"}
        p.state.progress.migration_summary = _make_summary(spaces_created=1)
        # Should not raise — HttpError is caught and logged
        p._delete_space_if_errors("spaces/abc123", "general")
        # Space should NOT be removed from created_spaces (delete failed)
        assert "general" in p.state.spaces.created_spaces

    def test_delete_propagates_unexpected_error(self):
        mock_chat = MagicMock()
        mock_chat.delete_space.side_effect = RuntimeError("truly unexpected")
        p = _make_channel_processor(
            config=MigrationConfig(cleanup_on_error=True), chat=mock_chat
        )
        p.state.spaces.created_spaces = {"general": "spaces/abc123"}
        p.state.progress.migration_summary = _make_summary(spaces_created=1)
        # Should raise — RuntimeError is not a known API/transport error
        with pytest.raises(RuntimeError):
            p._delete_space_if_errors("spaces/abc123", "general")


# ---------------------------------------------------------------------------
# _initialize_api_services tests
# ---------------------------------------------------------------------------


class TestInitializeApiServices:
    """Tests for _initialize_api_services()."""

    @patch("slack_chat_migrator.core.migrator.get_gcp_service")
    def test_services_initialized(self, mock_gcp_service, tmp_path):
        m = _make_migrator(tmp_path, dry_run=False)
        mock_chat = MagicMock()
        mock_drive = MagicMock()
        mock_gcp_service.side_effect = [mock_chat, mock_drive]

        # Mock dependent service initialization to avoid side effects
        with patch.object(m, "_initialize_dependent_services"):
            m._initialize_api_services()

        from slack_chat_migrator.services.chat_adapter import ChatAdapter
        from slack_chat_migrator.services.drive_adapter import DriveAdapter

        assert isinstance(m.chat, ChatAdapter)
        assert m.chat._svc is mock_chat
        assert isinstance(m.drive, DriveAdapter)
        assert m.drive._svc is mock_drive
        assert m._api_services_initialized is True

    def test_dry_run_uses_noop_services(self, tmp_path):
        """In dry-run mode, DryRunChatService wrapped in ChatAdapter is injected."""
        from slack_chat_migrator.services.chat.dry_run_service import DryRunChatService
        from slack_chat_migrator.services.chat_adapter import ChatAdapter
        from slack_chat_migrator.services.drive.dry_run_service import (
            DryRunDriveService,
        )
        from slack_chat_migrator.services.drive_adapter import DriveAdapter

        m = _make_migrator(tmp_path, dry_run=True)

        with patch.object(m, "_initialize_dependent_services"):
            m._initialize_api_services()

        assert isinstance(m.chat, ChatAdapter)
        assert isinstance(m.chat._svc, DryRunChatService)
        assert isinstance(m.drive, DriveAdapter)
        assert isinstance(m.drive._svc, DryRunDriveService)
        assert m._api_services_initialized is True

    @patch("slack_chat_migrator.core.migrator.get_gcp_service")
    def test_no_double_initialization(self, mock_gcp_service, tmp_path):
        m = _make_migrator(tmp_path)
        m._api_services_initialized = True
        m._initialize_api_services()
        mock_gcp_service.assert_not_called()


# ---------------------------------------------------------------------------
# cleanup_channel_handlers tests
# ---------------------------------------------------------------------------


class TestCleanupChannelHandlers:
    """Tests for cleanup_channel_handlers()."""

    def test_cleanup_flushes_and_closes(self, tmp_path):
        m = _make_migrator(tmp_path)
        handler = MagicMock()
        m.state.spaces.channel_handlers = {"general": handler}
        cleanup_channel_handlers(m.state)
        handler.flush.assert_called_once()
        handler.close.assert_called_once()
        assert m.state.spaces.channel_handlers == {}

    def test_cleanup_with_no_handlers(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.spaces.channel_handlers = {}
        # Should not raise
        cleanup_channel_handlers(m.state)

    def test_cleanup_with_empty_handlers(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.spaces.channel_handlers = {}
        # Should not raise — channel_handlers always exists on MigrationState
        cleanup_channel_handlers(m.state)

    def test_cleanup_handles_handler_error(self, tmp_path):
        m = _make_migrator(tmp_path)
        handler = MagicMock()
        handler.flush.side_effect = OSError("flush failed")
        m.state.spaces.channel_handlers = {"general": handler}
        # Should not raise despite handler error
        cleanup_channel_handlers(m.state)
        assert m.state.spaces.channel_handlers == {}

    def test_cleanup_multiple_handlers(self, tmp_path):
        m = _make_migrator(tmp_path)
        handler1 = MagicMock()
        handler2 = MagicMock()
        m.state.spaces.channel_handlers = {"general": handler1, "random": handler2}
        cleanup_channel_handlers(m.state)
        handler1.flush.assert_called_once()
        handler1.close.assert_called_once()
        handler2.flush.assert_called_once()
        handler2.close.assert_called_once()
        assert m.state.spaces.channel_handlers == {}


# ---------------------------------------------------------------------------
# user_resolver.get_internal_email tests
# ---------------------------------------------------------------------------


class TestGetInternalEmail:
    """Tests for user_resolver.get_internal_email()."""

    def test_mapped_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        result = m.user_resolver.get_internal_email("U001")
        assert result == "alice@example.com"

    def test_unmapped_user_returns_none(self, tmp_path):
        m = _make_migrator(tmp_path)
        result = m.user_resolver.get_internal_email("U_UNKNOWN")
        assert result is None

    def test_explicit_email_arg(self, tmp_path):
        m = _make_migrator(tmp_path)
        result = m.user_resolver.get_internal_email("U001", "override@example.com")
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
        result = m.user_resolver.get_internal_email("B001")
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
        result = m.user_resolver.get_internal_email("B001")
        assert result == "bot@example.com"


# ---------------------------------------------------------------------------
# user_resolver.get_user_data tests
# ---------------------------------------------------------------------------


class TestGetUserData:
    """Tests for user_resolver.get_user_data()."""

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
        data = m.user_resolver.get_user_data("U001")
        assert data is not None
        assert data["id"] == "U001"
        assert data["name"] == "alice"
        assert data["profile"]["email"] == "alice@example.com"

    def test_nonexistent_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        data = m.user_resolver.get_user_data("U_NONEXISTENT")
        assert data is None

    def test_caches_after_first_call(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.user_resolver.get_user_data("U001")
        assert m.user_resolver._users_data is not None
        assert "U001" in m.user_resolver._users_data
        assert m.user_resolver._users_data["U001"]["name"] == "alice"
        # Second call should use cache
        m.user_resolver.get_user_data("U001")


# ---------------------------------------------------------------------------
# user_resolver.handle_unmapped_user_message tests
# ---------------------------------------------------------------------------


class TestHandleUnmappedUserMessage:
    """Tests for user_resolver.handle_unmapped_user_message()."""

    def test_returns_admin_email_and_attributed_text(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.context.current_channel = "general"
        email, text = m.user_resolver.handle_unmapped_user_message(
            "U999", "Hello world"
        )
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
        m.state.context.current_channel = "general"
        _email, text = m.user_resolver.handle_unmapped_user_message("U002", "Hi")
        assert "Bob Jones" in text
        assert "bob@other.com" in text

    def test_attribution_with_user_id_only(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.context.current_channel = "general"
        _email, text = m.user_resolver.handle_unmapped_user_message("U_UNKNOWN", "Hi")
        assert "U_UNKNOWN" in text

    def test_attribution_with_user_mapping_override(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.config.user_mapping_overrides = {"U999": "mapped@example.com"}
        m.state.context.current_channel = "general"
        _email, text = m.user_resolver.handle_unmapped_user_message("U999", "Hi")
        assert "mapped@example.com" in text

    def test_tracks_unmapped_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.context.current_channel = "general"
        m.user_resolver.handle_unmapped_user_message("U999", "text")
        assert "U999" in m.unmapped_user_tracker.unmapped_users


# ---------------------------------------------------------------------------
# user_resolver.handle_unmapped_user_reaction tests
# ---------------------------------------------------------------------------


class TestHandleUnmappedUserReaction:
    """Tests for user_resolver.handle_unmapped_user_reaction()."""

    def test_returns_false(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.context.current_channel = "general"
        result = m.user_resolver.handle_unmapped_user_reaction(
            "U999", "thumbsup", "123.456"
        )
        assert result is False

    def test_tracks_unmapped_user(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.context.current_channel = "general"
        m.user_resolver.handle_unmapped_user_reaction("U999", "thumbsup", "123.456")
        assert "U999" in m.unmapped_user_tracker.unmapped_users

    def test_records_skipped_reaction(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.state.context.current_channel = "general"
        m.user_resolver.handle_unmapped_user_reaction("U999", "heart", "123.456")
        assert hasattr(m.state.users, "skipped_reactions")
        assert len(m.state.users.skipped_reactions) == 1
        assert m.state.users.skipped_reactions[0]["reaction"] == "heart"
        assert m.state.users.skipped_reactions[0]["user_id"] == "U999"


# ---------------------------------------------------------------------------
# log_migration_success tests
# ---------------------------------------------------------------------------


class TestLogMigrationSuccess:
    """Tests for log_migration_success()."""

    def test_logs_success_dry_run(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=True)
        m.state.progress.migration_summary = _make_summary(
            channels_processed=["general"],
            messages_created=10,
        )
        with caplog.at_level(logging.INFO, logger="slack_chat_migrator"):
            log_migration_success(m.state, m.dry_run, 60.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "DRY RUN" in messages
        assert "1.0 minutes" in messages

    def test_logs_success_real_run(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.progress.migration_summary = _make_summary(
            channels_processed=["general", "random"],
            spaces_created=2,
            messages_created=50,
            reactions_created=10,
            files_created=5,
        )
        with caplog.at_level(logging.INFO, logger="slack_chat_migrator"):
            log_migration_success(m.state, m.dry_run, 120.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "Channels processed: 2" in messages
        assert "Spaces created/updated: 2" in messages
        assert "Messages migrated: 50" in messages

    def test_logs_issues_when_present(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.progress.migration_summary = _make_summary(
            channels_processed=["general"],
            spaces_created=1,
            messages_created=10,
        )
        m.state.errors.channels_with_errors = ["general"]
        m.state.errors.incomplete_import_spaces = [("spaces/abc", "general")]
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            log_migration_success(m.state, m.dry_run, 30.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "Channels with errors: 1" in messages
        assert "Incomplete imports: 1" in messages

    def test_logs_no_work_done(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.progress.migration_summary = _default_migration_summary()
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            log_migration_success(m.state, m.dry_run, 5.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "INTERRUPTED" in messages


# ---------------------------------------------------------------------------
# log_migration_failure tests
# ---------------------------------------------------------------------------


class TestLogMigrationFailure:
    """Tests for log_migration_failure()."""

    def test_logs_generic_exception(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.progress.migration_summary = _make_summary(
            channels_processed=["general"],
            spaces_created=1,
            messages_created=5,
        )
        exc = RuntimeError("something broke")
        with caplog.at_level(logging.ERROR, logger="slack_chat_migrator"):
            log_migration_failure(m.state, m.dry_run, exc, 30.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "RuntimeError" in messages
        assert "something broke" in messages
        assert "FAILED" in messages

    def test_logs_keyboard_interrupt(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.progress.migration_summary = _make_summary(
            channels_processed=["general"],
            spaces_created=1,
            messages_created=5,
        )
        exc = KeyboardInterrupt()
        with caplog.at_level(logging.WARNING, logger="slack_chat_migrator"):
            log_migration_failure(m.state, m.dry_run, exc, 10.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "INTERRUPTED" in messages
        assert "User interruption" in messages

    def test_logs_dry_run_failure(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=True)
        m.state.progress.migration_summary = _default_migration_summary()
        exc = ValueError("bad config")
        with caplog.at_level(logging.ERROR, logger="slack_chat_migrator"):
            log_migration_failure(m.state, m.dry_run, exc, 2.0)
        messages = " ".join(r.message for r in caplog.records)
        assert "DRY RUN VALIDATION FAILED" in messages

    def test_logs_progress_before_failure(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=False)
        m.state.progress.migration_summary = _make_summary(
            channels_processed=["general", "random"],
            spaces_created=2,
            messages_created=100,
        )
        exc = Exception("api error")
        with caplog.at_level(logging.ERROR, logger="slack_chat_migrator"):
            log_migration_failure(m.state, m.dry_run, exc, 60.0)
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
        chat_service.list_spaces.return_value = {
            "spaces": [{"name": "spaces/abc"}, {"name": "spaces/def"}],
        }
        result = _list_all_spaces(chat_service)
        assert len(result) == 2
        assert result[0]["name"] == "spaces/abc"

    def test_multiple_pages(self):
        chat_service = MagicMock()
        # First call returns page with nextPageToken
        # Second call returns last page
        chat_service.list_spaces.side_effect = [
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
        chat_service.list_spaces.return_value = {}
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_http_error_returns_partial(self):
        from googleapiclient.errors import HttpError

        resp = MagicMock()
        resp.status = 500

        chat_service = MagicMock()
        chat_service.list_spaces.side_effect = HttpError(
            resp=resp, content=b"Server Error"
        )
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_refresh_error_returns_empty(self):
        from google.auth.exceptions import RefreshError

        chat_service = MagicMock()
        chat_service.list_spaces.side_effect = RefreshError("network error")
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_transport_error_returns_empty(self):
        from google.auth.exceptions import TransportError

        chat_service = MagicMock()
        chat_service.list_spaces.side_effect = TransportError("DNS failure")
        result = _list_all_spaces(chat_service)
        assert result == []

    def test_connection_error_propagates(self):
        chat_service = MagicMock()
        chat_service.list_spaces.side_effect = ConnectionError("refused")
        with pytest.raises(ConnectionError):
            _list_all_spaces(chat_service)


# ---------------------------------------------------------------------------
# cleanup_import_mode_spaces standalone function tests
# ---------------------------------------------------------------------------


class TestCleanupImportModeSpaces:
    """Tests for the cleanup_import_mode_spaces() standalone function."""

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_no_spaces_found(self, mock_list):
        mock_list.return_value = []
        chat_service = MagicMock()
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_no_spaces_in_import_mode(self, mock_list):
        mock_list.return_value = [{"name": "spaces/abc"}]
        chat_service = MagicMock()
        # get_space() returns space NOT in import mode
        chat_service.get_space.return_value = {"importMode": False}
        cleanup_import_mode_spaces(chat_service)
        chat_service.complete_import.assert_not_called()

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_completes_import_mode_spaces(self, mock_list):
        mock_list.return_value = [{"name": "spaces/abc"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        cleanup_import_mode_spaces(chat_service)
        chat_service.complete_import.assert_called_once_with("spaces/abc")

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_preserves_external_user_access(self, mock_list):
        mock_list.return_value = [{"name": "spaces/xyz"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": True,
        }
        cleanup_import_mode_spaces(chat_service)
        chat_service.complete_import.assert_called_once()
        chat_service.patch_space.assert_called_once_with(
            name="spaces/xyz",
            update_mask="externalUserAllowed",
            body={"externalUserAllowed": True},
        )

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_complete_import_http_error(self, mock_list):
        from googleapiclient.errors import HttpError

        mock_list.return_value = [{"name": "spaces/fail"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        resp = MagicMock()
        resp.status = 500
        chat_service.complete_import.side_effect = HttpError(
            resp=resp, content=b"Server Error"
        )
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_complete_import_refresh_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/fail"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        from google.auth.exceptions import RefreshError

        chat_service.complete_import.side_effect = RefreshError("boom")
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_complete_import_transport_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/fail"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        from google.auth.exceptions import TransportError

        chat_service.complete_import.side_effect = TransportError("network timeout")
        # Should not raise — TransportError is caught
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_skips_spaces_with_empty_name(self, mock_list):
        mock_list.return_value = [{"name": ""}, {"name": "spaces/ok"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": False,
        }
        cleanup_import_mode_spaces(chat_service)
        # Only the "spaces/ok" space should be checked
        chat_service.get_space.assert_called()

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_get_exception(self, mock_list):
        mock_list.return_value = [{"name": "spaces/err"}]
        chat_service = MagicMock()
        from google.auth.exceptions import RefreshError

        chat_service.get_space.side_effect = RefreshError("get failed")
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_get_transport_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/err"}]
        chat_service = MagicMock()
        from google.auth.exceptions import TransportError

        chat_service.get_space.side_effect = TransportError("DNS timeout")
        # Should not raise — TransportError is caught
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_patch_exception(self, mock_list):
        mock_list.return_value = [{"name": "spaces/ok"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": True,
        }
        from google.auth.exceptions import RefreshError

        chat_service.patch_space.side_effect = RefreshError("patch failed")
        # Should not raise
        cleanup_import_mode_spaces(chat_service)

    @patch("slack_chat_migrator.services.spaces.space_creator._list_all_spaces")
    def test_handles_patch_transport_error(self, mock_list):
        mock_list.return_value = [{"name": "spaces/ok"}]
        chat_service = MagicMock()
        chat_service.get_space.return_value = {
            "importMode": True,
            "externalUserAllowed": True,
        }
        from google.auth.exceptions import TransportError

        chat_service.patch_space.side_effect = TransportError("connection reset")
        # Should not raise — TransportError is caught
        cleanup_import_mode_spaces(chat_service)


# ---------------------------------------------------------------------------
# cleanup() instance method tests
# ---------------------------------------------------------------------------


class TestCleanup:
    """Tests for the run_cleanup() function."""

    def test_dry_run_skips_cleanup(self, tmp_path, caplog):
        m = _make_migrator(tmp_path, dry_run=True)
        with caplog.at_level(logging.INFO, logger="slack_chat_migrator"):
            run_cleanup(
                m.ctx,
                m.state,
                m.chat,
                m.user_resolver,
                getattr(m, "file_handler", None),
            )
        messages = " ".join(r.message for r in caplog.records)
        assert "DRY RUN" in messages

    def test_clears_current_channel(self, tmp_path):
        m = _make_migrator(tmp_path, dry_run=True)
        m.state.context.current_channel = "general"
        run_cleanup(
            m.ctx,
            m.state,
            m.chat,
            m.user_resolver,
            getattr(m, "file_handler", None),
        )
        assert m.state.context.current_channel is None


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


# ---------------------------------------------------------------------------
# _initialize_dependent_services tests
# ---------------------------------------------------------------------------


class TestInitializeDependentServices:
    """Tests for _initialize_dependent_services."""

    def test_creates_file_handler_and_attachment_processor(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.chat = MagicMock()
        m.drive = MagicMock()

        with (
            patch("slack_chat_migrator.core.migrator.load_existing_space_mappings"),
            patch("slack_chat_migrator.core.migrator.FileHandler") as fh_cls,
            patch(
                "slack_chat_migrator.core.migrator.MessageAttachmentProcessor"
            ) as map_cls,
        ):
            fh_cls.return_value = MagicMock()
            map_cls.return_value = MagicMock()
            m._initialize_dependent_services()
        fh_cls.assert_called_once()
        map_cls.assert_called_once()
        assert m.file_handler is fh_cls.return_value
        assert m.attachment_processor is map_cls.return_value

    def test_resets_mutable_state(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.chat = MagicMock()
        m.drive = MagicMock()

        m.state.spaces.created_spaces["ch1"] = "space1"
        m.state.context.current_channel = "ch1"
        with (
            patch("slack_chat_migrator.core.migrator.load_existing_space_mappings"),
            patch("slack_chat_migrator.core.migrator.FileHandler"),
            patch("slack_chat_migrator.core.migrator.MessageAttachmentProcessor"),
        ):
            m._initialize_dependent_services()
        assert m.state.spaces.created_spaces == {}
        assert m.state.context.current_channel is None

    def test_verbose_logs_debug(self, tmp_path):
        m = _make_migrator(tmp_path, verbose=True)
        m.chat = MagicMock()
        m.drive = MagicMock()

        with (
            patch("slack_chat_migrator.core.migrator.load_existing_space_mappings"),
            patch("slack_chat_migrator.core.migrator.FileHandler"),
            patch("slack_chat_migrator.core.migrator.MessageAttachmentProcessor"),
            patch("slack_chat_migrator.core.migrator.log_with_context") as mock_log,
        ):
            m._initialize_dependent_services()
        mock_log.assert_any_call(
            logging.DEBUG, "Migrator initialized with verbose logging enabled"
        )

    def test_calls_load_existing_space_mappings(self, tmp_path):
        m = _make_migrator(tmp_path)
        m.chat = MagicMock()
        m.drive = MagicMock()

        with (
            patch(
                "slack_chat_migrator.core.migrator.load_existing_space_mappings"
            ) as mock_load,
            patch("slack_chat_migrator.core.migrator.FileHandler"),
            patch("slack_chat_migrator.core.migrator.MessageAttachmentProcessor"),
        ):
            m._initialize_dependent_services()
        mock_load.assert_called_once_with(m.ctx, m.state, m.chat)


# ---------------------------------------------------------------------------
# migrate() tests
# ---------------------------------------------------------------------------


def _make_migrator_for_migrate(tmp_path, **kwargs):
    """Create a migrator with all deps stubbed for testing migrate()."""
    m = _make_migrator(tmp_path, **kwargs)
    m.chat = MagicMock()
    m.drive = MagicMock()
    m._api_services_initialized = True
    m.file_handler = MagicMock()
    m.attachment_processor = MagicMock()
    # Place output dir OUTSIDE the export root so it's not mistaken for a channel
    output_dir = tmp_path.parent / "migration_output"
    os.makedirs(output_dir, exist_ok=True)
    m.state.context.output_dir = str(output_dir)
    return m


class TestMigrate:
    """Tests for SlackToChatMigrator.migrate()."""

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_happy_path_returns_true(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path)
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        assert m.migrate() is True
        mock_cp.process_channel.assert_called_once()
        mock_save_cp.assert_called_once()
        mock_clear_cp.assert_called_once()
        mock_log_success.assert_called_once()

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_calls_reset_for_run(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path)
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        with patch.object(m.state, "reset_for_run") as mock_reset:
            m.migrate()
        mock_reset.assert_called_once()

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint")
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_checkpoint_resume_skips_completed(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        existing_cp = CheckpointData(
            started_at="2024-01-01T00:00:00Z",
            completed_channels={"general": "1704067200"},
        )
        mock_load_cp.return_value = existing_cp

        m = _make_migrator_for_migrate(tmp_path)
        mock_cp = MagicMock()
        mock_cp_cls.return_value = mock_cp

        assert m.migrate() is True
        # Channel "general" should be skipped — process_channel not called
        mock_cp.process_channel.assert_not_called()

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_abort_breaks_channel_loop(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        # Set up two channels
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}}
        ]
        channels = [
            {"id": "C001", "name": "general", "members": ["U001"]},
            {"id": "C002", "name": "random", "members": ["U001"]},
        ]
        m = _make_migrator_for_migrate(tmp_path)
        _setup_export(tmp_path, users=users, channels=channels)

        mock_cp = MagicMock()
        # First channel aborts
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=True, had_errors=True
        )
        mock_cp_cls.return_value = mock_cp

        m.migrate()
        # Only called once because abort breaks the loop
        assert mock_cp.process_channel.call_count == 1

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_channel_errors_skip_checkpoint(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path)
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=True
        )
        mock_cp_cls.return_value = mock_cp

        m.migrate()
        # Channel had errors — save_checkpoint should NOT be called
        mock_save_cp.assert_not_called()

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_failure")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_exception_logs_failure_and_reraises(
        self,
        mock_cp_cls,
        mock_load_cp,
        mock_log_failure,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path)
        mock_cp = MagicMock()
        mock_cp.process_channel.side_effect = RuntimeError("boom")
        mock_cp_cls.return_value = mock_cp

        with pytest.raises(RuntimeError, match="boom"):
            m.migrate()
        mock_log_failure.assert_called_once()
        # Cleanup still called in finally block
        mock_cleanup.assert_called()

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.load_space_mappings")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_update_mode_discovers_spaces(
        self,
        mock_cp_cls,
        mock_load_spaces,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path, update_mode=True)
        mock_load_spaces.return_value = {"general": "spaces/abc"}
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        m.migrate()
        mock_load_spaces.assert_called_once()
        assert m.state.spaces.created_spaces == {"general": "spaces/abc"}

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.load_space_mappings")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_update_mode_no_spaces_found(
        self,
        mock_cp_cls,
        mock_load_spaces,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
        caplog,
    ):
        m = _make_migrator_for_migrate(tmp_path, update_mode=True)
        mock_load_spaces.return_value = {}
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        with caplog.at_level(logging.WARNING):
            m.migrate()
        assert "No existing spaces found" in caplog.text

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_default_output_dir_created(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path)
        m.state.context.output_dir = None  # Force default path creation
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        m.migrate()
        # Output dir should have been set and created
        assert m.state.context.output_dir is not None
        assert "migration_logs/run_" in m.state.context.output_dir

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_dry_run_unmapped_user_summary(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
    ):
        m = _make_migrator_for_migrate(tmp_path, dry_run=True)
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        with patch(
            "slack_chat_migrator.core.migrator.log_unmapped_user_summary_for_dry_run"
        ) as mock_summary:
            m.migrate()
        mock_summary.assert_called_once()

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_success")
    @patch("slack_chat_migrator.core.migrator.clear_checkpoint")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_unmapped_users_logged_before_migration(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_clear_cp,
        mock_log_success,
        mock_cleanup,
        tmp_path,
        caplog,
    ):
        m = _make_migrator_for_migrate(tmp_path)
        m.unmapped_user_tracker.add_unmapped_user("U999", "general")
        mock_cp = MagicMock()
        mock_cp.process_channel.return_value = MagicMock(
            should_abort=False, had_errors=False
        )
        mock_cp_cls.return_value = mock_cp

        with caplog.at_level(logging.WARNING):
            m.migrate()
        assert "unmapped users during setup" in caplog.text

    @patch("slack_chat_migrator.core.migrator.cleanup_channel_handlers")
    @patch("slack_chat_migrator.core.migrator.log_migration_failure")
    @patch("slack_chat_migrator.core.migrator.save_checkpoint")
    @patch("slack_chat_migrator.core.migrator.load_checkpoint", return_value=None)
    @patch("slack_chat_migrator.core.migrator.log_space_mapping_conflicts")
    @patch("slack_chat_migrator.core.migrator.ChannelProcessor")
    def test_signal_handler_raises_keyboard_interrupt(
        self,
        mock_cp_cls,
        mock_conflicts,
        mock_load_cp,
        mock_save_cp,
        mock_log_failure,
        mock_cleanup,
        tmp_path,
    ):
        """SIGINT signal handler raises KeyboardInterrupt, which is caught by
        the ``except BaseException`` block -- log_migration_failure is called
        exactly once (no double-logging)."""
        import signal as signal_mod

        m = _make_migrator_for_migrate(tmp_path)
        mock_cp = MagicMock()
        # Simulate SIGINT during channel processing
        mock_cp.process_channel.side_effect = lambda ch: signal_mod.raise_signal(
            signal_mod.SIGINT
        )
        mock_cp_cls.return_value = mock_cp

        with pytest.raises(KeyboardInterrupt):
            m.migrate()

        # log_migration_failure should be called exactly once
        mock_log_failure.assert_called_once()
        args = mock_log_failure.call_args[0]
        assert isinstance(args[2], KeyboardInterrupt)
