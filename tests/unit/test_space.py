"""Unit tests for the space management module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_chat_migrator.constants import (
    DEFAULT_FALLBACK_JOIN_TIME,
    EARLIEST_MESSAGE_OFFSET_MINUTES,
    FIRST_MESSAGE_OFFSET_MINUTES,
    HISTORICAL_DELETE_TIME_OFFSET_SECONDS,
)
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.core.context import MigrationContext
from slack_chat_migrator.core.state import MigrationState, _default_migration_summary
from slack_chat_migrator.exceptions import SpacePermissionError
from slack_chat_migrator.services.spaces.historical_membership import add_users_to_space
from slack_chat_migrator.services.spaces.regular_membership import add_regular_members
from slack_chat_migrator.services.spaces.space_creator import (
    IMPORT_MODE_DAYS_LIMIT,
    channel_has_external_users,
    create_space,
)


def _make_ctx(
    user_map=None,
    users_without_email=None,
    workspace_domain="example.com",
    channels_meta=None,
    export_root=None,
    dry_run=False,
    workspace_admin="admin@example.com",
    bot_user_ids=None,
):
    """Create a MigrationContext with common test defaults."""
    return MigrationContext(
        export_root=export_root or Path("/fake/export"),
        creds_path="/fake/creds.json",
        workspace_admin=workspace_admin,
        workspace_domain=workspace_domain,
        dry_run=dry_run,
        update_mode=False,
        verbose=False,
        debug_api=False,
        config=MigrationConfig(),
        user_map=user_map or {},
        users_without_email=users_without_email
        if users_without_email is not None
        else [],
        bot_user_ids=bot_user_ids if bot_user_ids is not None else frozenset(),
        channels_meta=channels_meta or {},
        channel_id_to_name={},
        channel_name_to_id={},
    )


def _make_membership_deps(
    user_map=None,
    channels_meta=None,
    export_root=None,
    dry_run=False,
    workspace_admin="admin@example.com",
    bot_user_ids=None,
):
    """Create explicit deps for membership manager tests.

    Returns:
        Tuple of (ctx, state, chat, user_resolver).
    """
    ctx = _make_ctx(
        user_map=user_map,
        channels_meta=channels_meta,
        export_root=export_root,
        dry_run=dry_run,
        workspace_admin=workspace_admin,
        bot_user_ids=bot_user_ids,
    )
    state = MigrationState()
    state.progress.migration_summary = _default_migration_summary()
    state.spaces.created_spaces = {}
    state.users.external_users = set()
    state.context.current_channel = "general"
    chat = MagicMock()
    user_resolver = MagicMock()
    return ctx, state, chat, user_resolver


def _make_http_error(status, reason="error", content=b"{}"):
    """Create a mock HttpError with the given status code."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=content)


# ---------------------------------------------------------------------------
# channel_has_external_users
# ---------------------------------------------------------------------------


class TestChannelHasExternalUsers:
    """Tests for channel_has_external_users()."""

    def test_no_external_users(self):
        ctx = _make_ctx(
            user_map={"U001": "alice@example.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        user_resolver = MagicMock()
        user_resolver.is_external_user.return_value = False

        result = channel_has_external_users(ctx, user_resolver, "general")
        assert result is False

    def test_has_external_user(self):
        ctx = _make_ctx(
            user_map={"U001": "alice@example.com", "U002": "ext@other.com"},
            channels_meta={"general": {"members": ["U001", "U002"]}},
        )
        user_resolver = MagicMock()
        user_resolver.is_external_user.side_effect = lambda email: (
            email == "ext@other.com"
        )

        result = channel_has_external_users(ctx, user_resolver, "general")
        assert result is True

    def test_no_members_in_metadata(self, tmp_path):
        """When metadata has no members, scans message files."""
        ch_dir = tmp_path / "empty-channel"
        ch_dir.mkdir()

        ctx = _make_ctx(
            channels_meta={"empty-channel": {}},
            export_root=tmp_path,
        )
        user_resolver = MagicMock()

        result = channel_has_external_users(ctx, user_resolver, "empty-channel")
        assert result is False

    def test_unmapped_user_skipped(self):
        ctx = _make_ctx(
            user_map={},  # No mappings
            channels_meta={"general": {"members": ["U001"]}},
        )
        user_resolver = MagicMock()

        result = channel_has_external_users(ctx, user_resolver, "general")
        assert result is False

    def test_scans_message_files_for_users(self, tmp_path):
        """When metadata has no members, extracts user IDs from messages."""
        ch_dir = tmp_path / "dev"
        ch_dir.mkdir()
        msgs = [
            {"type": "message", "user": "U010", "text": "hello"},
            {"type": "message", "user": "U011", "text": "world"},
        ]
        (ch_dir / "2024-01-01.json").write_text(json.dumps(msgs))

        ctx = _make_ctx(
            user_map={"U010": "internal@example.com", "U011": "ext@other.com"},
            channels_meta={"dev": {}},
            export_root=tmp_path,
        )
        user_resolver = MagicMock()
        user_resolver.is_external_user.side_effect = lambda e: e == "ext@other.com"

        assert channel_has_external_users(ctx, user_resolver, "dev") is True

    def test_bot_user_not_counted_as_external(self):
        """Bot users flagged in users_without_email are not external."""
        ctx = _make_ctx(
            user_map={"U001": "bot@other.com"},
            users_without_email=[{"id": "U001", "is_bot": True}],
            channels_meta={"general": {"members": ["U001"]}},
        )
        user_resolver = MagicMock()
        user_resolver.is_external_user.return_value = True

        assert channel_has_external_users(ctx, user_resolver, "general") is False

    def test_app_user_not_counted_as_external(self):
        """App users flagged in users_without_email are not external."""
        ctx = _make_ctx(
            user_map={"U001": "app@other.com"},
            users_without_email=[{"id": "U001", "is_app_user": True}],
            channels_meta={"general": {"members": ["U001"]}},
        )
        user_resolver = MagicMock()
        user_resolver.is_external_user.return_value = True

        assert channel_has_external_users(ctx, user_resolver, "general") is False

    def test_malformed_json_file_handled(self, tmp_path):
        """Bad JSON in message files is gracefully handled."""
        ch_dir = tmp_path / "broken"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text("NOT JSON")

        ctx = _make_ctx(
            channels_meta={"broken": {}},
            export_root=tmp_path,
        )
        user_resolver = MagicMock()

        # Should not raise; returns False because no users found
        assert channel_has_external_users(ctx, user_resolver, "broken") is False

    def test_users_without_email_is_none(self):
        """Handles users_without_email being None instead of a list."""
        ctx = _make_ctx(
            user_map={"U001": "ext@other.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        # Force None via object.__setattr__ on frozen dataclass to test defensive code
        object.__setattr__(ctx, "users_without_email", None)
        user_resolver = MagicMock()
        user_resolver.is_external_user.return_value = True

        assert channel_has_external_users(ctx, user_resolver, "general") is True


# ---------------------------------------------------------------------------
# create_space
# ---------------------------------------------------------------------------


class TestCreateSpace:
    """Tests for create_space()."""

    def _setup(self, channels_meta=None, dry_run=False, user_map=None):
        """Common setup: returns (ctx, state, chat, user_resolver)."""
        ctx = _make_ctx(
            channels_meta=channels_meta or {},
            dry_run=dry_run,
            user_map=user_map or {},
        )
        state = MigrationState()
        state.progress.migration_summary = _default_migration_summary()
        chat = MagicMock()
        user_resolver = MagicMock()
        user_resolver.is_external_user.return_value = False
        return ctx, state, chat, user_resolver

    def test_dry_run_delegates_to_chat_service(self):
        """Dry run no longer short-circuits — DI stubs handle API calls."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"general": {"members": []}},
            dry_run=True,
        )
        chat.create_space.return_value = {"name": "spaces/dry-run-stub"}

        result = create_space(ctx, state, chat, user_resolver, "general")

        assert result == "spaces/dry-run-stub"
        assert state.progress.migration_summary["spaces_created"] == 1
        assert state.spaces.created_spaces["general"] == "spaces/dry-run-stub"
        chat.create_space.assert_called_once()

    def test_creates_space_via_api(self):
        """Non-dry-run calls the Google Chat API to create a space."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"dev": {"members": []}},
        )
        chat.create_space.return_value = {"name": "spaces/abc123"}

        result = create_space(ctx, state, chat, user_resolver, "dev")

        assert result == "spaces/abc123"
        assert state.progress.migration_summary["spaces_created"] == 1
        assert state.spaces.created_spaces["dev"] == "spaces/abc123"

    def test_space_body_has_import_mode(self):
        """The API request body includes importMode and threading state."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"dev": {"members": []}},
        )
        chat.create_space.return_value = {"name": "spaces/xyz"}

        create_space(ctx, state, chat, user_resolver, "dev")

        chat.create_space.assert_called_once()
        body = chat.create_space.call_args[0][0]
        assert body["importMode"] is True
        assert body["spaceType"] == "SPACE"
        assert body["spaceThreadingState"] == "THREADED_MESSAGES"
        assert body["displayName"] == "Slack #dev"

    def test_channel_creation_time_included(self):
        """When channel metadata has 'created', createTime is set on the space."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"dev": {"created": 1700000000, "members": []}},
        )
        chat.create_space.return_value = {"name": "spaces/xyz"}

        create_space(ctx, state, chat, user_resolver, "dev")

        body = chat.create_space.call_args[0][0]
        assert "createTime" in body

    def test_external_users_flag_set(self):
        """When channel has external users, externalUserAllowed is set."""
        ctx, state, chat, user_resolver = self._setup(
            user_map={"U001": "ext@other.com"},
            channels_meta={"dev": {"members": ["U001"]}},
        )
        user_resolver.is_external_user.return_value = True
        chat.create_space.return_value = {"name": "spaces/xyz"}

        create_space(ctx, state, chat, user_resolver, "dev")

        body = chat.create_space.call_args[0][0]
        assert body["externalUserAllowed"] is True

    def test_space_description_updated_with_purpose_and_topic(self):
        """When channel has purpose/topic, space description is patched."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={
                "dev": {
                    "members": [],
                    "purpose": {"value": "Development discussion"},
                    "topic": {"value": "Sprint 42"},
                }
            },
        )
        chat.create_space.return_value = {"name": "spaces/xyz"}

        create_space(ctx, state, chat, user_resolver, "dev")

        # patch_space() should have been called for the description update
        chat.patch_space.assert_called()

    def test_space_description_only_purpose(self):
        """When channel has only purpose (no topic), description still set."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={
                "dev": {
                    "members": [],
                    "purpose": {"value": "Dev chat"},
                    "topic": {"value": ""},
                }
            },
        )
        chat.create_space.return_value = {"name": "spaces/xyz"}

        create_space(ctx, state, chat, user_resolver, "dev")

        chat.patch_space.assert_called()

    def test_permission_denied_raises_space_permission_error(self):
        """403 PERMISSION_DENIED raises SpacePermissionError."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"dev": {"members": []}},
        )
        error = _make_http_error(403, content=b"PERMISSION_DENIED")
        chat.create_space.side_effect = error

        with pytest.raises(SpacePermissionError):
            create_space(ctx, state, chat, user_resolver, "dev")

    def test_other_http_error_reraises(self):
        """Non-403 HttpErrors propagate."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"dev": {"members": []}},
        )
        error = _make_http_error(500, content=b"Internal Server Error")
        chat.create_space.side_effect = error

        with pytest.raises(HttpError):
            create_space(ctx, state, chat, user_resolver, "dev")

    def test_patch_failure_does_not_raise(self):
        """HttpError during description patch is caught, space still returned."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={
                "dev": {
                    "members": [],
                    "purpose": {"value": "Dev"},
                    "topic": {"value": ""},
                }
            },
        )
        chat.create_space.return_value = {"name": "spaces/xyz"}
        # Patch fails with HttpError
        patch_error = _make_http_error(400, content=b"Bad Request")
        chat.patch_space.side_effect = patch_error

        result = create_space(ctx, state, chat, user_resolver, "dev")
        assert result == "spaces/xyz"

    def test_spaces_with_external_users_tracking(self):
        """create_space stores external user status for the space."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"dev": {"members": []}},
            dry_run=True,
        )
        chat.create_space.return_value = {"name": "spaces/dev"}

        create_space(ctx, state, chat, user_resolver, "dev")

        assert state.progress.spaces_with_external_users["spaces/dev"] is False

    def test_general_channel_display_name_in_api(self):
        """General channel gets '(General)' suffix in the API call."""
        ctx, state, chat, user_resolver = self._setup(
            channels_meta={"general": {"is_general": True, "members": []}},
        )
        chat.create_space.return_value = {"name": "spaces/gen"}

        create_space(ctx, state, chat, user_resolver, "general")

        body = chat.create_space.call_args[0][0]
        assert "(General)" in body["displayName"]


# ---------------------------------------------------------------------------
# add_users_to_space
# ---------------------------------------------------------------------------


class TestAddUsersToSpace:
    """Tests for add_users_to_space()."""

    def _setup_channel_dir(self, tmp_path, channel, messages):
        """Create a channel directory with message files."""
        ch_dir = tmp_path / channel
        ch_dir.mkdir(exist_ok=True)
        (ch_dir / "2024-01-01.json").write_text(json.dumps(messages))
        return ch_dir

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_dry_run_processes_via_noop_service(self, mock_tqdm, mock_sleep, tmp_path):
        """In dry run mode, API calls flow through the no-op service layer."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=True,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        # With DI, dry-run calls flow through mock (DryRunChatService in prod)
        chat.create_membership.assert_called()

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_adds_user_with_membership_body(self, mock_tqdm, mock_sleep, tmp_path):
        """Users are added to the space with createTime and deleteTime."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        # Verify that create_membership was called on the adapter
        chat.create_membership.assert_called()

        # Check the membership body
        call_kwargs = chat.create_membership.call_args.kwargs
        assert call_kwargs["parent"] == "spaces/dev"
        body = call_kwargs["body"]
        assert body["member"]["name"] == "users/alice@example.com"
        assert body["member"]["type"] == "HUMAN"
        assert "createTime" in body
        assert "deleteTime" in body

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_user_without_email_skipped(self, mock_tqdm, mock_sleep, tmp_path):
        """Users with no email mapping are skipped."""
        msgs = [{"type": "message", "user": "U999", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={},  # U999 not mapped
            channels_meta={"dev": {"members": []}},
            export_root=tmp_path,
        )

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        # create_membership should not be called since user has no email
        chat.create_membership.assert_not_called()

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_409_conflict_counted_as_success(self, mock_tqdm, mock_sleep, tmp_path):
        """409 Conflict (user already in space) is treated as success."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        error = _make_http_error(409, content=b"Conflict")
        chat.create_membership.side_effect = error

        # Should not raise
        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_other_http_error_counted_as_failure(self, mock_tqdm, mock_sleep, tmp_path):
        """Non-409 HttpErrors count as failures but don't raise."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        error = _make_http_error(500, content=b"Server Error")
        chat.create_membership.side_effect = error

        # Should not raise
        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_unexpected_error_counted_as_failure(self, mock_tqdm, mock_sleep, tmp_path):
        """Generic exceptions count as failures but don't raise."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        chat.create_membership.side_effect = RuntimeError("boom")

        # Should not raise
        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_join_time_from_channel_join_event(self, mock_tqdm, mock_sleep, tmp_path):
        """Explicit channel_join events are used as join times."""
        msgs = [
            {
                "type": "message",
                "subtype": "channel_join",
                "user": "U001",
                "ts": "1699000000.000000",
            },
            {"type": "message", "user": "U001", "ts": "1700000000.000000"},
        ]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        body = chat.create_membership.call_args.kwargs["body"]
        # The join time should use the channel_join timestamp (1699000000 -> 2023-11-03)
        assert "2023-11-03" in body["createTime"]

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_leave_time_from_channel_leave_event(self, mock_tqdm, mock_sleep, tmp_path):
        """channel_leave events set the leave time."""
        msgs = [
            {
                "type": "message",
                "subtype": "channel_join",
                "user": "U001",
                "ts": "1699000000.000000",
            },
            {"type": "message", "user": "U001", "ts": "1700000000.000000"},
            {
                "type": "message",
                "subtype": "channel_leave",
                "user": "U001",
                "ts": "1701000000.000000",
            },
        ]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": []}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        body = chat.create_membership.call_args.kwargs["body"]
        # Leave time should use the channel_leave timestamp (1701000000 -> 2023-11-26)
        assert "2023-11-26" in body["deleteTime"]

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_external_user_tracked(self, mock_tqdm, mock_sleep, tmp_path):
        """External users are added to state.users.external_users."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "ext@other.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "ext@other.com"
        ur.is_external_user.return_value = True

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        assert "ext@other.com" in state.users.external_users

    def test_active_users_stored_on_state(self, tmp_path):
        """Active users from metadata are stored on state for later use."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=True,
        )

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        assert "U001" in state.progress.active_users_by_channel["dev"]

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_metadata_members_added_with_default_join_time(
        self, mock_tqdm, mock_sleep, tmp_path
    ):
        """Members in metadata but not in messages get default join time."""
        # No messages at all in the channel
        ch_dir = tmp_path / "dev"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(json.dumps([]))

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U050": "quiet@example.com"},
            channels_meta={"dev": {"members": ["U050"]}},
            export_root=tmp_path,
        )
        ur.get_internal_email.return_value = "quiet@example.com"
        ur.is_external_user.return_value = False

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        body = chat.create_membership.call_args.kwargs["body"]
        assert body["createTime"] == DEFAULT_FALLBACK_JOIN_TIME

    def test_malformed_file_in_channel_dir(self, tmp_path):
        """Malformed JSON files in the channel directory are handled."""
        ch_dir = tmp_path / "broken"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text("INVALID JSON")

        ctx, state, chat, ur = _make_membership_deps(
            channels_meta={"broken": {"members": []}},
            export_root=tmp_path,
            dry_run=True,
        )

        # Should not raise
        add_users_to_space(ctx, state, chat, ur, "spaces/broken", "broken")

    @patch("slack_chat_migrator.services.spaces.historical_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.historical_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_bot_user_ids_filtered_from_membership(
        self, mock_tqdm, mock_sleep, tmp_path
    ):
        """Bot user IDs in ctx.bot_user_ids are excluded from membership."""
        msgs = [
            {"type": "message", "user": "U001", "ts": "1700000000.000000"},
            {"type": "message", "user": "B001", "ts": "1700000001.000000"},
        ]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001", "B001"]}},
            export_root=tmp_path,
            bot_user_ids=frozenset({"B001"}),
        )
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        add_users_to_space(ctx, state, chat, ur, "spaces/dev", "dev")

        # Only U001 should be added, B001 should be filtered out
        assert chat.create_membership.call_count == 1
        body = chat.create_membership.call_args.kwargs["body"]
        assert "alice@example.com" in body["member"]["name"]
        # B001 should not be in active_users
        assert "B001" not in state.progress.active_users_by_channel["dev"]


# ---------------------------------------------------------------------------
# add_regular_members
# ---------------------------------------------------------------------------


class TestAddRegularMembers:
    """Tests for add_regular_members()."""

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_dry_run_processes_via_noop_service(self, mock_tqdm, mock_sleep):
        """In dry run mode, API calls flow through the no-op service layer."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
            dry_run=True,
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False
        # Mock the members list for _verify_and_handle_admin
        chat.list_memberships.return_value = {"memberships": []}

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        # With DI, dry-run calls flow through mock (DryRunChatService in prod)
        chat.create_membership.assert_called()

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_adds_active_users_as_regular_members(self, mock_tqdm, mock_sleep):
        """Active users are added via the memberships API."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False
        # Members list for verification
        chat.list_memberships.return_value = {"memberships": []}

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        chat.create_membership.assert_called()
        body = chat.create_membership.call_args.kwargs["body"]
        assert body["member"]["name"] == "users/alice@example.com"
        assert body["member"]["type"] == "HUMAN"
        # No createTime/deleteTime for regular members
        assert "createTime" not in body
        assert "deleteTime" not in body

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_unmapped_user_skipped(self, mock_tqdm, mock_sleep):
        """Users with no email mapping are skipped."""
        ctx, state, chat, ur = _make_membership_deps()
        state.progress.active_users_by_channel = {"dev": {"U999"}}
        chat.list_memberships.return_value = {"memberships": []}

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        chat.create_membership.assert_not_called()

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_409_conflict_counted_as_success(self, mock_tqdm, mock_sleep):
        """409 Conflict is treated as a successful addition."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        error = _make_http_error(409, content=b"Conflict")
        chat.create_membership.side_effect = error
        chat.list_memberships.return_value = {"memberships": []}

        # Should not raise
        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_400_error_counted_as_failure(self, mock_tqdm, mock_sleep):
        """400 Bad Request is counted as failure."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        error = _make_http_error(400, content=b"Bad Request")
        chat.create_membership.side_effect = error
        chat.list_memberships.return_value = {"memberships": []}

        # Should not raise
        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_403_error_logged_with_extra_detail(self, mock_tqdm, mock_sleep):
        """403/404 errors get additional error logging."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        error = _make_http_error(403, content=b"Forbidden")
        chat.create_membership.side_effect = error
        chat.list_memberships.return_value = {"memberships": []}

        # Should not raise
        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_unexpected_exception_counted_as_failure(self, mock_tqdm, mock_sleep):
        """Generic exceptions are caught and counted as failures."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        chat.create_membership.side_effect = RuntimeError("boom")
        chat.list_memberships.return_value = {"memberships": []}

        # Should not raise
        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

    def test_no_active_users_tracked_returns_early(self):
        """When no active users are found at all, function returns early."""
        ctx, state, chat, ur = _make_membership_deps(
            export_root=Path("/nonexistent"),
        )
        state.progress.active_users_by_channel = {}

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        chat.create_membership.assert_not_called()

    def test_fallback_loads_from_channels_json(self, tmp_path):
        """When active_users_by_channel is missing, loads from channels.json."""
        channels_data = [{"name": "dev", "members": ["U001", "U002"]}]
        (tmp_path / "channels.json").write_text(json.dumps(channels_data))

        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com", "U002": "bob@example.com"},
            export_root=tmp_path,
            dry_run=True,
        )
        state.progress.active_users_by_channel = {}
        ur.get_internal_email.side_effect = lambda uid, email: email
        ur.is_external_user.return_value = False

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        # Verify the fallback loaded the members
        assert state.progress.active_users_by_channel["dev"] == ["U001", "U002"]

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_admin_removed_if_not_in_channel(self, mock_tqdm, mock_sleep):
        """Workspace admin is removed from space if not in the original channel."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={
                "U001": "alice@example.com",
                "U_ADMIN": "admin@example.com",
            },
            workspace_admin="admin@example.com",
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        # Admin is in the members list returned by the API
        chat.list_memberships.return_value = {
            "memberships": [
                {
                    "name": "spaces/dev/members/admin",
                    "member": {"name": "users/admin@example.com", "type": "HUMAN"},
                }
            ]
        }

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        # Admin should be removed (delete called with admin membership name)
        chat.delete_membership.assert_called()

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_admin_kept_if_in_channel(self, mock_tqdm, mock_sleep):
        """Workspace admin is NOT removed if they were in the original channel."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={
                "U001": "alice@example.com",
                "U_ADMIN": "admin@example.com",
            },
            workspace_admin="admin@example.com",
        )
        # Admin (U_ADMIN) IS in active users
        state.progress.active_users_by_channel = {"dev": {"U001", "U_ADMIN"}}
        ur.get_internal_email.side_effect = lambda uid, email: email
        ur.is_external_user.return_value = False

        chat.list_memberships.return_value = {
            "memberships": [
                {
                    "name": "spaces/dev/members/admin",
                    "member": {"name": "users/admin@example.com", "type": "HUMAN"},
                }
            ]
        }

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        # Admin should NOT be removed
        chat.delete_membership.assert_not_called()

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_external_user_enables_external_access(self, mock_tqdm, mock_sleep):
        """When active users include external users, external access is enabled."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "ext@other.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "ext@other.com"
        ur.is_external_user.return_value = True

        # Space currently doesn't have external users allowed
        chat.get_space.return_value = {"externalUserAllowed": False}
        chat.list_memberships.return_value = {"memberships": []}

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        # Space should be patched to enable external user access
        chat.patch_space.assert_called()

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_external_user_tracked_in_external_users_set(self, mock_tqdm, mock_sleep):
        """External users are added to state.users.external_users."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "ext@other.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "ext@other.com"
        ur.is_external_user.return_value = True

        chat.get_space.return_value = {"externalUserAllowed": True}
        chat.list_memberships.return_value = {"memberships": []}

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        assert "ext@other.com" in state.users.external_users

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_drive_folder_permissions_updated(self, mock_tqdm, mock_sleep):
        """Drive folder permissions are updated for active members."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False
        chat.list_memberships.return_value = {"memberships": []}

        # Set up file_handler with folder_manager
        file_handler = MagicMock()
        file_handler.folder_manager.get_channel_folder_id.return_value = "folder123"

        add_regular_members(ctx, state, chat, ur, file_handler, "spaces/dev", "dev")

        file_handler.folder_manager.set_channel_folder_permissions.assert_called_once()

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_verification_failure_does_not_raise(self, mock_tqdm, mock_sleep):
        """Failure during member verification doesn't propagate."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={"U001": "alice@example.com"},
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        # Make the list call fail
        chat.list_memberships.side_effect = HttpError(
            Response({"status": "500"}), b"cannot list"
        )

        # Should not raise
        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

    @patch("slack_chat_migrator.services.spaces.regular_membership.time.sleep")
    @patch(
        "slack_chat_migrator.services.spaces.regular_membership.tqdm",
        side_effect=lambda x, **kw: x,
    )
    def test_admin_found_by_email_field(self, mock_tqdm, mock_sleep):
        """Admin membership can be found via 'email' field instead of 'name'."""
        ctx, state, chat, ur = _make_membership_deps(
            user_map={
                "U001": "alice@example.com",
                "U_ADMIN": "admin@example.com",
            },
            workspace_admin="admin@example.com",
        )
        state.progress.active_users_by_channel = {"dev": {"U001"}}
        ur.get_internal_email.return_value = "alice@example.com"
        ur.is_external_user.return_value = False

        # Admin found via email field, not name field
        chat.list_memberships.return_value = {
            "memberships": [
                {
                    "name": "spaces/dev/members/admin",
                    "member": {
                        "name": "users/some-uid",
                        "email": "admin@example.com",
                        "type": "HUMAN",
                    },
                }
            ]
        }

        add_regular_members(ctx, state, chat, ur, None, "spaces/dev", "dev")

        # Admin should be removed
        chat.delete_membership.assert_called()


# ---------------------------------------------------------------------------
# Named constants
# ---------------------------------------------------------------------------


class TestNamedConstants:
    """Verify the named constants have correct values."""

    def test_import_mode_days_limit(self):
        assert IMPORT_MODE_DAYS_LIMIT == 90

    def test_default_fallback_join_time(self):
        assert DEFAULT_FALLBACK_JOIN_TIME == "2020-01-01T00:00:00Z"

    def test_historical_delete_offset(self):
        assert HISTORICAL_DELETE_TIME_OFFSET_SECONDS == 5

    def test_earliest_message_offset(self):
        assert EARLIEST_MESSAGE_OFFSET_MINUTES == 2

    def test_first_message_offset(self):
        assert FIRST_MESSAGE_OFFSET_MINUTES == 1
