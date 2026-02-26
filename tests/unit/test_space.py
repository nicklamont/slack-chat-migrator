"""Unit tests for the space management module."""

import json
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.state import MigrationState, _default_migration_summary
from slack_migrator.services.membership_manager import (
    DEFAULT_FALLBACK_JOIN_TIME,
    EARLIEST_MESSAGE_OFFSET_MINUTES,
    FIRST_MESSAGE_OFFSET_MINUTES,
    HISTORICAL_DELETE_TIME_OFFSET_SECONDS,
    add_regular_members,
    add_users_to_space,
)
from slack_migrator.services.space_creator import (
    IMPORT_MODE_DAYS_LIMIT,
    channel_has_external_users,
    create_space,
)


def _make_migrator(
    user_map=None,
    workspace_domain="example.com",
    channels_meta=None,
    export_root=None,
    dry_run=False,
    workspace_admin="admin@example.com",
):
    """Create a mock migrator with common attributes."""
    migrator = MagicMock()
    migrator.state = MigrationState()
    migrator.user_map = user_map or {}
    migrator.workspace_domain = workspace_domain
    migrator.channels_meta = channels_meta or {}
    migrator.users_without_email = []
    migrator.dry_run = dry_run
    migrator.workspace_admin = workspace_admin
    migrator.state.migration_summary = _default_migration_summary()
    migrator.state.created_spaces = {}
    migrator.state.external_users = set()
    migrator.config = MigrationConfig()
    migrator.state.current_channel = "general"
    if export_root:
        migrator.export_root = export_root
    return migrator


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
        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        migrator.user_resolver.is_external_user.return_value = False

        result = channel_has_external_users(migrator, "general")
        assert result is False

    def test_has_external_user(self):
        migrator = _make_migrator(
            user_map={"U001": "alice@example.com", "U002": "ext@other.com"},
            channels_meta={"general": {"members": ["U001", "U002"]}},
        )
        migrator.user_resolver.is_external_user.side_effect = lambda email: (
            email == "ext@other.com"
        )

        result = channel_has_external_users(migrator, "general")
        assert result is True

    def test_no_members_in_metadata(self, tmp_path):
        """When metadata has no members, scans message files."""
        # Create channel directory with no message files
        ch_dir = tmp_path / "empty-channel"
        ch_dir.mkdir()

        migrator = _make_migrator(
            channels_meta={"empty-channel": {}},
            export_root=tmp_path,
        )

        result = channel_has_external_users(migrator, "empty-channel")
        assert result is False

    def test_unmapped_user_skipped(self):
        migrator = _make_migrator(
            user_map={},  # No mappings
            channels_meta={"general": {"members": ["U001"]}},
        )

        result = channel_has_external_users(migrator, "general")
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

        migrator = _make_migrator(
            user_map={"U010": "internal@example.com", "U011": "ext@other.com"},
            channels_meta={"dev": {}},
            export_root=tmp_path,
        )
        migrator.user_resolver.is_external_user.side_effect = lambda e: (
            e == "ext@other.com"
        )

        assert channel_has_external_users(migrator, "dev") is True

    def test_bot_user_not_counted_as_external(self):
        """Bot users flagged in users_without_email are not external."""
        migrator = _make_migrator(
            user_map={"U001": "bot@other.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        migrator.users_without_email = [{"id": "U001", "is_bot": True}]
        migrator.user_resolver.is_external_user.return_value = True

        assert channel_has_external_users(migrator, "general") is False

    def test_app_user_not_counted_as_external(self):
        """App users flagged in users_without_email are not external."""
        migrator = _make_migrator(
            user_map={"U001": "app@other.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        migrator.users_without_email = [{"id": "U001", "is_app_user": True}]
        migrator.user_resolver.is_external_user.return_value = True

        assert channel_has_external_users(migrator, "general") is False

    def test_malformed_json_file_handled(self, tmp_path):
        """Bad JSON in message files is gracefully handled."""
        ch_dir = tmp_path / "broken"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text("NOT JSON")

        migrator = _make_migrator(
            channels_meta={"broken": {}},
            export_root=tmp_path,
        )

        # Should not raise; returns False because no users found
        assert channel_has_external_users(migrator, "broken") is False

    def test_users_without_email_is_none(self):
        """Handles users_without_email being None instead of a list."""
        migrator = _make_migrator(
            user_map={"U001": "ext@other.com"},
            channels_meta={"general": {"members": ["U001"]}},
        )
        migrator.users_without_email = None
        migrator.user_resolver.is_external_user.return_value = True

        assert channel_has_external_users(migrator, "general") is True


# ---------------------------------------------------------------------------
# create_space
# ---------------------------------------------------------------------------


class TestCreateSpace:
    """Tests for create_space()."""

    def test_dry_run_returns_space_name(self):
        """Dry run creates a fake space name without API calls."""
        migrator = _make_migrator(
            channels_meta={"general": {"members": []}},
            dry_run=True,
        )
        migrator.user_resolver.is_external_user.return_value = False

        result = create_space(migrator, "general")

        assert result == "spaces/general"
        assert migrator.state.migration_summary["spaces_created"] == 1
        assert migrator.state.created_spaces["general"] == "spaces/general"
        # chat.spaces().create() should NOT be called in dry run
        migrator.chat.spaces().create.assert_not_called()

    def test_dry_run_general_channel_display_name(self):
        """General channel gets '(General)' suffix in dry run."""
        migrator = _make_migrator(
            channels_meta={"general": {"is_general": True, "members": []}},
            dry_run=True,
        )
        migrator.user_resolver.is_external_user.return_value = False

        result = create_space(migrator, "general")
        assert result == "spaces/general"

    def test_creates_space_via_api(self):
        """Non-dry-run calls the Google Chat API to create a space."""
        migrator = _make_migrator(
            channels_meta={"dev": {"members": []}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_execute = MagicMock(return_value={"name": "spaces/abc123"})
        migrator.chat.spaces().create.return_value.execute = mock_execute

        result = create_space(migrator, "dev")

        assert result == "spaces/abc123"
        assert migrator.state.migration_summary["spaces_created"] == 1
        assert migrator.state.created_spaces["dev"] == "spaces/abc123"

    def test_space_body_has_import_mode(self):
        """The API request body includes importMode and threading state."""
        migrator = _make_migrator(
            channels_meta={"dev": {"members": []}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/xyz"}

        create_space(migrator, "dev")

        call_kwargs = mock_create.call_args
        body = (
            call_kwargs[1]["body"]
            if "body" in (call_kwargs[1] or {})
            else call_kwargs[0][0]
            if call_kwargs[0]
            else call_kwargs.kwargs["body"]
        )
        assert body["importMode"] is True
        assert body["spaceType"] == "SPACE"
        assert body["spaceThreadingState"] == "THREADED_MESSAGES"
        assert body["displayName"] == "Slack #dev"

    def test_channel_creation_time_included(self):
        """When channel metadata has 'created', createTime is set on the space."""
        migrator = _make_migrator(
            channels_meta={"dev": {"created": 1700000000, "members": []}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/xyz"}

        create_space(migrator, "dev")

        body = mock_create.call_args.kwargs["body"]
        assert "createTime" in body

    def test_external_users_flag_set(self):
        """When channel has external users, externalUserAllowed is set."""
        migrator = _make_migrator(
            user_map={"U001": "ext@other.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = True
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/xyz"}

        create_space(migrator, "dev")

        body = mock_create.call_args.kwargs["body"]
        assert body["externalUserAllowed"] is True

    def test_space_description_updated_with_purpose_and_topic(self):
        """When channel has purpose/topic, space description is patched."""
        migrator = _make_migrator(
            channels_meta={
                "dev": {
                    "members": [],
                    "purpose": {"value": "Development discussion"},
                    "topic": {"value": "Sprint 42"},
                }
            },
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/xyz"}

        create_space(migrator, "dev")

        # patch() should have been called for the description update
        migrator.chat.spaces().patch.assert_called()

    def test_space_description_only_purpose(self):
        """When channel has only purpose (no topic), description still set."""
        migrator = _make_migrator(
            channels_meta={
                "dev": {
                    "members": [],
                    "purpose": {"value": "Dev chat"},
                    "topic": {"value": ""},
                }
            },
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/xyz"}

        create_space(migrator, "dev")

        migrator.chat.spaces().patch.assert_called()

    def test_permission_denied_returns_error_string(self):
        """403 PERMISSION_DENIED returns an error marker, does not raise."""
        migrator = _make_migrator(
            channels_meta={"dev": {"members": []}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        error = _make_http_error(403, content=b"PERMISSION_DENIED")
        migrator.chat.spaces().create.return_value.execute.side_effect = error

        result = create_space(migrator, "dev")

        assert result == "ERROR_NO_PERMISSION_dev"

    def test_other_http_error_reraises(self):
        """Non-403 HttpErrors propagate."""
        migrator = _make_migrator(
            channels_meta={"dev": {"members": []}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        error = _make_http_error(500, content=b"Internal Server Error")
        migrator.chat.spaces().create.return_value.execute.side_effect = error

        import pytest

        with pytest.raises(HttpError):
            create_space(migrator, "dev")

    def test_patch_failure_does_not_raise(self):
        """HttpError during description patch is caught, space still returned."""
        migrator = _make_migrator(
            channels_meta={
                "dev": {
                    "members": [],
                    "purpose": {"value": "Dev"},
                    "topic": {"value": ""},
                }
            },
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/xyz"}
        # Patch fails with HttpError
        patch_error = _make_http_error(400, content=b"Bad Request")
        migrator.chat.spaces().patch.return_value.execute.side_effect = patch_error

        result = create_space(migrator, "dev")
        assert result == "spaces/xyz"

    def test_spaces_with_external_users_tracking(self):
        """create_space stores external user status for the space."""
        migrator = _make_migrator(
            channels_meta={"dev": {"members": []}},
            dry_run=True,
        )
        migrator.user_resolver.is_external_user.return_value = False

        create_space(migrator, "dev")

        assert migrator.state.spaces_with_external_users["spaces/dev"] is False

    def test_general_channel_display_name_in_api(self):
        """General channel gets '(General)' suffix in the API call."""
        migrator = _make_migrator(
            channels_meta={"general": {"is_general": True, "members": []}},
            dry_run=False,
        )
        migrator.user_resolver.is_external_user.return_value = False
        mock_create = migrator.chat.spaces().create
        mock_create.return_value.execute.return_value = {"name": "spaces/gen"}

        create_space(migrator, "general")

        body = mock_create.call_args.kwargs["body"]
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

    def test_dry_run_returns_early(self, tmp_path):
        """In dry run mode, no API calls are made."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=True,
        )

        add_users_to_space(migrator, "spaces/dev", "dev")

        # No API calls in dry run
        migrator.chat.spaces().members().create.assert_not_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_adds_user_with_membership_body(self, mock_tqdm, mock_sleep, tmp_path):
        """Users are added to the space with createTime and deleteTime."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        add_users_to_space(migrator, "spaces/dev", "dev")

        # Verify that create was called on the members API
        create_call = migrator.chat.spaces().members().create
        create_call.assert_called()

        # Check the membership body
        call_kwargs = create_call.call_args.kwargs
        assert call_kwargs["parent"] == "spaces/dev"
        body = call_kwargs["body"]
        assert body["member"]["name"] == "users/alice@example.com"
        assert body["member"]["type"] == "HUMAN"
        assert "createTime" in body
        assert "deleteTime" in body

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_user_without_email_skipped(self, mock_tqdm, mock_sleep, tmp_path):
        """Users with no email mapping are skipped."""
        msgs = [{"type": "message", "user": "U999", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={},  # U999 not mapped
            channels_meta={"dev": {"members": []}},
            export_root=tmp_path,
            dry_run=False,
        )

        add_users_to_space(migrator, "spaces/dev", "dev")

        # create should not be called since user has no email
        migrator.chat.spaces().members().create.return_value.execute.assert_not_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_409_conflict_counted_as_success(self, mock_tqdm, mock_sleep, tmp_path):
        """409 Conflict (user already in space) is treated as success."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        error = _make_http_error(409, content=b"Conflict")
        migrator.chat.spaces().members().create.return_value.execute.side_effect = error

        # Should not raise
        add_users_to_space(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_other_http_error_counted_as_failure(self, mock_tqdm, mock_sleep, tmp_path):
        """Non-409 HttpErrors count as failures but don't raise."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        error = _make_http_error(500, content=b"Server Error")
        migrator.chat.spaces().members().create.return_value.execute.side_effect = error

        # Should not raise
        add_users_to_space(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_unexpected_error_counted_as_failure(self, mock_tqdm, mock_sleep, tmp_path):
        """Generic exceptions count as failures but don't raise."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        migrator.chat.spaces().members().create.return_value.execute.side_effect = (
            RuntimeError("boom")
        )

        # Should not raise
        add_users_to_space(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
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

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        add_users_to_space(migrator, "spaces/dev", "dev")

        body = migrator.chat.spaces().members().create.call_args.kwargs["body"]
        # The join time should use the channel_join timestamp (1699000000 -> 2023-11-03)
        assert "2023-11-03" in body["createTime"]

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
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

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": []}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        add_users_to_space(migrator, "spaces/dev", "dev")

        body = migrator.chat.spaces().members().create.call_args.kwargs["body"]
        # Leave time should use the channel_leave timestamp (1701000000 -> 2023-11-26)
        assert "2023-11-26" in body["deleteTime"]

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_external_user_tracked(self, mock_tqdm, mock_sleep, tmp_path):
        """External users are added to migrator.state.external_users."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "ext@other.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "ext@other.com"
        migrator.user_resolver.is_external_user.return_value = True

        add_users_to_space(migrator, "spaces/dev", "dev")

        assert "ext@other.com" in migrator.state.external_users

    def test_active_users_stored_on_migrator(self, tmp_path):
        """Active users from metadata are stored on migrator for later use."""
        msgs = [{"type": "message", "user": "U001", "ts": "1700000000.000000"}]
        self._setup_channel_dir(tmp_path, "dev", msgs)

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com"},
            channels_meta={"dev": {"members": ["U001"]}},
            export_root=tmp_path,
            dry_run=True,
        )

        add_users_to_space(migrator, "spaces/dev", "dev")

        assert "U001" in migrator.state.active_users_by_channel["dev"]

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_metadata_members_added_with_default_join_time(
        self, mock_tqdm, mock_sleep, tmp_path
    ):
        """Members in metadata but not in messages get default join time."""
        # No messages at all in the channel
        ch_dir = tmp_path / "dev"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text(json.dumps([]))

        migrator = _make_migrator(
            user_map={"U050": "quiet@example.com"},
            channels_meta={"dev": {"members": ["U050"]}},
            export_root=tmp_path,
            dry_run=False,
        )
        migrator.user_resolver.get_internal_email.return_value = "quiet@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        add_users_to_space(migrator, "spaces/dev", "dev")

        body = migrator.chat.spaces().members().create.call_args.kwargs["body"]
        assert body["createTime"] == DEFAULT_FALLBACK_JOIN_TIME

    def test_malformed_file_in_channel_dir(self, tmp_path):
        """Malformed JSON files in the channel directory are handled."""
        ch_dir = tmp_path / "broken"
        ch_dir.mkdir()
        (ch_dir / "2024-01-01.json").write_text("INVALID JSON")

        migrator = _make_migrator(
            channels_meta={"broken": {"members": []}},
            export_root=tmp_path,
            dry_run=True,
        )

        # Should not raise
        add_users_to_space(migrator, "spaces/broken", "broken")


# ---------------------------------------------------------------------------
# add_regular_members
# ---------------------------------------------------------------------------


class TestAddRegularMembers:
    """Tests for add_regular_members()."""

    def test_dry_run_returns_early(self):
        """In dry run mode, no API calls are made."""
        migrator = _make_migrator(dry_run=True)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        add_regular_members(migrator, "spaces/dev", "dev")

        migrator.chat.spaces().members().create.assert_not_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_adds_active_users_as_regular_members(self, mock_tqdm, mock_sleep):
        """Active users are added via the memberships API."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False
        # Members list for verification
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        add_regular_members(migrator, "spaces/dev", "dev")

        create_call = migrator.chat.spaces().members().create
        create_call.assert_called()
        body = create_call.call_args.kwargs["body"]
        assert body["member"]["name"] == "users/alice@example.com"
        assert body["member"]["type"] == "HUMAN"
        # No createTime/deleteTime for regular members
        assert "createTime" not in body
        assert "deleteTime" not in body

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_unmapped_user_skipped(self, mock_tqdm, mock_sleep):
        """Users with no email mapping are skipped."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U999"}}
        migrator.user_map = {}  # no mapping
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        add_regular_members(migrator, "spaces/dev", "dev")

        migrator.chat.spaces().members().create.return_value.execute.assert_not_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_409_conflict_counted_as_success(self, mock_tqdm, mock_sleep):
        """409 Conflict is treated as a successful addition."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        error = _make_http_error(409, content=b"Conflict")
        migrator.chat.spaces().members().create.return_value.execute.side_effect = error
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        # Should not raise
        add_regular_members(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_400_error_counted_as_failure(self, mock_tqdm, mock_sleep):
        """400 Bad Request is counted as failure."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        error = _make_http_error(400, content=b"Bad Request")
        migrator.chat.spaces().members().create.return_value.execute.side_effect = error
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        # Should not raise
        add_regular_members(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_403_error_logged_with_extra_detail(self, mock_tqdm, mock_sleep):
        """403/404 errors get additional error logging."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        error = _make_http_error(403, content=b"Forbidden")
        migrator.chat.spaces().members().create.return_value.execute.side_effect = error
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        # Should not raise
        add_regular_members(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_unexpected_exception_counted_as_failure(self, mock_tqdm, mock_sleep):
        """Generic exceptions are caught and counted as failures."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        migrator.chat.spaces().members().create.return_value.execute.side_effect = (
            RuntimeError("boom")
        )
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        # Should not raise
        add_regular_members(migrator, "spaces/dev", "dev")

    def test_no_active_users_tracked_returns_early(self):
        """When no active users are found at all, function returns early."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {}
        # Remove the export_root/channels.json fallback path
        migrator.export_root = "/nonexistent"

        add_regular_members(migrator, "spaces/dev", "dev")

        migrator.chat.spaces().members().create.assert_not_called()

    def test_fallback_loads_from_channels_json(self, tmp_path):
        """When active_users_by_channel is missing, loads from channels.json."""
        channels_data = [{"name": "dev", "members": ["U001", "U002"]}]
        (tmp_path / "channels.json").write_text(json.dumps(channels_data))

        migrator = _make_migrator(
            user_map={"U001": "alice@example.com", "U002": "bob@example.com"},
            export_root=tmp_path,
            dry_run=True,
        )
        migrator.state.active_users_by_channel = {}
        migrator.user_resolver.get_internal_email.side_effect = lambda uid, email: email
        migrator.user_resolver.is_external_user.return_value = False

        add_regular_members(migrator, "spaces/dev", "dev")

        # Verify the fallback loaded the members
        assert migrator.state.active_users_by_channel["dev"] == ["U001", "U002"]

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_admin_removed_if_not_in_channel(self, mock_tqdm, mock_sleep):
        """Workspace admin is removed from space if not in the original channel."""
        migrator = _make_migrator(
            dry_run=False,
            workspace_admin="admin@example.com",
        )
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {
            "U001": "alice@example.com",
            "U_ADMIN": "admin@example.com",
        }
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        # Admin is in the members list returned by the API
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": [
                {
                    "name": "spaces/dev/members/admin",
                    "member": {"name": "users/admin@example.com", "type": "HUMAN"},
                }
            ]
        }

        add_regular_members(migrator, "spaces/dev", "dev")

        # Admin should be removed (delete called with admin membership name)
        migrator.chat.spaces().members().delete.assert_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_admin_kept_if_in_channel(self, mock_tqdm, mock_sleep):
        """Workspace admin is NOT removed if they were in the original channel."""
        migrator = _make_migrator(
            dry_run=False,
            workspace_admin="admin@example.com",
        )
        # Admin (U_ADMIN) IS in active users
        migrator.state.active_users_by_channel = {"dev": {"U001", "U_ADMIN"}}
        migrator.user_map = {
            "U001": "alice@example.com",
            "U_ADMIN": "admin@example.com",
        }
        migrator.user_resolver.get_internal_email.side_effect = lambda uid, email: email
        migrator.user_resolver.is_external_user.return_value = False

        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": [
                {
                    "name": "spaces/dev/members/admin",
                    "member": {"name": "users/admin@example.com", "type": "HUMAN"},
                }
            ]
        }

        add_regular_members(migrator, "spaces/dev", "dev")

        # Admin should NOT be removed
        migrator.chat.spaces().members().delete.assert_not_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_external_user_enables_external_access(self, mock_tqdm, mock_sleep):
        """When active users include external users, external access is enabled."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "ext@other.com"}
        migrator.user_resolver.get_internal_email.return_value = "ext@other.com"
        migrator.user_resolver.is_external_user.return_value = True

        # Space currently doesn't have external users allowed
        migrator.chat.spaces().get.return_value.execute.return_value = {
            "externalUserAllowed": False
        }
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        add_regular_members(migrator, "spaces/dev", "dev")

        # Space should be patched to enable external user access
        migrator.chat.spaces().patch.assert_called()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_external_user_tracked_in_external_users_set(self, mock_tqdm, mock_sleep):
        """External users are added to migrator.state.external_users."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "ext@other.com"}
        migrator.user_resolver.get_internal_email.return_value = "ext@other.com"
        migrator.user_resolver.is_external_user.return_value = True

        migrator.chat.spaces().get.return_value.execute.return_value = {
            "externalUserAllowed": True
        }
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        add_regular_members(migrator, "spaces/dev", "dev")

        assert "ext@other.com" in migrator.state.external_users

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_drive_folder_permissions_updated(self, mock_tqdm, mock_sleep):
        """Drive folder permissions are updated for active members."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
            "memberships": []
        }

        # Set up file_handler with folder_manager
        migrator.file_handler = MagicMock()
        migrator.file_handler.folder_manager.get_channel_folder_id.return_value = (
            "folder123"
        )

        add_regular_members(migrator, "spaces/dev", "dev")

        migrator.file_handler.folder_manager.set_channel_folder_permissions.assert_called_once()

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_verification_failure_does_not_raise(self, mock_tqdm, mock_sleep):
        """Failure during member verification doesn't propagate."""
        migrator = _make_migrator(dry_run=False)
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        # Make the list call fail
        migrator.chat.spaces().members().list.return_value.execute.side_effect = (
            HttpError(Response({"status": "500"}), b"cannot list")
        )

        # Should not raise
        add_regular_members(migrator, "spaces/dev", "dev")

    @patch("slack_migrator.services.membership_manager.time.sleep")
    @patch(
        "slack_migrator.services.membership_manager.tqdm", side_effect=lambda x, **kw: x
    )
    def test_admin_found_by_email_field(self, mock_tqdm, mock_sleep):
        """Admin membership can be found via 'email' field instead of 'name'."""
        migrator = _make_migrator(
            dry_run=False,
            workspace_admin="admin@example.com",
        )
        migrator.state.active_users_by_channel = {"dev": {"U001"}}
        migrator.user_map = {
            "U001": "alice@example.com",
            "U_ADMIN": "admin@example.com",
        }
        migrator.user_resolver.get_internal_email.return_value = "alice@example.com"
        migrator.user_resolver.is_external_user.return_value = False

        # Admin found via email field, not name field
        migrator.chat.spaces().members().list.return_value.execute.return_value = {
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

        add_regular_members(migrator, "spaces/dev", "dev")

        # Admin should be removed
        migrator.chat.spaces().members().delete.assert_called()


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
