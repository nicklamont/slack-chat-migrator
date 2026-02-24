"""Unit tests for the user resolver module."""

import json
from unittest.mock import MagicMock, patch

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.state import MigrationState
from slack_migrator.services.user_resolver import UserResolver
from slack_migrator.utils.user_validation import UnmappedUserTracker


def _make_resolver(
    channel="general",
    ignore_bots=False,
    user_map=None,
    workspace_admin="admin@example.com",
    workspace_domain="example.com",
    export_root="/tmp/export",
    user_mapping_overrides=None,
    state=None,
    config=None,
    chat=None,
    unmapped_user_tracker=None,
    creds_path=None,
):
    """Create a UserResolver with explicit dependencies for testing."""
    if state is None:
        state = MigrationState()
    state.current_channel = channel

    if config is None:
        config = MigrationConfig(
            ignore_bots=ignore_bots,
            user_mapping_overrides=user_mapping_overrides or {},
        )

    if chat is None:
        chat = MagicMock(name="admin_chat_service")

    if unmapped_user_tracker is None:
        unmapped_user_tracker = UnmappedUserTracker()

    if creds_path is None:
        creds_path = "/tmp/creds.json"

    return UserResolver(
        config=config,
        state=state,
        chat=chat,
        creds_path=creds_path,
        user_map=user_map or {},
        unmapped_user_tracker=unmapped_user_tracker,
        export_root=export_root,
        workspace_admin=workspace_admin,
        workspace_domain=workspace_domain,
    )


# ===========================================================================
# get_delegate
# ===========================================================================


class TestGetDelegate:
    """Tests for UserResolver.get_delegate."""

    def test_empty_email_returns_admin_chat(self):
        resolver = _make_resolver()

        result = resolver.get_delegate("")

        assert result is resolver.chat

    @patch("slack_migrator.services.user_resolver.get_gcp_service")
    def test_valid_email_first_call_creates_and_caches_service(self, mock_get_service):
        resolver = _make_resolver()
        mock_service = MagicMock(name="impersonated_service")
        mock_get_service.return_value = mock_service

        result = resolver.get_delegate("user@example.com")

        mock_get_service.assert_called_once_with(
            str(resolver.creds_path),
            "user@example.com",
            "chat",
            "v1",
            "general",
            max_retries=resolver.config.max_retries,
            retry_delay=resolver.config.retry_delay,
        )
        mock_service.spaces.return_value.list.return_value.execute.assert_called_once()
        assert resolver.state.valid_users["user@example.com"] is True
        assert resolver.state.chat_delegates["user@example.com"] is mock_service
        assert result is mock_service

    @patch("slack_migrator.services.user_resolver.get_gcp_service")
    def test_valid_email_already_cached_returns_from_cache(self, mock_get_service):
        resolver = _make_resolver()
        cached_service = MagicMock(name="cached_service")
        resolver.state.valid_users["user@example.com"] = True
        resolver.state.chat_delegates["user@example.com"] = cached_service

        result = resolver.get_delegate("user@example.com")

        mock_get_service.assert_not_called()
        assert result is cached_service

    @patch("slack_migrator.services.user_resolver.get_gcp_service")
    def test_http_error_falls_back_to_admin_chat(self, mock_get_service):
        resolver = _make_resolver()

        http_error = HttpError(resp=MagicMock(status=403), content=b"Forbidden")
        mock_service = MagicMock()
        mock_service.spaces.return_value.list.return_value.execute.side_effect = (
            http_error
        )
        mock_get_service.return_value = mock_service

        result = resolver.get_delegate("bad@example.com")

        assert result is resolver.chat
        assert resolver.state.valid_users["bad@example.com"] is False

    @patch("slack_migrator.services.user_resolver.get_gcp_service")
    def test_refresh_error_falls_back_to_admin_chat(self, mock_get_service):
        resolver = _make_resolver()

        mock_get_service.side_effect = RefreshError("token expired")

        result = resolver.get_delegate("expired@example.com")

        assert result is resolver.chat
        assert resolver.state.valid_users["expired@example.com"] is False

    @patch("slack_migrator.services.user_resolver.get_gcp_service")
    def test_invalid_user_cached_returns_admin_chat(self, mock_get_service):
        """Second call for a previously-failed user returns admin chat without retrying."""
        resolver = _make_resolver()
        resolver.state.valid_users["bad@example.com"] = False

        result = resolver.get_delegate("bad@example.com")

        mock_get_service.assert_not_called()
        assert result is resolver.chat


# ===========================================================================
# get_internal_email
# ===========================================================================


class TestGetInternalEmail:
    """Tests for UserResolver.get_internal_email."""

    def test_email_provided_directly(self):
        resolver = _make_resolver()

        result = resolver.get_internal_email("U001", "direct@example.com")

        assert result == "direct@example.com"

    def test_user_found_in_user_map(self):
        resolver = _make_resolver(user_map={"U001": "mapped@example.com"})

        result = resolver.get_internal_email("U001")

        assert result == "mapped@example.com"

    def test_user_not_in_user_map_returns_none_and_tracks(self):
        resolver = _make_resolver(user_map={})

        result = resolver.get_internal_email("U999")

        assert result is None
        assert "U999" in resolver.unmapped_user_tracker.unmapped_users

    def test_bot_user_with_ignore_bots_true_returns_none(self, tmp_path):
        users_json = [{"id": "B001", "is_bot": True, "real_name": "TestBot"}]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(
            ignore_bots=True,
            export_root=str(tmp_path),
            user_map={"B001": "bot@example.com"},
        )

        result = resolver.get_internal_email("B001")

        assert result is None

    def test_bot_user_with_ignore_bots_false_proceeds(self, tmp_path):
        users_json = [{"id": "B001", "is_bot": True, "real_name": "TestBot"}]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(
            ignore_bots=False,
            export_root=str(tmp_path),
            user_map={"B001": "bot@example.com"},
        )

        result = resolver.get_internal_email("B001")

        assert result == "bot@example.com"


# ===========================================================================
# get_user_data
# ===========================================================================


class TestGetUserData:
    """Tests for UserResolver.get_user_data."""

    def test_user_found(self, tmp_path):
        users_json = [
            {"id": "U001", "real_name": "Alice"},
            {"id": "U002", "real_name": "Bob"},
        ]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(export_root=str(tmp_path))

        result = resolver.get_user_data("U001")

        assert result == {"id": "U001", "real_name": "Alice"}

    def test_user_not_found(self, tmp_path):
        users_json = [{"id": "U001", "real_name": "Alice"}]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(export_root=str(tmp_path))

        result = resolver.get_user_data("U999")

        assert result is None

    def test_users_json_does_not_exist(self, tmp_path):
        resolver = _make_resolver(export_root=str(tmp_path))

        result = resolver.get_user_data("U001")

        assert result is None

    def test_users_json_invalid_json(self, tmp_path):
        users_file = tmp_path / "users.json"
        users_file.write_text("{invalid json content")

        resolver = _make_resolver(export_root=str(tmp_path))

        result = resolver.get_user_data("U001")

        assert result is None

    def test_cache_second_call_does_not_reread_file(self, tmp_path):
        users_json = [{"id": "U001", "real_name": "Alice"}]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(export_root=str(tmp_path))

        # First call loads data
        result1 = resolver.get_user_data("U001")
        assert result1 is not None

        # Modify the file on disk — should not affect cached data
        users_file.write_text(json.dumps([{"id": "U002", "real_name": "Bob"}]))

        # Second call uses cache
        result2 = resolver.get_user_data("U001")
        assert result2 == {"id": "U001", "real_name": "Alice"}

        # U002 should not be found because cache was not refreshed
        assert resolver.get_user_data("U002") is None


# ===========================================================================
# handle_unmapped_user_message
# ===========================================================================


class TestHandleUnmappedUserMessage:
    """Tests for UserResolver.handle_unmapped_user_message."""

    def test_user_with_override_email(self, tmp_path):
        resolver = _make_resolver(
            export_root=str(tmp_path),
            user_mapping_overrides={"U001": "override@example.com"},
        )

        admin_email, modified_text = resolver.handle_unmapped_user_message(
            "U001", "Hello world"
        )

        assert admin_email == "admin@example.com"
        assert "*[From: override@example.com]*" in modified_text
        assert "Hello world" in modified_text

    def test_user_with_real_name_and_email(self, tmp_path):
        users_json = [
            {
                "id": "U001",
                "profile": {"real_name": "Alice Smith", "email": "alice@ext.com"},
            }
        ]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(export_root=str(tmp_path))

        admin_email, modified_text = resolver.handle_unmapped_user_message(
            "U001", "Hello"
        )

        assert admin_email == "admin@example.com"
        assert "*[From: Alice Smith (alice@ext.com)]*" in modified_text
        assert "Hello" in modified_text

    def test_user_with_only_email(self, tmp_path):
        users_json = [
            {
                "id": "U001",
                "profile": {"email": "alice@ext.com"},
            }
        ]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(export_root=str(tmp_path))

        admin_email, modified_text = resolver.handle_unmapped_user_message(
            "U001", "Hello"
        )

        assert admin_email == "admin@example.com"
        assert "*[From: alice@ext.com]*" in modified_text

    def test_user_with_only_real_name(self, tmp_path):
        users_json = [
            {
                "id": "U001",
                "profile": {"real_name": "Alice Smith"},
            }
        ]
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps(users_json))

        resolver = _make_resolver(export_root=str(tmp_path))

        admin_email, modified_text = resolver.handle_unmapped_user_message(
            "U001", "Hello"
        )

        assert admin_email == "admin@example.com"
        assert "*[From: Alice Smith]*" in modified_text

    def test_user_not_in_data_falls_back_to_user_id(self, tmp_path):
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps([]))

        resolver = _make_resolver(export_root=str(tmp_path))

        admin_email, modified_text = resolver.handle_unmapped_user_message(
            "U999", "Hello"
        )

        assert admin_email == "admin@example.com"
        assert "*[From: U999]*" in modified_text
        assert "Hello" in modified_text

    def test_returns_admin_email_and_modified_text_tuple(self, tmp_path):
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps([]))

        resolver = _make_resolver(export_root=str(tmp_path))

        result = resolver.handle_unmapped_user_message("U001", "message body")

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert result[0] == "admin@example.com"
        assert result[1] == "*[From: U001]*\nmessage body"

    def test_tracks_unmapped_user(self, tmp_path):
        users_file = tmp_path / "users.json"
        users_file.write_text(json.dumps([]))

        resolver = _make_resolver(export_root=str(tmp_path))

        resolver.handle_unmapped_user_message("U001", "Hello")

        assert "U001" in resolver.unmapped_user_tracker.unmapped_users
        assert (
            "message_sender:general"
            in resolver.unmapped_user_tracker.user_contexts["U001"]
        )


# ===========================================================================
# handle_unmapped_user_reaction
# ===========================================================================


class TestHandleUnmappedUserReaction:
    """Tests for UserResolver.handle_unmapped_user_reaction."""

    def test_returns_false(self):
        resolver = _make_resolver()

        result = resolver.handle_unmapped_user_reaction(
            "U001", "thumbsup", "1234567890.123456"
        )

        assert result is False

    def test_tracks_in_unmapped_user_tracker(self):
        resolver = _make_resolver()

        resolver.handle_unmapped_user_reaction("U001", "thumbsup", "1234567890.123456")

        assert "U001" in resolver.unmapped_user_tracker.unmapped_users
        assert (
            "reaction:general" in resolver.unmapped_user_tracker.user_contexts["U001"]
        )

    def test_appends_to_skipped_reactions(self):
        resolver = _make_resolver()

        resolver.handle_unmapped_user_reaction("U001", "thumbsup", "1234567890.123456")

        assert len(resolver.state.skipped_reactions) == 1
        entry = resolver.state.skipped_reactions[0]
        assert entry["user_id"] == "U001"
        assert entry["reaction"] == "thumbsup"
        assert entry["message_ts"] == "1234567890.123456"
        assert entry["channel"] == "general"


# ===========================================================================
# is_external_user
# ===========================================================================


class TestIsExternalUser:
    """Tests for UserResolver.is_external_user."""

    def test_internal_domain_returns_false(self):
        resolver = _make_resolver(workspace_domain="example.com")

        assert resolver.is_external_user("alice@example.com") is False

    def test_external_domain_returns_true(self):
        resolver = _make_resolver(workspace_domain="example.com")

        assert resolver.is_external_user("alice@other.com") is True

    def test_none_email_returns_false(self):
        resolver = _make_resolver()

        assert resolver.is_external_user(None) is False

    def test_empty_string_returns_false(self):
        resolver = _make_resolver()

        assert resolver.is_external_user("") is False

    def test_no_workspace_domain_returns_false(self):
        resolver = _make_resolver(workspace_domain="")

        assert resolver.is_external_user("alice@example.com") is False

    def test_case_insensitive_comparison(self):
        resolver = _make_resolver(workspace_domain="Example.COM")

        assert resolver.is_external_user("alice@example.com") is False
        assert resolver.is_external_user("alice@EXAMPLE.COM") is False
        assert resolver.is_external_user("alice@Example.Com") is False
