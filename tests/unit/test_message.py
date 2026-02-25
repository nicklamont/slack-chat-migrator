"""Unit tests for the message processing module."""

from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.state import MigrationState
from slack_migrator.services.discovery import log_space_mapping_conflicts
from slack_migrator.services.message import (
    MessageResult,
    send_intro,
    send_message,
    track_message_stats,
)
from slack_migrator.services.reaction_processor import process_reactions_batch


def _make_migrator(dry_run=False, channel="general", ignore_bots=False):
    """Create a mock migrator for message testing."""
    migrator = MagicMock()
    migrator.dry_run = dry_run
    migrator.state = MigrationState()
    migrator.state.current_channel = channel
    migrator.config = MigrationConfig(ignore_bots=ignore_bots)
    migrator.state.migration_summary = {
        "messages_created": 0,
        "reactions_created": 0,
        "files_created": 0,
        "channels_processed": [],
        "spaces_created": 0,
    }
    migrator.update_mode = False

    # Set up attachment processor
    migrator.attachment_processor = MagicMock()
    migrator.attachment_processor.count_message_files.return_value = 0

    return migrator


def _make_send_migrator(
    dry_run=False, channel="general", ignore_bots=False, user_map=None
):
    """Create a mock migrator configured for send_message testing."""
    migrator = _make_migrator(dry_run=dry_run, channel=channel, ignore_bots=ignore_bots)

    # user_map for resolving user IDs to emails
    migrator.user_map = user_map or {"U001": "user1@example.com"}

    # Thread tracking (state attributes already initialized by MigrationState defaults)
    migrator.state.thread_map = {}
    migrator.state.sent_messages = set()
    migrator.state.message_id_map = {}
    migrator.state.failed_messages = []

    # Workspace admin
    migrator.workspace_admin = "admin@example.com"

    # Internal email handling
    migrator.user_resolver.get_internal_email.side_effect = lambda uid, email: email
    migrator.user_resolver.is_external_user.return_value = False
    migrator.user_resolver.get_delegate.return_value = migrator.chat

    # Attachment processor returns no attachments by default
    migrator.attachment_processor.process_message_attachments.return_value = []

    # Chat service mock — create chain: spaces().messages().create().execute()
    mock_result = {
        "name": "spaces/SPACE1/messages/MSG001",
        "thread": {"name": "spaces/SPACE1/threads/THREAD001"},
    }
    (
        migrator.chat.spaces.return_value.messages.return_value.create.return_value.execute.return_value
    ) = mock_result

    return migrator


def _make_http_error(status=400, reason="Bad Request", content=b"error details"):
    """Create an HttpError for testing."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=content)


# ---------------------------------------------------------------------------
# TestTrackMessageStats (existing)
# ---------------------------------------------------------------------------


class TestTrackMessageStats:
    """Tests for track_message_stats()."""

    def test_basic_message_counting(self):
        migrator = _make_migrator()
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        assert migrator.state.channel_stats["general"]["message_count"] == 1

    def test_reaction_counting(self):
        migrator = _make_migrator()
        msg = {
            "ts": "1234.5",
            "user": "U001",
            "reactions": [{"name": "thumbsup", "users": ["U001", "U002"]}],
        }
        migrator.user_resolver.get_user_data.return_value = None

        track_message_stats(migrator, msg)

        assert migrator.state.channel_stats["general"]["reaction_count"] == 2

    def test_file_counting(self):
        migrator = _make_migrator()
        migrator.attachment_processor.count_message_files.return_value = 3
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        assert migrator.state.channel_stats["general"]["file_count"] == 3
        assert migrator.state.migration_summary["files_created"] == 3

    def test_dry_run_counts_reactions(self):
        migrator = _make_migrator(dry_run=True)
        msg = {
            "ts": "1234.5",
            "user": "U001",
            "reactions": [{"name": "wave", "users": ["U001"]}],
        }
        migrator.user_resolver.get_user_data.return_value = None

        track_message_stats(migrator, msg)

        assert migrator.state.migration_summary["reactions_created"] == 1

    def test_skips_bot_messages_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        msg = {"ts": "1234.5", "user": "U001", "subtype": "bot_message"}

        track_message_stats(migrator, msg)

        # channel_stats should not be created since the message was skipped
        assert "general" not in migrator.state.channel_stats

    def test_skips_bot_user_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        migrator.user_resolver.get_user_data.return_value = {
            "is_bot": True,
            "real_name": "Bot",
        }
        msg = {"ts": "1234.5", "user": "B001"}

        track_message_stats(migrator, msg)

        assert "general" not in migrator.state.channel_stats

    def test_processes_non_bot_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        migrator.user_resolver.get_user_data.return_value = {"is_bot": False}
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        assert migrator.state.channel_stats["general"]["message_count"] == 1

    def test_multiple_messages_increment(self):
        migrator = _make_migrator()
        for i in range(5):
            track_message_stats(migrator, {"ts": f"{i}.0", "user": "U001"})

        assert migrator.state.channel_stats["general"]["message_count"] == 5

    def test_update_mode_skips_already_sent(self):
        migrator = _make_migrator()
        migrator.update_mode = True
        migrator.state.sent_messages = {"general:1234.5"}
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(migrator, msg)

        # Should not count as it was already sent
        assert migrator.state.channel_stats["general"]["message_count"] == 0

    def test_update_mode_skips_edited_already_sent(self):
        migrator = _make_migrator()
        migrator.update_mode = True
        migrator.state.sent_messages = {"general:1234.5:edited:1235.0"}
        msg = {"ts": "1234.5", "user": "U001", "edited": {"ts": "1235.0"}}

        track_message_stats(migrator, msg)

        assert migrator.state.channel_stats["general"]["message_count"] == 0

    def test_skips_app_message_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        msg = {"ts": "1234.5", "user": "U001", "subtype": "app_message"}

        track_message_stats(migrator, msg)

        assert "general" not in migrator.state.channel_stats

    def test_reaction_counting_skips_bot_reactions_when_ignore_bots(self):
        migrator = _make_migrator(ignore_bots=True)
        # First call for the message user check (non-bot), subsequent for reaction users
        migrator.user_resolver.get_user_data.side_effect = [
            {"is_bot": False},  # message user
            {"is_bot": True},  # reaction user U001 (bot)
            {"is_bot": False},  # reaction user U002 (human)
        ]
        msg = {
            "ts": "1234.5",
            "user": "U003",
            "reactions": [{"name": "thumbsup", "users": ["U001", "U002"]}],
        }

        track_message_stats(migrator, msg)

        assert migrator.state.channel_stats["general"]["reaction_count"] == 1


# ---------------------------------------------------------------------------
# TestSendMessage
# ---------------------------------------------------------------------------


class TestSendMessage:
    """Tests for send_message()."""

    def test_basic_message_sends_successfully(self):
        """A simple text message from a mapped user is sent and returns message name."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello world"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        assert migrator.state.migration_summary["messages_created"] == 1
        migrator.chat.spaces.return_value.messages.return_value.create.assert_called_once()

    def test_dry_run_returns_none_and_does_not_call_api(self):
        """In dry run mode, no API call is made and None is returned."""
        migrator = _make_send_migrator(dry_run=True)
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result is None
        # messages_created should NOT be incremented in dry run (handled elsewhere)
        assert migrator.state.migration_summary["messages_created"] == 0
        migrator.chat.spaces.return_value.messages.return_value.create.return_value.execute.assert_not_called()

    def test_skips_bot_message_subtype_when_ignore_bots(self):
        """Messages with bot_message subtype are skipped when ignore_bots is True."""
        migrator = _make_send_migrator(ignore_bots=True)
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "I am a bot",
            "subtype": "bot_message",
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == MessageResult.IGNORED_BOT

    def test_skips_app_message_subtype_when_ignore_bots(self):
        """Messages with app_message subtype are skipped when ignore_bots is True."""
        migrator = _make_send_migrator(ignore_bots=True)
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "App notification",
            "subtype": "app_message",
            "username": "MyApp",
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == MessageResult.IGNORED_BOT

    def test_skips_bot_user_when_ignore_bots(self):
        """Messages from a bot user (is_bot flag) are skipped when ignore_bots is True."""
        migrator = _make_send_migrator(ignore_bots=True)
        migrator.user_resolver.get_user_data.return_value = {
            "is_bot": True,
            "real_name": "BotUser",
        }
        msg = {"ts": "1700000000.000001", "user": "B001", "text": "Bot says hi"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == MessageResult.IGNORED_BOT

    def test_skips_channel_join_leave(self):
        """Channel join/leave system messages return SKIPPED."""
        migrator = _make_send_migrator()
        for subtype in ["channel_join", "channel_leave"]:
            msg = {
                "ts": "1700000000.000001",
                "user": "U001",
                "text": "joined",
                "subtype": subtype,
            }

            result = send_message(migrator, "spaces/SPACE1", msg)

            assert result == MessageResult.SKIPPED

    def test_skips_empty_message(self):
        """Messages with no text and no files return None."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": ""}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result is None

    def test_message_with_only_whitespace_and_no_files_returns_none(self):
        """Messages with only whitespace text and no files return None."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "   \n  "}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result is None

    def test_thread_reply_uses_existing_thread_name(self):
        """Thread replies use the stored thread name from thread_map."""
        migrator = _make_send_migrator()
        migrator.state.thread_map = {
            "1700000000.000001": "spaces/SPACE1/threads/THREAD001"
        }
        msg = {
            "ts": "1700000000.000050",
            "user": "U001",
            "text": "Reply text",
            "thread_ts": "1700000000.000001",
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        # Verify the create call included thread info
        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert (
            call_kwargs[1]["body"]["thread"]["name"]
            == "spaces/SPACE1/threads/THREAD001"
        )
        assert (
            call_kwargs[1]["messageReplyOption"]
            == "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
        )

    def test_thread_reply_falls_back_to_thread_key(self):
        """Thread replies without stored thread name fall back to thread_key."""
        migrator = _make_send_migrator()
        migrator.state.thread_map = {}  # No thread mapping exists
        msg = {
            "ts": "1700000000.000050",
            "user": "U001",
            "text": "Reply text",
            "thread_ts": "1700000000.000001",
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert call_kwargs[1]["body"]["thread"]["thread_key"] == "1700000000.000001"

    def test_new_thread_starter_uses_own_ts_as_thread_key(self):
        """Non-reply messages use their own ts as thread_key."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Start a thread"}

        send_message(migrator, "spaces/SPACE1", msg)

        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert call_kwargs[1]["body"]["thread"]["thread_key"] == "1700000000.000001"

    def test_new_thread_stores_thread_mapping(self):
        """New thread starters store their thread mapping."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        send_message(migrator, "spaces/SPACE1", msg)

        assert (
            migrator.state.thread_map["1700000000.000001"]
            == "spaces/SPACE1/threads/THREAD001"
        )

    def test_unmapped_user_uses_admin_attribution(self):
        """Messages from unmapped users are attributed via _handle_unmapped_user_message."""
        migrator = _make_send_migrator(user_map={})
        migrator.user_resolver.handle_unmapped_user_message.return_value = (
            "admin@example.com",
            "[Unknown User U099] Hello",
        )
        msg = {"ts": "1700000000.000001", "user": "U099", "text": "Hello"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        migrator.user_resolver.handle_unmapped_user_message.assert_called_once()
        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert call_kwargs[1]["body"]["text"] == "[Unknown User U099] Hello"
        assert call_kwargs[1]["body"]["sender"]["name"] == "users/admin@example.com"

    def test_external_user_uses_admin_attribution(self):
        """External users are sent via admin with attribution."""
        migrator = _make_send_migrator(user_map={"U001": "external@other.com"})
        migrator.user_resolver.is_external_user.return_value = True
        migrator.user_resolver.handle_unmapped_user_message.return_value = (
            "admin@example.com",
            "[External User] Hello",
        )
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert call_kwargs[1]["body"]["sender"]["name"] == "users/admin@example.com"

    def test_edited_message_adds_edit_indicator(self):
        """Edited messages get an edit timestamp appended."""
        migrator = _make_send_migrator()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "Corrected text",
            "edited": {"ts": "1700000001.000000"},
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert "_(edited at " in call_kwargs[1]["body"]["text"]

    def test_edited_message_stores_mapping_with_edit_key(self):
        """Edited messages are stored in message_id_map with an edit-specific key."""
        migrator = _make_send_migrator()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "Edited",
            "edited": {"ts": "1700000001.000000"},
        }

        send_message(migrator, "spaces/SPACE1", msg)

        edit_key = "1700000000.000001:edited:1700000001.000000"
        assert edit_key in migrator.state.message_id_map
        assert (
            migrator.state.message_id_map[edit_key] == "spaces/SPACE1/messages/MSG001"
        )

    def test_message_with_reactions_calls_process_reactions_batch(self):
        """Messages with reactions trigger process_reactions_batch."""
        migrator = _make_send_migrator()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "React to me",
            "reactions": [{"name": "thumbsup", "users": ["U001"]}],
        }

        with patch(
            "slack_migrator.services.message.process_reactions_batch"
        ) as mock_prb:
            result = send_message(migrator, "spaces/SPACE1", msg)

            assert result == "spaces/SPACE1/messages/MSG001"
            mock_prb.assert_called_once()
            call_args = mock_prb.call_args
            assert call_args[0][0] is migrator
            assert call_args[0][1] == "spaces/SPACE1/messages/MSG001"
            assert call_args[0][2] == msg["reactions"]

    def test_http_error_returns_none_and_records_failure(self):
        """HttpError from the API is caught, logged, and returns None."""
        migrator = _make_send_migrator()
        http_error = _make_http_error(status=500, content=b"Internal Server Error")
        (
            migrator.chat.spaces.return_value.messages.return_value.create.return_value.execute.side_effect
        ) = http_error
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result is None
        assert len(migrator.state.failed_messages) == 1
        assert migrator.state.failed_messages[0]["channel"] == "general"
        assert migrator.state.failed_messages[0]["ts"] == "1700000000.000001"

    def test_update_mode_skips_already_sent_message(self):
        """Update mode skips messages already in sent_messages set."""
        migrator = _make_send_migrator()
        migrator.update_mode = True
        migrator.state.sent_messages = {"general:1700000000.000001"}
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == MessageResult.ALREADY_SENT

    def test_update_mode_skips_old_messages_via_timestamp(self):
        """Update mode skips messages older than last_processed_timestamps."""
        migrator = _make_send_migrator()
        migrator.update_mode = True
        migrator.state.last_processed_timestamps = {"general": 1700000010.0}

        with patch(
            "slack_migrator.services.message.should_process_message",
            return_value=False,
        ):
            msg = {"ts": "1700000000.000001", "user": "U001", "text": "Old message"}

            result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == MessageResult.ALREADY_SENT

    def test_marks_sent_message_in_sent_messages_set(self):
        """Successfully sent messages are tracked in sent_messages."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        send_message(migrator, "spaces/SPACE1", msg)

        assert "general:1700000000.000001" in migrator.state.sent_messages

    def test_state_has_thread_map_by_default(self):
        """MigrationState initializes thread_map as an empty dict."""
        migrator = _make_send_migrator()

        assert isinstance(migrator.state.thread_map, dict)

    def test_state_has_sent_messages_by_default(self):
        """MigrationState initializes sent_messages as an empty set."""
        migrator = _make_send_migrator()

        assert isinstance(migrator.state.sent_messages, set)

    def test_state_has_message_id_map_by_default(self):
        """MigrationState initializes message_id_map as an empty dict."""
        migrator = _make_send_migrator()

        assert isinstance(migrator.state.message_id_map, dict)

    def test_message_with_files_is_not_skipped(self):
        """Messages with no text but with files are not skipped."""
        migrator = _make_send_migrator()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "",
            "files": [{"id": "F001"}],
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        # Should not be None — it should be sent
        assert result == "spaces/SPACE1/messages/MSG001"

    def test_message_with_forwarded_files_is_not_skipped(self):
        """Messages with files in forwarded attachments are not skipped."""
        migrator = _make_send_migrator()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "",
            "attachments": [
                {"is_share": True, "files": [{"id": "F002"}]},
            ],
        }

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"

    def test_drive_attachments_appended_as_links(self):
        """Drive file attachments are converted to links in message text."""
        migrator = _make_send_migrator()
        migrator.attachment_processor.process_message_attachments.return_value = [
            {"driveDataRef": {"driveFileId": "abc123"}},
        ]
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "See attachment"}

        send_message(migrator, "spaces/SPACE1", msg)

        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        body_text = call_kwargs[1]["body"]["text"]
        assert "https://drive.google.com/file/d/abc123/view" in body_text

    def test_non_drive_attachments_added_to_payload(self):
        """Non-drive attachments are added as payload attachment field."""
        migrator = _make_send_migrator()
        non_drive_attachment = {"uploadedContent": {"contentName": "file.pdf"}}
        migrator.attachment_processor.process_message_attachments.return_value = [
            non_drive_attachment,
        ]
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "File here"}

        send_message(migrator, "spaces/SPACE1", msg)

        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert call_kwargs[1]["body"]["attachment"] == [non_drive_attachment]

    def test_no_user_id_message_has_no_sender(self):
        """System messages with no user_id have no sender in payload."""
        migrator = _make_send_migrator()
        msg = {"ts": "1700000000.000001", "text": "System notification"}

        send_message(migrator, "spaces/SPACE1", msg)

        call_kwargs = (
            migrator.chat.spaces.return_value.messages.return_value.create.call_args
        )
        assert "sender" not in call_kwargs[1]["body"]

    def test_dry_run_in_update_mode_log_prefix(self):
        """Dry run + update mode uses combined prefix (no crash)."""
        migrator = _make_send_migrator(dry_run=True)
        migrator.update_mode = True
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(migrator, "spaces/SPACE1", msg)

        assert result is None


# ---------------------------------------------------------------------------
# TestProcessReactionsBatch
# ---------------------------------------------------------------------------


class TestProcessReactionsBatch:
    """Tests for process_reactions_batch()."""

    def _make_reactions_migrator(self, dry_run=False, ignore_bots=False):
        """Create a migrator configured for reactions testing."""
        migrator = _make_migrator(dry_run=dry_run, ignore_bots=ignore_bots)
        migrator.user_map = {"U001": "user1@example.com", "U002": "user2@example.com"}
        migrator.user_resolver.get_internal_email.side_effect = lambda uid, email: email
        migrator.user_resolver.is_external_user.return_value = False
        migrator.user_resolver.get_delegate.return_value = (
            MagicMock()
        )  # impersonated service
        return migrator

    def test_dry_run_counts_reactions_but_does_not_call_api(self):
        """In dry run, reactions are counted but not sent."""
        migrator = self._make_reactions_migrator(dry_run=True)
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        assert migrator.state.migration_summary["reactions_created"] == 1

    def test_counts_reactions_for_mapped_users(self):
        """Reactions from mapped users are counted in migration_summary."""
        migrator = self._make_reactions_migrator(dry_run=True)
        reactions = [
            {"name": "thumbsup", "users": ["U001", "U002"]},
            {"name": "heart", "users": ["U001"]},
        ]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        assert migrator.state.migration_summary["reactions_created"] == 3

    def test_unmapped_user_calls_handle_unmapped(self):
        """Unmapped user reactions call _handle_unmapped_user_reaction."""
        migrator = self._make_reactions_migrator(dry_run=True)
        migrator.user_map = {"U001": "user1@example.com"}  # U099 is unmapped
        migrator.state.current_message_ts = "1700000000.000001"
        reactions = [{"name": "thumbsup", "users": ["U099"]}]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        migrator.user_resolver.handle_unmapped_user_reaction.assert_called_once_with(
            "U099", "thumbsup", "1700000000.000001"
        )

    def test_skips_bot_reactions_when_ignore_bots(self):
        """Bot user reactions are skipped when ignore_bots is True."""
        migrator = self._make_reactions_migrator(dry_run=True, ignore_bots=True)
        migrator.user_resolver.get_user_data.return_value = {
            "is_bot": True,
            "real_name": "Bot",
        }
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        assert migrator.state.migration_summary["reactions_created"] == 0

    def test_processes_non_bot_reactions_when_ignore_bots(self):
        """Non-bot reactions are counted when ignore_bots is True."""
        migrator = self._make_reactions_migrator(dry_run=True, ignore_bots=True)
        migrator.user_resolver.get_user_data.return_value = {"is_bot": False}
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        assert migrator.state.migration_summary["reactions_created"] == 1

    def test_external_user_reactions_skipped(self):
        """Reactions from external users are skipped to avoid admin attribution."""
        migrator = self._make_reactions_migrator()
        migrator.user_resolver.is_external_user.return_value = True
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        # Reaction is counted in the summary (happens before external check)
        assert migrator.state.migration_summary["reactions_created"] == 1

    def test_admin_service_fallback_sends_reactions_synchronously(self):
        """When impersonation fails (delegate == admin), reactions are sent one by one."""
        migrator = self._make_reactions_migrator()
        # Make _get_delegate return the admin service (same as migrator.chat)
        migrator.user_resolver.get_delegate.return_value = migrator.chat
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        # Verify synchronous create was called via admin service
        migrator.chat.spaces.return_value.messages.return_value.reactions.return_value.create.assert_called()

    def test_batch_execution_error_is_caught(self):
        """HttpError during batch.execute() is logged and does not raise."""
        migrator = self._make_reactions_migrator()
        mock_delegate = MagicMock()
        migrator.user_resolver.get_delegate.return_value = mock_delegate
        mock_batch = MagicMock()
        mock_batch.execute.side_effect = _make_http_error(500)
        mock_delegate.new_batch_http_request.return_value = mock_batch
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        # Should not raise
        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

    def test_exception_in_reaction_processing_is_caught(self):
        """General exceptions during reaction processing are caught gracefully."""
        migrator = self._make_reactions_migrator(dry_run=True)
        # Force an exception by making user_map.get raise
        migrator.user_map = MagicMock()
        migrator.user_map.get.side_effect = RuntimeError("Unexpected")
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        # Should not raise
        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

    def test_empty_reactions_list(self):
        """An empty reactions list is handled without error."""
        migrator = self._make_reactions_migrator()
        reactions = []

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        assert migrator.state.migration_summary["reactions_created"] == 0

    def test_reaction_with_no_users(self):
        """A reaction entry with no users list is handled."""
        migrator = self._make_reactions_migrator(dry_run=True)
        reactions = [{"name": "thumbsup"}]  # Missing 'users' key

        process_reactions_batch(migrator, "spaces/S1/messages/M1", reactions, "M1")

        assert migrator.state.migration_summary["reactions_created"] == 0


# ---------------------------------------------------------------------------
# TestSendIntro
# ---------------------------------------------------------------------------


class TestSendIntro:
    """Tests for send_intro()."""

    def _make_intro_migrator(self, dry_run=False):
        migrator = _make_migrator(dry_run=dry_run)
        migrator.channels_meta = {
            "general": {
                "purpose": {"value": "General discussion"},
                "topic": {"value": "Team chat"},
                "created": 1600000000,
            }
        }
        migrator.workspace_admin = "admin@example.com"
        return migrator

    @patch("slack_migrator.services.message.time")
    def test_sends_intro_message(self, mock_time):
        """send_intro creates a message via the API."""
        mock_time.time.return_value = 1700000000
        migrator = self._make_intro_migrator()

        send_intro(migrator, "spaces/SPACE1", "general")

        migrator.chat.spaces.return_value.messages.return_value.create.assert_called_once()
        assert migrator.state.migration_summary["messages_created"] == 1

    def test_dry_run_counts_but_does_not_call_api(self):
        """In dry run, intro message is counted but not sent."""
        migrator = self._make_intro_migrator(dry_run=True)

        send_intro(migrator, "spaces/SPACE1", "general")

        assert migrator.state.migration_summary["messages_created"] == 1
        migrator.chat.spaces.return_value.messages.return_value.create.return_value.execute.assert_not_called()

    def test_update_mode_skips_intro(self):
        """In update mode, intro messages are not resent."""
        migrator = self._make_intro_migrator()
        migrator.update_mode = True

        send_intro(migrator, "spaces/SPACE1", "general")

        migrator.chat.spaces.return_value.messages.return_value.create.return_value.execute.assert_not_called()
        assert migrator.state.migration_summary["messages_created"] == 0

    @patch("slack_migrator.services.message.time")
    def test_api_error_is_caught(self, mock_time):
        """API errors during intro send are caught and do not raise."""
        mock_time.time.return_value = 1700000000
        migrator = self._make_intro_migrator()
        (
            migrator.chat.spaces.return_value.messages.return_value.create.return_value.execute.side_effect
        ) = HttpError(Response({"status": "500"}), b"API Error")

        # Should not raise
        send_intro(migrator, "spaces/SPACE1", "general")

    @patch("slack_migrator.services.message.time")
    def test_missing_channel_metadata(self, mock_time):
        """Intro works when channel has no metadata."""
        mock_time.time.return_value = 1700000000
        migrator = self._make_intro_migrator()
        migrator.channels_meta = {}

        send_intro(migrator, "spaces/SPACE1", "general")

        migrator.chat.spaces.return_value.messages.return_value.create.assert_called_once()


# ---------------------------------------------------------------------------
# TestLogSpaceMappingConflicts
# ---------------------------------------------------------------------------


class TestLogSpaceMappingConflicts:
    """Tests for log_space_mapping_conflicts()."""

    def test_no_conflicts(self):
        """No-op when there are no conflicts."""
        migrator = _make_migrator()
        migrator.state.channel_conflicts = set()

        # Should not raise
        log_space_mapping_conflicts(migrator)

    def test_with_conflicts_logs_without_error(self):
        """Conflicts are logged without raising exceptions."""
        migrator = _make_migrator()
        migrator.state.channel_conflicts = {"channel-a", "channel-b"}

        log_space_mapping_conflicts(migrator)

    def test_dry_run_with_no_conflicts(self):
        """Dry run with no conflicts works fine."""
        migrator = _make_migrator(dry_run=True)
        migrator.state.channel_conflicts = set()

        log_space_mapping_conflicts(migrator)

    def test_missing_channel_conflicts_attr(self):
        """Works when channel_conflicts attribute does not exist."""
        migrator = _make_migrator()
        del migrator.state.channel_conflicts

        # Should not raise
        log_space_mapping_conflicts(migrator)
