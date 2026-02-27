"""Unit tests for the message processing module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.context import MigrationContext
from slack_migrator.core.state import MigrationState, _default_migration_summary
from slack_migrator.services.discovery import log_space_mapping_conflicts
from slack_migrator.services.message_sender import (
    MessageResult,
    send_intro,
    send_message,
    track_message_stats,
)
from slack_migrator.services.reaction_processor import process_reactions_batch


def _make_ctx(
    dry_run=False,
    ignore_bots=False,
    update_mode=False,
    user_map=None,
    workspace_admin="admin@example.com",
    channels_meta=None,
):
    """Create a MigrationContext for message testing."""
    return MigrationContext(
        export_root=Path("/fake/export"),
        creds_path="/fake/creds.json",
        workspace_admin=workspace_admin,
        workspace_domain="example.com",
        dry_run=dry_run,
        update_mode=update_mode,
        verbose=False,
        debug_api=False,
        config=MigrationConfig(ignore_bots=ignore_bots),
        user_map=user_map or {},
        users_without_email=[],
        channels_meta=channels_meta or {},
        channel_id_to_name={},
        channel_name_to_id={},
    )


def _make_state(channel="general"):
    """Create a MigrationState with current_channel set."""
    state = MigrationState()
    state.context.current_channel = channel
    state.progress.migration_summary = _default_migration_summary()
    return state


def _make_send_deps(
    dry_run=False,
    channel="general",
    ignore_bots=False,
    user_map=None,
    update_mode=False,
):
    """Create (ctx, state, chat, user_resolver, attachment_processor) for send_message tests."""
    ctx = _make_ctx(
        dry_run=dry_run,
        ignore_bots=ignore_bots,
        update_mode=update_mode,
        user_map=user_map or {"U001": "user1@example.com"},
    )
    state = _make_state(channel)
    chat = MagicMock()
    user_resolver = MagicMock()
    attachment_processor = MagicMock()

    # Internal email handling
    user_resolver.get_internal_email.side_effect = lambda uid, email: email
    user_resolver.is_external_user.return_value = False
    user_resolver.get_delegate.return_value = chat

    # Attachment processor defaults
    attachment_processor.process_message_attachments.return_value = []
    attachment_processor.count_message_files.return_value = 0

    # Chat service mock — create chain: spaces().messages().create().execute()
    mock_result = {
        "name": "spaces/SPACE1/messages/MSG001",
        "thread": {"name": "spaces/SPACE1/threads/THREAD001"},
    }
    (
        chat.spaces.return_value.messages.return_value.create.return_value.execute.return_value
    ) = mock_result

    return ctx, state, chat, user_resolver, attachment_processor


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

    def _setup(self, dry_run=False, ignore_bots=False, update_mode=False):
        ctx = _make_ctx(
            dry_run=dry_run, ignore_bots=ignore_bots, update_mode=update_mode
        )
        state = _make_state()
        user_resolver = MagicMock()
        attachment_processor = MagicMock()
        attachment_processor.count_message_files.return_value = 0
        return ctx, state, user_resolver, attachment_processor

    def test_basic_message_counting(self):
        ctx, state, ur, ap = self._setup()
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.channel_stats["general"]["message_count"] == 1

    def test_reaction_counting(self):
        ctx, state, ur, ap = self._setup()
        msg = {
            "ts": "1234.5",
            "user": "U001",
            "reactions": [{"name": "thumbsup", "users": ["U001", "U002"]}],
        }
        ur.get_user_data.return_value = None

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.channel_stats["general"]["reaction_count"] == 2

    def test_file_counting(self):
        ctx, state, ur, ap = self._setup()
        ap.count_message_files.return_value = 3
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.channel_stats["general"]["file_count"] == 3
        assert state.progress.migration_summary["files_created"] == 3

    def test_dry_run_counts_reactions(self):
        ctx, state, ur, ap = self._setup(dry_run=True)
        msg = {
            "ts": "1234.5",
            "user": "U001",
            "reactions": [{"name": "wave", "users": ["U001"]}],
        }
        ur.get_user_data.return_value = None

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.migration_summary["reactions_created"] == 1

    def test_skips_bot_messages_when_ignore_bots(self):
        ctx, state, ur, ap = self._setup(ignore_bots=True)
        msg = {"ts": "1234.5", "user": "U001", "subtype": "bot_message"}

        track_message_stats(ctx, state, ur, ap, msg)

        # channel_stats should not be created since the message was skipped
        assert "general" not in state.progress.channel_stats

    def test_skips_bot_user_when_ignore_bots(self):
        ctx, state, ur, ap = self._setup(ignore_bots=True)
        ur.get_user_data.return_value = {
            "is_bot": True,
            "real_name": "Bot",
        }
        msg = {"ts": "1234.5", "user": "B001"}

        track_message_stats(ctx, state, ur, ap, msg)

        assert "general" not in state.progress.channel_stats

    def test_processes_non_bot_when_ignore_bots(self):
        ctx, state, ur, ap = self._setup(ignore_bots=True)
        ur.get_user_data.return_value = {"is_bot": False}
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.channel_stats["general"]["message_count"] == 1

    def test_multiple_messages_increment(self):
        ctx, state, ur, ap = self._setup()
        for i in range(5):
            track_message_stats(ctx, state, ur, ap, {"ts": f"{i}.0", "user": "U001"})

        assert state.progress.channel_stats["general"]["message_count"] == 5

    def test_update_mode_skips_already_sent(self):
        ctx, state, ur, ap = self._setup(update_mode=True)
        state.messages.sent_messages = {"general:1234.5"}
        msg = {"ts": "1234.5", "user": "U001"}

        track_message_stats(ctx, state, ur, ap, msg)

        # Should not count as it was already sent
        assert state.progress.channel_stats["general"]["message_count"] == 0

    def test_update_mode_skips_edited_already_sent(self):
        ctx, state, ur, ap = self._setup(update_mode=True)
        state.messages.sent_messages = {"general:1234.5:edited:1235.0"}
        msg = {"ts": "1234.5", "user": "U001", "edited": {"ts": "1235.0"}}

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.channel_stats["general"]["message_count"] == 0

    def test_skips_app_message_when_ignore_bots(self):
        ctx, state, ur, ap = self._setup(ignore_bots=True)
        msg = {"ts": "1234.5", "user": "U001", "subtype": "app_message"}

        track_message_stats(ctx, state, ur, ap, msg)

        assert "general" not in state.progress.channel_stats

    def test_reaction_counting_skips_bot_reactions_when_ignore_bots(self):
        ctx, state, ur, ap = self._setup(ignore_bots=True)
        # First call for the message user check (non-bot), subsequent for reaction users
        ur.get_user_data.side_effect = [
            {"is_bot": False},  # message user
            {"is_bot": True},  # reaction user U001 (bot)
            {"is_bot": False},  # reaction user U002 (human)
        ]
        msg = {
            "ts": "1234.5",
            "user": "U003",
            "reactions": [{"name": "thumbsup", "users": ["U001", "U002"]}],
        }

        track_message_stats(ctx, state, ur, ap, msg)

        assert state.progress.channel_stats["general"]["reaction_count"] == 1


# ---------------------------------------------------------------------------
# TestSendMessage
# ---------------------------------------------------------------------------


class TestSendMessage:
    """Tests for send_message()."""

    def test_basic_message_sends_successfully(self):
        """A simple text message from a mapped user is sent and returns message name."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello world"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        assert state.progress.migration_summary["messages_created"] == 1
        chat.spaces.return_value.messages.return_value.create.assert_called_once()

    def test_dry_run_returns_none_and_does_not_call_api(self):
        """In dry run mode, no API call is made and None is returned."""
        ctx, state, chat, ur, ap = _make_send_deps(dry_run=True)
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result is None
        # messages_created should NOT be incremented in dry run (handled elsewhere)
        assert state.progress.migration_summary["messages_created"] == 0
        chat.spaces.return_value.messages.return_value.create.return_value.execute.assert_not_called()

    def test_skips_bot_message_subtype_when_ignore_bots(self):
        """Messages with bot_message subtype are skipped when ignore_bots is True."""
        ctx, state, chat, ur, ap = _make_send_deps(ignore_bots=True)
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "I am a bot",
            "subtype": "bot_message",
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == MessageResult.IGNORED_BOT

    def test_skips_app_message_subtype_when_ignore_bots(self):
        """Messages with app_message subtype are skipped when ignore_bots is True."""
        ctx, state, chat, ur, ap = _make_send_deps(ignore_bots=True)
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "App notification",
            "subtype": "app_message",
            "username": "MyApp",
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == MessageResult.IGNORED_BOT

    def test_skips_bot_user_when_ignore_bots(self):
        """Messages from a bot user (is_bot flag) are skipped when ignore_bots is True."""
        ctx, state, chat, ur, ap = _make_send_deps(ignore_bots=True)
        ur.get_user_data.return_value = {
            "is_bot": True,
            "real_name": "BotUser",
        }
        msg = {"ts": "1700000000.000001", "user": "B001", "text": "Bot says hi"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == MessageResult.IGNORED_BOT

    def test_skips_channel_join_leave(self):
        """Channel join/leave system messages return SKIPPED."""
        ctx, state, chat, ur, ap = _make_send_deps()
        for subtype in ["channel_join", "channel_leave"]:
            msg = {
                "ts": "1700000000.000001",
                "user": "U001",
                "text": "joined",
                "subtype": subtype,
            }

            result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

            assert result == MessageResult.SKIPPED

    def test_skips_empty_message(self):
        """Messages with no text and no files return None."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": ""}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result is None

    def test_message_with_only_whitespace_and_no_files_returns_none(self):
        """Messages with only whitespace text and no files return None."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "   \n  "}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result is None

    def test_thread_reply_uses_existing_thread_name(self):
        """Thread replies use the stored thread name from thread_map."""
        ctx, state, chat, ur, ap = _make_send_deps()
        state.messages.thread_map = {
            "1700000000.000001": "spaces/SPACE1/threads/THREAD001"
        }
        msg = {
            "ts": "1700000000.000050",
            "user": "U001",
            "text": "Reply text",
            "thread_ts": "1700000000.000001",
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        # Verify the create call included thread info
        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
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
        ctx, state, chat, ur, ap = _make_send_deps()
        state.messages.thread_map = {}  # No thread mapping exists
        msg = {
            "ts": "1700000000.000050",
            "user": "U001",
            "text": "Reply text",
            "thread_ts": "1700000000.000001",
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert call_kwargs[1]["body"]["thread"]["thread_key"] == "1700000000.000001"

    def test_new_thread_starter_uses_own_ts_as_thread_key(self):
        """Non-reply messages use their own ts as thread_key."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Start a thread"}

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert call_kwargs[1]["body"]["thread"]["thread_key"] == "1700000000.000001"

    def test_new_thread_stores_thread_mapping(self):
        """New thread starters store their thread mapping."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert (
            state.messages.thread_map["1700000000.000001"]
            == "spaces/SPACE1/threads/THREAD001"
        )

    def test_unmapped_user_uses_admin_attribution(self):
        """Messages from unmapped users are attributed via _handle_unmapped_user_message."""
        ctx, state, chat, ur, ap = _make_send_deps(user_map={})
        ur.handle_unmapped_user_message.return_value = (
            "admin@example.com",
            "[Unknown User U099] Hello",
        )
        msg = {"ts": "1700000000.000001", "user": "U099", "text": "Hello"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        ur.handle_unmapped_user_message.assert_called_once()
        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert call_kwargs[1]["body"]["text"] == "[Unknown User U099] Hello"
        assert call_kwargs[1]["body"]["sender"]["name"] == "users/admin@example.com"

    def test_external_user_uses_admin_attribution(self):
        """External users are sent via admin with attribution."""
        ctx, state, chat, ur, ap = _make_send_deps(
            user_map={"U001": "external@other.com"}
        )
        ur.is_external_user.return_value = True
        ur.handle_unmapped_user_message.return_value = (
            "admin@example.com",
            "[External User] Hello",
        )
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert call_kwargs[1]["body"]["sender"]["name"] == "users/admin@example.com"

    def test_edited_message_adds_edit_indicator(self):
        """Edited messages get an edit timestamp appended."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "Corrected text",
            "edited": {"ts": "1700000001.000000"},
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"
        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert "_(edited at " in call_kwargs[1]["body"]["text"]

    def test_edited_message_stores_mapping_with_edit_key(self):
        """Edited messages are stored in message_id_map with an edit-specific key."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "Edited",
            "edited": {"ts": "1700000001.000000"},
        }

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        edit_key = "1700000000.000001:edited:1700000001.000000"
        assert edit_key in state.messages.message_id_map
        assert (
            state.messages.message_id_map[edit_key] == "spaces/SPACE1/messages/MSG001"
        )

    def test_message_with_reactions_calls_process_reactions_batch(self):
        """Messages with reactions trigger process_reactions_batch."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "React to me",
            "reactions": [{"name": "thumbsup", "users": ["U001"]}],
        }

        with patch(
            "slack_migrator.services.message_sender.process_reactions_batch"
        ) as mock_prb:
            result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

            assert result == "spaces/SPACE1/messages/MSG001"
            mock_prb.assert_called_once()
            call_args = mock_prb.call_args
            assert call_args[0][0] is ctx
            assert call_args[0][1] is state
            assert call_args[0][2] is chat
            assert call_args[0][3] is ur
            assert call_args[0][4] == "spaces/SPACE1/messages/MSG001"
            assert call_args[0][5] == msg["reactions"]

    def test_http_error_returns_none_and_records_failure(self):
        """HttpError from the API is caught, logged, and returns None."""
        ctx, state, chat, ur, ap = _make_send_deps()
        http_error = _make_http_error(status=500, content=b"Internal Server Error")
        (
            chat.spaces.return_value.messages.return_value.create.return_value.execute.side_effect
        ) = http_error
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result is None
        assert len(state.messages.failed_messages) == 1
        assert state.messages.failed_messages[0]["channel"] == "general"
        assert state.messages.failed_messages[0]["ts"] == "1700000000.000001"

    def test_update_mode_skips_already_sent_message(self):
        """Update mode skips messages already in sent_messages set."""
        ctx, state, chat, ur, ap = _make_send_deps(update_mode=True)
        state.messages.sent_messages = {"general:1700000000.000001"}
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == MessageResult.ALREADY_SENT

    def test_update_mode_skips_old_messages_via_timestamp(self):
        """Update mode skips messages older than last_processed_timestamps."""
        ctx, state, chat, ur, ap = _make_send_deps(update_mode=True)
        state.progress.last_processed_timestamps = {"general": 1700000010.0}

        with patch(
            "slack_migrator.services.message_sender.should_process_message",
            return_value=False,
        ):
            msg = {"ts": "1700000000.000001", "user": "U001", "text": "Old message"}

            result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == MessageResult.ALREADY_SENT

    def test_marks_sent_message_in_sent_messages_set(self):
        """Successfully sent messages are tracked in sent_messages."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert "general:1700000000.000001" in state.messages.sent_messages

    def test_state_has_thread_map_by_default(self):
        """MigrationState initializes thread_map as an empty dict."""
        state = _make_state()

        assert isinstance(state.messages.thread_map, dict)

    def test_state_has_sent_messages_by_default(self):
        """MigrationState initializes sent_messages as an empty set."""
        state = _make_state()

        assert isinstance(state.messages.sent_messages, set)

    def test_state_has_message_id_map_by_default(self):
        """MigrationState initializes message_id_map as an empty dict."""
        state = _make_state()

        assert isinstance(state.messages.message_id_map, dict)

    def test_message_with_files_is_not_skipped(self):
        """Messages with no text but with files are not skipped."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "",
            "files": [{"id": "F001"}],
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        # Should not be None — it should be sent
        assert result == "spaces/SPACE1/messages/MSG001"

    def test_message_with_forwarded_files_is_not_skipped(self):
        """Messages with files in forwarded attachments are not skipped."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {
            "ts": "1700000000.000001",
            "user": "U001",
            "text": "",
            "attachments": [
                {"is_share": True, "files": [{"id": "F002"}]},
            ],
        }

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result == "spaces/SPACE1/messages/MSG001"

    def test_drive_attachments_appended_as_links(self):
        """Drive file attachments are converted to links in message text."""
        ctx, state, chat, ur, ap = _make_send_deps()
        ap.process_message_attachments.return_value = [
            {"driveDataRef": {"driveFileId": "abc123"}},
        ]
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "See attachment"}

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        body_text = call_kwargs[1]["body"]["text"]
        assert "https://drive.google.com/file/d/abc123/view" in body_text

    def test_non_drive_attachments_added_to_payload(self):
        """Non-drive attachments are added as payload attachment field."""
        ctx, state, chat, ur, ap = _make_send_deps()
        non_drive_attachment = {"uploadedContent": {"contentName": "file.pdf"}}
        ap.process_message_attachments.return_value = [
            non_drive_attachment,
        ]
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "File here"}

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert call_kwargs[1]["body"]["attachment"] == [non_drive_attachment]

    def test_no_user_id_message_has_no_sender(self):
        """System messages with no user_id have no sender in payload."""
        ctx, state, chat, ur, ap = _make_send_deps()
        msg = {"ts": "1700000000.000001", "text": "System notification"}

        send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        call_kwargs = chat.spaces.return_value.messages.return_value.create.call_args
        assert "sender" not in call_kwargs[1]["body"]

    def test_dry_run_in_update_mode_log_prefix(self):
        """Dry run + update mode uses combined prefix (no crash)."""
        ctx, state, chat, ur, ap = _make_send_deps(dry_run=True, update_mode=True)
        msg = {"ts": "1700000000.000001", "user": "U001", "text": "Hello"}

        result = send_message(ctx, state, chat, ur, ap, "spaces/SPACE1", msg)

        assert result is None


# ---------------------------------------------------------------------------
# TestProcessReactionsBatch
# ---------------------------------------------------------------------------


class TestProcessReactionsBatch:
    """Tests for process_reactions_batch()."""

    def _setup(self, dry_run=False, ignore_bots=False):
        """Create (ctx, state, chat, user_resolver) for reactions testing."""
        ctx = _make_ctx(
            dry_run=dry_run,
            ignore_bots=ignore_bots,
            user_map={"U001": "user1@example.com", "U002": "user2@example.com"},
        )
        state = _make_state()
        chat = MagicMock()
        ur = MagicMock()
        ur.get_internal_email.side_effect = lambda uid, email: email
        ur.is_external_user.return_value = False
        ur.get_delegate.return_value = MagicMock()  # impersonated service
        return ctx, state, chat, ur

    def test_dry_run_counts_reactions_but_does_not_call_api(self):
        """In dry run, reactions are counted but not sent."""
        ctx, state, chat, ur = self._setup(dry_run=True)
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        assert state.progress.migration_summary["reactions_created"] == 1

    def test_counts_reactions_for_mapped_users(self):
        """Reactions from mapped users are counted in migration_summary."""
        ctx, state, chat, ur = self._setup(dry_run=True)
        reactions = [
            {"name": "thumbsup", "users": ["U001", "U002"]},
            {"name": "heart", "users": ["U001"]},
        ]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        assert state.progress.migration_summary["reactions_created"] == 3

    def test_unmapped_user_calls_handle_unmapped(self):
        """Unmapped user reactions call _handle_unmapped_user_reaction."""
        ctx = _make_ctx(
            dry_run=True,
            user_map={"U001": "user1@example.com"},  # U099 is unmapped
        )
        state = _make_state()
        state.context.current_message_ts = "1700000000.000001"
        chat = MagicMock()
        ur = MagicMock()
        ur.get_internal_email.side_effect = lambda uid, email: email
        ur.is_external_user.return_value = False
        reactions = [{"name": "thumbsup", "users": ["U099"]}]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        ur.handle_unmapped_user_reaction.assert_called_once_with(
            "U099", "thumbsup", "1700000000.000001"
        )

    def test_skips_bot_reactions_when_ignore_bots(self):
        """Bot user reactions are skipped when ignore_bots is True."""
        ctx, state, chat, ur = self._setup(dry_run=True, ignore_bots=True)
        ur.get_user_data.return_value = {
            "is_bot": True,
            "real_name": "Bot",
        }
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        assert state.progress.migration_summary["reactions_created"] == 0

    def test_processes_non_bot_reactions_when_ignore_bots(self):
        """Non-bot reactions are counted when ignore_bots is True."""
        ctx, state, chat, ur = self._setup(dry_run=True, ignore_bots=True)
        ur.get_user_data.return_value = {"is_bot": False}
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        assert state.progress.migration_summary["reactions_created"] == 1

    def test_external_user_reactions_skipped(self):
        """Reactions from external users are skipped to avoid admin attribution."""
        ctx, state, chat, ur = self._setup()
        ur.is_external_user.return_value = True
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        # Reaction is counted in the summary (happens before external check)
        assert state.progress.migration_summary["reactions_created"] == 1

    def test_admin_service_fallback_sends_reactions_synchronously(self):
        """When impersonation fails (delegate == admin), reactions are sent one by one."""
        ctx, state, chat, ur = self._setup()
        # Make get_delegate return the admin service (same as chat)
        ur.get_delegate.return_value = chat
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        # Verify synchronous create was called via admin service
        chat.spaces.return_value.messages.return_value.reactions.return_value.create.assert_called()

    def test_batch_execution_error_is_caught(self):
        """HttpError during batch.execute() is logged and does not raise."""
        ctx, state, chat, ur = self._setup()
        mock_delegate = MagicMock()
        ur.get_delegate.return_value = mock_delegate
        mock_batch = MagicMock()
        mock_batch.execute.side_effect = _make_http_error(500)
        mock_delegate.new_batch_http_request.return_value = mock_batch
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        # Should not raise
        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

    def test_exception_in_reaction_processing_is_caught(self):
        """General exceptions during reaction processing are caught gracefully."""
        state = _make_state()
        chat = MagicMock()
        ur = MagicMock()
        # Force an exception by using a user_map that raises on .get()
        # We need ctx.user_map to raise, but it's frozen. Use a mock ctx for this test.
        mock_ctx = MagicMock()
        mock_ctx.config.ignore_bots = False
        mock_ctx.user_map = MagicMock()
        mock_ctx.user_map.get.side_effect = RuntimeError("Unexpected")
        mock_ctx.dry_run = True
        reactions = [{"name": "thumbsup", "users": ["U001"]}]

        # Should not raise
        process_reactions_batch(
            mock_ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

    def test_empty_reactions_list(self):
        """An empty reactions list is handled without error."""
        ctx, state, chat, ur = self._setup()
        reactions = []

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        assert state.progress.migration_summary["reactions_created"] == 0

    def test_reaction_with_no_users(self):
        """A reaction entry with no users list is handled."""
        ctx, state, chat, ur = self._setup(dry_run=True)
        reactions = [{"name": "thumbsup"}]  # Missing 'users' key

        process_reactions_batch(
            ctx, state, chat, ur, "spaces/S1/messages/M1", reactions, "M1"
        )

        assert state.progress.migration_summary["reactions_created"] == 0


# ---------------------------------------------------------------------------
# TestSendIntro
# ---------------------------------------------------------------------------


_DEFAULT_INTRO_META = {
    "general": {
        "purpose": {"value": "General discussion"},
        "topic": {"value": "Team chat"},
        "created": 1600000000,
    }
}


class TestSendIntro:
    """Tests for send_intro()."""

    def _setup(self, dry_run=False, update_mode=False, channels_meta=None):
        ctx = _make_ctx(
            dry_run=dry_run,
            update_mode=update_mode,
            channels_meta=channels_meta
            if channels_meta is not None
            else _DEFAULT_INTRO_META,
        )
        state = _make_state()
        chat = MagicMock()
        return ctx, state, chat

    @patch("slack_migrator.services.message_sender.time")
    def test_sends_intro_message(self, mock_time):
        """send_intro creates a message via the API."""
        mock_time.time.return_value = 1700000000
        ctx, state, chat = self._setup()

        send_intro(ctx, state, chat, "spaces/SPACE1", "general")

        chat.spaces.return_value.messages.return_value.create.assert_called_once()
        assert state.progress.migration_summary["messages_created"] == 1

    @patch("slack_migrator.services.message_sender.time")
    def test_dry_run_sends_via_noop_service(self, mock_time):
        """In dry run, intro message is sent (handled by DryRunChatService)."""
        mock_time.time.return_value = 1700000000
        ctx, state, chat = self._setup(dry_run=True)

        send_intro(ctx, state, chat, "spaces/SPACE1", "general")

        assert state.progress.migration_summary["messages_created"] == 1
        chat.spaces.return_value.messages.return_value.create.assert_called_once()

    def test_update_mode_skips_intro(self):
        """In update mode, intro messages are not resent."""
        ctx, state, chat = self._setup(update_mode=True)

        send_intro(ctx, state, chat, "spaces/SPACE1", "general")

        chat.spaces.return_value.messages.return_value.create.return_value.execute.assert_not_called()
        assert state.progress.migration_summary["messages_created"] == 0

    @patch("slack_migrator.services.message_sender.time")
    def test_api_error_is_caught(self, mock_time):
        """API errors during intro send are caught and do not raise."""
        mock_time.time.return_value = 1700000000
        ctx, state, chat = self._setup()
        (
            chat.spaces.return_value.messages.return_value.create.return_value.execute.side_effect
        ) = HttpError(Response({"status": "500"}), b"API Error")

        # Should not raise
        send_intro(ctx, state, chat, "spaces/SPACE1", "general")

    @patch("slack_migrator.services.message_sender.time")
    def test_missing_channel_metadata(self, mock_time):
        """Intro works when channel has no metadata."""
        mock_time.time.return_value = 1700000000
        ctx, state, chat = self._setup(channels_meta={})

        send_intro(ctx, state, chat, "spaces/SPACE1", "general")

        chat.spaces.return_value.messages.return_value.create.assert_called_once()


# ---------------------------------------------------------------------------
# TestLogSpaceMappingConflicts
# ---------------------------------------------------------------------------


class TestLogSpaceMappingConflicts:
    """Tests for log_space_mapping_conflicts()."""

    def test_no_conflicts(self):
        """No-op when there are no conflicts."""
        state = _make_state()
        state.errors.channel_conflicts = set()

        # Should not raise
        log_space_mapping_conflicts(state)

    def test_with_conflicts_logs_without_error(self):
        """Conflicts are logged without raising exceptions."""
        state = _make_state()
        state.errors.channel_conflicts = {"channel-a", "channel-b"}

        log_space_mapping_conflicts(state)

    def test_dry_run_with_no_conflicts(self):
        """Dry run with no conflicts works fine."""
        state = _make_state()
        state.errors.channel_conflicts = set()

        log_space_mapping_conflicts(state, dry_run=True)

    def test_empty_channel_conflicts(self):
        """Works when channel_conflicts is empty."""
        state = _make_state()
        state.errors.channel_conflicts = set()

        # Should not raise
        log_space_mapping_conflicts(state)
