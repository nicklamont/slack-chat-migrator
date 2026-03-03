"""Message sending, tracking, and error handling for Slack-to-Chat migration."""

from __future__ import annotations

import datetime
import logging
import time
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError

from slack_migrator.constants import BOT_SUBTYPES, SYSTEM_SUBTYPES
from slack_migrator.services.discovery import should_process_message
from slack_migrator.services.message_builder import (
    build_message_payload,
    build_user_map_with_overrides,
    generate_message_id,
    process_attachments,
)
from slack_migrator.services.reaction_processor import process_reactions_batch
from slack_migrator.types import FailedMessage, MessageResult, SendResult
from slack_migrator.utils.api import slack_ts_to_rfc3339
from slack_migrator.utils.formatting import parse_slack_blocks
from slack_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState
    from slack_migrator.services.chat_adapter import ChatAdapter
    from slack_migrator.services.user_resolver import UserResolver


def _is_bot_message(
    config: Any,
    user_resolver: Any,
    message: dict[str, Any],
    user_id: str,
    channel: str,
    ts: str,
) -> bool:
    """Return True if the message is from a bot and bots should be ignored."""
    if not config.ignore_bots:
        return False

    # Check for bot messages by subtype (system bots like USLACKBOT)
    if message.get("subtype") in BOT_SUBTYPES:
        bot_name = message.get("username", user_id or "Unknown Bot")
        log_with_context(
            logging.DEBUG,
            f"Skipping bot message from {bot_name} (subtype: {message.get('subtype')}) - ignore_bots enabled",
            channel=channel,
            ts=ts,
            user_id=user_id,
            bot_name=bot_name,
        )
        return True

    # Check for user-based bots (bots in users.json)
    if user_id:
        user_data = user_resolver.get_user_data(user_id)
        if user_data and user_data.get("is_bot", False):
            log_with_context(
                logging.DEBUG,
                f"Skipping message from bot user {user_id} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                channel=channel,
                ts=ts,
                user_id=user_id,
            )
            return True

    return False


def _is_already_sent_in_update_mode(
    state: MigrationState,
    is_update_mode: bool,
    message_key: str,
    channel: str,
    ts: str,
    user_id: str,
) -> bool:
    """Return True if this message was already sent in a previous update-mode run."""
    if not is_update_mode:
        return False

    # Check if older than last processed timestamp
    if state.progress.last_processed_timestamps:
        last_timestamp = state.progress.last_processed_timestamps.get(channel, 0)
        if last_timestamp > 0 and not should_process_message(last_timestamp, ts):
            log_with_context(
                logging.INFO,
                f"[UPDATE MODE] Skipping message TS={ts} from user={user_id} (older than last processed timestamp)",
                channel=channel,
                ts=ts,
                user_id=user_id,
                last_timestamp=last_timestamp,
            )
            return True

    # Check the sent_messages set for additional protection
    if message_key in state.messages.sent_messages:
        log_with_context(
            logging.INFO,
            f"[UPDATE MODE] Skipping already sent message TS={ts} from user={user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
        return True

    return False


def _is_empty_message(message: dict[str, Any]) -> bool:
    """Return True if the message has no text content and no file attachments."""
    text = parse_slack_blocks(message)

    has_files = "files" in message
    if not has_files:
        for attachment in message.get("attachments", []):
            if (
                attachment.get("is_share") or attachment.get("is_msg_unfurl")
            ) and "files" in attachment:
                has_files = True
                break

    return not text.strip() and not has_files


def _count_reactions_excluding_bots(
    config: Any,
    user_resolver: Any,
    message: dict[str, Any],
) -> int:
    """Count reactions on a message, excluding bot reactions when configured."""
    count = 0
    for reaction in message.get("reactions", []):
        for uid in reaction.get("users", []):
            if config.ignore_bots:
                user_data = user_resolver.get_user_data(uid)
                if user_data and user_data.get("is_bot", False):
                    continue
            count += 1
    return count


def _should_skip_message(
    ctx: MigrationContext,
    state: MigrationState,
    user_resolver: Any,
    message: dict[str, Any],
    ts: str,
    user_id: str,
    thread_ts: str | None,
    channel: str,
    is_edited: bool,
    edited_ts: str,
    message_key: str,
) -> tuple[bool, MessageResult | None]:
    """Check whether a message should be skipped before processing.

    Handles bot checks, update-mode deduplication, dry-run early return,
    system subtype filtering, and empty message detection.

    Returns:
        A ``(should_skip, return_value)`` tuple.  When *should_skip* is
        ``True``, the caller should return *return_value* immediately.
    """
    if _is_bot_message(ctx.config, user_resolver, message, user_id, channel, ts):
        return True, MessageResult.IGNORED_BOT

    if _is_already_sent_in_update_mode(
        state, ctx.update_mode, message_key, channel, ts, user_id
    ):
        return True, MessageResult.ALREADY_SENT

    if ctx.dry_run:
        log_with_context(
            logging.DEBUG,
            f"{ctx.log_prefix}Would send message TS={ts} from user={user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
            is_thread_reply=(thread_ts is not None and thread_ts != ts),
        )
        return True, None

    # System subtype skip
    if message.get("subtype") in SYSTEM_SUBTYPES:
        log_with_context(
            logging.DEBUG,
            f"Skipping {message.get('subtype')} message from {user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
        return True, MessageResult.SKIPPED

    if _is_empty_message(message):
        log_with_context(
            logging.DEBUG,
            f"Skipping empty message from {user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
        return True, None

    return False, None


def _handle_send_result(
    ctx: MigrationContext,
    state: MigrationState,
    chat: ChatAdapter,
    user_resolver: Any,
    result: dict[str, Any],
    message: dict[str, Any],
    message_name: str | None,
    message_key: str,
    ts: str,
    edited_ts: str,
    thread_ts: str | None,
    channel: str | None,
    is_edited: bool,
    is_thread_reply: bool,
) -> None:
    """Process a successful API response after sending a message.

    Updates ``messages_created`` counter, ``message_id_map``, ``thread_map``,
    ``sent_messages``, and triggers reaction processing when applicable.
    """
    state.progress.migration_summary["messages_created"] += 1

    # Store the message ID mapping for potential future edits
    if message_name:
        # For edited messages, store with a special key that includes the edit timestamp
        if is_edited:
            edit_key = f"{ts}:edited:{edited_ts}"
            state.messages.message_id_map[edit_key] = message_name
            log_with_context(
                logging.DEBUG,
                f"Stored message ID mapping for edited message: {edit_key} -> {message_name}",
                channel=channel,
                ts=ts,
                edited_ts=edited_ts,
            )
        else:
            state.messages.message_id_map[ts] = message_name

    # Store thread mapping for both parent messages and thread replies
    if message_name:
        thread_name = result.get("thread", {}).get("name")

        # Debug log the thread information from the API response
        log_with_context(
            logging.DEBUG,
            f"API response thread info - name: {thread_name}, is_thread_reply: {is_thread_reply}, thread_ts: {thread_ts}",
            channel=channel,
            ts=ts,
        )

        if thread_name:
            if not is_thread_reply:
                # For new thread starters, store the mapping using their own timestamp
                state.messages.thread_map[str(ts)] = thread_name
                log_with_context(
                    logging.DEBUG,
                    f"Stored new thread mapping: {ts} -> {thread_name}",
                    channel=channel,
                    ts=ts,
                )
            else:
                # For thread replies, ensure the original thread timestamp mapping exists
                thread_ts_str = str(thread_ts)
                if thread_ts_str not in state.messages.thread_map:
                    # Store the mapping using the original thread timestamp
                    state.messages.thread_map[thread_ts_str] = thread_name
                    log_with_context(
                        logging.DEBUG,
                        f"Stored thread mapping from reply: {thread_ts_str} -> {thread_name}",
                        channel=channel,
                        ts=ts,
                        thread_ts=thread_ts_str,
                    )
                else:
                    # Verify the mapping is consistent
                    existing_thread_name = state.messages.thread_map[thread_ts_str]
                    if existing_thread_name != thread_name:
                        log_with_context(
                            logging.WARNING,
                            f"Thread name mismatch! Expected {existing_thread_name}, got {thread_name} for thread {thread_ts_str}",
                            channel=channel,
                            ts=ts,
                            thread_ts=thread_ts_str,
                        )
                    else:
                        log_with_context(
                            logging.DEBUG,
                            f"Confirmed existing thread mapping: {thread_ts_str} -> {thread_name}",
                            channel=channel,
                            ts=ts,
                            thread_ts=thread_ts_str,
                        )
        else:
            log_with_context(
                logging.WARNING,
                f"No thread name returned in API response for message {ts} {'(thread reply)' if is_thread_reply else '(new thread)'}",
                channel=channel,
                ts=ts,
            )

    # Process reactions if any
    if "reactions" in message and message_name:
        # Store the current message timestamp for context in reaction processing
        state.context.current_message_ts = ts

        # The message_id for reactions should be the final segment of the message_name
        final_message_id = message_name.split("/")[-1]
        log_with_context(
            logging.DEBUG,
            f"Processing {len(message['reactions'])} reaction types for message {ts}",
            channel=channel,
            ts=ts,
            message_id=final_message_id,
        )
        process_reactions_batch(
            ctx,
            state,
            chat,
            user_resolver,
            message_name,
            message["reactions"],
            final_message_id,
        )

    log_with_context(
        logging.DEBUG,
        f"Successfully sent message TS={ts} \u2192 {message_name}",
        channel=channel,
        ts=ts,
        message_name=message_name,
    )

    # Mark this message as successfully sent to avoid duplicates
    state.messages.sent_messages.add(message_key)


def _handle_send_error(
    state: MigrationState,
    error: HttpError,
    message: dict[str, Any],
    ts: str,
    channel: str | None,
) -> SendResult:
    """Log and record an ``HttpError`` that occurred while sending a message.

    Returns a :class:`SendResult` encoding the error details.
    """
    error_message = f"Failed to send message: {error}"
    status_code: int | None = error.resp.status if hasattr(error, "resp") else None
    error_code_display = status_code if status_code is not None else "unknown"
    error_details = (
        error.content.decode("utf-8") if hasattr(error, "content") else str(error)
    )

    log_with_context(
        logging.ERROR,
        error_message,
        channel=channel,
        ts=ts,
        error_code=error_code_display,
        error_details=error_details[:500] + ("..." if len(error_details) > 500 else ""),
    )

    # Add to failed messages list for reporting
    failed_msg = FailedMessage(
        channel=channel or "unknown",
        ts=ts,
        error=f"{error_message} (Code: {error_code_display})",
        error_details=error_details,
        payload=message,
    )
    state.messages.failed_messages.append(failed_msg)

    retryable = status_code is not None and (status_code == 429 or status_code >= 500)
    return SendResult(
        error=error_message,
        error_code=status_code,
        retryable=retryable,
    )


def _resolve_chat_service(
    chat: ChatAdapter,
    user_resolver: UserResolver,
    user_email: str | None,
    channel: str,
    ts: str,
    user_id: str,
) -> ChatAdapter:
    """Return the appropriate Chat service for sending a message.

    Uses impersonation when available for the user, otherwise falls back
    to the admin service.
    """
    if not user_email or user_resolver.is_external_user(user_email):
        return chat

    chat_service = user_resolver.get_delegate(user_email)

    if chat_service != chat:
        log_with_context(
            logging.DEBUG,
            f"Using impersonated service for user {user_email}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
    else:
        log_with_context(
            logging.DEBUG,
            f"Using admin service for user {user_email} (impersonation not available)",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )

    return chat_service


def send_message(
    ctx: MigrationContext,
    state: MigrationState,
    chat: ChatAdapter,
    user_resolver: Any,
    attachment_processor: Any,
    space: str,
    message: dict[str, Any],
    user_map_with_overrides: dict[str, str] | None = None,
) -> SendResult:
    """Send a message to a Google Chat space.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups and impersonation.
        attachment_processor: MessageAttachmentProcessor for file handling.
        space: The space ID to send the message to.
        message: The Slack message to convert and send.
        user_map_with_overrides: Pre-computed user map with overrides applied.
            If None, an empty dict is used (callers should compute this once
            per channel via :func:`build_user_map_with_overrides`).

    Returns:
        A :class:`SendResult` encoding success, skip, or failure.
    """
    # Extract basic message info for logging
    ts = message.get("ts", "")
    user_id = message.get("user", "")
    thread_ts = message.get("thread_ts")
    channel = state.context.current_channel
    if channel is None:
        log_with_context(
            logging.ERROR,
            "send_message called without current_channel set",
            ts=ts,
        )
        return SendResult(error="No current channel set")

    # Check for edited messages
    edited = message.get("edited", {})
    edited_ts = edited.get("ts", "") if edited else ""
    is_edited = bool(edited_ts)

    # Create a message key that includes edit information if present
    message_key = f"{channel}:{ts}"
    if is_edited:
        message_key = f"{channel}:{ts}:edited:{edited_ts}"

    # Check all early-return / skip conditions
    should_skip, skip_result = _should_skip_message(
        ctx,
        state,
        user_resolver,
        message,
        ts,
        user_id,
        thread_ts,
        channel,
        is_edited,
        edited_ts,
        message_key,
    )
    if should_skip:
        if skip_result is not None:
            return SendResult(skipped=skip_result)
        # None from _should_skip_message means dry-run or empty message —
        # neither a real success nor an intentional skip with a named reason.
        return SendResult()

    is_update_mode = ctx.update_mode

    # Build the full message payload (text, sender, thread info)
    payload, user_email, is_thread_reply, message_reply_option = build_message_payload(
        ctx,
        state,
        user_resolver,
        message,
        ts,
        user_id,
        thread_ts,
        channel,
        is_edited,
        edited_ts,
        user_map_with_overrides
        if user_map_with_overrides is not None
        else build_user_map_with_overrides(ctx, user_resolver),
    )

    # Log with appropriate mode indicator
    mode_prefix = ""
    if is_update_mode:
        mode_prefix = "[UPDATE MODE] "

    log_with_context(
        logging.DEBUG,
        f"{mode_prefix}Sending message TS={ts} from user={user_id}{' (thread reply)' if is_thread_reply else ''}",
        channel=channel,
        ts=ts,
        user_id=user_id,
        is_thread_reply=is_thread_reply,
    )

    try:
        chat_service = _resolve_chat_service(
            chat, user_resolver, user_email, channel, ts, user_id
        )

        message_id = generate_message_id(ts, is_edited, edited_ts)

        process_attachments(
            user_resolver,
            attachment_processor,
            message,
            channel,
            space,
            user_id,
            user_email,
            chat_service,
            payload,
            ts,
        )

        # Send the message using the appropriate service
        log_with_context(
            logging.DEBUG,
            f"Complete message payload for {ts}: {payload}",
            channel=channel,
            ts=ts,
        )
        result = chat_service.create_message(
            parent=space,
            body=payload,
            message_id=message_id,
            message_reply_option=message_reply_option,
        )
        message_name: str | None = result.get("name")

        _handle_send_result(
            ctx,
            state,
            chat,
            user_resolver,
            result,
            message,
            message_name,
            message_key,
            ts,
            edited_ts,
            thread_ts,
            channel,
            is_edited,
            is_thread_reply,
        )

        return SendResult(message_name=message_name)
    except HttpError as e:
        return _handle_send_error(state, e, message, ts, channel)


def track_message_stats(
    ctx: MigrationContext,
    state: MigrationState,
    user_resolver: Any,
    attachment_processor: Any,
    m: dict[str, Any],
) -> None:
    """Handle tracking message stats in both dry run and normal mode.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        user_resolver: UserResolver for bot-user lookups.
        attachment_processor: Processor for counting message files.
        m: A single Slack message dictionary.
    """
    channel = state.context.current_channel
    if channel is None:
        return
    ts = m.get("ts", "")
    user_id = m.get("user", "")

    if _is_bot_message(ctx.config, user_resolver, m, user_id, channel, ts):
        return

    if channel not in state.progress.channel_stats:
        state.progress.channel_stats[channel] = {
            "message_count": 0,
            "reaction_count": 0,
            "file_count": 0,
        }

    # Skip stats for already-sent messages in update mode
    if ctx.update_mode:
        message_key = f"{channel}:{ts}"
        edited = m.get("edited", {})
        edited_ts = edited.get("ts", "") if edited else ""
        if edited_ts:
            message_key = f"{channel}:{ts}:edited:{edited_ts}"

        if message_key in state.messages.sent_messages:
            log_with_context(
                logging.DEBUG,
                f"[UPDATE MODE] Skipping stats for already sent message {ts}",
                channel=channel,
                ts=ts,
            )
            return

    state.progress.channel_stats[channel]["message_count"] += 1

    # Track reactions
    if "reactions" in m:
        reaction_count = _count_reactions_excluding_bots(ctx.config, user_resolver, m)
        state.progress.channel_stats[channel]["reaction_count"] += reaction_count

        if ctx.dry_run:
            state.progress.migration_summary["reactions_created"] += reaction_count
            log_with_context(
                logging.DEBUG,
                f"{ctx.log_prefix}Counted {reaction_count} reactions for message {ts}",
                channel=channel,
                ts=ts,
            )

    # Track files
    file_count = attachment_processor.count_message_files(m)
    if file_count > 0:
        mode_prefix = "[UPDATE MODE] " if ctx.update_mode else ""
        log_with_context(
            logging.DEBUG,
            f"{mode_prefix}Found {file_count} files to process in message {ts}",
            channel=channel,
            ts=ts,
        )
        state.progress.channel_stats[channel]["file_count"] += file_count
        state.progress.migration_summary["files_created"] += file_count


def send_intro(
    ctx: MigrationContext,
    state: MigrationState,
    chat: ChatAdapter,
    space: str,
    channel: str,
) -> None:
    """Send an intro message with channel metadata.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        space: Google Chat space resource name to send into.
        channel: Slack channel name for metadata lookup.
    """
    # Check if we're in update mode - if so, don't send intro message again
    is_update_mode = ctx.update_mode
    if is_update_mode:
        log_with_context(
            logging.INFO,
            f"[UPDATE MODE] Skipping intro message for channel {channel}",
            channel=channel,
        )
        return

    # Get channel metadata
    meta = ctx.channels_meta.get(channel, {})

    # Format the intro message
    intro_text = f"🔄 *Migrated from Slack #{channel}*\n\n"

    # Add channel purpose and topic if available
    purpose = meta.get("purpose", {}).get("value", "")
    topic = meta.get("topic", {}).get("value", "")

    if purpose:
        intro_text += f"*Purpose:* {purpose}\n\n"
    if topic:
        intro_text += f"*Topic:* {topic}\n\n"

    # Add channel creation date if available
    created_ts = meta.get("created")
    if created_ts:
        created_date = datetime.datetime.fromtimestamp(created_ts).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        intro_text += f"*Created:* {created_date}\n\n"

    # Add migration info
    intro_text += (
        f"*Migration Date:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )

    # Log the action
    log_with_context(
        logging.INFO,
        f"{ctx.log_prefix}Sending intro message to space {space} for channel {channel}",
        channel=channel,
    )

    # Create the message
    try:
        message_body = {
            "text": intro_text,
            "createTime": slack_ts_to_rfc3339(f"{int(time.time())}.000000"),
            # Explicitly set the sender as the workspace admin
            "sender": {"type": "HUMAN", "name": f"users/{ctx.workspace_admin}"},
        }

        # Send the message
        chat.create_message(parent=space, body=message_body)
        # Increment the counter
        state.progress.migration_summary["messages_created"] += 1

        log_with_context(
            logging.INFO, f"Sent intro message to space {space}", channel=channel
        )
    except HttpError as e:
        log_with_context(
            logging.WARNING,
            f"Failed to send intro message to space {space}: {e}",
            channel=channel,
            error=str(e),
        )
