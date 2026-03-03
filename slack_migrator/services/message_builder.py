"""Message payload construction for Slack-to-Chat message transformation."""

from __future__ import annotations

import datetime
import hashlib
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from slack_migrator.utils.api import slack_ts_to_rfc3339
from slack_migrator.utils.formatting import convert_formatting, parse_slack_blocks
from slack_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState


MESSAGE_ID_MAX_LENGTH = 63
CLIENT_MESSAGE_PREFIX = "client-slack-"
CLIENT_EDIT_PREFIX = "client-slack-edit-"


def _build_message_payload(
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
) -> tuple[dict[str, Any], str | None, bool, str | None]:
    """Build the Google Chat message payload from a Slack message.

    Handles text formatting, sender resolution (internal / external / unmapped),
    edit indicators, and thread routing.

    Returns:
        A tuple of ``(payload, user_email, is_thread_reply, message_reply_option)``.
    """
    # Extract text from Slack blocks (rich formatting) or fallback to plain text
    text = parse_slack_blocks(message)

    # Create a mapping dictionary that has all user mapping overrides applied for ALL users
    user_map_with_overrides = {}

    # Map ALL user IDs with their proper overrides by iterating over .items()
    # This ensures we never miss any mentions and handles all potential edge cases
    for slack_user_id, email in ctx.user_map.items():
        internal_email = user_resolver.get_internal_email(slack_user_id, email)
        if internal_email:  # Only add if we got a valid email back
            user_map_with_overrides[slack_user_id] = internal_email

    # Set current message context for enhanced user tracking
    state.context.current_message_ts = ts

    # Convert Slack formatting to Google Chat formatting using the correct mapping
    formatted_text = convert_formatting(
        text,
        user_map_with_overrides,
        state=state,
        unmapped_user_tracker=getattr(user_resolver, "unmapped_user_tracker", None),
    )

    # For edited messages, add an edit indicator
    if is_edited:
        edit_time = datetime.datetime.fromtimestamp(float(edited_ts)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        formatted_text = f"{formatted_text}\n\n_(edited at {edit_time})_"

    # Convert Slack timestamp to RFC3339 format for Google Chat
    create_time = slack_ts_to_rfc3339(ts)

    # Prepare the message payload
    payload: dict[str, Any] = {"createTime": create_time}

    # Set the sender if available
    user_email = ctx.user_map.get(user_id)
    final_text = formatted_text

    if user_email:
        # Get the internal email for this user (now just returns the mapped email)
        internal_email = user_resolver.get_internal_email(user_id, user_email)
        if internal_email:
            # Check if this is an external user - if so, use admin with attribution
            if user_resolver.is_external_user(internal_email):
                # External user - send via admin with attribution
                admin_email, attributed_text = (
                    user_resolver.handle_unmapped_user_message(user_id, formatted_text)
                )
                final_text = attributed_text
                payload["sender"] = {"type": "HUMAN", "name": f"users/{admin_email}"}
            else:
                # Regular internal user - send directly
                payload["sender"] = {"type": "HUMAN", "name": f"users/{internal_email}"}
        else:
            # This shouldn't happen if user_email exists, but handle it gracefully
            admin_email, attributed_text = user_resolver.handle_unmapped_user_message(
                user_id, formatted_text
            )
            final_text = attributed_text
            payload["sender"] = {"type": "HUMAN", "name": f"users/{admin_email}"}
    elif user_id:  # We have a user_id but no mapping
        # Handle unmapped user with new graceful approach
        admin_email, attributed_text = user_resolver.handle_unmapped_user_message(
            user_id, formatted_text
        )
        final_text = attributed_text
        payload["sender"] = {"type": "HUMAN", "name": f"users/{admin_email}"}
    # If no user_id at all (system messages, etc.), leave sender empty

    # Add message text (potentially modified for unmapped user attribution)
    payload["text"] = final_text

    # Log the final payload for debugging
    log_with_context(
        logging.DEBUG,
        f"Final formatted text for message {ts}: '{final_text}'",
        channel=channel,
        ts=ts,
    )

    # Handle thread replies
    is_thread_reply = False
    message_reply_option = None

    if thread_ts and thread_ts != ts:  # This is a thread reply
        is_thread_reply = True

        # Convert thread_ts to string for consistent lookup
        thread_ts_str = str(thread_ts)

        # Check if we have the thread name from a previous message
        existing_thread_name = state.messages.thread_map.get(thread_ts_str)

        log_with_context(
            logging.DEBUG,
            f"Processing thread reply: ts={ts}, thread_ts={thread_ts_str}, existing_thread_name={existing_thread_name}",
            channel=channel,
            ts=ts,
            thread_ts=thread_ts_str,
        )

        if existing_thread_name:
            # Use the existing thread name to reply to the correct thread
            payload["thread"] = {"name": existing_thread_name}
            log_with_context(
                logging.DEBUG,
                f"Message {ts} is replying to existing thread {existing_thread_name} (original ts={thread_ts_str})",
                channel=channel,
                ts=ts,
                thread_ts=thread_ts_str,
                thread_name=existing_thread_name,
            )
        else:
            # Fallback to thread_key if we don't have the thread name yet
            # This might happen if messages are processed out of order
            payload["thread"] = {"thread_key": thread_ts_str}
            log_with_context(
                logging.WARNING,
                f"Message {ts} is replying to thread {thread_ts_str} but no thread name found, using thread_key fallback",
                channel=channel,
                ts=ts,
                thread_ts=thread_ts_str,
            )

        # Set message reply option to fallback to new thread if needed
        message_reply_option = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
    else:
        # For new thread starters, use their own timestamp as the thread_key
        payload["thread"] = {"thread_key": str(ts)}
        log_with_context(
            logging.DEBUG,
            f"Creating new thread with thread.thread_key: {ts}",
            channel=channel,
            ts=ts,
        )

    return payload, user_email, is_thread_reply, message_reply_option


def _generate_message_id(ts: str, is_edited: bool, edited_ts: str) -> str:
    """Generate a unique Google Chat ``messageId`` for a Slack message.

    Combines the Slack timestamp, current wall-clock milliseconds, and a
    random UUID fragment.  Falls back to an MD5-based shorter form when
    the raw ID exceeds the Google Chat 63-character limit.
    """
    clean_ts = ts.replace(".", "-")
    current_ms = int(time.time() * 1000)
    unique_id = str(uuid.uuid4()).replace("-", "")[:8]

    # For edited messages, include the edited timestamp in the message ID
    if is_edited:
        message_id = f"{CLIENT_EDIT_PREFIX}{clean_ts}-{current_ms}-{unique_id}"
    else:
        message_id = f"{CLIENT_MESSAGE_PREFIX}{clean_ts}-{current_ms}-{unique_id}"

    # Ensure the ID is within the Google Chat character limit
    if len(message_id) > MESSAGE_ID_MAX_LENGTH:
        # Hash the timestamps and use a shorter ID format
        hash_input = ts
        if is_edited:
            hash_input = f"{ts}:{edited_ts}"

        hash_obj = hashlib.md5(hash_input.encode())  # noqa: S324 — not used for security
        hash_digest = hash_obj.hexdigest()[:8]

        # Create a shorter ID that still maintains uniqueness
        if is_edited:
            message_id = (
                f"{CLIENT_EDIT_PREFIX}{hash_digest}-{current_ms}-{unique_id[:4]}"
            )
        else:
            message_id = (
                f"{CLIENT_MESSAGE_PREFIX}{hash_digest}-{current_ms}-{unique_id[:4]}"
            )

    return message_id


def _process_attachments(
    user_resolver: Any,
    attachment_processor: Any,
    message: dict[str, Any],
    channel: str,
    space: str,
    user_id: str,
    user_email: str | None,
    chat_service: Any,
    payload: dict[str, Any],
    ts: str,
) -> None:
    """Process file attachments and update *payload* in-place.

    Drive-file attachments are converted to inline links appended to the
    message text.  Non-Drive attachments are added to the ``attachment``
    field of the payload.
    """
    # For impersonated users, ensure they have access to any drive files
    sender_email = None
    if user_email and not user_resolver.is_external_user(user_email):
        sender_email = user_email

    # Process file attachments with the sender's email to ensure proper permissions
    attachments = attachment_processor.process_message_attachments(
        message,
        channel,
        space,
        user_id,
        chat_service,
        sender_email=sender_email,
    )

    # TEMPORARY SOLUTION: For Drive files, append links to message text instead of using attachments
    # This is because the Drive file attachment method is not working correctly
    drive_links = []
    non_drive_attachments = []

    if attachments:
        for attachment in attachments:
            if "driveDataRef" in attachment:
                # This is a Drive file attachment
                drive_file_id = attachment.get("driveDataRef", {}).get("driveFileId")
                if drive_file_id:
                    # Construct standard Drive link from the file ID
                    drive_link = f"https://drive.google.com/file/d/{drive_file_id}/view"
                    drive_links.append(drive_link)
                    log_with_context(
                        logging.DEBUG,
                        f"Converting Drive attachment to link: {drive_link}",
                        channel=channel,
                        ts=ts,
                        drive_file_id=drive_file_id,
                    )
            else:
                # Keep non-Drive attachments as they are
                non_drive_attachments.append(attachment)

        # If we have Drive links, append them to the message text
        if drive_links:
            # Only add newlines if the message text is not empty
            if payload["text"].strip():
                links_text = "\n\n" + "\n".join(
                    [f"\U0001f4ce {link}" for link in drive_links]
                )
            else:
                # If message is empty, don't add extra newlines
                links_text = "\n".join([f"\U0001f4ce {link}" for link in drive_links])

            payload["text"] = payload["text"] + links_text
            log_with_context(
                logging.DEBUG,
                f"Appended {len(drive_links)} Drive links to message text for {ts}",
                channel=channel,
                ts=ts,
            )

        # Only add non-Drive attachments to the payload
        if non_drive_attachments:
            payload["attachment"] = non_drive_attachments
            log_with_context(
                logging.DEBUG,
                f"Added {len(non_drive_attachments)} non-Drive attachments to message payload for {ts}",
                channel=channel,
                ts=ts,
            )
    else:
        log_with_context(
            logging.DEBUG,
            f"No attachments processed for message {ts}",
            channel=channel,
            ts=ts,
        )
