"""
Functions for handling message processing during Slack to Google Chat migration
"""

import datetime
import hashlib
import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

from slack_migrator.utils.api import slack_ts_to_rfc3339
from slack_migrator.utils.formatting import convert_formatting, parse_slack_blocks
from slack_migrator.utils.logging import (
    log_with_context,
)


def process_reactions_batch(
    migrator, message_name: str, reactions: List[Dict], message_id: str
):
    """Process reactions for a message in import mode."""

    def reaction_callback(request_id, response, exception):
        if exception:
            log_with_context(
                logging.WARNING,
                "Failed to add reaction in batch",
                error=str(exception),
                message_id=message_id,
                request_id=request_id,
                channel=getattr(migrator, "current_channel", None),
            )
        else:
            log_with_context(
                logging.DEBUG,
                "Successfully added reaction in batch",
                message_id=message_id,
                request_id=request_id,
                channel=getattr(migrator, "current_channel", None),
            )

    # Group reactions by user for batch processing
    requests_by_user: Dict[str, List[str]] = defaultdict(list)
    reaction_count = 0

    log_with_context(
        logging.DEBUG,
        f"Processing {len(reactions)} reaction types for message {message_id}",
        message_id=message_id,
        channel=getattr(migrator, "current_channel", None),
    )

    for react in reactions:
        try:
            # Convert Slack emoji name to Unicode emoji if possible
            import emoji

            emo = emoji.emojize(f":{react['name']}:", language="alias")
            emoji_name = react["name"]
            emoji_users = react.get("users", [])

            log_with_context(
                logging.DEBUG,
                f"Processing emoji :{emoji_name}: with {len(emoji_users)} users",
                message_id=message_id,
                emoji=emoji_name,
                channel=getattr(migrator, "current_channel", None),
            )

            for uid in emoji_users:
                # Check if this reaction is from a bot and bots should be ignored
                if migrator.config.get("ignore_bots", False):
                    user_data = migrator._get_user_data(uid)
                    if user_data and user_data.get("is_bot", False):
                        log_with_context(
                            logging.DEBUG,
                            f"Skipping reaction :{emoji_name}: from bot user {uid} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                            message_id=message_id,
                            emoji=emoji_name,
                            user_id=uid,
                            channel=getattr(migrator, "current_channel", None),
                        )
                        continue

                email = migrator.user_map.get(uid)
                if email:
                    # Get the internal email for this user (handles external users)
                    internal_email = migrator._get_internal_email(uid, email)
                    if internal_email:  # Only process if we get a valid internal email
                        requests_by_user[internal_email].append(emo)
                        reaction_count += 1  # Count every reaction we process
                    # If internal_email is None, this user was filtered out (e.g., ignored bot)
                else:
                    # Handle unmapped user reaction with new graceful approach
                    reaction_name = react.get("name", "unknown")
                    message_ts = getattr(migrator, "current_message_ts", "unknown")
                    # Call the handler and actually use the return value to decide whether to process
                    should_process = migrator._handle_unmapped_user_reaction(
                        uid, reaction_name, message_ts
                    )
                    # Skip this reaction as _handle_unmapped_user_reaction returns False
                    continue
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to process reaction {react.get('name')}: {str(e)}",
                message_id=message_id,
                error=str(e),
                channel=getattr(migrator, "current_channel", None),
            )

    # Always increment the reaction count, regardless of dry run mode
    migrator.migration_summary["reactions_created"] += reaction_count

    if migrator.dry_run:
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would add {reaction_count} reactions from {len(requests_by_user)} users to message {message_id}",
            message_id=message_id,
            channel=getattr(migrator, "current_channel", None),
        )
        return

    log_with_context(
        logging.DEBUG,
        f"Adding {reaction_count} reactions from {len(requests_by_user)} users to message {message_id}",
        message_id=message_id,
        channel=getattr(migrator, "current_channel", None),
    )

    user_batches: Dict[str, BatchHttpRequest] = {}

    for email, emojis in requests_by_user.items():
        # Always skip external users' reactions to avoid false attribution to admin
        if migrator._is_external_user(email):
            log_with_context(
                logging.INFO,
                f"Skipping {len(emojis)} reactions from external user {email} to avoid admin attribution",
                message_id=message_id,
                user=email,
                channel=getattr(migrator, "current_channel", None),
            )
            continue

        # Process reactions normally for mapped internal users
        # We already skipped unmapped users earlier in the code

        svc = migrator._get_delegate(email)
        # If impersonation failed, svc will be the admin service.
        # We process these synchronously as we can't batch across users.
        if svc == migrator.chat:
            log_with_context(
                logging.DEBUG,
                f"Using admin account for user {email} (impersonation not available)",
                message_id=message_id,
                user=email,
                channel=getattr(migrator, "current_channel", None),
            )

            for emo in emojis:
                try:
                    reaction_body = {"emoji": {"unicode": emo}}
                    result = (
                        migrator.chat.spaces()
                        .messages()
                        .reactions()
                        .create(parent=message_name, body=reaction_body)
                        .execute()
                    )
                except HttpError as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction for external user: {e}",
                        error_code=e.resp.status,
                        user=email,
                        message_id=message_id,
                        channel=getattr(migrator, "current_channel", None),
                    )
            continue

        svc = migrator._get_delegate(email)
        # If impersonation failed, svc will be the admin service.
        # We process these synchronously as we can't batch across users.
        if svc == migrator.chat:
            log_with_context(
                logging.DEBUG,
                f"Using admin account for user {email} (impersonation not available)",
                message_id=message_id,
                user=email,
                channel=getattr(migrator, "current_channel", None),
            )

            for emo in emojis:
                try:
                    # Format reaction body according to the import documentation
                    # https://developers.google.com/workspace/chat/import-data
                    reaction_body = {"emoji": {"unicode": emo}}
                    result = (
                        svc.spaces()
                        .messages()
                        .reactions()
                        .create(parent=message_name, body=reaction_body)
                        .execute()
                    )
                except HttpError as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction: {e}",
                        error_code=e.resp.status,
                        user=email,
                        message_id=message_id,
                        channel=getattr(migrator, "current_channel", None),
                    )
            continue

        # For impersonated users, create batches
        log_with_context(
            logging.DEBUG,
            f"Creating batch request for user {email} with {len(emojis)} reactions",
            message_id=message_id,
            user=email,
            channel=getattr(migrator, "current_channel", None),
        )

        if email not in user_batches:
            user_batches[email] = svc.new_batch_http_request(callback=reaction_callback)

        for emo in emojis:
            # Format reaction body according to the import documentation
            # https://developers.google.com/workspace/chat/import-data
            reaction_body = {"emoji": {"unicode": emo}}

            try:
                # Make sure we're using the correct API method format
                # The Google Chat API expects spaces().messages().reactions().create()
                request = (
                    svc.spaces()
                    .messages()
                    .reactions()
                    .create(parent=message_name, body=reaction_body)
                )
                user_batches[email].add(request)
            except AttributeError as e:
                # Handle the 'Resource' object has no attribute 'create' error
                log_with_context(
                    logging.WARNING,
                    f"Failed to create reaction request: {e}. Falling back to direct API call.",
                    message_id=message_id,
                    user=email,
                    emoji=emo,
                    channel=getattr(migrator, "current_channel", None),
                )
                # Fall back to direct API call
                try:
                    result = (
                        svc.spaces()
                        .messages()
                        .reactions()
                        .create(parent=message_name, body=reaction_body)
                        .execute()
                    )
                except Exception as inner_e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction in fallback mode: {inner_e}",
                        message_id=message_id,
                        user=email,
                        emoji=emo,
                    )

    for email, batch in user_batches.items():
        try:
            log_with_context(
                logging.DEBUG,
                f"Executing batch request for user {email}",
                message_id=message_id,
                user=email,
                channel=getattr(migrator, "current_channel", None),
            )
            batch.execute()
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Reaction batch execution failed for user {email}: {e}",
                message_id=message_id,
                user=email,
                channel=getattr(migrator, "current_channel", None),
                error=str(e),
            )


def send_message(migrator, space: str, message: Dict) -> Optional[str]:
    """Send a message to a Google Chat space.

    This method handles converting a Slack message to a Google Chat message format
    and sending it to the specified space.

    Args:
        migrator: The migrator instance
        space: The space ID to send the message to
        message: The Slack message to convert and send

    Returns:
        The message name of the sent message, or None if there was an error
    """
    # Ensure thread_map exists
    if not hasattr(migrator, "thread_map") or migrator.thread_map is None:
        migrator.thread_map = {}

    # Ensure sent_messages tracking set exists
    if not hasattr(migrator, "sent_messages"):
        migrator.sent_messages = set()

    # Ensure message_id_map exists to track Slack ts -> Google Chat message name mapping
    if not hasattr(migrator, "message_id_map"):
        migrator.message_id_map = {}

    # Extract basic message info for logging
    ts = message.get("ts", "")
    user_id = message.get("user", "")
    thread_ts = message.get("thread_ts")
    channel = migrator.current_channel

    # Check if this message is from a bot and bots should be ignored
    if migrator.config.get("ignore_bots", False):
        # Check for bot messages by subtype (covers system bots like USLACKBOT that aren't in users.json)
        if message.get("subtype") in ["bot_message", "app_message"]:
            bot_name = message.get("username", user_id or "Unknown Bot")
            log_with_context(
                logging.DEBUG,
                f"Skipping bot message from {bot_name} (subtype: {message.get('subtype')}) - ignore_bots enabled",
                channel=channel,
                ts=ts,
                user_id=user_id,
                bot_name=bot_name,
            )
            return "IGNORED_BOT"

        # Also check for user-based bots (bots that are in users.json)
        if user_id:
            user_data = migrator._get_user_data(user_id)
            if user_data and user_data.get("is_bot", False):
                log_with_context(
                    logging.DEBUG,
                    f"Skipping message from bot user {user_id} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                    channel=channel,
                    ts=ts,
                    user_id=user_id,
                )
                return "IGNORED_BOT"

    # Check for edited messages
    edited = message.get("edited", {})
    edited_ts = edited.get("ts", "") if edited else ""
    is_edited = bool(edited_ts)

    # Create a message key that includes edit information if present
    message_key = f"{channel}:{ts}"
    if is_edited:
        message_key = f"{channel}:{ts}:edited:{edited_ts}"

    # Check if this message has already been sent successfully, but only in update mode
    # In create mode, we always send messages regardless of what was sent before
    is_update_mode = getattr(migrator, "update_mode", False)

    # First, check if this message is older than the last processed timestamp
    if is_update_mode and hasattr(migrator, "last_processed_timestamps"):
        last_timestamp = migrator.last_processed_timestamps.get(channel, 0)
        if last_timestamp > 0:
            # Import the function to check if we should process this message
            from slack_migrator.services.discovery import should_process_message

            if not should_process_message(last_timestamp, ts):
                log_with_context(
                    logging.INFO,
                    f"[UPDATE MODE] Skipping message TS={ts} from user={user_id} (older than last processed timestamp)",
                    channel=channel,
                    ts=ts,
                    user_id=user_id,
                    last_timestamp=last_timestamp,
                )
                # Return a placeholder to indicate success
                return "ALREADY_SENT"

    # Also check the sent_messages set for additional protection
    if is_update_mode and message_key in migrator.sent_messages:
        log_with_context(
            logging.INFO,
            f"[UPDATE MODE] Skipping already sent message TS={ts} from user={user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
        # Return a placeholder to indicate success
        return "ALREADY_SENT"

    # Only increment the message count in non-dry run mode
    # In dry run mode, this is handled in the migrate method
    if not migrator.dry_run:
        migrator.migration_summary["messages_created"] += 1

    if migrator.dry_run:
        mode_prefix = "[DRY RUN]"
        if is_update_mode:
            mode_prefix = "[DRY RUN] [UPDATE MODE]"

        log_with_context(
            logging.DEBUG,
            f"{mode_prefix} Would send message TS={ts} from user={user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
            is_thread_reply=(thread_ts is not None and thread_ts != ts),
        )
        return None

    # Skip messages with no text content (like channel join/leave messages)
    if message.get("subtype") in ["channel_join", "channel_leave"]:
        log_with_context(
            logging.DEBUG,
            f"Skipping {message.get('subtype')} message from {user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
        return "SKIPPED"

    # Extract text from Slack blocks (rich formatting) or fallback to plain text
    text = parse_slack_blocks(message)

    # Skip empty messages
    if not text.strip() and "files" not in message:
        log_with_context(
            logging.DEBUG,
            f"Skipping empty message from {user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
        )
        return None

    # Create a mapping dictionary that has all user mapping overrides applied for ALL users
    user_map_with_overrides = {}

    # Map ALL user IDs with their proper overrides by iterating over .items()
    # This ensures we never miss any mentions and handles all potential edge cases
    for slack_user_id, email in migrator.user_map.items():
        internal_email = migrator._get_internal_email(slack_user_id, email)
        if internal_email:  # Only add if we got a valid email back
            user_map_with_overrides[slack_user_id] = internal_email

    # Set current message context for enhanced user tracking
    migrator.current_message_ts = ts

    # Convert Slack formatting to Google Chat formatting using the correct mapping
    formatted_text = convert_formatting(text, user_map_with_overrides, migrator)

    # For edited messages, add an edit indicator
    if is_edited:
        edit_time = datetime.datetime.fromtimestamp(float(edited_ts)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        formatted_text = f"{formatted_text}\n\n_(edited at {edit_time})_"

    # Convert Slack timestamp to RFC3339 format for Google Chat
    create_time = slack_ts_to_rfc3339(ts)

    # Prepare the message payload
    payload: Dict[str, Any] = {"createTime": create_time}

    # Set the sender if available
    user_email = migrator.user_map.get(user_id)
    sender_email = None
    final_text = formatted_text

    if user_email:
        # Get the internal email for this user (now just returns the mapped email)
        internal_email = migrator._get_internal_email(user_id, user_email)
        if internal_email:
            # Check if this is an external user - if so, use admin with attribution
            if migrator._is_external_user(internal_email):
                # External user - send via admin with attribution
                admin_email, attributed_text = migrator._handle_unmapped_user_message(
                    user_id, formatted_text
                )
                sender_email = admin_email
                final_text = attributed_text
                payload["sender"] = {"type": "HUMAN", "name": f"users/{admin_email}"}
            else:
                # Regular internal user - send directly
                sender_email = internal_email
                payload["sender"] = {"type": "HUMAN", "name": f"users/{internal_email}"}
        else:
            # This shouldn't happen if user_email exists, but handle it gracefully
            admin_email, attributed_text = migrator._handle_unmapped_user_message(
                user_id, formatted_text
            )
            sender_email = admin_email
            final_text = attributed_text
            payload["sender"] = {"type": "HUMAN", "name": f"users/{admin_email}"}
    elif user_id:  # We have a user_id but no mapping
        # Handle unmapped user with new graceful approach
        admin_email, attributed_text = migrator._handle_unmapped_user_message(
            user_id, formatted_text
        )
        sender_email = admin_email
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
        existing_thread_name = migrator.thread_map.get(thread_ts_str)

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
        # Get the appropriate service for this user (impersonation)
        chat_service = migrator.chat  # Default to admin service

        # If we have a valid user email, try to use impersonation
        if user_email and not migrator._is_external_user(user_email):
            chat_service = migrator._get_delegate(user_email)

            # Log whether we're using impersonation or falling back to admin
            if chat_service != migrator.chat:
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

        # Generate a message ID that's unique for each attempt
        # This avoids conflicts when retrying
        import time
        import uuid

        # Create a truly unique ID by combining:
        # 1. A prefix to identify the source
        # 2. The timestamp from Slack (cleaned)
        # 3. Current execution time to ensure uniqueness across retries
        # 4. A UUID to guarantee uniqueness
        clean_ts = ts.replace(".", "-")
        current_ms = int(time.time() * 1000)
        unique_id = str(uuid.uuid4()).replace("-", "")[:8]

        # For edited messages, include the edited timestamp in the message ID
        if is_edited:
            # Use a different format for edited messages to avoid conflicts
            clean_edited_ts = edited_ts.replace(".", "-")
            # Include both timestamps to ensure uniqueness
            message_id = f"client-slack-edit-{clean_ts}-{current_ms}-{unique_id}"
        else:
            message_id = f"client-slack-{clean_ts}-{current_ms}-{unique_id}"

        # Ensure the ID is within the 63-character limit
        if len(message_id) > 63:
            # Hash the timestamps and use a shorter ID format
            hash_input = ts
            if is_edited:
                hash_input = f"{ts}:{edited_ts}"

            hash_obj = hashlib.md5(hash_input.encode())
            hash_digest = hash_obj.hexdigest()[:8]

            # Create a shorter ID that still maintains uniqueness
            if is_edited:
                message_id = (
                    f"client-slack-edit-{hash_digest}-{current_ms}-{unique_id[:4]}"
                )
            else:
                message_id = f"client-slack-{hash_digest}-{current_ms}-{unique_id[:4]}"

        result = None

        # Process file attachments using the user's service to avoid permission issues
        # This ensures that the attachment tokens are created by the same user who will send the message

        # For impersonated users, ensure they have access to any drive files
        sender_email = None
        if user_email and not migrator._is_external_user(user_email):
            sender_email = user_email

        # Process file attachments with the sender's email to ensure proper permissions
        attachments = migrator.attachment_processor.process_message_attachments(
            message, channel, space, user_id, chat_service, sender_email=sender_email
        )

        # TEMPORARY SOLUTION: For Drive files, append links to message text instead of using attachments
        # This is because the Drive file attachment method is not working correctly
        drive_links = []
        non_drive_attachments = []

        if attachments:
            for attachment in attachments:
                if "driveDataRef" in attachment:
                    # This is a Drive file attachment
                    drive_file_id = attachment.get("driveDataRef", {}).get(
                        "driveFileId"
                    )
                    if drive_file_id:
                        # Construct standard Drive link from the file ID
                        drive_link = (
                            f"https://drive.google.com/file/d/{drive_file_id}/view"
                        )
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
                        [f"📎 {link}" for link in drive_links]
                    )
                else:
                    # If message is empty, don't add extra newlines
                    links_text = "\n".join([f"📎 {link}" for link in drive_links])

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

        # Create a new message with the Google Chat API
        # In import mode, we create separate messages for edits
        request_params = {
            "parent": space,
            "body": payload,
            "messageId": message_id,
        }

        # Add messageReplyOption if needed
        if message_reply_option:
            request_params["messageReplyOption"] = message_reply_option

        # Send the message using the appropriate service
        log_with_context(
            logging.DEBUG,
            f"Complete message payload for {ts}: {payload}",
            channel=channel,
            ts=ts,
        )
        result = chat_service.spaces().messages().create(**request_params).execute()
        message_name = result.get("name")

        # Store the message ID mapping for potential future edits
        if message_name:
            # For edited messages, store with a special key that includes the edit timestamp
            if is_edited:
                edit_key = f"{ts}:edited:{edited_ts}"
                migrator.message_id_map[edit_key] = message_name
                log_with_context(
                    logging.DEBUG,
                    f"Stored message ID mapping for edited message: {edit_key} -> {message_name}",
                    channel=channel,
                    ts=ts,
                    edited_ts=edited_ts,
                )
            else:
                migrator.message_id_map[ts] = message_name

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
                    migrator.thread_map[str(ts)] = thread_name
                    log_with_context(
                        logging.DEBUG,
                        f"Stored new thread mapping: {ts} -> {thread_name}",
                        channel=channel,
                        ts=ts,
                    )
                else:
                    # For thread replies, ensure the original thread timestamp mapping exists
                    thread_ts_str = str(thread_ts)
                    if thread_ts_str not in migrator.thread_map:
                        # Store the mapping using the original thread timestamp
                        migrator.thread_map[thread_ts_str] = thread_name
                        log_with_context(
                            logging.DEBUG,
                            f"Stored thread mapping from reply: {thread_ts_str} -> {thread_name}",
                            channel=channel,
                            ts=ts,
                            thread_ts=thread_ts_str,
                        )
                    else:
                        # Verify the mapping is consistent
                        existing_thread_name = migrator.thread_map[thread_ts_str]
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
            migrator.current_message_ts = ts

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
                migrator, message_name, message["reactions"], final_message_id
            )

        log_with_context(
            logging.DEBUG,
            f"Successfully sent message TS={ts} → {message_name}",
            channel=channel,
            ts=ts,
            message_name=message_name,
        )

        # Mark this message as successfully sent to avoid duplicates
        migrator.sent_messages.add(message_key)

        return message_name
    except HttpError as e:
        error_message = f"Failed to send message: {e}"
        error_code = e.resp.status if hasattr(e, "resp") else "unknown"
        error_details = e.content.decode("utf-8") if hasattr(e, "content") else str(e)

        log_with_context(
            logging.ERROR,
            error_message,
            channel=channel,
            ts=ts,
            error_code=error_code,
            error_details=error_details[:500]
            + ("..." if len(error_details) > 500 else ""),
        )

        # Add to failed messages list for reporting
        failed_msg = {
            "channel": channel,
            "ts": ts,
            "error": f"{error_message} (Code: {error_code})",
            "error_details": error_details,
            "payload": message,
        }
        migrator.failed_messages.append(failed_msg)

        return None


def track_message_stats(migrator, m):
    """Handle tracking message stats in both dry run and normal mode."""
    # Get the current channel being processed
    channel = migrator.current_channel
    ts = m.get("ts", "")
    user_id = m.get("user", "")

    # Check if this message is from a bot and bots should be ignored
    if migrator.config.get("ignore_bots", False):
        # Check for bot messages by subtype (covers system bots like USLACKBOT that aren't in users.json)
        if m.get("subtype") in ["bot_message", "app_message"]:
            bot_name = m.get("username", user_id or "Unknown Bot")
            log_with_context(
                logging.DEBUG,
                f"Skipping stats tracking for bot message from {bot_name} (subtype: {m.get('subtype')}) - ignore_bots enabled",
                channel=channel,
                ts=ts,
                user_id=user_id,
                bot_name=bot_name,
            )
            return

        # Also check for user-based bots (bots that are in users.json)
        if user_id:
            user_data = migrator._get_user_data(user_id)
            if user_data and user_data.get("is_bot", False):
                log_with_context(
                    logging.DEBUG,
                    f"Skipping stats tracking for bot message from {user_id} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                    channel=channel,
                    ts=ts,
                    user_id=user_id,
                )
                return

    # Check if we're in update mode
    is_update_mode = getattr(migrator, "update_mode", False)

    # Initialize channel stats if not already done
    if not hasattr(migrator, "channel_stats"):
        migrator.channel_stats = {}

    if channel not in migrator.channel_stats:
        migrator.channel_stats[channel] = {
            "message_count": 0,
            "reaction_count": 0,
            "file_count": 0,
        }

    # In update mode, we might need to skip stats tracking for messages
    # that have already been processed
    if is_update_mode:
        # Ensure sent_messages tracking set exists
        if not hasattr(migrator, "sent_messages"):
            migrator.sent_messages = set()

        message_key = f"{channel}:{ts}"
        edited = m.get("edited", {})
        edited_ts = edited.get("ts", "") if edited else ""
        if edited_ts:
            message_key = f"{channel}:{ts}:edited:{edited_ts}"

        # If this message has already been sent in a previous run, don't count it
        if message_key in migrator.sent_messages:
            log_with_context(
                logging.DEBUG,
                f"[UPDATE MODE] Skipping stats for already sent message {ts}",
                channel=channel,
                ts=ts,
            )
            return

    # Increment message count for this channel
    migrator.channel_stats[channel]["message_count"] += 1

    # Track reactions
    reaction_count = 0
    if "reactions" in m:
        # Count reactions excluding bots if ignore_bots is enabled
        for reaction in m["reactions"]:
            for user_id in reaction.get("users", []):
                # Skip bot reactions if ignore_bots is enabled
                if migrator.config.get("ignore_bots", False):
                    user_data = migrator._get_user_data(user_id)
                    if user_data and user_data.get("is_bot", False):
                        continue
                reaction_count += 1

        migrator.channel_stats[channel]["reaction_count"] += reaction_count

        # Also increment the global reaction count in dry run mode
        # (in normal mode this is done by process_reactions_batch)
        if migrator.dry_run:
            migrator.migration_summary["reactions_created"] += reaction_count

            mode_prefix = "[DRY RUN]"
            if is_update_mode:
                mode_prefix = "[DRY RUN] [UPDATE MODE]"

            log_with_context(
                logging.DEBUG,
                f"{mode_prefix} Counted {reaction_count} reactions for message {ts}",
                channel=channel,
                ts=ts,
            )

    # Track files using attachment processor
    file_count = migrator.attachment_processor.count_message_files(m)
    if file_count > 0:
        mode_prefix = ""
        if is_update_mode:
            mode_prefix = "[UPDATE MODE] "

        log_with_context(
            logging.DEBUG,
            f"{mode_prefix}Found {file_count} files to process in message {ts}",
            channel=channel,
            ts=ts,
        )
        migrator.channel_stats[channel]["file_count"] += file_count

        # Also increment the global file count
        migrator.migration_summary["files_created"] += file_count

    # We don't need to process files here - they are handled in send_message


def send_intro(migrator, space: str, channel: str):
    """Send an intro message with channel metadata."""
    # Check if we're in update mode - if so, don't send intro message again
    is_update_mode = getattr(migrator, "update_mode", False)
    if is_update_mode:
        log_with_context(
            logging.INFO,
            f"[UPDATE MODE] Skipping intro message for channel {channel}",
            channel=channel,
        )
        return

    # Get channel metadata
    meta = migrator.channels_meta.get(channel, {})

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
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Sending intro message to space {space} for channel {channel}",
        channel=channel,
    )

    if migrator.dry_run:
        # In dry run mode, just count the message
        migrator.migration_summary["messages_created"] += 1
        return

    # Create the message
    try:
        message_body = {
            "text": intro_text,
            "createTime": slack_ts_to_rfc3339(f"{time.time()}.000000"),
            # Explicitly set the sender as the workspace admin
            "sender": {"type": "HUMAN", "name": f"users/{migrator.workspace_admin}"},
        }

        # Send the message
        result = (
            migrator.chat.spaces()
            .messages()
            .create(parent=space, body=message_body)
            .execute()
        )
        # Increment the counter
        migrator.migration_summary["messages_created"] += 1

        log_with_context(
            logging.INFO, f"Sent intro message to space {space}", channel=channel
        )
    except Exception as e:
        log_with_context(
            logging.WARNING,
            f"Failed to send intro message to space {space}: {e}",
            channel=channel,
            error=str(e),
        )


def log_space_mapping_conflicts(migrator):
    """
    Log information about space mapping conflicts that need to be resolved.
    """
    if migrator.dry_run:
        log_with_context(logging.INFO, "[DRY RUN] Checking for space mapping conflicts")

    # Log any conflicts that should be added to config
    if hasattr(migrator, "channel_conflicts") and migrator.channel_conflicts:
        log_with_context(
            logging.WARNING,
            f"Found {len(migrator.channel_conflicts)} channels with duplicate space conflicts",
        )
        log_with_context(
            logging.WARNING,
            "Add the following entries to your config.yaml to resolve conflicts:",
        )
        log_with_context(logging.WARNING, "space_mapping:")
        for channel_name in migrator.channel_conflicts:
            log_with_context(
                logging.WARNING,
                f'  "{channel_name}": "<space_id>"  # Replace with the desired space ID',
            )


def load_space_mappings(migrator):
    """Load space mappings for update mode.

    This uses the Google Chat API for discovery and the config file for overrides.
    No persisted mapping files are used anymore.

    Returns:
        dict: Mapping from channel names to space IDs, or empty dict if not found
    """
    try:
        # Initialize the channel_id_to_space_id mapping if not present
        if not hasattr(migrator, "channel_id_to_space_id"):
            migrator.channel_id_to_space_id = {}

        # Use API discovery to find spaces
        from slack_migrator.services.discovery import discover_existing_spaces

        discovered_spaces, duplicate_spaces = discover_existing_spaces(migrator)

        # Log the discovery results
        if discovered_spaces:
            log_with_context(
                logging.INFO,
                f"Discovered {len(discovered_spaces)} existing spaces via API",
            )

        # Look for space_mapping overrides in config
        if "space_mapping" in migrator.config:
            space_mapping = migrator.config["space_mapping"]
            if space_mapping:  # Only proceed if space_mapping is not None or empty
                log_with_context(
                    logging.INFO,
                    f"Found {len(space_mapping)} space mapping overrides in config",
                )

                # Apply space mappings from config (overriding API discovery)
                for channel_name, space_id in space_mapping.items():
                    channel_id = migrator.channel_name_to_id.get(channel_name, "")
                    if channel_id:
                        # Override any discovered mapping with the config value
                        migrator.channel_id_to_space_id[channel_id] = space_id

                        # Also update the name-based mapping for backward compatibility
                        discovered_spaces[channel_name] = f"spaces/{space_id}"
                    else:
                        log_with_context(
                            logging.WARNING,
                            f"Channel '{channel_name}' in space_mapping config not found in workspace",
                        )

        return discovered_spaces if discovered_spaces else {}

    except Exception as e:
        log_with_context(
            logging.WARNING, f"Failed to load space mappings: {e}", error=str(e)
        )
        return {}
