"""
Functions for handling message processing during Slack to Google Chat migration
"""

import json
import logging
import time
from collections import defaultdict
from typing import Dict, List, Optional, Any
import datetime
import os
import hashlib

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

from slack_migrator.utils.logging import logger, log_with_context, log_api_request, log_api_response
from slack_migrator.utils.api import retry, slack_ts_to_rfc3339
from slack_migrator.utils.formatting import convert_formatting


def process_reactions_batch(migrator, message_name: str, reactions: List[Dict], message_id: str):
    """Process reactions for a message in import mode."""

    def reaction_callback(request_id, response, exception):
        if exception:
            log_with_context(
                logging.WARNING,
                "Failed to add reaction in batch",
                error=str(exception),
                message_id=message_id,
                request_id=request_id,
            )
        else:
            log_with_context(
                logging.DEBUG,
                "Successfully added reaction in batch",
                message_id=message_id,
                request_id=request_id,
            )

    # Group reactions by user for batch processing
    requests_by_user: Dict[str, List[str]] = defaultdict(list)
    reaction_count = 0
    
    log_with_context(
        logging.DEBUG,
        f"Processing {len(reactions)} reaction types for message {message_id}",
        message_id=message_id
    )
    
    for react in reactions:
        try:
            # Convert Slack emoji name to Unicode emoji if possible
            import emoji

            emo = emoji.emojize(f":{react['name']}:", language="alias")
            emoji_name = react['name']
            emoji_users = react.get("users", [])
            
            log_with_context(
                logging.DEBUG,
                f"Processing emoji :{emoji_name}: with {len(emoji_users)} users",
                message_id=message_id,
                emoji=emoji_name
            )
            
            for uid in emoji_users:
                email = migrator.user_map.get(uid)
                if email:
                    # Get the internal email for this user (handles external users)
                    internal_email = migrator._get_internal_email(uid, email)
                    requests_by_user[internal_email].append(emo)
                    reaction_count += 1  # Count every reaction we process
                else:
                    log_with_context(
                        logging.WARNING,
                        f"No email mapping found for user {uid}, skipping reaction",
                        message_id=message_id,
                        emoji=emoji_name,
                        user_id=uid
                    )
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to process reaction {react.get('name')}: {str(e)}",
                message_id=message_id,
                error=str(e)
            )
    
    # Always increment the reaction count, regardless of dry run mode
    migrator.migration_summary["reactions_created"] += reaction_count
    
    if migrator.dry_run:
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would add {reaction_count} reactions from {len(requests_by_user)} users to message {message_id}",
            message_id=message_id
        )
        return
    
    log_with_context(
        logging.DEBUG,
        f"Adding {reaction_count} reactions from {len(requests_by_user)} users to message {message_id}",
        message_id=message_id
    )
        
    user_batches: Dict[str, BatchHttpRequest] = {}

    for email, emojis in requests_by_user.items():
        # Skip external users who cannot be impersonated
        if migrator._is_external_user(email):
            log_with_context(
                logging.DEBUG,
                f"Adding {len(emojis)} reactions from external user {email} using admin account",
                message_id=message_id,
                user=email
            )
            # For external users, use the admin account to add reactions
            for emo in emojis:
                try:
                    reaction_body = {"emoji": {"unicode": emo}}
                    log_api_request(
                        "POST", 
                        "chat.spaces.messages.reactions.create", 
                        reaction_body, 
                        message_id=message_id,
                        user=email
                    )

                    result = migrator.chat.spaces().messages().reactions().create(
                        parent=message_name, body=reaction_body
                    ).execute()
                    
                    log_api_response(
                        200, 
                        "chat.spaces.messages.reactions.create", 
                        result, 
                        message_id=message_id,
                        user=email
                    )
                except HttpError as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction for external user: {e}",
                        error_code=e.resp.status,
                        user=email,
                        message_id=message_id
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
                user=email
            )
            
            for emo in emojis:
                try:
                    # Format reaction body according to the import documentation
                    # https://developers.google.com/workspace/chat/import-data
                    reaction_body = {"emoji": {"unicode": emo}}
                    log_api_request(
                        "POST", 
                        "chat.spaces.messages.reactions.create", 
                        reaction_body, 
                        message_id=message_id,
                        user=email
                    )

                    result = svc.spaces().messages().reactions().create(
                        parent=message_name, body=reaction_body
                    ).execute()
                    
                    log_api_response(
                        200, 
                        "chat.spaces.messages.reactions.create", 
                        result, 
                        message_id=message_id,
                        user=email
                    )
                except HttpError as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction: {e}",
                        error_code=e.resp.status,
                        user=email,
                        message_id=message_id
                    )
            continue

        # For impersonated users, create batches
        log_with_context(
            logging.DEBUG,
            f"Creating batch request for user {email} with {len(emojis)} reactions",
            message_id=message_id,
            user=email
        )
        
        if email not in user_batches:
            user_batches[email] = svc.new_batch_http_request(
                callback=reaction_callback
            )

        for emo in emojis:
            # Format reaction body according to the import documentation
            # https://developers.google.com/workspace/chat/import-data
            reaction_body = {"emoji": {"unicode": emo}}

            user_batches[email].add(
                svc.spaces()
                .messages()
                .reactions()
                .create(parent=message_name, body=reaction_body)
            )

    for email, batch in user_batches.items():
        try:
            log_with_context(
                logging.DEBUG,
                f"Executing batch request for user {email}",
                message_id=message_id,
                user=email
            )
            batch.execute()
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Reaction batch execution failed for user {email}: {e}",
                message_id=message_id,
                user=email,
                error=str(e)
            )


@retry()
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
        
    # Extract basic message info for logging
    ts = message.get("ts", "")
    user_id = message.get("user", "")
    thread_ts = message.get("thread_ts")
    channel = migrator.current_channel
    
    # Check if this message has already been sent successfully
    message_key = f"{channel}:{ts}"
    if message_key in migrator.sent_messages:
        log_with_context(
            logging.INFO,
            f"Skipping already sent message TS={ts} from user={user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id
        )
        # Return a placeholder to indicate success
        return "ALREADY_SENT"
    
    # Only increment the message count in non-dry run mode
    # In dry run mode, this is handled in the migrate method
    if not migrator.dry_run:
        migrator.migration_summary["messages_created"] += 1
    
    if migrator.dry_run:
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would send message TS={ts} from user={user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id,
            is_thread_reply=(thread_ts is not None and thread_ts != ts)
        )
        return None

    # Skip messages with no text content (like channel join/leave messages)
    if message.get("subtype") in ["channel_join", "channel_leave"]:
        log_with_context(
            logging.DEBUG,
            f"Skipping {message.get('subtype')} message from {user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id
        )
        return "SKIPPED"
        
    # Skip messages from bots or apps we don't want to migrate
    if message.get("subtype") in ["bot_message", "app_message"]:
        bot_name = message.get("username", "Unknown Bot")
        if bot_name in migrator.config.get("skip_bots", []):
            log_with_context(
                logging.INFO,
                f"Skipping message from bot: {bot_name}",
                channel=channel,
                ts=ts,
                bot=bot_name
            )
            return "SKIPPED"

    text = message.get("text", "")

    # Skip empty messages
    if not text.strip() and "files" not in message:
        log_with_context(
            logging.DEBUG,
            f"Skipping empty message from {user_id}",
            channel=channel,
            ts=ts,
            user_id=user_id
        )
        return None
        
    # Convert Slack formatting to Google Chat formatting
    formatted_text = convert_formatting(text, migrator.user_map)
    
    # Add placeholder text for messages with files but no text
    if not formatted_text.strip() and "files" in message:
        formatted_text = "Shared a file"
    
    # Convert Slack timestamp to RFC3339 format for Google Chat
    create_time = slack_ts_to_rfc3339(ts)

    # Prepare the message payload
    payload: Dict[str, Any] = {"createTime": create_time}

    # Set the sender if available
    user_email = migrator.user_map.get(user_id)
    if user_email:
        # Get the internal email for this user (handles external users)
        internal_email = migrator._get_internal_email(user_id, user_email)
        
        payload["sender"] = {"type": "HUMAN", "name": f"users/{internal_email}"}

    # Add message text
    payload["text"] = formatted_text
    
    # Handle thread replies
    is_thread_reply = False
    message_reply_option = None
    
    if thread_ts and thread_ts != ts:  # This is a thread reply
        is_thread_reply = True
        
        # Convert thread_ts to string for consistent lookup
        thread_ts_str = str(thread_ts)
        
        # Use thread.thread_key for all thread replies
        payload["thread"] = {"thread_key": thread_ts_str}
        # Set message reply option to fallback to new thread if needed
        message_reply_option = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"
        
        log_with_context(
            logging.DEBUG,
            f"Message {ts} is a reply to thread {thread_ts_str}",
            channel=channel,
            ts=ts,
            thread_ts=thread_ts_str
        )
    else:
        # For new thread starters, use their own timestamp as the thread_key
        payload["thread"] = {"thread_key": str(ts)}
        log_with_context(
            logging.DEBUG,
            f"Creating new thread with thread.thread_key: {ts}",
            channel=channel,
            ts=ts
        )

    log_with_context(
        logging.DEBUG,
        f"Sending message TS={ts} from user={user_id}{' (thread reply)' if is_thread_reply else ''}",
        channel=channel,
        ts=ts,
        user_id=user_id,
        is_thread_reply=is_thread_reply
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
                    user_id=user_id
                )
            else:
                log_with_context(
                    logging.DEBUG,
                    f"Using admin service for user {user_email} (impersonation not available)",
                    channel=channel,
                    ts=ts,
                    user_id=user_id
                )
        
        # Generate a consistent message ID based on the timestamp and channel
        # This ensures the same message always gets the same ID
        clean_ts = ts.replace(".", "-")
        message_id_base = f"{channel}-{clean_ts}"
        
        # Create a consistent hash for this specific message
        hash_obj = hashlib.md5(message_id_base.encode())
        hash_digest = hash_obj.hexdigest()[:8]
        message_id = f"slack-{clean_ts}-{hash_digest}"
        
        # Ensure the ID is within the 63-character limit
        if len(message_id) > 63:
            # Use a shorter ID format with just the hash
            message_id = f"slack-{hash_digest}"
        
        # Prepare API call parameters
        request_params = {
            "parent": space,
            "body": payload,
            "messageId": message_id,
        }
        
        # Add messageReplyOption if needed
        if message_reply_option:
            request_params["messageReplyOption"] = message_reply_option
        
        # Send the message using the appropriate service
        log_api_request("POST", "chat.spaces.messages.create", payload, channel=channel, ts=ts)
        result = chat_service.spaces().messages().create(**request_params).execute()
        log_api_response(200, "chat.spaces.messages.create", result, channel=channel, ts=ts)

        message_name = result.get("name")
        
        # Store thread mapping for the parent message
        if message_name and not is_thread_reply:
            thread_name = result.get("thread", {}).get("name")
            
            if thread_name:
                # Store using string keys for consistency
                migrator.thread_map[str(ts)] = thread_name
                log_with_context(
                    logging.DEBUG,
                    f"Stored new thread mapping: {ts} -> {thread_name}",
                    channel=channel,
                    ts=ts,
                )
            else:
                log_with_context(
                    logging.WARNING,
                    f"No thread name returned in API response for new thread starter",
                    channel=channel,
                    ts=ts
                )

        # Process reactions if any
        if "reactions" in message and message_name:
            # The message_id for reactions should be the final segment of the message_name
            final_message_id = message_name.split("/")[-1]
            log_with_context(
                logging.DEBUG,
                f"Processing {len(message['reactions'])} reaction types for message {ts}",
                channel=channel,
                ts=ts,
                message_id=final_message_id
            )
            process_reactions_batch(
                migrator, message_name, message["reactions"], final_message_id
            )

        # Process file attachments if any
        if "files" in message and message_name and len(message["files"]) > 0:
            log_with_context(
                logging.DEBUG,
                f"Processing {len(message['files'])} file attachments for message {ts}",
                channel=channel,
                ts=ts,
                message_id=message_name.split("/")[-1]
            )
            migrator.file_handler.process_attachments(
                chat_service, message_name, message_name.split("/")[-1], message["files"], channel
            )

        log_with_context(
            logging.DEBUG,
            f"Successfully sent message TS={ts} → {message_name}",
            channel=channel,
            ts=ts,
            message_name=message_name
        )
        
        # Mark this message as successfully sent to avoid duplicates
        message_key = f"{channel}:{ts}"
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
            error_details=error_details[:500] + ("..." if len(error_details) > 500 else "")
        )
        
        # Add to failed messages list for reporting
        failed_msg = {
            "channel": channel,
            "ts": ts,
            "error": f"{error_message} (Code: {error_code})",
            "error_details": error_details,
            "payload": message
        }
        migrator.failed_messages.append(failed_msg)
        
        return None


def track_message_stats(migrator, m):
    """Handle tracking message stats in both dry run and normal mode."""
    # Get the current channel being processed
    channel = migrator.current_channel
    ts = m.get("ts", "unknown")
    
    # Initialize channel stats if not already done
    if not hasattr(migrator, "channel_stats"):
        migrator.channel_stats = {}
    
    if channel not in migrator.channel_stats:
        migrator.channel_stats[channel] = {
            "message_count": 0,
            "reaction_count": 0,
            "file_count": 0
        }
    
    # Increment message count for this channel
    migrator.channel_stats[channel]["message_count"] += 1
    
    # Track reactions
    reaction_count = 0
    if "reactions" in m:
        reaction_count = sum(len(r.get("users", [])) for r in m["reactions"])
        migrator.channel_stats[channel]["reaction_count"] += reaction_count
        
        # Also increment the global reaction count in dry run mode
        # (in normal mode this is done by process_reactions_batch)
        if migrator.dry_run:
            migrator.migration_summary["reactions_created"] += reaction_count
            log_with_context(
                logging.DEBUG,
                f"[DRY RUN] Counted {reaction_count} reactions for message {ts}",
                channel=channel,
                ts=ts
            )
    
    # Track files
    file_count = len(m.get("files", []))
    if file_count > 0:
        log_with_context(
            logging.DEBUG,
            f"Found {file_count} files to process in message {ts}",
            channel=channel,
            ts=ts
        )
        migrator.channel_stats[channel]["file_count"] += file_count
        
        # Also increment the global file count
        if migrator.dry_run:
            migrator.migration_summary["files_created"] += file_count
        
    # Process files if not in dry run mode
    if not migrator.dry_run and file_count > 0:
        for fobj in m.get("files", []):
            migrator.file_handler.upload_file(fobj, channel)


def save_thread_mappings(migrator, channel: str):
    """Save thread mappings to a file for this channel.
    
    This allows thread relationships to be maintained across multiple migration runs.
    """
    if migrator.dry_run:
        log_with_context(
            logging.INFO,
            f"[DRY RUN] Would save thread mappings for channel {channel}",
            channel=channel
        )
        return
        
    try:
        # Make sure thread_map exists
        if not hasattr(migrator, "thread_map") or migrator.thread_map is None:
            migrator.thread_map = {}
            
        # Debug: log the thread map contents before saving
        log_with_context(
            logging.DEBUG,
            f"Thread map before saving: {json.dumps(migrator.thread_map, indent=2)}",
            channel=channel
        )
        
        # Use the thread_mappings directory in the output structure if available
        if hasattr(migrator, "output_dirs") and "thread_mappings" in migrator.output_dirs:
            thread_map_dir = migrator.output_dirs["thread_mappings"]
            os.makedirs(thread_map_dir, exist_ok=True)
            thread_map_file = os.path.join(thread_map_dir, f"{channel}_thread_map.json")
        else:
            # Fallback to the old location
            thread_map_file = migrator.export_root / f".{channel}_thread_map.json"
        
        # Get the space ID associated with this channel
        space_name = migrator.created_spaces.get(channel)
        if not space_name:
            log_with_context(
                logging.WARNING,
                f"No space found for channel {channel}, cannot filter thread mappings",
                channel=channel
            )
            return
            
        # Extract the space ID from the space name (format: spaces/SPACE_ID)
        space_id = space_name.split('/')[-1]
        
        # Filter thread mappings for this channel only by matching the space ID
        channel_thread_map = {}
        channel_count = 0
        
        for ts, thread_name in migrator.thread_map.items():
            # Check if this thread belongs to the current channel's space
            if space_id in str(thread_name):
                # Always store as string keys
                channel_thread_map[str(ts)] = thread_name
                channel_count += 1
        
        log_with_context(
            logging.INFO,
            f"Saving {channel_count} thread mappings for channel {channel}",
            channel=channel,
            file=str(thread_map_file)
        )
        
        # Save only this channel's thread mappings
        with open(thread_map_file, 'w') as f:
            json.dump(channel_thread_map, f)
        
        log_with_context(
            logging.INFO, 
            f"Saved {channel_count} thread mappings for channel {channel}",
            channel=channel,
            file=str(thread_map_file)
        )
            
    except Exception as e:
        log_with_context(
            logging.WARNING,
            f"Failed to save thread mappings for {channel}: {e}",
            channel=channel,
            error=str(e)
        )


def load_thread_mappings(migrator, channel: str):
    """Load thread mappings from a file for this channel.
    
    This allows thread relationships to be maintained across multiple migration runs.
    """
    try:
        # Ensure thread map is initialized
        if not hasattr(migrator, "thread_map") or migrator.thread_map is None:
            migrator.thread_map = {}
            
        # Check first in the thread_mappings directory in the output structure
        thread_map_file = None
        if hasattr(migrator, "output_dirs") and "thread_mappings" in migrator.output_dirs:
            thread_map_dir = migrator.output_dirs["thread_mappings"]
            thread_map_file_new = os.path.join(thread_map_dir, f"{channel}_thread_map.json")
            if os.path.exists(thread_map_file_new):
                thread_map_file = thread_map_file_new
                log_with_context(
                    logging.DEBUG,
                    f"Found thread map file in new location: {thread_map_file}",
                    channel=channel
                )
        
        # If not found, try the old location
        if not thread_map_file:
            thread_map_file_old = migrator.export_root / f".{channel}_thread_map.json"
            if thread_map_file_old.exists():
                thread_map_file = thread_map_file_old
                log_with_context(
                    logging.DEBUG,
                    f"Found thread map file in old location: {thread_map_file}",
                    channel=channel
                )
        
        # If found in either location, load it
        if thread_map_file and (os.path.exists(thread_map_file) if isinstance(thread_map_file, str) else thread_map_file.exists()):
            with open(thread_map_file) as f:
                loaded_map = json.load(f)
                
                # Update the thread map with the loaded mappings
                for ts, thread_name in loaded_map.items():
                    # Store as string keys for consistency
                    migrator.thread_map[str(ts)] = thread_name
            
            log_with_context(
                logging.INFO,
                f"Loaded {len(loaded_map)} thread mappings from {thread_map_file}",
                channel=channel
            )
            
            # Debug log to verify thread mappings
            log_with_context(
                logging.DEBUG,
                f"Thread map after loading: {json.dumps(dict(list(migrator.thread_map.items())[:5]), indent=2)} (showing first 5 items)",
                channel=channel
            )
        else:
            log_with_context(
                logging.INFO,
                f"No thread map file found for channel {channel}",
                channel=channel
            )
            
    except Exception as e:
        log_with_context(
            logging.WARNING,
            f"Failed to load thread mappings for {channel}: {e}",
            channel=channel,
            error=str(e)
        )
        # Initialize empty thread map if loading failed
        if not hasattr(migrator, "thread_map") or migrator.thread_map is None:
            migrator.thread_map = {}


def send_intro(migrator, space: str, channel: str):
    """Send an intro message with channel metadata."""
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
    intro_text += f"*Migration Date:* {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"

    # Log the action
    log_with_context(
        logging.INFO,
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Sending intro message to space {space} for channel {channel}",
        channel=channel
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
            "sender": {"type": "HUMAN", "name": f"users/{migrator.workspace_admin}"}
        }

        # Send the message
        log_api_request("POST", "chat.spaces.messages.create", message_body, channel=channel)
        result = migrator.chat.spaces().messages().create(
            parent=space, body=message_body
        ).execute()
        log_api_response(200, "chat.spaces.messages.create", result, channel=channel)

        # Increment the counter
        migrator.migration_summary["messages_created"] += 1

        log_with_context(
            logging.INFO,
            f"Sent intro message to space {space}",
            channel=channel
        )
    except Exception as e:
        log_with_context(
            logging.WARNING,
            f"Failed to send intro message to space {space}: {e}",
            channel=channel,
            error=str(e)
        ) 