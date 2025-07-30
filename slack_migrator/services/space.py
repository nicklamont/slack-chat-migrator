"""
Functions for managing Google Chat spaces during Slack migration
"""

import json
import logging
import sys
import time
import datetime
from typing import Dict, Any, Set

from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.utils.logging import logger, log_with_context
from slack_migrator.utils.api import retry, slack_ts_to_rfc3339, set_global_retry_config
from slack_migrator.utils.formatting import convert_formatting


def channel_has_external_users(migrator, channel: str) -> bool:
    """Check if a channel has external users that need access.
    
    Args:
        migrator: The SlackToChatMigrator instance
        channel: The channel name to check
        
    Returns:
        True if the channel has external users (excluding bots), False otherwise
    """
    # Get channel metadata for members
    meta = migrator.channels_meta.get(channel, {})
    members = meta.get("members", [])
    
    # If no members in metadata, check message history
    if not members:
        ch_dir = migrator.export_root / channel
        user_ids = set()
        
        # Scan message files for unique user IDs
        for jf in ch_dir.glob("*.json"):
            try:
                with open(jf) as f:
                    msgs = json.load(f)
                for m in msgs:
                    if m.get("type") == "message" and "user" in m and m["user"]:
                        user_ids.add(m["user"])
            except Exception as e:
                logger.warning(f"Failed to process {jf} when checking for external users: {e}")
        
        members = list(user_ids)
    
    # Check if any member is an external user (excluding bots)
    for user_id in members:
        # Get email from user map
        email = migrator.user_map.get(user_id)
        if not email:
            continue
            
        # Check if this is an external user (not a bot)
        # Ensure users_without_email is a list before iterating
        users_without_email = getattr(migrator, "users_without_email", []) or []
        
        # Find user info in users_without_email
        user_info = None
        for u in users_without_email:
            if u.get("id") == user_id:
                user_info = u
                break
                
        # Check if user is a bot
        is_bot = False
        if user_info:
            is_bot = user_info.get("is_bot", False) or user_info.get("is_app_user", False)
        
        if migrator._is_external_user(email) and not is_bot:
            logger.info(f"Channel {channel} has external user {user_id} with email {email}")
            return True
            
    return False


@retry()
def create_space(migrator, channel: str) -> str:
    """Create a Google Chat space for a Slack channel in import mode."""
    # Ensure global retry config is set
    if hasattr(migrator, 'config'):
        set_global_retry_config(migrator.config)
    # Get channel metadata
    meta = migrator.channels_meta.get(channel, {})
    display_name = f"Slack #{channel}"

    # Check if this is the general/default channel
    is_general = meta.get("is_general", False)
    if is_general:
        display_name += " (General)"

    # If channel has a creation time in metadata, use it
    channel_created = meta.get("created")
    create_time = None
    if channel_created:
        # Convert Unix timestamp to RFC3339 format
        create_time = slack_ts_to_rfc3339(f"{channel_created}.000000")
        logger.debug(f"Using original channel creation time: {create_time}")

    # Create a space in import mode according to the documentation
    # https://developers.google.com/workspace/chat/import-data
    body = {
        "displayName": display_name,
        "spaceType": "SPACE",
        "importMode": True,  # Boolean value for import mode
        "spaceThreadingState": "THREADED_MESSAGES",  # Explicitly enable threading
    }

    log_with_context(
        logging.INFO,
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Creating import mode space for {display_name}",
        channel=channel,
    )

    # If we have original creation time, add it
    if create_time:
        body["createTime"] = create_time

    # Check if this channel has external users that need access
    has_external_users = channel_has_external_users(migrator, channel)
    if has_external_users:
        body['externalUserAllowed'] = True
        logger.info(f"{'[DRY RUN] ' if migrator.dry_run else ''}Enabling external user access for channel {channel}")

    # Store space name (either real or generated)
    space_name = None
    
    if migrator.dry_run:
        # In dry run mode, increment the counter but don't make API call
        migrator.migration_summary["spaces_created"] += 1
        # Use a consistent space name format for tracking
        space_name = f"spaces/{channel}"
        logger.info(
            f"[DRY RUN] Would create space {space_name} for channel {channel} in import mode with threading enabled"
        )
    else:
        try:
            # Create the space in import mode
            space = migrator.chat.spaces().create(body=body).execute()
            space_name = space["name"]
            logger.info(
                f"Created space {space_name} for channel {channel} in import mode with threading enabled"
            )

            # Add warning about 90-day limit for import mode
            logger.warning(
                f"IMPORTANT: Space {space_name} is in import mode. Per Google Chat API restrictions, "
                "import mode must be completed within 90 days or the space will be automatically deleted."
            )

            # If channel has a purpose or topic, update the space details
            purpose = meta.get("purpose", {}).get("value", "")
            topic = meta.get("topic", {}).get("value", "")

            if purpose or topic:
                description = ""
                if purpose:
                    description += f"Purpose: {purpose}\n\n"
                if topic:
                    description += f"Topic: {topic}"

                if description:
                    try:
                        # Update space with description
                        space_details = {
                            "spaceDetails": {"description": description.strip()}
                        }

                        update_mask = "spaceDetails.description"

                        migrator.chat.spaces().patch(
                            name=space_name, updateMask=update_mask, body=space_details
                        ).execute()

                        logger.info(
                            f"Updated space {space_name} with description from channel metadata"
                        )
                    except HttpError as e:
                        logger.warning(f"Failed to update space description: {e}")
        except HttpError as e:
            if e.resp.status == 403 and "PERMISSION_DENIED" in str(e):
                # Log the error but don't raise an exception
                log_with_context(
                    logging.WARNING, f"Error setting up channel {channel}: {e}"
                )
                return f"ERROR_NO_PERMISSION_{channel}"
            else:
                # For other errors, re-raise
                raise

    # Store the created space in the migrator
    migrator.created_spaces[channel] = space_name
    
    # Store whether this space has external users for later reference
    if not hasattr(migrator, "spaces_with_external_users"):
        migrator.spaces_with_external_users = {}
    migrator.spaces_with_external_users[space_name] = has_external_users

    return space_name


@retry()
def send_intro(migrator, space: str, channel: str):
    """Send an introductory message with channel purpose and topic."""
    # Ensure global retry config is set
    if hasattr(migrator, 'config'):
        set_global_retry_config(migrator.config)
    # Skip if space couldn't be created due to permissions
    if space.startswith("ERROR_NO_PERMISSION_") or space.startswith("DRY_"):
        return

    meta = migrator.channels_meta.get(channel, {})

    # Get purpose and topic from channel metadata
    purpose = meta.get("purpose", {}).get("value", "")
    topic = meta.get("topic", {}).get("value", "")

    # Skip if no metadata available
    if not purpose and not topic:
        logger.info(f"No purpose or topic found for channel {channel}")
        return

    if migrator.dry_run:
        migrator.migration_summary["messages_created"] += 1
        return

    # Format the message with both purpose and topic if available
    message_parts = []
    if purpose:
        message_parts.append(
            f"*Channel Purpose:* {convert_formatting(purpose, migrator.user_map)}"
        )
    if topic:
        message_parts.append(
            f"*Channel Topic:* {convert_formatting(topic, migrator.user_map)}"
        )

    formatted_text = "\n\n".join(message_parts)

    # Get the earliest timestamp from the channel files to use as createTime
    earliest_ts = None
    ch_dir = migrator.export_root / channel

    for jf in sorted(ch_dir.glob("*.json")):
        try:
            with open(jf) as f:
                msgs = json.load(f)
            if msgs:
                # Find the earliest message timestamp
                for m in sorted(msgs, key=lambda m: float(m.get("ts", "0"))):
                    if "ts" in m:
                        earliest_ts = m["ts"]
                        break
                if earliest_ts:
                    break
        except Exception as e:
            logger.warning(
                f"Failed to get earliest timestamp for channel {channel}: {e}"
            )

    # Use the earliest timestamp or default to current time
    if earliest_ts:
        create_time = slack_ts_to_rfc3339(earliest_ts)
    else:
        create_time = slack_ts_to_rfc3339(f"{int(time.time())}.000000")

    # Build request body for message creation
    body = {
        "text": formatted_text,
        "createTime": create_time,
        "sender": {"type": "HUMAN", "name": f"users/{migrator.workspace_admin}"},
    }

    try:
        migrator.chat.spaces().messages().create(parent=space, body=body).execute()
        logger.info(
            f"Sent intro message with purpose and topic for channel {channel}"
        )
    except HttpError as e:
        log_with_context(
            logging.WARNING,
            "Failed to send intro message",
            channel=channel,
            error_code=e.resp.status,
            error_body=(
                e.content.decode("utf-8") if hasattr(e, "content") else str(e)
            ),
        )


def test_space_creation(migrator):
    """Test if we can create a space to verify permissions."""
    log_with_context(
        logging.INFO, "Testing space creation to verify permissions..."
    )
    try:
        test_space = {
            "displayName": "Test Import Space",
            "spaceType": "SPACE",
            "importMode": True,  # Boolean value instead of string
        }
        result = migrator.chat.spaces().create(body=test_space).execute()
        space_name = result.get("name")
        log_with_context(
            logging.INFO, f"Successfully created test space: {space_name}"
        )

        # Clean up by deleting the test space
        migrator.chat.spaces().delete(name=space_name).execute()
        log_with_context(logging.INFO, "Test space deleted successfully")
    except HttpError as e:
        log_with_context(logging.ERROR, f"Failed to create test space: {e}")
        log_with_context(
            logging.ERROR,
            "Please check your service account permissions and domain-wide delegation",
        )
        log_with_context(
            logging.ERROR, "Run check_admin_sdk_setup.py to diagnose the issue"
        )
        sys.exit(1)

    # Also verify that we can create a space with external users allowed
    log_with_context(
        logging.INFO, "Testing space creation with external users allowed..."
    )
    test_space_external = {
        "displayName": "Test External Import Space",
        "spaceType": "SPACE",
        "importMode": True,
        "externalUserAllowed": True,
    }
    try:
        ext_result = migrator.chat.spaces().create(body=test_space_external).execute()
        ext_space_name = ext_result.get("name")
        log_with_context(
            logging.INFO,
            f"Successfully created test space with external users: {ext_space_name}",
        )

        # Verify that externalUserAllowed is set correctly
        space_info = migrator.chat.spaces().get(name=ext_space_name).execute()
        external_users_allowed = space_info.get("externalUserAllowed", False)
        
        if external_users_allowed:
            log_with_context(
                logging.INFO,
                "External user access is properly supported in this workspace",
            )
        else:
            log_with_context(
                logging.WARNING,
                "Space was created with externalUserAllowed=True but the flag was not set in the response",
            )

        # Clean up
        migrator.chat.spaces().delete(name=ext_space_name).execute()
        log_with_context(logging.INFO, "Test external space deleted successfully")
    except HttpError as e:
        log_with_context(
            logging.WARNING, f"Failed to create test space with external users: {e}"
        )
        log_with_context(logging.WARNING, "External user support might be limited")


def add_users_to_space(migrator, space: str, channel: str):
    """Add users to a space in import mode.

    This adds all users who were active in the Slack channel to the Google Chat space.
    For import mode, we need to create historical memberships for users.
    The function uses join/leave messages in Slack export to determine membership periods.

    IMPORTANT: Memberships must be created with timestamps before any messages from those users.
    IMPORTANT: In import mode, ALL memberships require both createTime AND deleteTime in the PAST.
    """
    # Map to track user join/leave times and store info about who is currently active
    user_membership: Dict[str, Dict[str, Any]] = {}
    active_users: Set[str] = set()  # Track users who are still active for adding after import
    ch_dir = migrator.export_root / channel

    # First pass: identify all users and their join/leave events
    for jf in sorted(ch_dir.glob("*.json")):
        try:
            with open(jf) as f:
                msgs = json.load(f)
            for m in msgs:
                # Track users who sent messages
                if m.get("type") == "message" and "user" in m and m["user"]:
                    user_id = m["user"]
                    timestamp = slack_ts_to_rfc3339(m["ts"])

                    if user_id not in user_membership:
                        user_membership[user_id] = {
                            "join_time": None,
                            "leave_time": None,
                            "active": True,  # Assume active by default
                            "first_message_time": timestamp,
                        }
                        active_users.add(user_id)  # Initially mark as active
                    else:
                        # Track earliest message time
                        if timestamp < user_membership[user_id].get(
                            "first_message_time", timestamp
                        ):
                            user_membership[user_id][
                                "first_message_time"
                            ] = timestamp

                # Check for join/leave messages
                if (
                    m.get("type") == "message"
                    and m.get("subtype") == "channel_join"
                    and "user" in m
                ):
                    user_id = m["user"]
                    timestamp = slack_ts_to_rfc3339(m["ts"])
                    if user_id not in user_membership:
                        user_membership[user_id] = {
                            "join_time": timestamp,
                            "leave_time": None,
                            "active": True,
                            "first_message_time": None,
                        }
                        active_users.add(user_id)  # Add to active users
                    else:
                        # Update join time if it's earlier than existing
                        if (
                            not user_membership[user_id]["join_time"]
                            or timestamp < user_membership[user_id]["join_time"]
                        ):
                            user_membership[user_id]["join_time"] = timestamp
                            user_membership[user_id]["active"] = True
                            active_users.add(user_id)  # Mark as active

                elif (
                    m.get("type") == "message"
                    and m.get("subtype") == "channel_leave"
                    and "user" in m
                ):
                    user_id = m["user"]
                    timestamp = slack_ts_to_rfc3339(m["ts"])
                    if user_id in user_membership:
                        # Update leave time if it's later than existing
                        if (
                            not user_membership[user_id]["leave_time"]
                            or timestamp > user_membership[user_id]["leave_time"]
                        ):
                            user_membership[user_id]["leave_time"] = timestamp
                            user_membership[user_id]["active"] = False
                            if user_id in active_users:
                                active_users.remove(
                                    user_id
                                )  # Remove from active users
        except Exception as e:
            logger.warning(
                f"Failed to process file {jf} when collecting user membership data: {e}"
            )

    # Also check the channel metadata for current members
    meta = migrator.channels_meta.get(channel, {})
    if "members" in meta and isinstance(meta["members"], list):
        for user_id in meta["members"]:
            active_users.add(user_id)  # These users are definitely active now
            if user_id not in user_membership:
                # If user is in metadata but not seen in messages, add them with default times
                user_membership[user_id] = {
                    "join_time": "2020-01-01T00:00:00Z",  # Default time
                    "leave_time": None,
                    "active": True,
                    "first_message_time": None,
                }

    # Store active users in class variable to add back after import completes
    if not hasattr(migrator, "active_users_by_channel"):
        migrator.active_users_by_channel = {}
    migrator.active_users_by_channel[channel] = active_users
    
    # Log what we're doing
    logger.info(
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Adding {len(user_membership)} users to space {space} for channel {channel}"
    )
    
    if migrator.dry_run:
        # In dry run mode, just count and return
        return

    # Get channel creation time from metadata to use as fallback
    # (We can't get space info in import mode and don't need to try)
    channel_creation_time = None
    meta = migrator.channels_meta.get(channel, {})
    if meta.get("created"):
        channel_creation_time = slack_ts_to_rfc3339(f"{meta['created']}.000000")
        logger.debug(f"Using channel creation time as fallback: {channel_creation_time}")

    # Set import time (current time minus 5 seconds) as the deleteTime for all historical memberships
    # According to Google Chat API, in import mode all memberships must have deleteTime in the past
    current_time = datetime.datetime.now(datetime.timezone.utc)
    historical_delete_time = (
        (current_time - datetime.timedelta(seconds=5))
        .isoformat()
        .replace("+00:00", "Z")
    )
    logger.info(
        f"Using {historical_delete_time} as historical membership delete time for import mode"
    )
    
    # Find the earliest message time across all users as the ultimate fallback
    earliest_message_time = None
    for _, membership in user_membership.items():
        if membership.get("first_message_time"):
            if earliest_message_time is None or membership["first_message_time"] < earliest_message_time:
                earliest_message_time = membership["first_message_time"]
    
    # Default join time cascade:
    # 1. Explicit channel_join event (already set)
    # 2. User's first message time minus 1 minute
    # 3. Channel creation time from metadata
    # 4. Earliest message time in the channel minus 2 minutes
    # 5. Last resort default time
    default_join_time = "2020-01-01T00:00:00Z"
    if earliest_message_time:
        try:
            # Convert to datetime, subtract 2 minutes for safety, and convert back
            if earliest_message_time.endswith("Z"):
                earliest_message_time = earliest_message_time[:-1] + "+00:00"
            earliest_dt = datetime.datetime.fromisoformat(earliest_message_time)
            earliest_join_dt = earliest_dt - datetime.timedelta(minutes=2)
            default_join_time = earliest_join_dt.isoformat().replace("+00:00", "Z")
            logger.debug(f"Using earliest message time minus 2 minutes as default join time: {default_join_time}")
        except ValueError:
            # Keep the default if parsing fails
            pass
    elif channel_creation_time:
        default_join_time = channel_creation_time
    
    # Set join times for users missing them
    for user_id, membership in user_membership.items():
        if not membership["join_time"]:
            # If user has messages, use first message time minus 1 minute
            if membership.get("first_message_time"):
                try:
                    msg_time = membership["first_message_time"]
                    if msg_time.endswith("Z"):
                        msg_time = msg_time[:-1] + "+00:00"
                    dt = datetime.datetime.fromisoformat(msg_time)
                    join_dt = dt - datetime.timedelta(minutes=1)
                    membership["join_time"] = join_dt.isoformat().replace("+00:00", "Z")
                    logger.debug(f"User {user_id}: Setting join time to 1 minute before first message")
                except ValueError:
                    # If parsing fails, use the default join time
                    membership["join_time"] = default_join_time
            else:
                # No messages from this user, use default join time
                membership["join_time"] = default_join_time

        # For import mode: ALL memberships must have a deleteTime in the PAST
        # If the user has an explicit leave time from a channel_leave event, use it
        # Otherwise, set deleteTime to current time minus a few seconds for all users
        # We'll re-add active users after import completes
        if not membership["leave_time"]:
            membership["leave_time"] = historical_delete_time

    # Add each user to the space as historical membership
    added_count = 0
    failed_count = 0

    pbar = tqdm(user_membership.items(), desc=f"Adding members to {channel}")
    for user_id, membership in pbar:
        user_email = migrator.user_map.get(user_id)

        if not user_email:
            logger.warning(f"No email mapping found for user {user_id}")
            continue

        # Get the internal email for this user (handles external users)
        internal_email = migrator._get_internal_email(user_id, user_email)

        # Track external users for message attribution
        if migrator._is_external_user(user_email):
            logger.info(
                f"Adding external user {user_id} with internal email {internal_email} as historical member"
            )
            migrator.external_users.add(user_email)

        try:
            # Create historical membership for this user
            # In import mode, both createTime AND deleteTime are required
            # The deleteTime MUST be in the past
            membership_body = {
                "member": {"name": f"users/{internal_email}", "type": "HUMAN"},
                "createTime": membership["join_time"],
                "deleteTime": membership["leave_time"],
            }

            log_with_context(
                logging.DEBUG,
                f"Adding user {internal_email} with createTime={membership['join_time']}, deleteTime={membership['leave_time']}",
                user=internal_email,
                channel=channel,
            )

            # Use the admin user for adding members
            migrator.chat.spaces().members().create(
                parent=space, body=membership_body
            ).execute()

            added_count += 1
            logger.debug(
                f"Added user {internal_email} to space {space} as historical membership"
            )
        except HttpError as e:
            # If we get a 409 conflict, the user might already be in the space
            if e.resp.status == 409:
                logger.warning(
                    f"User {internal_email} might already be in space {space}: {e}"
                )
                added_count += 1
            else:
                log_with_context(
                    logging.WARNING,
                    f"Failed to add user {internal_email} to space {space}",
                    error_code=e.resp.status,
                    error_message=str(e),
                )
                failed_count += 1
        except Exception as e:
            logger.warning(
                f"Unexpected error adding user {internal_email} to space {space}: {e}"
            )
            failed_count += 1

        # Add a small delay to avoid rate limiting
        time.sleep(0.1)

    # Log summary
    active_count = len(active_users)
    logger.info(
        f"Added {added_count} users to space {space} as historical memberships, {failed_count} failed"
    )
    logger.info(
        f"Tracked {active_count} active users to add back after import completes"
    )


def add_regular_members(migrator, space: str, channel: str):
    """Add regular members to a space after import mode is complete.

    After completing import mode, this method adds back all active members
    to the space as regular members. This ensures that users have access
    to the space after migration.
    """
    # Get the list of active users we saved during add_users_to_space
    if (
        not hasattr(migrator, "active_users_by_channel")
        or channel not in migrator.active_users_by_channel
    ):
        logger.warning(
            f"No active users tracked for channel {channel}, can't add regular members"
        )
        return

    active_users = migrator.active_users_by_channel[channel]
    logger.info(
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Adding {len(active_users)} regular members to space {space} for channel {channel}"
    )
    
    # Check if any active users are external
    has_external_users = False
    for user_id in active_users:
        user_email = migrator.user_map.get(user_id)
        if user_email and migrator._is_external_user(user_email):
            has_external_users = True
            break
    
    # If we have external users, ensure the space has externalUserAllowed=True
    if has_external_users:
        logger.info(f"{'[DRY RUN] ' if migrator.dry_run else ''}Enabling external user access for space {space} before adding members")
        
        if not migrator.dry_run:
            try:
                # Get current space settings
                space_info = migrator.chat.spaces().get(name=space).execute()
                external_users_allowed = space_info.get("externalUserAllowed", False)
                
                # If external users are not allowed, update the space
                if not external_users_allowed:
                    update_body = {
                        "externalUserAllowed": True
                    }
                    update_mask = "externalUserAllowed"
                    migrator.chat.spaces().patch(
                        name=space,
                        updateMask=update_mask,
                        body=update_body
                    ).execute()
                    logger.info(f"Successfully enabled external user access for space {space}")
            except Exception as e:
                logger.warning(f"Failed to enable external user access for space {space}: {e}")

    # In dry run mode, just log and return
    if migrator.dry_run:
        return
        
    # Add each active user as a regular member
    added_count = 0
    failed_count = 0

    pbar = tqdm(active_users, desc=f"Adding regular members to {channel}")
    for user_id in pbar:
        user_email = migrator.user_map.get(user_id)

        if not user_email:
            logger.warning(f"No email mapping found for user {user_id}")
            continue

        # Get the internal email for this user (handles external users)
        internal_email = migrator._get_internal_email(user_id, user_email)

        # Track external users for message attribution
        if migrator._is_external_user(user_email):
            logger.info(
                f"Adding external user {user_id} with internal email {internal_email} as regular member"
            )
            migrator.external_users.add(user_email)

        try:
            # Create regular membership without time constraints
            membership_body = {
                "member": {"name": f"users/{internal_email}", "type": "HUMAN"}
            }

            log_with_context(
                logging.DEBUG,
                f"Adding user {internal_email} as regular member",
                user=internal_email,
                channel=channel,
            )

            # Use the admin user for adding members
            migrator.chat.spaces().members().create(
                parent=space, body=membership_body
            ).execute()

            added_count += 1
            logger.debug(
                f"Added user {internal_email} to space {space} as regular member"
            )
        except HttpError as e:
            # If we get a 409 conflict, the user might already be in the space
            if e.resp.status == 409:
                logger.warning(
                    f"User {internal_email} might already be in space {space}: {e}"
                )
                added_count += 1
            else:
                log_with_context(
                    logging.WARNING,
                    f"Failed to add user {internal_email} as regular member to space {space}",
                    error_code=e.resp.status,
                    error_message=str(e),
                )
                failed_count += 1
        except Exception as e:
            logger.warning(
                f"Unexpected error adding user {internal_email} to space {space} as regular member: {e}"
            )
            failed_count += 1

        # Add a small delay to avoid rate limiting
        time.sleep(0.1)

    # Log summary
    logger.info(
        f"Added {added_count} regular members to space {space}, {failed_count} failed"
    ) 