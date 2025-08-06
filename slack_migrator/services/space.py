"""
Functions for managing Google Chat spaces during Slack migration
"""

import datetime
import json
import logging
import time
from typing import Any, Dict, Set

from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.utils.api import slack_ts_to_rfc3339
from slack_migrator.utils.logging import log_with_context, logger


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
                log_with_context(
                    logging.WARNING,
                    f"Failed to process {jf} when checking for external users: {e}",
                    channel=channel,
                )

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
            is_bot = user_info.get("is_bot", False) or user_info.get(
                "is_app_user", False
            )

        if migrator._is_external_user(email) and not is_bot:
            log_with_context(
                logging.INFO,
                f"Channel {channel} has external user {user_id} with email {email}",
                channel=channel,
            )
            return True

    return False


def create_space(migrator, channel: str) -> str:
    """Create a Google Chat space for a Slack channel in import mode."""
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
        log_with_context(
            logging.DEBUG,
            f"Using original channel creation time: {create_time}",
            channel=channel,
        )

    # Create a space in import mode according to the documentation
    # https://developers.google.com/workspace/chat/import-data
    body = {
        "displayName": display_name,
        "spaceType": "SPACE",
        "importMode": True,
        "spaceThreadingState": "THREADED_MESSAGES",
    }

    log_with_context(
        logging.DEBUG,
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Creating import mode space for {display_name}",
        channel=channel,
    )

    # If we have original creation time, add it
    if create_time:
        body["createTime"] = create_time

    # Check if this channel has external users that need access
    has_external_users = channel_has_external_users(migrator, channel)
    if has_external_users:
        body["externalUserAllowed"] = True
        log_with_context(
            logging.INFO,
            f"{'[DRY RUN] ' if migrator.dry_run else ''}Enabling external user access for channel {channel}",
            channel=channel,
        )

    # Store space name (either real or generated)
    space_name = None

    if migrator.dry_run:
        # In dry run mode, increment the counter but don't make API call
        migrator.migration_summary["spaces_created"] += 1
        # Use a consistent space name format for tracking
        space_name = f"spaces/{channel}"
        log_with_context(
            logging.INFO,
            f"[DRY RUN] Would create space {space_name} for channel {channel} in import mode with threading enabled",
            channel=channel,
        )
    else:
        try:
            # Create the space in import mode
            space = migrator.chat.spaces().create(body=body).execute()
            space_name = space["name"]

            # Increment the spaces created counter
            migrator.migration_summary["spaces_created"] += 1

            log_with_context(
                logging.INFO,
                f"Created space {space_name} for channel {channel} in import mode with threading enabled",
                channel=channel,
                space_name=space_name,
            )

            # Add warning about 90-day limit for import mode
            log_with_context(
                logging.DEBUG,
                f"IMPORTANT: Space {space_name} is in import mode. Per Google Chat API restrictions, "
                "import mode must be completed within 90 days or the space will be automatically deleted.",
                channel=channel,
                space_name=space_name,
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

                        update_mask = "spaceDetails"

                        migrator.chat.spaces().patch(
                            name=space_name, updateMask=update_mask, body=space_details
                        ).execute()

                        log_with_context(
                            logging.INFO,
                            f"Updated space {space_name} with description from channel metadata",
                            channel=channel,
                        )
                    except HttpError as e:
                        log_with_context(
                            logging.WARNING,
                            f"Failed to update space description: {e}",
                            channel=channel,
                        )
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


def add_users_to_space(migrator, space: str, channel: str):
    """Add users to a space as historical members."""
    log_with_context(
        logging.DEBUG,
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Adding historical memberships for channel {channel}",
        channel=channel,
    )

    # Map to track user join/leave times and store info about who is currently active
    user_membership: Dict[str, Dict[str, Any]] = {}
    active_users: Set[str] = (
        set()
    )  # Track users who are still active for adding after import
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
                            user_membership[user_id]["first_message_time"] = timestamp

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
                                active_users.remove(user_id)  # Remove from active users
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to process file {jf} when collecting user membership data: {e}",
                channel=channel,
            )

    # The channel metadata (channels.json) is the most reliable and definitive source for active members
    # Reset active_users to ensure only the members from channels.json are considered active
    active_users = set()  # Clear any users previously marked as active from messages

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
    # We'll use this for both regular membership and file permissions
    if not hasattr(migrator, "active_users_by_channel"):
        migrator.active_users_by_channel = {}

    # Log active user counts for debugging
    log_with_context(
        logging.DEBUG,
        f"Identified {len(active_users)} active users for channel {channel}",
        channel=channel,
    )
    migrator.active_users_by_channel[channel] = active_users

    # Log what we're doing
    log_with_context(
        logging.DEBUG,
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Adding {len(user_membership)} users to space {space} for channel {channel}",
        channel=channel,
        space=space,
        user_count=len(user_membership),
    )

    if migrator.dry_run:
        # In dry run mode, just count and return
        return

    # Check if the workspace admin is in the active users
    # Google Chat automatically adds the creator as a member, but we only want them if they were in the channel
    admin_email = migrator.workspace_admin
    admin_user_id = None

    # Look up the admin's Slack user ID if they had one (they'll be in user_map if they were in Slack)
    for slack_user_id, email in migrator.user_map.items():
        if email.lower() == admin_email.lower():
            admin_user_id = slack_user_id
            break

    # If we found a user ID for the admin, check if they were in the channel
    admin_in_channel = False
    if admin_user_id:
        admin_in_channel = admin_user_id in active_users

    log_with_context(
        logging.DEBUG,
        f"Workspace admin ({admin_email}) {'was' if admin_in_channel else 'was not'} in original Slack channel {channel}",
        channel=channel,
    )

    # Get channel creation time from metadata to use as fallback
    # (We can't get space info in import mode and don't need to try)
    channel_creation_time = None
    meta = migrator.channels_meta.get(channel, {})
    if meta.get("created"):
        channel_creation_time = slack_ts_to_rfc3339(f"{meta['created']}.000000")
        log_with_context(
            logging.DEBUG,
            f"Using channel creation time as fallback: {channel_creation_time}",
            channel=channel,
        )

    # Set import time (current time minus 5 seconds) as the deleteTime for all historical memberships
    # According to Google Chat API, in import mode all memberships must have deleteTime in the past
    current_time = datetime.datetime.now(datetime.timezone.utc)
    historical_delete_time = (
        (current_time - datetime.timedelta(seconds=5))
        .isoformat()
        .replace("+00:00", "Z")
    )
    log_with_context(
        logging.DEBUG,
        f"Using {historical_delete_time} as historical membership delete time for import mode",
        channel=channel,
    )

    # Find the earliest message time across all users as the ultimate fallback
    earliest_message_time = None
    for _, membership in user_membership.items():
        if membership.get("first_message_time"):
            if (
                earliest_message_time is None
                or membership["first_message_time"] < earliest_message_time
            ):
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
            log_with_context(
                logging.DEBUG,
                f"Using earliest message time minus 2 minutes as default join time: {default_join_time}",
                channel=channel,
            )
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
                    log_with_context(
                        logging.DEBUG,
                        f"User {user_id}: Setting join time to 1 minute before first message",
                        user_id=user_id,
                        channel=channel,
                    )
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

    pbar = tqdm(user_membership.items(), desc=f"Adding historical members to {channel}")
    for user_id, membership in pbar:
        user_email = migrator.user_map.get(user_id)

        if not user_email:
            log_with_context(
                logging.ERROR,
                f"No email mapping found for user {user_id} - cannot add to space",
                user_id=user_id,
                channel=channel,
            )
            # This will be automatically tracked in _get_internal_email when user lookup fails
            continue

        # Get the internal email for this user (handles external users)
        internal_email = migrator._get_internal_email(user_id, user_email)

        # Track external users for message attribution
        if migrator._is_external_user(user_email):
            log_with_context(
                logging.INFO,
                f"Adding external user {user_id} with internal email {internal_email} as historical member",
                user_id=user_id,
                user_email=user_email,
                channel=channel,
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
            log_with_context(
                logging.DEBUG,
                f"Added user {internal_email} to space {space} as historical membership",
                user=internal_email,
                channel=channel,
            )
        except HttpError as e:
            # If we get a 409 conflict, the user might already be in the space
            if e.resp.status == 409:
                log_with_context(
                    logging.WARNING,
                    f"User {internal_email} might already be in space {space}: {e}",
                    user=internal_email,
                    channel=channel,
                )
                added_count += 1
            else:
                log_with_context(
                    logging.WARNING,
                    f"Failed to add user {internal_email} to space {space}",
                    error_code=e.resp.status,
                    error_message=str(e),
                    channel=channel,
                )
                failed_count += 1
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Unexpected error adding user {internal_email} to space {space}: {e}",
                user_email=internal_email,
                space=space,
                channel=migrator.current_channel,
            )
            failed_count += 1

        # Add a small delay to avoid rate limiting
        time.sleep(0.1)

    # Log summary
    active_count = len(active_users)
    log_with_context(
        logging.INFO,
        f"Added {added_count} users to space {space} as historical memberships, {failed_count} failed",
        channel=channel,
    )
    log_with_context(
        logging.DEBUG,
        f"Tracked {active_count} active users to add back after import completes",
        channel=channel,
    )


def add_regular_members(migrator, space: str, channel: str):
    """Add regular members to a space after import mode is complete.

    After completing import mode, this method adds back all active members
    to the space as regular members. This ensures that users have access
    to the space after migration.

    This method also updates any channel folder permissions to ensure only
    active members have access to shared files.
    """
    # Initialize the active_users_by_channel attribute if it doesn't exist
    if not hasattr(migrator, "active_users_by_channel"):
        migrator.active_users_by_channel = {}

    # Get the list of active users we saved during add_users_to_space
    if channel not in migrator.active_users_by_channel:
        # If we don't have active users for this channel, try to get them from the channel directory
        log_with_context(
            logging.WARNING,
            f"No active users tracked for channel {channel}, attempting to load from channel data",
            channel=channel,
        )

        try:
            # Try to load channel members from the channel data
            from pathlib import Path

            export_root = Path(migrator.export_root)
            channels_file = export_root / "channels.json"

            if channels_file.exists():
                with open(channels_file, "r") as f:
                    channels_data = json.load(f)

                for ch in channels_data:
                    if ch.get("name") == channel:
                        # Found the channel, get its members
                        members = ch.get("members", [])
                        log_with_context(
                            logging.INFO,
                            f"Found {len(members)} members for channel {channel} in channels.json",
                            channel=channel,
                        )
                        migrator.active_users_by_channel[channel] = members
                        break
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to load channel members from channels.json: {e}",
                channel=channel,
            )

    # If we still don't have active users, we can't proceed
    if channel not in migrator.active_users_by_channel:
        log_with_context(
            logging.ERROR,
            f"No active users found for channel {channel}, can't add regular members",
            channel=channel,
        )
        return

    active_users = migrator.active_users_by_channel[channel]
    log_with_context(
        logging.DEBUG,
        f"{'[DRY RUN] ' if migrator.dry_run else ''}Adding {len(active_users)} regular members to space {space} for channel {channel}",
        channel=channel,
    )

    # Collect emails of all active users, both for space membership and file permissions
    active_user_emails = []

    # Check if any active users are external
    has_external_users = False
    for user_id in active_users:
        user_email = migrator.user_map.get(user_id)
        if user_email:
            # Get the internal email for proper handling
            internal_email = migrator._get_internal_email(user_id, user_email)
            if internal_email and internal_email not in active_user_emails:
                active_user_emails.append(internal_email)

            # Track if we have external users
            if migrator._is_external_user(user_email):
                has_external_users = True

    # If we have external users, ensure the space has externalUserAllowed=True
    if has_external_users:
        log_with_context(
            logging.INFO,
            f"{'[DRY RUN] ' if migrator.dry_run else ''}Enabling external user access for space {space} before adding members",
            channel=channel,
        )

        if not migrator.dry_run:
            try:
                # Get current space settings
                space_info = migrator.chat.spaces().get(name=space).execute()
                external_users_allowed = space_info.get("externalUserAllowed", False)

                # If external users are not allowed, update the space
                if not external_users_allowed:
                    update_body = {"externalUserAllowed": True}
                    update_mask = "externalUserAllowed"
                    migrator.chat.spaces().patch(
                        name=space, updateMask=update_mask, body=update_body
                    ).execute()
                    log_with_context(
                        logging.INFO,
                        f"Successfully enabled external user access for space {space}",
                        channel=channel,
                    )
            except Exception as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to enable external user access for space {space}: {e}",
                    channel=channel,
                )

    # In dry run mode, just log and return
    if migrator.dry_run:
        return

    # Add each active user as a regular member
    added_count = 0
    failed_count = 0

    pbar = tqdm(active_users, desc=f"Adding current members to {channel}")
    for user_id in pbar:
        user_email = migrator.user_map.get(user_id)
        membership_body = {}  # Ensure membership_body is always defined

        if not user_email:
            # Track unmapped user for space membership
            log_with_context(
                logging.ERROR,  # Escalated from WARNING to ERROR
                f"🚨 CRITICAL: No email mapping found for user {user_id} - cannot add as regular member",
                user_id=user_id,
                channel=channel,
            )
            # This will be automatically tracked in _get_internal_email when user lookup fails
            continue

        # Get the internal email for this user (handles external users)
        internal_email = migrator._get_internal_email(user_id, user_email)

        # Track external users for message attribution
        if migrator._is_external_user(user_email):
            log_with_context(
                logging.INFO,
                f"Adding external user {user_id} with email {user_email} as regular member",
                user_id=user_id,
                user_email=user_email,
                channel=channel,
            )
            migrator.external_users.add(user_email)

        try:
            # Log which user we're trying to add
            log_with_context(
                logging.DEBUG,  # Changed from INFO for less verbose output
                f"Attempting to add user {user_email if migrator._is_external_user(user_email) else internal_email} as regular member",
                user=(
                    user_email
                    if migrator._is_external_user(user_email)
                    else internal_email
                ),
                channel=channel,
            )

            # Create regular membership without time constraints - use the correct format for Google Chat API
            # The key is that we need to format the member properly

            # For internal users, use the name format with internal email
            membership_body = {
                "member": {"name": f"users/{internal_email}", "type": "HUMAN"}
            }

            # API request details are already logged by API utilities
            # Use the admin user for adding members
            migrator.chat.spaces().members().create(
                parent=space, body=membership_body
            ).execute()

            added_count += 1
            log_with_context(
                logging.DEBUG,
                f"Added user {internal_email} to space {space} as regular member",
                user=internal_email,
                channel=channel,
            )
        except HttpError as e:
            # If we get a 409 conflict, the user might already be in the space
            if e.resp.status == 409:
                log_with_context(
                    logging.WARNING,
                    f"User {internal_email} might already be in space {space}: {e}",
                    user=internal_email,
                    channel=channel,
                )
                added_count += 1
            elif e.resp.status == 400:
                # Bad request means there's an issue with the format according to API requirements
                log_with_context(
                    logging.ERROR,
                    f"Bad request (400) when adding user {internal_email} - check API documentation for correct format",
                    error_message=str(e),
                    channel=channel,
                )
                failed_count += 1
            else:
                log_with_context(
                    logging.WARNING,
                    f"Failed to add user {internal_email} as regular member to space {space}",
                    error_code=e.resp.status,
                    error_message=str(e),
                    request_body=json.dumps(membership_body),
                    channel=channel,
                )
                failed_count += 1

                # If we get a 403 or 404, log additional details to help troubleshoot
                if e.resp.status in [403, 404]:
                    log_with_context(
                        logging.ERROR,
                        f"Permission denied or resource not found when adding {internal_email}. "
                        f"Check that the user exists and the service account has permission to modify the space.",
                        space=space,
                        user=internal_email,
                        channel=channel,
                    )
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Unexpected error adding user {internal_email} to space {space} as regular member: {e}",
                channel=channel,
            )
            failed_count += 1

            # Log the full exception for debugging
            import traceback

            log_with_context(
                logging.DEBUG,
                f"Exception traceback: {traceback.format_exc()}",
                user=internal_email,
                channel=channel,
            )

        # Add a small delay to avoid rate limiting
        time.sleep(0.1)

    # Log summary
    log_with_context(
        logging.INFO,
        f"Added {added_count} regular members to space {space}, {failed_count} failed",
        channel=channel,
    )

    # Verify the members were added
    try:
        log_with_context(
            logging.DEBUG, f"Verifying members added to space {space}", channel=channel
        )

        # List members to verify they were added
        members_result = migrator.chat.spaces().members().list(parent=space).execute()
        members = members_result.get("memberships", [])
        actual_member_count = len(members)

        # Just log the count, detailed API response is already logged by the API utilities
        log_with_context(
            logging.DEBUG,
            f"Space {space} has {actual_member_count} members after adding {added_count} regular members",
            channel=channel,
        )

        # Check if workspace admin needs to be removed because they weren't in the original channel
        admin_email = migrator.workspace_admin
        admin_user_id = None

        log_with_context(
            logging.DEBUG,
            f"Checking if workspace admin ({admin_email}) should be in space {space} for channel {channel}",
            channel=channel,
        )

        # Look up the admin's Slack user ID if they had one
        for slack_user_id, email in migrator.user_map.items():
            if email.lower() == admin_email.lower():
                admin_user_id = slack_user_id
                log_with_context(
                    logging.DEBUG,
                    f"Found Slack user ID for admin: {slack_user_id}",
                    channel=channel,
                )
                break

        if not admin_user_id:
            log_with_context(
                logging.DEBUG,
                f"Workspace admin ({admin_email}) was not found in Slack user map",
                channel=channel,
            )

        # If admin is not in the active users list for this channel, they should be removed
        admin_in_channel = False
        if admin_user_id and admin_user_id in active_users:
            admin_in_channel = True
            log_with_context(
                logging.DEBUG,
                f"Workspace admin ({admin_email}) was in the original Slack channel - will keep in space",
                channel=channel,
            )
        else:
            log_with_context(
                logging.DEBUG,
                f"Workspace admin ({admin_email}) was NOT in the original Slack channel - will attempt removal",
                channel=channel,
            )

        if not admin_in_channel:
            # Find the admin in the members list to get their membership ID
            admin_membership = None
            for member in members:
                # Check member name for exact admin email match
                member_name = member.get("member", {}).get("name", "")
                # Check both exact match "users/{email}" and case-insensitive match
                if (
                    member_name == f"users/{admin_email}"
                    or member_name.lower() == f"users/{admin_email.lower()}"
                ):
                    admin_membership = member.get("name")
                    break

                # Also check for email field which might be present instead of name
                member_email = member.get("member", {}).get("email", "")
                if member_email and (
                    member_email == admin_email
                    or member_email.lower() == admin_email.lower()
                ):
                    admin_membership = member.get("name")
                    break

            if admin_membership:
                log_with_context(
                    logging.INFO,
                    f"Removing workspace admin ({admin_email}) from space {space} because they weren't in the original Slack channel {channel}",
                    channel=channel,
                )

                # Remove the admin from the space
                try:
                    migrator.chat.spaces().members().delete(
                        name=admin_membership
                    ).execute()
                    log_with_context(
                        logging.INFO,
                        f"Successfully removed workspace admin from space {space}",
                        channel=channel,
                    )
                except Exception as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to remove workspace admin from space {space}: {e}",
                        channel=channel,
                    )
            else:
                log_with_context(
                    logging.DEBUG,  # Changed from DEBUG to INFO for better visibility
                    f"Workspace admin ({admin_email}) membership not found in space {space}",
                    channel=channel,
                )

                # Log the members we found for debugging
                log_with_context(
                    logging.DEBUG,
                    f"Members in space {space}: {[member.get('member', {}).get('name', '') for member in members]}",
                    channel=channel,
                )
    except Exception as e:
        log_with_context(
            logging.WARNING,
            f"Failed to verify members in space {space}: {e}",
            channel=channel,
        )

    # Update Drive folder permissions for this channel to ensure only active members have access
    if hasattr(migrator, "file_handler") and hasattr(
        migrator.file_handler, "folder_manager"
    ):
        # First check if we have a channel folder
        folder_id = None

        try:
            # Get the channel folder if it exists
            folder_id = migrator.file_handler.folder_manager.get_channel_folder_id(
                channel,
                migrator.file_handler._root_folder_id,
                migrator.file_handler._shared_drive_id,
            )

            if folder_id:
                # Step 6: Update file permissions
                log_with_context(
                    logging.INFO,
                    f"{'[DRY RUN] ' if migrator.dry_run else ''}Step 6/6: Updating file permissions for {channel} folder to match {len(active_user_emails)} active members",
                    channel=channel,
                    folder_id=folder_id,
                )

                if not migrator.dry_run:
                    # Update permissions to ensure only active members have access
                    migrator.file_handler.folder_manager.set_channel_folder_permissions(
                        folder_id,
                        channel,
                        active_user_emails,
                        migrator.file_handler._shared_drive_id,
                    )
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Error updating channel folder permissions: {e}",
                channel=channel,
                error=str(e),
            )
