"""
Functions for creating Google Chat spaces during Slack migration.

Handles space creation in import mode, including external user detection
and space metadata configuration.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from googleapiclient.errors import HttpError

from slack_migrator.utils.api import slack_ts_to_rfc3339
from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.migrator import SlackToChatMigrator

# Named constant for import mode time limit
IMPORT_MODE_DAYS_LIMIT = 90


def channel_has_external_users(migrator: SlackToChatMigrator, channel: str) -> bool:
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

        if migrator.user_resolver.is_external_user(email) and not is_bot:
            log_with_context(
                logging.INFO,
                f"Channel {channel} has external user {user_id} with email {email}",
                channel=channel,
            )
            return True

    return False


def create_space(migrator: SlackToChatMigrator, channel: str) -> str:
    """Create a Google Chat space for a Slack channel in import mode.

    Args:
        migrator: The migrator instance providing API services and config.
        channel: Slack channel name to create a space for.

    Returns:
        The Google Chat space resource name (e.g. ``spaces/AAAA``),
        or an ``ERROR_NO_PERMISSION_`` sentinel on 403 errors.
    """
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
        migrator.state.migration_summary["spaces_created"] += 1
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
            space = migrator.chat.spaces().create(body=body).execute()  # type: ignore[union-attr]
            space_name = space["name"]

            # Increment the spaces created counter
            migrator.state.migration_summary["spaces_created"] += 1

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
                f"import mode must be completed within {IMPORT_MODE_DAYS_LIMIT} days or the space will be automatically deleted.",
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

                        migrator.chat.spaces().patch(  # type: ignore[union-attr]
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
    migrator.state.created_spaces[channel] = space_name

    # Store whether this space has external users for later reference
    if not hasattr(migrator.state, "spaces_with_external_users"):
        migrator.state.spaces_with_external_users = {}
    migrator.state.spaces_with_external_users[space_name] = has_external_users

    return space_name
