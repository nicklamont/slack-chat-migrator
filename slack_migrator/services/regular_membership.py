"""Regular membership management for post-import space setup."""

from __future__ import annotations

import json
import logging
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.constants import (
    HTTP_BAD_REQUEST,
    HTTP_CONFLICT,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
)
from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState


def _collect_active_user_emails(
    ctx: MigrationContext,
    chat: Any,
    user_resolver: Any,
    space: str,
    channel: str,
    active_users: set[str] | list[str],
) -> list[str]:
    """Collect internal email addresses for active users and enable external access.

    Resolves each active Slack user ID to an internal email via the user
    resolver.  If any external users are found, ensures the Google Chat space
    has ``externalUserAllowed`` set to ``True``.

    Args:
        ctx: Immutable migration context.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups.
        space: Google Chat space resource name.
        channel: Slack channel name for log context.
        active_users: Iterable of Slack user IDs considered active.

    Returns:
        De-duplicated list of internal email addresses for all active users.
    """
    active_user_emails: list[str] = []

    # Check if any active users are external
    has_external_users = False
    for user_id in active_users:
        user_email = ctx.user_map.get(user_id)
        if user_email:
            # Get the internal email for proper handling
            internal_email = user_resolver.get_internal_email(user_id, user_email)
            if internal_email and internal_email not in active_user_emails:
                active_user_emails.append(internal_email)

            # Track if we have external users
            if user_resolver.is_external_user(user_email):
                has_external_users = True

    # If we have external users, ensure the space has externalUserAllowed=True
    if has_external_users:
        log_with_context(
            logging.INFO,
            f"{ctx.log_prefix}Enabling external user access for space {space} before adding members",
            channel=channel,
        )

        try:
            # Get current space settings
            space_info = chat.spaces().get(name=space).execute()
            external_users_allowed = space_info.get("externalUserAllowed", False)

            # If external users are not allowed, update the space
            if not external_users_allowed:
                update_body = {"externalUserAllowed": True}
                update_mask = "externalUserAllowed"
                chat.spaces().patch(
                    name=space, updateMask=update_mask, body=update_body
                ).execute()
                log_with_context(
                    logging.INFO,
                    f"Successfully enabled external user access for space {space}",
                    channel=channel,
                )
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to enable external user access for space {space}: {e}",
                channel=channel,
            )

    return active_user_emails


def _add_regular_members_batch(
    ctx: MigrationContext,
    state: MigrationState,
    chat: Any,
    user_resolver: Any,
    space: str,
    channel: str,
    active_users: set[str] | list[str],
) -> int:
    """Add active users to a Google Chat space as regular members.

    Iterates over *active_users*, resolves each to an internal email via the
    user resolver, and creates a regular membership.  Handles HTTP 409
    (conflict / already exists), HTTP 400 (bad request), and other errors
    individually per user.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups.
        space: Google Chat space resource name.
        channel: Slack channel name for log context.
        active_users: Iterable of Slack user IDs to add.

    Returns:
        The number of members successfully added (including 409 conflicts
        treated as already present).
    """
    added_count = 0
    failed_count = 0

    pbar = tqdm(active_users, desc=f"Adding current members to {channel}")
    for user_id in pbar:
        user_email = ctx.user_map.get(user_id)
        membership_body = {}  # Ensure membership_body is always defined

        if not user_email:
            # Track unmapped user for space membership
            log_with_context(
                logging.ERROR,  # Escalated from WARNING to ERROR
                f"\U0001f6a8 CRITICAL: No email mapping found for user {user_id} - cannot add as regular member",
                user_id=user_id,
                channel=channel,
            )
            # This will be automatically tracked in _get_internal_email when user lookup fails
            continue

        # Get the internal email for this user (handles external users)
        internal_email = user_resolver.get_internal_email(user_id, user_email)

        # Track external users for message attribution
        if user_resolver.is_external_user(user_email):
            log_with_context(
                logging.INFO,
                f"Adding external user {user_id} with email {user_email} as regular member",
                user_id=user_id,
                user_email=user_email,
                channel=channel,
            )
            state.users.external_users.add(user_email)

        try:
            # Log which user we're trying to add
            log_with_context(
                logging.DEBUG,  # Changed from INFO for less verbose output
                f"Attempting to add user {user_email if user_resolver.is_external_user(user_email) else internal_email} as regular member",
                user=(
                    user_email
                    if user_resolver.is_external_user(user_email)
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
            chat.spaces().members().create(parent=space, body=membership_body).execute()

            added_count += 1
            log_with_context(
                logging.DEBUG,
                f"Added user {internal_email} to space {space} as regular member",
                user=internal_email,
                channel=channel,
            )
        except HttpError as e:
            # If we get a 409 conflict, the user might already be in the space
            if e.resp.status == HTTP_CONFLICT:
                log_with_context(
                    logging.WARNING,
                    f"User {internal_email} might already be in space {space}: {e}",
                    user=internal_email,
                    channel=channel,
                )
                added_count += 1
            elif e.resp.status == HTTP_BAD_REQUEST:
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
                if e.resp.status in (HTTP_FORBIDDEN, HTTP_NOT_FOUND):
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

    return added_count


def _find_admin_user_id(
    user_map: dict[str, str], admin_email: str, channel: str
) -> str | None:
    """Look up the workspace admin's Slack user ID from the user map.

    Args:
        user_map: Mapping of Slack user IDs to Google email addresses.
        admin_email: The admin's email address.
        channel: Channel name for log context.

    Returns:
        The Slack user ID if found, else None.
    """
    for slack_user_id, email in user_map.items():
        if email.lower() == admin_email.lower():
            log_with_context(
                logging.DEBUG,
                f"Found Slack user ID for admin: {slack_user_id}",
                channel=channel,
            )
            return slack_user_id
    log_with_context(
        logging.DEBUG,
        f"Workspace admin ({admin_email}) was not found in Slack user map",
        channel=channel,
    )
    return None


def _find_admin_membership(
    members: list[dict[str, Any]], admin_email: str
) -> str | None:
    """Find the admin's membership resource name in the members list.

    Args:
        members: List of membership dicts from the Chat API.
        admin_email: The admin's email address.

    Returns:
        The membership resource name if found, else None.
    """
    for member in members:
        member_name = member.get("member", {}).get("name", "")
        if (
            member_name == f"users/{admin_email}"
            or member_name.lower() == f"users/{admin_email.lower()}"
        ):
            return member.get("name")
        member_email = member.get("member", {}).get("email", "")
        if member_email and member_email.lower() == admin_email.lower():
            return member.get("name")
    return None


def _verify_and_handle_admin(
    ctx: MigrationContext,
    chat: Any,
    space: str,
    channel: str,
    active_users: set[str] | list[str],
    added_count: int,
) -> None:
    """Verify members were added and remove workspace admin if appropriate.

    Lists the current space members to verify the batch add succeeded, then
    checks whether the workspace admin was part of the original Slack channel.
    If the admin was *not* in the channel, attempts to remove them from the
    Google Chat space (they were auto-added as the space creator).

    Args:
        ctx: Immutable migration context.
        chat: Google Chat API service (admin).
        space: Google Chat space resource name.
        channel: Slack channel name for log context.
        active_users: Set/list of Slack user IDs considered active in the channel.
        added_count: Number of members successfully added (for log message).
    """
    try:
        log_with_context(
            logging.DEBUG, f"Verifying members added to space {space}", channel=channel
        )

        members_result = chat.spaces().members().list(parent=space).execute()
        members = members_result.get("memberships", [])

        log_with_context(
            logging.DEBUG,
            f"Space {space} has {len(members)} members after adding {added_count} regular members",
            channel=channel,
        )

        admin_email = ctx.workspace_admin
        log_with_context(
            logging.DEBUG,
            f"Checking if workspace admin ({admin_email}) should be in space {space} for channel {channel}",
            channel=channel,
        )

        # Look up admin's Slack user ID
        admin_user_id = _find_admin_user_id(ctx.user_map, admin_email, channel)

        # If admin is in the original channel, keep them — nothing to do
        if admin_user_id and admin_user_id in active_users:
            log_with_context(
                logging.DEBUG,
                f"Workspace admin ({admin_email}) was in the original Slack channel - will keep in space",
                channel=channel,
            )
            return

        log_with_context(
            logging.DEBUG,
            f"Workspace admin ({admin_email}) was NOT in the original Slack channel - will attempt removal",
            channel=channel,
        )

        admin_membership = _find_admin_membership(members, admin_email)
        if not admin_membership:
            log_with_context(
                logging.DEBUG,
                f"Workspace admin ({admin_email}) membership not found in space {space}",
                channel=channel,
            )
            log_with_context(
                logging.DEBUG,
                f"Members in space {space}: {[m.get('member', {}).get('name', '') for m in members]}",
                channel=channel,
            )
            return

        log_with_context(
            logging.INFO,
            f"Removing workspace admin ({admin_email}) from space {space} because they weren't in the original Slack channel {channel}",
            channel=channel,
        )
        try:
            chat.spaces().members().delete(name=admin_membership).execute()
            log_with_context(
                logging.INFO,
                f"Successfully removed workspace admin from space {space}",
                channel=channel,
            )
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to remove workspace admin from space {space}: {e}",
                channel=channel,
            )
    except HttpError as e:
        log_with_context(
            logging.WARNING,
            f"Failed to verify members in space {space}: {e}",
            channel=channel,
        )


def _update_folder_permissions(
    file_handler: Any,
    channel: str,
    active_user_emails: list[str],
) -> None:
    """Update Drive folder permissions to match active channel members.

    If the file_handler has a ``folder_manager``, looks up the channel's
    Drive folder and updates its permissions so only the given
    *active_user_emails* have access.

    Args:
        file_handler: FileHandler instance (may be None).
        channel: Slack channel name for folder lookup and log context.
        active_user_emails: Email addresses that should have folder access.
    """
    if not (file_handler is not None and hasattr(file_handler, "folder_manager")):
        return

    root_folder_id = file_handler._root_folder_id
    if root_folder_id is None:
        return

    folder_id = None

    try:
        # Get the channel folder if it exists
        folder_id = file_handler.folder_manager.get_channel_folder_id(
            channel,
            root_folder_id,
            file_handler._shared_drive_id,
        )

        if folder_id:
            # Step 6: Update file permissions
            log_with_context(
                logging.INFO,
                f"Step 6/6: Updating file permissions for {channel} folder to match {len(active_user_emails)} active members",
                channel=channel,
                folder_id=folder_id,
            )

            # Update permissions to ensure only active members have access
            file_handler.folder_manager.set_channel_folder_permissions(
                folder_id,
                channel,
                active_user_emails,
                file_handler._shared_drive_id,
            )
    except HttpError as e:
        log_with_context(
            logging.WARNING,
            f"Error updating channel folder permissions: {e}",
            channel=channel,
            error=str(e),
        )


def add_regular_members(
    ctx: MigrationContext,
    state: MigrationState,
    chat: Any,
    user_resolver: Any,
    file_handler: Any,
    space: str,
    channel: str,
) -> None:
    """Add regular members to a space after import mode is complete.

    After completing import mode, this method adds back all active members
    to the space as regular members. This ensures that users have access
    to the space after migration.

    This method also updates any channel folder permissions to ensure only
    active members have access to shared files.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups.
        file_handler: FileHandler instance (may be None).
        space: Google Chat space resource name (e.g. ``spaces/AAAA``).
        channel: Slack channel name used for log context and data lookup.
    """
    # Get the list of active users we saved during add_users_to_space
    if channel not in state.progress.active_users_by_channel:
        # If we don't have active users for this channel, try to get them from the channel directory
        log_with_context(
            logging.WARNING,
            f"No active users tracked for channel {channel}, attempting to load from channel data",
            channel=channel,
        )

        try:
            # Try to load channel members from the channel data
            export_root = Path(ctx.export_root)
            channels_file = export_root / "channels.json"

            if channels_file.exists():
                with open(channels_file) as f:
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
                        state.progress.active_users_by_channel[channel] = members
                        break
        except (OSError, json.JSONDecodeError) as e:
            log_with_context(
                logging.ERROR,
                f"Failed to load channel members from channels.json: {e}",
                channel=channel,
            )

    # If we still don't have active users, we can't proceed
    if channel not in state.progress.active_users_by_channel:
        log_with_context(
            logging.ERROR,
            f"No active users found for channel {channel}, can't add regular members",
            channel=channel,
        )
        return

    active_users = state.progress.active_users_by_channel[channel]
    log_with_context(
        logging.DEBUG,
        f"{ctx.log_prefix}Adding {len(active_users)} regular members to space {space} for channel {channel}",
        channel=channel,
    )

    active_user_emails = _collect_active_user_emails(
        ctx, chat, user_resolver, space, channel, active_users
    )

    added_count = _add_regular_members_batch(
        ctx, state, chat, user_resolver, space, channel, active_users
    )

    _verify_and_handle_admin(ctx, chat, space, channel, active_users, added_count)

    _update_folder_permissions(file_handler, channel, active_user_emails)
