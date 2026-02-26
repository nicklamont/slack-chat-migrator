"""
Functions for managing Google Chat space memberships during Slack migration.

Handles adding historical members during import mode and regular members
after import mode completion, including external user handling and
admin membership management.
"""

from __future__ import annotations

import datetime
import json
import logging
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.constants import (
    CHANNEL_JOIN_SUBTYPE,
    CHANNEL_LEAVE_SUBTYPE,
    DEFAULT_FALLBACK_JOIN_TIME,
    EARLIEST_MESSAGE_OFFSET_MINUTES,
    FIRST_MESSAGE_OFFSET_MINUTES,
    HISTORICAL_DELETE_TIME_OFFSET_SECONDS,
    HTTP_BAD_REQUEST,
    HTTP_CONFLICT,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
)
from slack_migrator.utils.api import slack_ts_to_rfc3339
from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState


def _collect_user_membership_data(  # noqa: C901
    ctx: MigrationContext, state: MigrationState, channel: str
) -> tuple[dict[str, dict[str, Any]], set[str]]:
    """Collect user participation data from message files and channel metadata.

    Scans all JSON message files in the channel directory to build a map of
    user membership events (join/leave times, first message times). Then
    augments with the definitive member list from channel metadata.

    Also stores the active user set on ``state.active_users_by_channel``
    for later use by :func:`add_regular_members`.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        channel: Slack channel name.

    Returns:
        A tuple of ``(user_membership, active_users)`` where *user_membership*
        maps Slack user IDs to dicts with ``join_time``, ``leave_time``,
        ``active``, and ``first_message_time`` keys, and *active_users* is
        the set of user IDs considered currently active.
    """
    user_membership: dict[str, dict[str, Any]] = {}
    active_users: set[str] = set()
    ch_dir = ctx.export_root / channel

    # First pass: identify all users and their join/leave events
    for jf in sorted(ch_dir.glob("*.json")):
        try:
            with open(jf) as f:
                msgs = json.load(f)
            for m in msgs:
                if m.get("type") != "message":
                    continue

                # Track users who sent messages (all message types)
                user_id = m.get("user")
                if user_id:
                    timestamp = slack_ts_to_rfc3339(m["ts"])
                    if user_id not in user_membership:
                        user_membership[user_id] = {
                            "join_time": None,
                            "leave_time": None,
                            "active": True,
                            "first_message_time": timestamp,
                        }
                        active_users.add(user_id)
                    elif timestamp < user_membership[user_id].get(
                        "first_message_time", timestamp
                    ):
                        user_membership[user_id]["first_message_time"] = timestamp

                # Check for join/leave messages
                subtype = m.get("subtype")
                if subtype == CHANNEL_JOIN_SUBTYPE and "user" in m:
                    user_id = m["user"]
                    timestamp = slack_ts_to_rfc3339(m["ts"])
                    if user_id not in user_membership:
                        user_membership[user_id] = {
                            "join_time": timestamp,
                            "leave_time": None,
                            "active": True,
                            "first_message_time": None,
                        }
                        active_users.add(user_id)
                    elif (
                        not user_membership[user_id]["join_time"]
                        or timestamp < user_membership[user_id]["join_time"]
                    ):
                        user_membership[user_id]["join_time"] = timestamp
                        user_membership[user_id]["active"] = True
                        active_users.add(user_id)

                elif subtype == CHANNEL_LEAVE_SUBTYPE and "user" in m:
                    user_id = m["user"]
                    timestamp = slack_ts_to_rfc3339(m["ts"])
                    if user_id not in user_membership:
                        continue
                    if (
                        not user_membership[user_id]["leave_time"]
                        or timestamp > user_membership[user_id]["leave_time"]
                    ):
                        user_membership[user_id]["leave_time"] = timestamp
                        user_membership[user_id]["active"] = False
                        active_users.discard(user_id)
        except (OSError, json.JSONDecodeError) as e:
            log_with_context(
                logging.WARNING,
                f"Failed to process file {jf} when collecting user membership data: {e}",
                channel=channel,
            )

    # The channel metadata (channels.json) is the most reliable and definitive source for active members
    # Reset active_users to ensure only the members from channels.json are considered active
    active_users = set()  # Clear any users previously marked as active from messages

    meta = ctx.channels_meta.get(channel, {})
    if "members" in meta and isinstance(meta["members"], list):
        for user_id in meta["members"]:
            active_users.add(user_id)  # These users are definitely active now
            if user_id not in user_membership:
                # If user is in metadata but not seen in messages, add them with default times
                user_membership[user_id] = {
                    "join_time": DEFAULT_FALLBACK_JOIN_TIME,
                    "leave_time": None,
                    "active": True,
                    "first_message_time": None,
                }

    # Store active users to add back after import completes
    # We'll use this for both regular membership and file permissions
    # Log active user counts for debugging
    log_with_context(
        logging.DEBUG,
        f"Identified {len(active_users)} active users for channel {channel}",
        channel=channel,
    )
    state.active_users_by_channel[channel] = active_users

    return user_membership, active_users


def _compute_membership_times(
    ctx: MigrationContext,
    channel: str,
    user_membership: dict[str, dict[str, Any]],
) -> None:
    """Fill in missing join and leave times for historical memberships.

    Mutates *user_membership* in place, applying the following cascade for
    ``join_time``:

    1. Explicit ``channel_join`` event (already set by caller).
    2. User's first message time minus 1 minute.
    3. Channel creation time from metadata.
    4. Earliest message time in the channel minus 2 minutes.
    5. :data:`DEFAULT_FALLBACK_JOIN_TIME` as the last resort.

    For ``leave_time``, any user without an explicit ``channel_leave`` event
    gets the current UTC time minus
    :data:`HISTORICAL_DELETE_TIME_OFFSET_SECONDS` seconds, as required by the
    Google Chat import-mode API.

    Args:
        ctx: Immutable migration context (used for channel metadata).
        channel: Slack channel name for log context and metadata lookup.
        user_membership: Mutable mapping of user IDs to membership dicts.
    """
    # Get channel creation time from metadata to use as fallback
    # (We can't get space info in import mode and don't need to try)
    channel_creation_time = None
    meta = ctx.channels_meta.get(channel, {})
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
        (
            current_time
            - datetime.timedelta(seconds=HISTORICAL_DELETE_TIME_OFFSET_SECONDS)
        )
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
    default_join_time = DEFAULT_FALLBACK_JOIN_TIME
    if earliest_message_time:
        try:
            # Convert to datetime, subtract 2 minutes for safety, and convert back
            if earliest_message_time.endswith("Z"):
                earliest_message_time = earliest_message_time[:-1] + "+00:00"
            earliest_dt = datetime.datetime.fromisoformat(earliest_message_time)
            earliest_join_dt = earliest_dt - datetime.timedelta(
                minutes=EARLIEST_MESSAGE_OFFSET_MINUTES
            )
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
                    join_dt = dt - datetime.timedelta(
                        minutes=FIRST_MESSAGE_OFFSET_MINUTES
                    )
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


def _add_historical_members_batch(
    ctx: MigrationContext,
    state: MigrationState,
    chat: Any,
    user_resolver: Any,
    space: str,
    channel: str,
    user_membership: dict[str, dict[str, Any]],
    active_users: set[str],
) -> None:
    """Add historical memberships to a Google Chat space via the API.

    Iterates over *user_membership*, resolves each Slack user ID to an
    internal email address, and creates an import-mode membership with the
    computed ``createTime`` and ``deleteTime``.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups.
        space: Google Chat space resource name (e.g. ``spaces/AAAA``).
        channel: Slack channel name for log context.
        user_membership: Mapping of Slack user IDs to membership dicts
            (must already have ``join_time`` and ``leave_time`` populated).
        active_users: Set of user IDs considered active (used for summary log).
    """
    added_count = 0
    failed_count = 0

    pbar = tqdm(user_membership.items(), desc=f"Adding historical members to {channel}")
    for user_id, membership in pbar:
        user_email = ctx.user_map.get(user_id)

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
        internal_email = user_resolver.get_internal_email(user_id, user_email)

        # Track external users for message attribution
        if user_resolver.is_external_user(user_email):
            log_with_context(
                logging.INFO,
                f"Adding external user {user_id} with internal email {internal_email} as historical member",
                user_id=user_id,
                user_email=user_email,
                channel=channel,
            )
            state.external_users.add(user_email)

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
            chat.spaces().members().create(parent=space, body=membership_body).execute()

            added_count += 1
            log_with_context(
                logging.DEBUG,
                f"Added user {internal_email} to space {space} as historical membership",
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
                channel=state.current_channel,
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


def add_users_to_space(
    ctx: MigrationContext,
    state: MigrationState,
    chat: Any,
    user_resolver: Any,
    space: str,
    channel: str,
) -> None:
    """Add users to a space as historical members.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups.
        space: Google Chat space resource name (e.g. ``spaces/AAAA``).
        channel: Slack channel name used for log context and data lookup.
    """
    log_with_context(
        logging.DEBUG,
        f"{ctx.log_prefix}Adding historical memberships for channel {channel}",
        channel=channel,
    )

    user_membership, active_users = _collect_user_membership_data(ctx, state, channel)

    # Log what we're doing
    log_with_context(
        logging.DEBUG,
        f"{ctx.log_prefix}Adding {len(user_membership)} users to space {space} for channel {channel}",
        channel=channel,
        space=space,
        user_count=len(user_membership),
    )

    # Check if the workspace admin is in the active users
    # Google Chat automatically adds the creator as a member, but we only want them if they were in the channel
    admin_email = ctx.workspace_admin
    admin_user_id = None

    # Look up the admin's Slack user ID if they had one (they'll be in user_map if they were in Slack)
    for slack_user_id, email in ctx.user_map.items():
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

    _compute_membership_times(ctx, channel, user_membership)

    _add_historical_members_batch(
        ctx, state, chat, user_resolver, space, channel, user_membership, active_users
    )


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
            state.external_users.add(user_email)

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
    if channel not in state.active_users_by_channel:
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
                        state.active_users_by_channel[channel] = members
                        break
        except (OSError, json.JSONDecodeError) as e:
            log_with_context(
                logging.ERROR,
                f"Failed to load channel members from channels.json: {e}",
                channel=channel,
            )

    # If we still don't have active users, we can't proceed
    if channel not in state.active_users_by_channel:
        log_with_context(
            logging.ERROR,
            f"No active users found for channel {channel}, can't add regular members",
            channel=channel,
        )
        return

    active_users = state.active_users_by_channel[channel]
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
