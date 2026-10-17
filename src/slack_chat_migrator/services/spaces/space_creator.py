"""
Functions for managing Google Chat spaces during Slack migration.

Handles space creation in import mode, listing, and standalone cleanup
of spaces stuck in import mode.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    HTTP_FORBIDDEN,
    IMPORT_MODE_DAYS_LIMIT,
    PERMISSION_DENIED_ERROR,
    SPACE_NAME_PREFIX,
    SPACE_THREADING_STATE,
    SPACE_TYPE,
    SPACES_PAGE_SIZE,
)
from slack_chat_migrator.exceptions import SpacePermissionError
from slack_chat_migrator.utils.api import slack_ts_to_rfc3339
from slack_chat_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_chat_migrator.core.context import MigrationContext
    from slack_chat_migrator.core.state import MigrationState
    from slack_chat_migrator.services.chat_adapter import ChatAdapter
    from slack_chat_migrator.services.user_resolver import UserResolver


def channel_has_external_users(
    ctx: MigrationContext, user_resolver: UserResolver, channel: str
) -> bool:
    """Check if a channel has external users that need access.

    Args:
        ctx: Immutable migration context.
        user_resolver: UserResolver for external-user checks.
        channel: The channel name to check.

    Returns:
        True if the channel has external users (excluding bots), False otherwise.
    """
    # Get channel metadata for members
    meta = ctx.channels_meta.get(channel, {})
    members = meta.get("members", [])

    # If no members in metadata, check message history
    if not members:
        ch_dir = ctx.export_root / channel
        user_ids: set[str] = set()

        # Scan message files for unique user IDs
        for jf in ch_dir.glob("*.json"):
            try:
                with open(jf, encoding="utf-8") as f:
                    msgs = json.load(f)
                for m in msgs:
                    if m.get("type") == "message" and "user" in m and m["user"]:
                        user_ids.add(m["user"])
            except (OSError, json.JSONDecodeError) as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to process {jf} when checking for external users: {e}",
                    channel=channel,
                )

        members = list(user_ids)

    # Check if any member is an external user (excluding bots)
    for user_id in members:
        # Get email from user map
        email = ctx.user_map.get(user_id)
        if not email:
            continue

        # Check if this is an external user (not a bot)
        users_without_email = ctx.users_without_email or []

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

        if user_resolver.is_external_user(email) and not is_bot:
            log_with_context(
                logging.INFO,
                f"Channel {channel} has external user {user_id} with email {email}",
                channel=channel,
            )
            return True

    return False


def create_space(
    ctx: MigrationContext,
    state: MigrationState,
    chat: ChatAdapter,
    user_resolver: UserResolver,
    channel: str,
) -> str:
    """Create a Google Chat space for a Slack channel in import mode.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service.
        user_resolver: UserResolver for external-user checks.
        channel: Slack channel name to create a space for.

    Returns:
        The Google Chat space resource name (e.g. ``spaces/AAAA``).

    Raises:
        SpacePermissionError: On 403 PERMISSION_DENIED errors.
    """
    # Get channel metadata
    meta = ctx.channels_meta.get(channel, {})
    display_name = f"{SPACE_NAME_PREFIX}{channel}"

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
    body: dict[str, Any] = {
        "displayName": display_name,
        "spaceType": SPACE_TYPE,
        "importMode": True,
        "spaceThreadingState": SPACE_THREADING_STATE,
    }

    log_with_context(
        logging.DEBUG,
        f"{ctx.log_prefix}Creating import mode space for {display_name}",
        channel=channel,
    )

    # If we have original creation time, add it
    if create_time:
        body["createTime"] = create_time

    # Check if this channel has external users that need access
    has_external_users = channel_has_external_users(ctx, user_resolver, channel)
    if has_external_users:
        body["externalUserAllowed"] = True
        log_with_context(
            logging.INFO,
            f"{ctx.log_prefix}Enabling external user access for channel {channel}",
            channel=channel,
        )

    try:
        # Create the space in import mode
        space = chat.create_space(body)
        space_name: str = space["name"]

        # Increment the spaces created counter
        state.progress.migration_summary["spaces_created"] += 1

        log_with_context(
            logging.INFO,
            f"{ctx.log_prefix}Created space {space_name} for channel {channel} in import mode with threading enabled",
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

                    chat.patch_space(
                        name=space_name,
                        update_mask=update_mask,
                        body=space_details,
                    )

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
        if e.resp.status == HTTP_FORBIDDEN and PERMISSION_DENIED_ERROR in str(e):
            log_with_context(
                logging.WARNING, f"Error setting up channel {channel}: {e}"
            )
            raise SpacePermissionError(channel) from e
        else:
            # For other errors, re-raise
            raise

    # Store the created space in state
    state.spaces.created_spaces[channel] = space_name

    # Store whether this space has external users for later reference
    state.progress.spaces_with_external_users[space_name] = has_external_users

    return space_name


@dataclass
class ImportCompletionResult:
    """Outcome of completing import mode on a set of spaces.

    Attributes:
        candidates: Spaces found in import mode (space resource dicts).
        completed: Names of spaces whose import was completed.
        failed: Space name -> error, for completions that failed.
        errors: Other problems (listing or status-check failures).
        cancelled: True if the user declined the confirmation.
    """

    candidates: list[dict[str, Any]] = field(default_factory=list)
    completed: list[str] = field(default_factory=list)
    failed: dict[str, str] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    cancelled: bool = False

    @property
    def ok(self) -> bool:
        """True when nothing failed and the run wasn't cancelled."""
        return not self.failed and not self.errors and not self.cancelled


def _list_all_spaces(
    chat_service: ChatAdapter, errors: list[str] | None = None
) -> list[dict[str, Any]]:
    """Paginate through ``spaces().list()`` and return every space.

    Args:
        chat_service: An authenticated Google Chat API service resource.
        errors: If given, a listing failure is appended to it.

    Returns:
        List of space resource dicts.
    """
    spaces: list[dict[str, Any]] = []
    page_token = None
    while True:
        try:
            response = chat_service.list_spaces(
                page_size=SPACES_PAGE_SIZE, page_token=page_token
            )
            spaces.extend(response.get("spaces", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        except HttpError as http_e:
            message = (
                f"HTTP error listing spaces during cleanup: {http_e} "
                f"(Status: {http_e.resp.status})"
            )
            log_with_context(logging.ERROR, message)
            if errors is not None:
                errors.append(message)
            break
        except (RefreshError, TransportError) as e:
            log_with_context(logging.ERROR, f"Failed to list spaces: {e}")
            if errors is not None:
                errors.append(f"Failed to list spaces: {e}")
            break
    return spaces


def find_import_mode_spaces(chat_service: ChatAdapter) -> ImportCompletionResult:
    """Find this tool's spaces that are still in import mode.

    Only spaces whose display name starts with :data:`SPACE_NAME_PREFIX`
    (``"Slack #"``) are considered, so spaces created by other tools or the
    permission check are never touched.

    Note that ``spaces.list`` only returns spaces the impersonated admin is
    a member of, and import-mode spaces have no members, so some may not be
    visible.

    Args:
        chat_service: An authenticated Google Chat API service resource.

    Returns:
        A result whose ``candidates`` are the import-mode spaces found and
        whose ``errors`` lists listing or lookup failures.
    """
    result = ImportCompletionResult()
    for space in _list_all_spaces(chat_service, result.errors):
        space_name = space.get("name", "")
        if not space_name:
            continue
        listed_name = space.get("displayName", "") or ""
        if "displayName" in space and not listed_name.startswith(SPACE_NAME_PREFIX):
            continue
        space_info = space
        if "importMode" not in space or "displayName" not in space:
            # The listing didn't include what we need; read the space.
            try:
                space_info = {**space, **chat_service.get_space(space_name)}
            except (HttpError, RefreshError, TransportError) as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to check space {space_name}: {e}",
                )
                result.errors.append(f"{space_name}: {e}")
                continue
        if not (space_info.get("displayName") or "").startswith(SPACE_NAME_PREFIX):
            continue
        if space_info.get("importMode"):
            result.candidates.append(space_info)
    return result


def complete_import_mode_spaces(
    chat_service: ChatAdapter, result: ImportCompletionResult
) -> ImportCompletionResult:
    """Call ``completeImport`` on each of ``result.candidates``.

    Successes go to ``result.completed`` and failures (with the error) to
    ``result.failed``; external-user access is restored where it was set.

    Args:
        chat_service: An authenticated Google Chat API service resource.
        result: The result of :func:`find_import_mode_spaces`.

    Returns:
        The same *result*, updated.
    """
    for space_info in result.candidates:
        space_name = space_info.get("name", "")
        try:
            chat_service.complete_import(space_name)
        except HttpError as http_e:
            log_with_context(
                logging.ERROR,
                f"HTTP error completing import for {space_name}: {http_e} "
                f"(Status: {http_e.resp.status})",
            )
            result.failed[space_name] = f"HTTP {http_e.resp.status}: {http_e}"
            continue
        except (RefreshError, TransportError) as e:
            log_with_context(
                logging.ERROR,
                f"Failed to complete import for {space_name}: {e}",
            )
            result.failed[space_name] = str(e)
            continue

        result.completed.append(space_name)
        log_with_context(
            logging.INFO,
            f"Completed import mode for space: {space_name}",
        )

        # Preserve external user access if it was set
        if space_info.get("externalUserAllowed"):
            try:
                chat_service.patch_space(
                    name=space_name,
                    update_mask="externalUserAllowed",
                    body={"externalUserAllowed": True},
                )
                log_with_context(
                    logging.INFO,
                    f"Preserved external user access for: {space_name}",
                )
            except (HttpError, RefreshError, TransportError) as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to preserve external user access for {space_name}: {e}",
                )
    return result


def cleanup_import_mode_spaces(
    chat_service: ChatAdapter,
    confirm: Callable[[list[dict[str, Any]]], bool] | None = None,
) -> ImportCompletionResult:
    """Complete import mode on this tool's spaces still stuck in import mode.

    This is a standalone version of the cleanup logic that only requires a
    Chat API service client — no export data or user mappings needed.
    It finds the import-mode spaces named ``Slack #...`` and, once
    *confirm* approves the list, calls ``completeImport()`` on each.

    Member-adding is skipped because that requires export data.  Users can
    run ``slack-chat-migrator migrate --resume`` afterwards to add members.

    Args:
        chat_service: An authenticated Google Chat API service resource.
        confirm: Called with the candidate spaces before anything is
            changed; returning False cancels.  None means no confirmation.

    Returns:
        The outcome, listing completed and failed spaces.
    """
    log_with_context(logging.INFO, "Running standalone cleanup...")

    result = find_import_mode_spaces(chat_service)
    if not result.candidates:
        log_with_context(
            logging.INFO,
            f"No spaces named '{SPACE_NAME_PREFIX}...' found in import mode.",
        )
        return result

    log_with_context(
        logging.INFO,
        f"Found {len(result.candidates)} space(s) still in import mode.",
    )
    if confirm is not None and not confirm(result.candidates):
        result.cancelled = True
        log_with_context(logging.INFO, "Import completion cancelled.")
        return result

    complete_import_mode_spaces(chat_service, result)
    log_with_context(
        logging.INFO,
        f"Standalone cleanup completed: {len(result.completed)} completed,"
        f" {len(result.failed)} failed.",
    )
    return result
