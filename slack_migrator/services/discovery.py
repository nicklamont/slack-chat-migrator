"""
Functions for discovering existing Google Chat resources for migration resumption
"""

from __future__ import annotations

import datetime
import logging
import time
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError

from slack_migrator.constants import SPACE_NAME_PREFIX, SPACES_PAGE_SIZE
from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState


def _fetch_all_migration_spaces(
    chat: Any,
    channel_name_to_id: dict[str, str],
    state: MigrationState,
) -> dict[str, list[dict[str, Any]]]:
    """Paginate through Google Chat API to find spaces matching the migration pattern.

    Args:
        chat: Google Chat API service.
        channel_name_to_id: Mapping of channel names to Slack channel IDs.
        state: Mutable migration state.

    Returns:
        Dict mapping channel names to lists of space info dicts.

    Raises:
        HttpError: If the API call fails.
    """
    all_spaces_by_channel: dict[str, list[dict[str, Any]]] = {}
    prefix = SPACE_NAME_PREFIX

    page_token = None
    while True:
        request = chat.spaces().list(pageSize=SPACES_PAGE_SIZE, pageToken=page_token)
        response = request.execute()

        for space in response.get("spaces", []):
            display_name = space.get("displayName", "")
            space_name = space.get("name", "")
            space_id = space_name.split("/")[-1] if space_name else ""

            if not (display_name and display_name.startswith(prefix)):
                continue

            channel_name = display_name[len(prefix) :].strip()
            if not channel_name:
                continue

            space_info = {
                "display_name": display_name,
                "space_name": space_name,
                "space_id": space_id,
                "space_type": space.get("spaceType", ""),
                "member_count": 0,
                "create_time": space.get("createTime", "Unknown"),
            }

            if channel_name not in all_spaces_by_channel:
                all_spaces_by_channel[channel_name] = []
            all_spaces_by_channel[channel_name].append(space_info)

            # First-seen ID mapping
            channel_id = channel_name_to_id.get(channel_name, "")
            if channel_id and channel_id not in state.channel_id_to_space_id:
                state.channel_id_to_space_id[channel_id] = space_id

        page_token = response.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)

    return all_spaces_by_channel


def _resolve_duplicate_spaces(
    chat: Any,
    channel_name_to_id: dict[str, str],
    state: MigrationState,
    all_spaces_by_channel: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, str], dict[str, list[dict[str, Any]]]]:
    """Separate unique and duplicate space mappings, enriching duplicates with member counts.

    Args:
        chat: Google Chat API service.
        channel_name_to_id: Mapping of channel names to Slack channel IDs.
        state: Mutable migration state.
        all_spaces_by_channel: All discovered spaces grouped by channel name.

    Returns:
        Tuple of (space_mappings, duplicate_spaces).
    """
    space_mappings: dict[str, str] = {}
    duplicate_spaces: dict[str, list[dict[str, Any]]] = {}

    for channel_name, spaces in all_spaces_by_channel.items():
        # Default to first space found
        space_mappings[channel_name] = spaces[0]["space_name"]

        if len(spaces) == 1:
            channel_id = channel_name_to_id.get(channel_name, "")
            if channel_id:
                state.channel_id_to_space_id[channel_id] = spaces[0]["space_id"]
            continue

        # Duplicate — enrich with member counts for disambiguation
        duplicate_spaces[channel_name] = spaces
        for space_info in spaces:
            try:
                members_response = (
                    chat.spaces()
                    .members()
                    .list(parent=space_info["space_name"], pageSize=1)
                    .execute()
                )
                if "memberships" in members_response:
                    count = len(members_response.get("memberships", []))
                    space_info["member_count"] = (
                        f"{count}+" if "nextPageToken" in members_response else count
                    )
            except HttpError as e:
                log_with_context(
                    logging.DEBUG,
                    f"Error fetching members for space {space_info['space_name']}: {e}",
                )

        # Remove ambiguous ID mapping
        channel_id = channel_name_to_id.get(channel_name, "")
        if channel_id and channel_id in state.channel_id_to_space_id:
            log_with_context(
                logging.WARNING,
                f"Removing ambiguous ID mapping for channel {channel_name} (ID: {channel_id})",
            )
            del state.channel_id_to_space_id[channel_id]

    return space_mappings, duplicate_spaces


def _log_duplicate_spaces(
    duplicate_spaces: dict[str, list[dict[str, Any]]],
) -> None:
    """Log details about channels with multiple matching spaces.

    Args:
        duplicate_spaces: Dict mapping channel names to lists of conflicting space info.
    """
    if not duplicate_spaces:
        return

    log_with_context(
        logging.WARNING,
        f"Found {len(duplicate_spaces)} channels with duplicate spaces: {', '.join(duplicate_spaces.keys())}",
    )
    for channel_name, spaces in duplicate_spaces.items():
        log_with_context(
            logging.WARNING,
            f"Channel '{channel_name}' has {len(spaces)} duplicate spaces:",
        )
        for i, space_info in enumerate(spaces):
            log_with_context(
                logging.WARNING,
                f"  Space {i + 1}: {space_info['display_name']} (ID: {space_info['space_id']}, "
                f"Type: {space_info['space_type']}, Members: {space_info['member_count']}, "
                f"Created: {space_info['create_time']})",
            )


def discover_existing_spaces(
    chat: Any,
    channel_name_to_id: dict[str, str],
    state: MigrationState,
) -> tuple[dict[str, str], dict[str, list[dict[str, Any]]]]:
    """Query Google Chat API to find spaces matching the migration naming pattern.

    Searches for spaces with names matching "Slack #<channel-name>". Detects
    and reports duplicate spaces to help users disambiguate via config.

    Args:
        chat: Google Chat API service.
        channel_name_to_id: Mapping of channel names to Slack channel IDs.
        state: Mutable migration state.

    Returns:
        Tuple of (space_mappings, duplicate_spaces) where space_mappings maps
        channel names to space resource names and duplicate_spaces maps channel
        names to lists of conflicting space info dicts.
    """
    log_with_context(
        logging.INFO,
        "Discovering existing Google Chat spaces that may have been created by previous migrations",
    )

    space_mappings: dict[str, str] = {}
    duplicate_spaces: dict[str, list[dict[str, Any]]] = {}

    try:
        all_spaces_by_channel = _fetch_all_migration_spaces(
            chat, channel_name_to_id, state
        )
        space_mappings, duplicate_spaces = _resolve_duplicate_spaces(
            chat, channel_name_to_id, state, all_spaces_by_channel
        )
        _log_duplicate_spaces(duplicate_spaces)

        spaces_found = sum(len(s) for s in all_spaces_by_channel.values())
        log_with_context(
            logging.INFO,
            f"Found {spaces_found} existing spaces matching migration pattern across {len(all_spaces_by_channel)} channels",
        )
        log_with_context(
            logging.INFO,
            f"Created {len(state.channel_id_to_space_id)} channel ID to space ID mappings",
        )

    except HttpError as e:
        log_with_context(
            logging.WARNING,
            f"Error discovering spaces: {e}",
            error=str(e),
        )

    return space_mappings, duplicate_spaces


def get_last_message_timestamp(chat: Any, channel: str, space: str) -> float:
    """
    Query Google Chat API to get the timestamp of the last message in a space.

    This helps determine where to resume migration - we'll only import messages
    that are newer than the most recent message in the space.

    Args:
        chat: Google Chat API service.
        channel: The Slack channel name.
        space: The Google Chat space name (format: spaces/{space_id}).

    Returns:
        float: Unix timestamp of the last message, or 0 if no messages.
    """
    log_with_context(
        logging.DEBUG,
        f"Finding last message timestamp in space for channel {channel}",
        channel=channel,
    )

    last_message_time: float = 0.0

    try:
        # We only need the most recent message, so limit to 1 result sorted by createTime desc
        request = (
            chat.spaces()
            .messages()
            .list(parent=space, pageSize=1, orderBy="createTime desc")
        )
        response = request.execute()

        messages = response.get("messages", [])
        if messages:
            # Get the first (most recent) message
            message = messages[0]
            create_time = message.get("createTime", "")

            if create_time:
                # Convert RFC3339 time to Unix timestamp
                if "Z" in create_time:
                    dt = datetime.datetime.fromisoformat(
                        create_time.replace("Z", "+00:00")
                    )
                elif "+" in create_time or "-" in create_time[-6:]:
                    dt = datetime.datetime.fromisoformat(create_time)
                else:
                    dt = datetime.datetime.fromisoformat(create_time + "+00:00")

                last_message_time = dt.timestamp()

                log_with_context(
                    logging.INFO,
                    f"Last message in {channel} was at {dt.strftime('%Y-%m-%d %H:%M:%S')}",
                    channel=channel,
                    timestamp=last_message_time,
                )
            else:
                log_with_context(
                    logging.WARNING,
                    f"Found message in {channel} but it has no createTime",
                    channel=channel,
                )
        # Don't log here - the caller will log with more context

    except HttpError as e:
        log_with_context(
            logging.WARNING,
            f"Error getting last message time for channel {channel}: {e}",
            channel=channel,
            error=str(e),
        )

    return last_message_time


def should_process_message(last_timestamp: float, message_ts: str) -> bool:
    """
    Determine if a message should be processed based on its timestamp.

    Args:
        last_timestamp: The Unix timestamp of the last processed message
        message_ts: The Slack timestamp string (e.g., "1609459200.000000")

    Returns:
        bool: True if the message should be processed, False otherwise
    """
    try:
        # Convert Slack timestamp to float
        message_time = float(message_ts.split(".")[0])

        # Compare with last message timestamp
        return message_time > last_timestamp
    except (ValueError, IndexError):
        # If we can't parse the timestamp, process the message to be safe
        return True


def log_space_mapping_conflicts(state: MigrationState, dry_run: bool = False) -> None:
    """Log information about space mapping conflicts that need to be resolved.

    Args:
        state: Migration state containing conflict data.
        dry_run: Whether this is a dry-run execution.
    """
    if dry_run:
        log_with_context(logging.INFO, "[DRY RUN] Checking for space mapping conflicts")

    # Log any conflicts that should be added to config
    if hasattr(state, "channel_conflicts") and state.channel_conflicts:
        log_with_context(
            logging.WARNING,
            f"Found {len(state.channel_conflicts)} channels with duplicate space conflicts",
        )
        log_with_context(
            logging.WARNING,
            "Add the following entries to your config.yaml to resolve conflicts:",
        )
        log_with_context(logging.WARNING, "space_mapping:")
        for channel_name in state.channel_conflicts:
            log_with_context(
                logging.WARNING,
                f'  "{channel_name}": "<space_id>"  # Replace with the desired space ID',
            )


def load_existing_space_mappings(  # noqa: C901
    ctx: MigrationContext, state: MigrationState, chat: Any
) -> None:
    """Load existing space mappings from Google Chat API into migrator state.

    In update mode, discovers spaces via the API and resolves duplicate-space
    conflicts using ``state.space_mapping`` overrides.  In regular import mode
    this is a no-op because we create new spaces rather than reusing existing ones.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service.
    """
    if not ctx.update_mode:
        log_with_context(
            logging.INFO,
            "Import mode: Will create new spaces (not discovering existing spaces)",
        )
        return

    try:
        log_with_context(
            logging.INFO, "[UPDATE MODE] Discovering existing Google Chat spaces"
        )

        discovered_spaces, duplicate_spaces = discover_existing_spaces(
            chat, ctx.channel_name_to_id, state
        )

        # --- Resolve duplicate-space conflicts --------------------------------
        if duplicate_spaces:
            space_mapping = state.space_mapping

            log_with_context(
                logging.WARNING,
                f"Found {len(duplicate_spaces)} channels with duplicate spaces",
            )

            unresolved_conflicts: list[str] = []
            resolved_conflicts: list[str] = []

            for channel_name, spaces in duplicate_spaces.items():
                if space_mapping and channel_name in space_mapping:
                    configured_space_id = space_mapping[channel_name]

                    matching_space = None
                    for space_info in spaces:
                        if space_info["space_id"] == configured_space_id:
                            matching_space = space_info
                            break

                    if matching_space:
                        log_with_context(
                            logging.INFO,
                            f"Using configured space mapping for channel"
                            f" '{channel_name}': {configured_space_id}",
                        )
                        discovered_spaces[channel_name] = matching_space["space_name"]
                        resolved_conflicts.append(channel_name)
                    else:
                        unresolved_conflicts.append(channel_name)
                        state.channel_conflicts.add(channel_name)
                        log_with_context(
                            logging.ERROR,
                            f"Configured space ID for channel '{channel_name}'"
                            f" ({configured_space_id}) doesn't match any"
                            " discovered spaces",
                        )
                else:
                    unresolved_conflicts.append(channel_name)
                    state.channel_conflicts.add(channel_name)
                    log_with_context(
                        logging.ERROR,
                        f"Channel '{channel_name}' has {len(spaces)} duplicate"
                        " spaces and no mapping in config",
                    )
                    log_with_context(
                        logging.ERROR,
                        "Please add a space_mapping entry to config.yaml"
                        " to disambiguate:",
                    )
                    log_with_context(logging.ERROR, "space_mapping:")
                    for space_info in spaces:
                        log_with_context(
                            logging.ERROR,
                            f"  # {space_info['display_name']}"
                            f" (Members: {space_info['member_count']},"
                            f" Created: {space_info['create_time']})",
                        )
                        log_with_context(
                            logging.ERROR,
                            f'  "{channel_name}": "{space_info["space_id"]}"',
                        )

            if unresolved_conflicts:
                for ch in unresolved_conflicts:
                    state.migration_issues[ch] = (
                        "Duplicate spaces found"
                        " - requires disambiguation in config.yaml"
                    )
                log_with_context(
                    logging.ERROR,
                    f"Found unresolved duplicate space conflicts for channels:"
                    f" {', '.join(unresolved_conflicts)}."
                    " These channels will be marked as failed."
                    " Add space_mapping entries to config.yaml to resolve.",
                )

            if resolved_conflicts:
                log_with_context(
                    logging.INFO,
                    f"Successfully resolved space conflicts for channels:"
                    f" {', '.join(resolved_conflicts)}",
                )

        # --- Apply discovered spaces to state ---------------------------------
        if discovered_spaces:
            log_with_context(
                logging.INFO,
                f"Found {len(discovered_spaces)} existing spaces in Google Chat",
            )

            for channel_name, space_name in discovered_spaces.items():
                space_id = (
                    space_name.split("/")[-1]
                    if space_name.startswith("spaces/")
                    else space_name
                )

                mode_info = "[UPDATE MODE] " if ctx.update_mode else ""
                log_with_context(
                    logging.INFO,
                    f"{mode_info}Will use existing space {space_id}"
                    f" for channel '{channel_name}'",
                    channel=channel_name,
                )

            for channel_name, space_name in discovered_spaces.items():
                state.channel_to_space[channel_name] = space_name

                space_id = (
                    space_name.split("/")[-1]
                    if space_name.startswith("spaces/")
                    else space_name
                )

                channel_id = ctx.channel_name_to_id.get(channel_name, "")
                if channel_id:
                    state.channel_id_to_space_id[channel_id] = space_id
                    log_with_context(
                        logging.DEBUG,
                        f"Mapped channel ID {channel_id} to space ID {space_id}",
                    )

                if ctx.update_mode:
                    state.created_spaces[channel_name] = space_name

            log_with_context(
                logging.INFO,
                f"Space discovery complete:"
                f" {len(state.channel_to_space)} channels have"
                " existing spaces, others will create new spaces",
            )
        else:
            log_with_context(logging.INFO, "No existing spaces found in Google Chat")

    except HttpError as e:
        log_with_context(logging.ERROR, f"Failed to load existing space mappings: {e}")
        if not ctx.dry_run:
            raise


def load_space_mappings(
    chat: Any, channel_name_to_id: dict[str, str], state: MigrationState
) -> dict[str, str]:
    """Load space mappings for update mode.

    This uses the Google Chat API for discovery and the config file for overrides.
    No persisted mapping files are used anymore.

    Args:
        chat: Google Chat API service.
        channel_name_to_id: Mapping of channel names to Slack channel IDs.
        state: Mutable migration state.

    Returns:
        Mapping from channel names to space IDs, or empty dict if not found.
    """
    try:
        # Initialize the channel_id_to_space_id mapping if not present
        if not hasattr(state, "channel_id_to_space_id"):
            state.channel_id_to_space_id = {}

        # Use API discovery to find spaces
        discovered_spaces, _duplicate_spaces = discover_existing_spaces(
            chat, channel_name_to_id, state
        )

        # Log the discovery results
        if discovered_spaces:
            log_with_context(
                logging.INFO,
                f"Discovered {len(discovered_spaces)} existing spaces via API",
            )

        # Look for space_mapping overrides in state
        space_mapping = state.space_mapping
        if space_mapping:
            log_with_context(
                logging.INFO,
                f"Found {len(space_mapping)} space mapping overrides in config",
            )

            # Apply space mappings from config (overriding API discovery)
            for channel_name, space_id in space_mapping.items():
                channel_id = channel_name_to_id.get(channel_name, "")
                if channel_id:
                    # Override any discovered mapping with the config value
                    state.channel_id_to_space_id[channel_id] = space_id

                    # Also update the name-based mapping for backward compatibility
                    discovered_spaces[channel_name] = f"spaces/{space_id}"
                else:
                    log_with_context(
                        logging.WARNING,
                        f"Channel '{channel_name}' in space_mapping config not found in workspace",
                    )

        return discovered_spaces if discovered_spaces else {}

    except HttpError as e:
        log_with_context(
            logging.WARNING, f"Failed to load space mappings: {e}", error=str(e)
        )
        return {}
