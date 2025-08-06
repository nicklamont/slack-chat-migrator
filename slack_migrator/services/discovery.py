"""
Functions for discovering existing Google Chat resources for migration resumption
"""

import logging
import time

from googleapiclient.errors import HttpError

from slack_migrator.utils.logging import log_with_context


def discover_existing_spaces(migrator):
    """
    Query Google Chat API to find spaces that match our Slack channel naming pattern.

    This function searches for spaces that appear to have been created by this migration tool
    by looking for spaces with names matching the pattern "Slack #<channel-name>".

    If multiple spaces have the same channel name, this will detect the conflict and
    report the conflicting spaces to help users disambiguate in the config.

    Args:
        migrator: The SlackToChatMigrator instance

    Returns:
        tuple: (space_mappings, duplicate_spaces)
            - space_mappings: Dict mapping channel names to Google Chat space names
            - duplicate_spaces: Dict mapping channel names to lists of conflicting space information
    """
    log_with_context(
        logging.INFO,
        "Discovering existing Google Chat spaces that may have been created by previous migrations",
        channel=None,  # This is a global operation, not channel-specific
    )

    # Track all spaces by channel name to detect duplicates
    all_spaces_by_channel = {}
    space_mappings = {}
    duplicate_spaces = {}

    # Initialize the channel_id_to_space_id mapping if it doesn't exist
    if not hasattr(migrator, "channel_id_to_space_id"):
        migrator.channel_id_to_space_id = {}

    spaces_found = 0
    prefix = "Slack #"  # The prefix we use for migrated spaces

    try:
        # Paginate through all spaces accessible to the service account
        page_token = None
        while True:
            request = migrator.chat.spaces().list(pageSize=100, pageToken=page_token)
            response = request.execute()

            # Process each space
            for space in response.get("spaces", []):
                display_name = space.get("displayName", "")
                space_name = space.get(
                    "name", ""
                )  # This is the format "spaces/{space_id}"
                space_id = space_name.split("/")[-1] if space_name else ""
                space_type = space.get("spaceType", "")

                # Check if this is a space created by our migration tool
                if display_name and display_name.startswith(prefix):
                    # Extract the channel name from the display name
                    channel_name = display_name[len(prefix) :].strip()

                    if channel_name:
                        # Gather additional metadata for potential disambiguation
                        space_info = {
                            "display_name": display_name,
                            "space_name": space_name,
                            "space_id": space_id,
                            "space_type": space_type,
                            "member_count": 0,  # Will be populated later if needed
                            "create_time": space.get("createTime", "Unknown"),
                        }

                        # Track all spaces for this channel name
                        if channel_name not in all_spaces_by_channel:
                            all_spaces_by_channel[channel_name] = []

                        all_spaces_by_channel[channel_name].append(space_info)
                        spaces_found += 1

                        # Get channel ID directly from our mapping
                        channel_id = migrator.channel_name_to_id.get(channel_name, "")
                        if channel_id:
                            # Associate the space ID with this channel ID
                            # (only for first occurrence - duplicates will be handled later)
                            if channel_id not in migrator.channel_id_to_space_id:
                                migrator.channel_id_to_space_id[channel_id] = space_id

            # Get the next page token
            page_token = response.get("nextPageToken")
            if not page_token:
                break

            # Add a small delay to avoid rate limiting
            time.sleep(0.2)

        # Now process the collected spaces and identify duplicates
        for channel_name, spaces in all_spaces_by_channel.items():
            if len(spaces) == 1:
                # No duplicates, just map the channel to the space
                space_mappings[channel_name] = spaces[0]["space_name"]

                # Try to create an ID-based mapping as well
                channel_id = migrator.channel_name_to_id.get(channel_name, "")
                if channel_id:
                    # This will overwrite any previous entry
                    migrator.channel_id_to_space_id[channel_id] = spaces[0]["space_id"]
            else:
                # Multiple spaces with the same channel name
                # Store the first one by default, but also track the conflict
                space_mappings[channel_name] = spaces[0]["space_name"]
                duplicate_spaces[channel_name] = spaces

                # Get more metadata for each space to help with disambiguation
                for space_info in spaces:
                    try:
                        # Try to get member count for each duplicate space
                        members_response = (
                            migrator.chat.spaces()
                            .members()
                            .list(
                                parent=space_info["space_name"],
                                pageSize=1,  # Just need the count, not the actual members
                            )
                            .execute()
                        )

                        # Store the member count if available
                        if "memberships" in members_response:
                            space_info["member_count"] = len(
                                members_response.get("memberships", [])
                            )

                            # If we need more members, we could paginate here
                            if "nextPageToken" in members_response:
                                space_info["member_count"] = (
                                    str(space_info["member_count"]) + "+"
                                )
                    except Exception as e:
                        # Just log the error and continue
                        log_with_context(
                            logging.DEBUG,
                            f"Error fetching members for space {space_info['space_name']}: {e}",
                        )

                # For channels with duplicate spaces, remove any ID-based mappings
                # until the user disambiguates via space_mapping config
                channel_id = migrator.channel_name_to_id.get(channel_name, "")
                if channel_id and channel_id in migrator.channel_id_to_space_id:
                    log_with_context(
                        logging.WARNING,
                        f"Removing ambiguous ID mapping for channel {channel_name} (ID: {channel_id})",
                    )
                    del migrator.channel_id_to_space_id[channel_id]

        # Log duplicate spaces
        if duplicate_spaces:
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
                        f"  Space {i+1}: {space_info['display_name']} (ID: {space_info['space_id']}, "
                        f"Type: {space_info['space_type']}, Members: {space_info['member_count']}, "
                        f"Created: {space_info['create_time']})",
                    )

    except HttpError as e:
        log_with_context(
            logging.WARNING,
            f"Error discovering spaces: {e}",
            error=str(e),
            channel=None,  # Global operation
        )

    log_with_context(
        logging.INFO,
        f"Found {spaces_found} existing spaces matching migration pattern across {len(all_spaces_by_channel)} channels",
        channel=None,  # Global operation
    )

    if hasattr(migrator, "channel_id_to_space_id"):
        log_with_context(
            logging.INFO,
            f"Created {len(migrator.channel_id_to_space_id)} channel ID to space ID mappings",
            channel=None,  # Global operation
        )

    return space_mappings, duplicate_spaces


def get_last_message_timestamp(migrator, channel: str, space: str):
    """
    Query Google Chat API to get the timestamp of the last message in a space.

    This helps determine where to resume migration - we'll only import messages
    that are newer than the most recent message in the space.

    Args:
        migrator: The SlackToChatMigrator instance
        channel: The Slack channel name
        space: The Google Chat space name (format: spaces/{space_id})

    Returns:
        float: Unix timestamp of the last message, or 0 if no messages
    """
    log_with_context(
        logging.DEBUG,
        f"Finding last message timestamp in space for channel {channel}",
        channel=channel,
    )

    last_message_time = 0

    try:
        # We only need the most recent message, so limit to 1 result sorted by createTime desc
        request = (
            migrator.chat.spaces()
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
                import datetime

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
