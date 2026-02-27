"""File permission management for Drive uploads."""

from __future__ import annotations

import logging
from typing import Any

from googleapiclient.errors import HttpError

from slack_migrator.utils.logging import log_with_context

logger = logging.getLogger("slack_migrator")


def transfer_file_ownership(
    drive_service: Any,
    file_id: str,
    new_owner_email: str,
) -> bool:
    """Transfer ownership of a file to a new owner.

    Args:
        drive_service: The Google Drive API service instance.
        file_id: ID of the file to transfer.
        new_owner_email: Email of the new owner.

    Returns:
        True if successful, False otherwise.
    """
    try:
        permission = {
            "type": "user",
            "role": "owner",
            "emailAddress": new_owner_email,
        }
        drive_service.permissions().create(
            fileId=file_id,
            body=permission,
            transferOwnership=True,
            sendNotificationEmail=False,
        ).execute()

        log_with_context(
            logging.DEBUG,
            f"Transferred ownership of file {file_id} to {new_owner_email}",
        )

        return True

    except HttpError as e:
        log_with_context(logging.WARNING, f"Failed to transfer file ownership: {e}")
        return False


def share_file_with_members(
    drive_service: Any,
    drive_file_id: str,
    channel: str,
    shared_drive_id: str | None,
    active_users_by_channel: dict[str, set[str]],
    user_map: dict[str, str],
) -> bool:
    """Share a Drive file with all active members of a channel.

    If the file is already in a shared folder with proper permissions,
    this function will skip setting individual permissions.

    Args:
        drive_service: The Google Drive API service instance.
        drive_file_id: The ID of the Drive file to share.
        channel: The channel name to get members from.
        shared_drive_id: The shared drive ID, or None if not using shared drives.
        active_users_by_channel: Mapping of channel names to sets of active user IDs.
        user_map: Mapping of Slack user IDs to Google email addresses.

    Returns:
        True if sharing was successful, False otherwise.
    """
    try:
        # First check if this file is already in a shared folder
        if shared_drive_id:
            # For shared drives, check if file is in the shared drive
            file_info = (
                drive_service.files()
                .get(fileId=drive_file_id, fields="parents", supportsAllDrives=True)
                .execute()
            )
        else:
            file_info = (
                drive_service.files()
                .get(fileId=drive_file_id, fields="parents")
                .execute()
            )

        parent_folders = file_info.get("parents", [])

        # Check if any of the parent folders is our channel folder or shared drive
        for parent_id in parent_folders:
            try:
                if shared_drive_id:
                    folder_info = (
                        drive_service.files()
                        .get(fileId=parent_id, fields="name", supportsAllDrives=True)
                        .execute()
                    )
                else:
                    folder_info = (
                        drive_service.files()
                        .get(fileId=parent_id, fields="name")
                        .execute()
                    )

                folder_name = folder_info.get("name", "")

                # If this is our channel folder or shared drive, we don't need
                # to set individual permissions
                if folder_name == channel or parent_id == shared_drive_id:
                    log_with_context(
                        logging.DEBUG,
                        f"File {drive_file_id} is already in shared folder for channel {channel}, skipping individual permissions",
                        channel=channel,
                        file_id=drive_file_id,
                    )
                    return True
            except HttpError:
                logger.debug("Failed to get folder info, continuing", exc_info=True)
                continue

        # If we're here, the file is not in a channel folder, so we need to set
        # individual permissions
        if channel not in active_users_by_channel:
            log_with_context(
                logging.WARNING,
                f"No active users tracked for channel {channel}, can't share file",
                channel=channel,
            )
            return False

        active_users = active_users_by_channel[channel]
        emails_to_share = []

        # Get emails for all active users (INCLUDING external users for channel
        # folder access)
        for user_id in active_users:
            email = user_map.get(user_id)
            if email:
                emails_to_share.append(
                    email
                )  # Include ALL users, both internal and external

        if not emails_to_share:
            log_with_context(
                logging.WARNING,
                f"No valid emails found for active users in channel {channel}",
                channel=channel,
            )
            return False

        # Share the file with each user
        log_with_context(
            logging.DEBUG,
            f"Sharing Drive file {drive_file_id} with {len(emails_to_share)} users",
            channel=channel,
        )

        for email in emails_to_share:
            try:
                # Create a permission for the user
                permission = {
                    "type": "user",
                    "role": "reader",
                    "emailAddress": email,
                }
                if shared_drive_id:
                    drive_service.permissions().create(
                        fileId=drive_file_id,
                        body=permission,
                        sendNotificationEmail=False,
                        supportsAllDrives=True,
                    ).execute()
                else:
                    drive_service.permissions().create(
                        fileId=drive_file_id,
                        body=permission,
                        sendNotificationEmail=False,
                    ).execute()

            except HttpError as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to share file with {email}: {e}",
                    channel=channel,
                    file_id=drive_file_id,
                )

        log_with_context(
            logging.DEBUG,
            "Successfully shared Drive file with channel members",
            channel=channel,
            file_id=drive_file_id,
        )
        return True

    except HttpError as e:
        log_with_context(
            logging.ERROR,
            f"Failed to share Drive file: {e}",
            channel=channel,
            file_id=drive_file_id,
        )
        # Re-raise to trigger retry
        raise
