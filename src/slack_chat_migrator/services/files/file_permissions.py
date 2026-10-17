"""File permission management for Drive uploads."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter

from googleapiclient.errors import HttpError

from slack_chat_migrator.services.drive_adapter import PermissionChange
from slack_chat_migrator.utils.logging import log_with_context

logger = logging.getLogger("slack_chat_migrator")


def ownership_transfer_change(file_id: str, new_owner_email: str) -> PermissionChange:
    """Describe an ownership transfer, for batching with other grants.

    Args:
        file_id: ID of the file to transfer.
        new_owner_email: Email of the new owner.

    Returns:
        The PermissionChange to pass to ``DriveAdapter.apply_permission_changes``.
    """
    return PermissionChange(
        file_id,
        body={"type": "user", "role": "owner", "emailAddress": new_owner_email},
        transfer_ownership=True,
    )


def transfer_file_ownership(
    drive_service: DriveAdapter,
    file_id: str,
    new_owner_email: str,
) -> bool:
    """Transfer ownership of a file to a new owner.

    Args:
        drive_service: Typed Drive API adapter.
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
        drive_service.create_permission(
            file_id=file_id,
            body=permission,
            transfer_ownership=True,
            send_notification_email=False,
        )

        log_with_context(
            logging.DEBUG,
            f"Transferred ownership of file {file_id} to {new_owner_email}",
        )

        return True

    except HttpError as e:
        log_with_context(logging.WARNING, f"Failed to transfer file ownership: {e}")
        return False
