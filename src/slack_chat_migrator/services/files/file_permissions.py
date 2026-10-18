"""File permission management for Drive uploads."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter

from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    PermissionChange,
    is_drive_call_error,
)
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
        True if successful; False if Drive refused it or the call failed
        (an HttpError, a network error or credentials that can't be
        refreshed, logged here).
    """
    try:
        permission = {
            "type": "user",
            "role": "owner",
            "emailAddress": new_owner_email,
        }
        # Drive emails the new owner; that can't be turned off for transfers.
        drive_service.create_permission(
            file_id=file_id,
            body=permission,
            transfer_ownership=True,
        )

        log_with_context(
            logging.DEBUG,
            f"Transferred ownership of file {file_id} to {new_owner_email}",
        )

        return True

    except DRIVE_CALL_CANDIDATES as e:
        if not is_drive_call_error(e):
            raise
        log_with_context(
            logging.WARNING,
            f"Failed to transfer ownership of {file_id} to {new_owner_email}: {e}",
        )
        return False
