"""
Shared Drive management for Google Drive integration.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DRIVES_PAGE_SIZE,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
)
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.exceptions import APIError, ConfigError, DriveQuotaError
from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    drive_quota_error,
    http_status,
    is_drive_call_error,
    quota_reason,
)
from slack_chat_migrator.utils.api import (
    escape_drive_query_value,
    is_rate_limit_error,
)
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter


def shared_drives_unavailable(error: BaseException) -> bool:
    """Whether *error* says this account can't use or create shared drives.

    Only a definitive refusal counts: a 403 without a rate-limit or quota
    reason (``drive_adapter.QUOTA_REASONS``), or a 404.  Network errors, credentials
    that can't be refreshed, rate limits, quotas, 5xx and other statuses say
    nothing about shared drives, so they never justify storing attachments
    somewhere else.
    """
    if (
        not isinstance(error, HttpError)
        or is_rate_limit_error(error)
        or quota_reason(error)
    ):
        return False
    return http_status(error) in (
        HTTP_FORBIDDEN,
        HTTP_NOT_FOUND,
    )


#: What the admin may do in a shared drive: add files at all, and (as a
#: Manager) make folders limited-access.
_CAPABILITY_FIELDS = "capabilities(canAddChildren, canManageMembers)"


def _api_error(action: str, error: BaseException) -> APIError | DriveQuotaError:
    """The error for a failed *action*: DriveQuotaError for a used-up quota.

    The adapter raises quota errors as DriveQuotaError itself; this covers a
    Drive service that doesn't.
    """
    return drive_quota_error(error, action) or APIError(f"Could not {action}: {error}")


class SharedDriveManager:
    """Manages Google Drive shared drives for the migration process."""

    def __init__(
        self,
        drive_service: DriveAdapter,
        config: MigrationConfig,
    ) -> None:
        """Initialize the SharedDriveManager.

        Args:
            drive_service: Typed Drive API adapter
            config: Migration configuration
        """
        self.drive_service = drive_service
        self.config = config
        #: Why the last :meth:`get_or_create_shared_drive` found no drive.
        self.last_error: str | None = None

    def get_or_create_shared_drive(self) -> str | None:
        """Get or create the shared drive for storing attachments.

        Returns:
            Shared drive ID, or None when Drive says this account can't use
            or create shared drives (see :func:`shared_drives_unavailable`);
            the reason is then in :attr:`last_error`.

        Raises:
            ConfigError: If ``shared_drive.id`` is configured and Drive
                answers 404, or 403 without a rate-limit or quota reason:
                the ID is wrong or the admin isn't a member.  A new drive is
                never created in its place.
            DriveQuotaError: For a used-up Drive quota, naming it and the
                fix.
            APIError: For any other failure: a network error, credentials
                that can't be refreshed, a rate limit, a 5xx, or another HTTP
                error.  The caller must not fall back to another location
                for these.  Also raised when
                Drive says the admin can't add files to the drive found or
                configured (``capabilities.canAddChildren`` is false).
        """
        self.last_error = None
        # Get shared drive configuration
        shared_drive_name = self.config.shared_drive.name
        shared_drive_id = self.config.shared_drive.id

        # If no shared drive specified, use default name
        if not shared_drive_name and not shared_drive_id:
            shared_drive_name = "Imported Slack Attachments"

        # Use the configured shared drive ID, or stop
        if shared_drive_id:
            try:
                drive_info = self.drive_service.get_drive(
                    drive_id=shared_drive_id, fields=f"id, name, {_CAPABILITY_FIELDS}"
                )
            except DRIVE_CALL_CANDIDATES as e:
                if not is_drive_call_error(e):
                    raise
                if shared_drives_unavailable(e):
                    raise ConfigError(
                        f"The shared drive configured as shared_drive.id"
                        f" ({shared_drive_id}) is not accessible (HTTP"
                        f" {http_status(e)}). Check the ID and that the workspace"
                        " admin is a member of the drive, or remove"
                        " shared_drive.id to use a drive found or created by"
                        " name."
                    ) from e
                raise _api_error(
                    f"open the shared drive configured as shared_drive.id"
                    f" ({shared_drive_id})",
                    e,
                ) from e
            self._check_capabilities(drive_info, shared_drive_id)
            log_with_context(
                logging.INFO,
                f"Using configured shared drive: {drive_info.get('name', 'Unknown')} (ID: {shared_drive_id})",
            )
            drive_id_result: str | None = shared_drive_id
            return drive_id_result

        # Find existing shared drive by name or create new one
        return self._find_or_create_shared_drive(shared_drive_name)

    def _check_capabilities(self, drive: Mapping[str, Any], drive_id: str) -> None:
        """Stop if the admin can't add files to *drive*; warn about restriction.

        Only a capability Drive reports as false counts; a response without
        ``capabilities`` isn't checked (each upload then reports any
        refusal).

        Args:
            drive: The shared drive resource, with ``capabilities``.
            drive_id: Its ID, for the messages.

        Raises:
            APIError: If ``capabilities.canAddChildren`` is false.
        """
        capabilities = drive.get("capabilities")
        if not isinstance(capabilities, Mapping):
            log_with_context(
                logging.DEBUG,
                f"Drive returned no capabilities for shared drive {drive_id};"
                " not checking that the admin can add files to it",
            )
            return
        name = drive.get("name")
        label = f"{name!r} ({drive_id})" if name else drive_id
        if capabilities.get("canAddChildren") is False:
            raise APIError(
                f"The workspace admin can't add files to shared drive {label}."
                " Make them a Manager or Content manager of the drive, or set"
                " shared_drive.id or shared_drive.name to a drive they can add"
                " files to."
            )
        if (
            capabilities.get("canManageMembers") is False
            and self.config.restrict_private_channel_folders
        ):
            log_with_context(
                logging.WARNING,
                f"The workspace admin isn't a Manager of shared drive {label},"
                " and only a Manager can make a folder limited-access: the files"
                " of private channels, group DMs and DMs will fail to upload."
                " Make the admin a Manager, or set"
                " restrict_private_channel_folders: false to accept drive-wide"
                " visibility.",
            )

    def _find_drives_by_name(self, drive_name: str) -> list[dict[str, Any]]:
        """Return every shared drive named *drive_name*, across all pages.

        Args:
            drive_name: Exact drive name.

        Returns:
            Matching drive resources (``id``, ``name`` and ``capabilities``).
        """
        query = f"name = '{escape_drive_query_value(drive_name)}'"
        matches: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            response = self.drive_service.list_drives(
                page_size=DRIVES_PAGE_SIZE,
                page_token=page_token,
                q=query,
                fields=f"nextPageToken, drives(id, name, {_CAPABILITY_FIELDS})",
            )
            matches.extend(
                drive
                for drive in response.get("drives", [])
                if drive.get("name") == drive_name and drive.get("id")
            )
            page_token = response.get("nextPageToken")
            if not page_token:
                return matches

    def _find_or_create_shared_drive(self, drive_name: str) -> str | None:
        """Find an existing shared drive by name or create a new one.

        Every page of ``drives.list`` is searched, so a drive that isn't on
        the first page is still found instead of being created again.

        Args:
            drive_name: Name of the shared drive to find or create

        Returns:
            Shared drive ID, or None if shared drives are unavailable to
            this account (:attr:`last_error` says why)

        Raises:
            APIError: For any other failure (see
                :meth:`get_or_create_shared_drive`).
        """
        try:
            # First, search for existing shared drives with this name
            log_with_context(
                logging.INFO, f"Searching for existing shared drive: {drive_name}"
            )

            matches = self._find_drives_by_name(drive_name)
            if matches:
                found_drive_id: str = matches[0]["id"]
                if len(matches) > 1:
                    log_with_context(
                        logging.WARNING,
                        f"Found {len(matches)} shared drives named {drive_name!r}"
                        f" ({', '.join(d['id'] for d in matches)}); using"
                        f" {found_drive_id}. Set shared_drive.id in config.yaml"
                        " to choose one explicitly.",
                    )
                log_with_context(
                    logging.INFO,
                    f"Found existing shared drive: {drive_name} (ID: {found_drive_id})."
                    " Set shared_drive.id to this ID to pin it for future runs.",
                )
                self._check_capabilities(matches[0], found_drive_id)
                return found_drive_id

            # Create new shared drive if not found
            log_with_context(logging.INFO, f"Creating new shared drive: {drive_name}")

            # Generate a unique request ID for idempotent creation
            request_id = str(uuid.uuid4())

            drive_metadata = {"name": drive_name}

            created_drive = self.drive_service.create_drive(
                body=drive_metadata, request_id=request_id
            )

            created_drive_id: str | None = created_drive.get("id")
            if not created_drive_id:
                raise APIError(
                    f"Drive returned no ID for the new shared drive {drive_name!r}"
                )

            log_with_context(
                logging.INFO,
                f"Successfully created shared drive: {drive_name} (ID: {created_drive_id})."
                " Set shared_drive.id to this ID to pin it for future runs.",
            )

            return created_drive_id

        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            if not shared_drives_unavailable(e):
                raise _api_error(
                    f"find or create shared drive {drive_name!r}", e
                ) from e
            self.last_error = (
                f"this account can't use or create shared drive {drive_name!r}: {e}"
            )
            log_with_context(
                logging.WARNING,
                f"Shared drive {drive_name!r} is unavailable to this account: {e}",
            )
            return None
