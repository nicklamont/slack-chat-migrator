"""
Shared Drive management for Google Drive integration.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING

from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DRIVES_PAGE_SIZE,
    HTTP_FORBIDDEN,
    HTTP_NOT_FOUND,
)
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.exceptions import APIError, ConfigError
from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    http_status,
    is_drive_call_error,
)
from slack_chat_migrator.utils.api import escape_drive_query_value, is_rate_limit_error
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter


def shared_drives_unavailable(error: BaseException) -> bool:
    """Whether *error* says this account can't use or create shared drives.

    Only a definitive refusal counts: a 403 that isn't a rate limit, or a
    404.  Network errors, credentials that can't be refreshed, 5xx and
    other statuses say nothing about shared drives, so they never justify
    storing attachments somewhere else.
    """
    if not isinstance(error, HttpError) or is_rate_limit_error(error):
        return False
    return http_status(error) in (
        HTTP_FORBIDDEN,
        HTTP_NOT_FOUND,
    )


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
                refuses to open it.  A new drive is never created in its
                place.
            APIError: If Drive couldn't be asked: a network error,
                credentials that can't be refreshed, or an HTTP error that
                doesn't mean shared drives are unavailable.  The caller must
                not fall back to another location for these.
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
                drive_info = self.drive_service.get_drive(drive_id=shared_drive_id)
            except HttpError as e:
                raise ConfigError(
                    f"The shared drive configured as shared_drive.id"
                    f" ({shared_drive_id}) is not accessible (HTTP {http_status(e)})."
                    " Check the ID and that the workspace admin is a member of"
                    " the drive, or remove shared_drive.id to use a drive found"
                    " or created by name."
                ) from e
            except DRIVE_CALL_CANDIDATES as e:
                if not is_drive_call_error(e):
                    raise
                raise APIError(
                    f"Could not open the shared drive configured as"
                    f" shared_drive.id ({shared_drive_id}): {e}"
                ) from e
            log_with_context(
                logging.INFO,
                f"Using configured shared drive: {drive_info.get('name', 'Unknown')} (ID: {shared_drive_id})",
            )
            drive_id_result: str | None = shared_drive_id
            return drive_id_result

        # Find existing shared drive by name or create new one
        return self._find_or_create_shared_drive(shared_drive_name)

    def _find_drives_by_name(self, drive_name: str) -> list[dict[str, str]]:
        """Return every shared drive named *drive_name*, across all pages.

        Args:
            drive_name: Exact drive name.

        Returns:
            Matching drive resources (``id`` and ``name``).
        """
        query = f"name = '{escape_drive_query_value(drive_name)}'"
        matches: list[dict[str, str]] = []
        page_token: str | None = None
        while True:
            response = self.drive_service.list_drives(
                page_size=DRIVES_PAGE_SIZE,
                page_token=page_token,
                q=query,
                fields="nextPageToken, drives(id, name)",
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
                found_drive_id: str | None = matches[0]["id"]
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
                raise APIError(
                    f"Could not find or create shared drive {drive_name!r}: {e}"
                ) from e
            self.last_error = (
                f"this account can't use or create shared drive {drive_name!r}: {e}"
            )
            log_with_context(
                logging.WARNING,
                f"Shared drive {drive_name!r} is unavailable to this account: {e}",
            )
            return None
