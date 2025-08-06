"""
Shared Drive management for Google Drive integration.
"""

import logging
import uuid
from typing import Optional

from slack_migrator.utils.logging import (
    log_with_context,
)


class SharedDriveManager:
    """Manages Google Drive shared drives for the migration process."""

    def __init__(self, drive_service, config: dict, dry_run: bool = False):
        """Initialize the SharedDriveManager.

        Args:
            drive_service: Google Drive API service instance
            config: Configuration dictionary
            dry_run: Whether to run in dry run mode
        """
        self.drive_service = drive_service
        self.config = config
        self.dry_run = dry_run

    def validate_shared_drive(self, shared_drive_id: str) -> bool:
        """Validate that a shared drive exists and is accessible.

        Args:
            shared_drive_id: ID of the shared drive to validate

        Returns:
            True if valid and accessible, False otherwise
        """
        if self.dry_run:
            return True

        try:
            self.drive_service.drives().get(driveId=shared_drive_id).execute()
            return True
        except Exception as e:
            log_with_context(
                logging.WARNING, f"Shared drive {shared_drive_id} not accessible: {e}"
            )
            return False

    def get_or_create_shared_drive(self) -> Optional[str]:
        """Get or create the shared drive for storing attachments.

        Returns:
            Shared drive ID if successful, None otherwise
        """
        if self.dry_run:
            return "DRY_RUN_SHARED_DRIVE"

        try:
            # Get shared drive configuration
            shared_drive_config = self.config.get("shared_drive", {})
            shared_drive_name = shared_drive_config.get("name")
            shared_drive_id = shared_drive_config.get("id")

            # If no shared drive specified, use default name
            if not shared_drive_name and not shared_drive_id:
                shared_drive_name = "Imported Slack Attachments"

            # Try to use specified shared drive ID
            if shared_drive_id:
                try:
                    drive_info = (
                        self.drive_service.drives()
                        .get(driveId=shared_drive_id)
                        .execute()
                    )
                    log_with_context(
                        logging.INFO,
                        f"Using configured shared drive: {drive_info.get('name', 'Unknown')} (ID: {shared_drive_id})",
                    )
                    return shared_drive_id
                except Exception as e:
                    log_with_context(
                        logging.ERROR,
                        f"Configured shared drive ID {shared_drive_id} not accessible: {e}. Will create new one.",
                    )

            # Find existing shared drive by name or create new one
            if shared_drive_name:
                return self._find_or_create_shared_drive(shared_drive_name)

            return None

        except Exception as e:
            log_with_context(
                logging.ERROR, f"Failed to get or create shared drive: {e}"
            )
            return None

    def _find_or_create_shared_drive(self, drive_name: str) -> Optional[str]:
        """Find an existing shared drive by name or create a new one.

        Args:
            drive_name: Name of the shared drive to find or create

        Returns:
            Shared drive ID if successful, None otherwise
        """
        try:
            # First, search for existing shared drives with this name
            log_with_context(
                logging.INFO, f"Searching for existing shared drive: {drive_name}"
            )

            drives_list = self.drive_service.drives().list().execute()
            drives = drives_list.get("drives", [])

            for drive in drives:
                if drive.get("name") == drive_name:
                    drive_id = drive.get("id")
                    log_with_context(
                        logging.INFO,
                        f"Found existing shared drive: {drive_name} (ID: {drive_id})",
                    )
                    return drive_id

            # Create new shared drive if not found
            log_with_context(logging.INFO, f"Creating new shared drive: {drive_name}")

            # Generate a unique request ID for idempotent creation
            request_id = str(uuid.uuid4())

            drive_metadata = {"name": drive_name}

            created_drive = (
                self.drive_service.drives()
                .create(body=drive_metadata, requestId=request_id)
                .execute()
            )

            drive_id = created_drive.get("id")

            log_with_context(
                logging.INFO,
                f"Successfully created shared drive: {drive_name} (ID: {drive_id})",
            )

            return drive_id

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to find or create shared drive {drive_name}: {e}",
            )
            return None
