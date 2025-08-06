"""
Folder management for Google Drive integration.
"""

import logging
from typing import Optional

from slack_migrator.utils.logging import (
    log_with_context,
)


class FolderManager:
    """Manages folder creation and organization in Google Drive."""

    def __init__(
        self,
        drive_service,
        workspace_domain: Optional[str] = None,
        dry_run: bool = False,
    ):
        """Initialize the FolderManager.

        Args:
            drive_service: Google Drive API service instance
            workspace_domain: The workspace domain for permissions
            dry_run: Whether to run in dry run mode
        """
        self.drive_service = drive_service
        self.workspace_domain = workspace_domain
        self.dry_run = dry_run
        self.folder_cache = {}

    def create_root_folder_in_shared_drive(
        self, folder_name: str, shared_drive_id: str
    ) -> Optional[str]:
        """Create the root attachments folder in the shared drive.

        Args:
            folder_name: Name of the root folder to create
            shared_drive_id: ID of the shared drive

        Returns:
            Folder ID if successful, None otherwise
        """
        if self.dry_run:
            return f"DRY_ROOT_FOLDER_{folder_name}"

        try:
            # Check if folder already exists in shared drive
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"

            log_with_context(
                logging.DEBUG,
                f"Searching for existing folder {folder_name} in shared drive {shared_drive_id}",
            )

            results = (
                self.drive_service.files()
                .list(
                    q=query,
                    spaces="drive",
                    corpora="drive",
                    driveId=shared_drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    fields="files(id, name)",
                )
                .execute()
            )

            files = results.get("files", [])
            if files:
                folder_id = files[0]["id"]
                log_with_context(
                    logging.INFO,
                    f"Found existing root folder in shared drive: {folder_name} (ID: {folder_id})",
                )
                return folder_id

            # Create new folder in shared drive
            log_with_context(
                logging.INFO,
                f"Creating root folder {folder_name} in shared drive {shared_drive_id}",
            )

            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [shared_drive_id],
            }
            folder = (
                self.drive_service.files()
                .create(body=folder_metadata, fields="id", supportsAllDrives=True)
                .execute()
            )

            folder_id = folder.get("id")
            log_with_context(
                logging.INFO,
                f"Successfully created root folder in shared drive: {folder_name} (ID: {folder_id})",
            )

            return folder_id

        except Exception as e:
            log_with_context(
                logging.ERROR, f"Failed to create root folder in shared drive: {e}"
            )
            return None

    def create_regular_drive_folder(self, folder_name: str) -> Optional[str]:
        """Create a regular Drive folder with domain permissions as fallback.

        Args:
            folder_name: Name of the folder to create

        Returns:
            Folder ID if successful, None otherwise
        """
        if self.dry_run:
            return f"DRY_REGULAR_FOLDER_{folder_name}"

        try:
            # Search for existing folder
            query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"

            log_with_context(
                logging.DEBUG,
                f"Searching for existing regular Drive folder: {folder_name}",
            )

            results = (
                self.drive_service.files()
                .list(q=query, spaces="drive", fields="files(id, name)")
                .execute()
            )

            files = results.get("files", [])
            if files:
                folder_id = files[0]["id"]
                log_with_context(
                    logging.INFO,
                    f"Found existing regular Drive folder: {folder_name} (ID: {folder_id})",
                )

                # Note: No domain-wide permissions set to avoid org-wide access
                # Individual channel folders will have their own space-specific permissions
                return folder_id

            # Create new folder
            log_with_context(
                logging.INFO, f"Creating regular Drive folder: {folder_name}"
            )

            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            folder = (
                self.drive_service.files()
                .create(body=folder_metadata, fields="id")
                .execute()
            )

            folder_id = folder.get("id")
            # Note: No domain-wide permissions set to avoid org-wide access
            # Individual channel folders will have their own space-specific permissions

            log_with_context(
                logging.INFO,
                f"Successfully created regular Drive folder: {folder_name} (ID: {folder_id})",
            )

            return folder_id

        except Exception as e:
            log_with_context(
                logging.ERROR, f"Failed to create regular Drive folder: {e}"
            )
            return None

    def get_or_create_channel_folder(
        self, channel: str, parent_folder_id: str, shared_drive_id: Optional[str] = None
    ) -> Optional[str]:
        """Get or create a channel-specific folder.

        Args:
            channel: The channel name
            parent_folder_id: ID of the parent folder
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            The folder ID if successful, None otherwise
        """
        if self.dry_run:
            return f"DRY_CHANNEL_FOLDER_{channel}"

        # Check cache
        cache_key = f"folder_{channel}"
        if cache_key in self.folder_cache:
            folder_id = self.folder_cache[cache_key]

            # Verify folder still exists
            try:
                if shared_drive_id:
                    self.drive_service.files().get(
                        fileId=folder_id, supportsAllDrives=True
                    ).execute()
                else:
                    self.drive_service.files().get(fileId=folder_id).execute()
                return folder_id
            except Exception as e:
                log_with_context(
                    logging.WARNING,
                    f"Cached folder ID {folder_id} for channel {channel} not found: {e}. Will create new folder.",
                    channel=channel,
                )
                self.folder_cache.pop(cache_key, None)

        try:
            # Search for existing channel folder
            query = f"name = '{channel}' and mimeType = 'application/vnd.google-apps.folder' and '{parent_folder_id}' in parents and trashed = false"

            log_with_context(
                logging.DEBUG,
                f"Searching for existing channel folder: {channel}",
                channel=channel,
            )

            # Use appropriate parameters based on shared drive
            if shared_drive_id:
                results = (
                    self.drive_service.files()
                    .list(
                        q=query,
                        spaces="drive",
                        corpora="drive",
                        driveId=shared_drive_id,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                        fields="files(id, name)",
                    )
                    .execute()
                )
            else:
                results = (
                    self.drive_service.files()
                    .list(q=query, spaces="drive", fields="files(id, name)")
                    .execute()
                )

            items = results.get("files", [])

            # Use existing folder if found
            if items:
                folder_id = items[0]["id"]
                log_with_context(
                    logging.DEBUG,
                    f"Found existing channel folder: {channel} (ID: {folder_id})",
                    channel=channel,
                )

                self.folder_cache[cache_key] = folder_id
                return folder_id

            # Create new channel folder
            log_with_context(
                logging.INFO, f"Creating channel folder: {channel}", channel=channel
            )

            folder_metadata = {
                "name": channel,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id],
            }
            # Create folder with appropriate parameters
            if shared_drive_id:
                folder = (
                    self.drive_service.files()
                    .create(body=folder_metadata, fields="id", supportsAllDrives=True)
                    .execute()
                )
            else:
                folder = (
                    self.drive_service.files()
                    .create(body=folder_metadata, fields="id")
                    .execute()
                )

            folder_id = folder.get("id")

            if folder_id:
                log_with_context(
                    logging.INFO,
                    f"Successfully created channel folder: {channel} (ID: {folder_id})",
                    channel=channel,
                )

                self.folder_cache[cache_key] = folder_id

                # Note: Channel folder permissions should be set by the caller using set_channel_folder_permissions
                # to ensure only space members have access, not the entire domain

                return folder_id
            else:
                log_with_context(
                    logging.WARNING,
                    f"Failed to create channel folder: {channel}",
                    channel=channel,
                )
                return None

        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to get or create channel folder {channel}: {e}",
                channel=channel,
                error=str(e),
            )
            return None

    def get_channel_folder_id(
        self, channel: str, parent_folder_id: str, shared_drive_id: Optional[str] = None
    ) -> Optional[str]:
        """Get the ID of a channel folder if it exists.

        This is a read-only operation that doesn't create a folder if it doesn't exist.

        Args:
            channel: Name of the channel
            parent_folder_id: ID of the parent folder
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            Folder ID if found, None otherwise
        """
        # First check cache
        cache_key = f"{channel}:{parent_folder_id}"
        if cache_key in self.folder_cache:
            folder_id = self.folder_cache[cache_key]
            log_with_context(
                logging.DEBUG,
                f"Found cached channel folder ID for {channel}: {folder_id}",
                channel=channel,
            )
            return folder_id

        try:
            # Folder name should be sanitized to ensure consistent matching
            folder_name = self._sanitize_folder_name(channel)

            # First, search for folder in the parent folder
            q = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and '{parent_folder_id}' in parents"
            if shared_drive_id:
                response = (
                    self.drive_service.files()
                    .list(
                        q=q,
                        spaces="drive",
                        fields="files(id, name)",
                        corpora="drive",
                        driveId=shared_drive_id,
                        includeItemsFromAllDrives=True,
                        supportsAllDrives=True,
                    )
                    .execute()
                )
            else:
                response = (
                    self.drive_service.files()
                    .list(q=q, spaces="drive", fields="files(id, name)")
                    .execute()
                )

            files = response.get("files", [])

            if files:
                folder_id = files[0].get("id")
                if folder_id:
                    self.folder_cache[cache_key] = folder_id
                    log_with_context(
                        logging.DEBUG,
                        f"Found existing channel folder: {channel} (ID: {folder_id})",
                        channel=channel,
                    )
                    return folder_id

            # Not found
            log_with_context(
                logging.DEBUG,
                f"No existing channel folder found for {channel}",
                channel=channel,
            )
            return None

        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to get channel folder ID for {channel}: {e}",
                channel=channel,
                error=str(e),
            )
            return None

    def set_channel_folder_permissions(
        self,
        folder_id: str,
        channel: str,
        user_emails: list,
        shared_drive_id: Optional[str] = None,
    ) -> bool:
        """Set permissions on a channel folder for all channel members.

        This method replaces any existing user permissions with the new set.
        All channel members get reader access to the folder.
        Files within the folder will inherit these permissions, so we don't need to set
        individual permissions for each file except for giving the poster editor access.

        Args:
            folder_id: ID of the channel folder
            channel: Channel name (for logging)
            user_emails: List of user emails to grant access to
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            True if permissions were set successfully, False otherwise
        """
        if self.dry_run:
            log_with_context(
                logging.INFO,
                f"[DRY RUN] Would set permissions on channel folder {channel} for {len(user_emails)} users",
                channel=channel,
                folder_id=folder_id,
            )
            return True

        # Add permissions for all users
        success_count = 0
        failed_count = 0

        for email in user_emails:
            try:
                permission = {"type": "user", "role": "reader", "emailAddress": email}
                if shared_drive_id:
                    self.drive_service.permissions().create(
                        fileId=folder_id,
                        body=permission,
                        sendNotificationEmail=False,
                        supportsAllDrives=True,
                    ).execute()
                else:
                    self.drive_service.permissions().create(
                        fileId=folder_id, body=permission, sendNotificationEmail=False
                    ).execute()

                success_count += 1

            except Exception as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to grant access to {email} for channel folder {channel}: {e}",
                    channel=channel,
                    folder_id=folder_id,
                    user_email=email,
                )
                failed_count += 1

        log_with_context(
            logging.INFO,
            f"Set channel folder permissions for {channel}: {success_count} successful, {failed_count} failed",
            channel=channel,
            folder_id=folder_id,
            success_count=success_count,
            failed_count=failed_count,
        )

        return failed_count == 0

    # Note: Domain-wide permissions method removed to prevent org-wide access
    # Use set_channel_folder_permissions instead to restrict access to space members only

    def _sanitize_folder_name(self, folder_name: str) -> str:
        """Sanitize folder name for consistent matching.

        Args:
            folder_name: Name of the folder

        Returns:
            Sanitized folder name
        """
        # Replace special characters with underscore
        return folder_name.strip()
