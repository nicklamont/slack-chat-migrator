"""
Google Drive file upload functionality.
"""

import hashlib
import logging
import mimetypes
from typing import Any, Dict, Optional, Tuple

# Third-party imports
# pylint: disable=import-error
from googleapiclient.http import MediaFileUpload

from slack_migrator.utils.logging import (
    log_with_context,
)


class DriveFileUploader:
    """Handles file uploads to Google Drive."""

    def __init__(
        self,
        drive_service,
        workspace_domain: Optional[str] = None,
        dry_run: bool = False,
        service_account_email: Optional[str] = None,
    ):
        """Initialize the DriveFileUploader.

        Args:
            drive_service: Google Drive API service instance
            workspace_domain: The workspace domain for permissions
            dry_run: Whether to run in dry run mode
            service_account_email: The email of the service account to grant access to
        """
        self.drive_service = drive_service
        self.workspace_domain = workspace_domain
        self.dry_run = dry_run
        self.service_account_email = service_account_email
        self.file_hash_cache = {}
        self.folders_pre_cached = set()
        self.migrator = None  # Will be set by the FileHandler when it's created

    def _get_current_channel(self):
        """Helper method to get the current channel from the migrator.

        Returns:
            Current channel name or None if not available
        """
        if (
            hasattr(self, "migrator")
            and self.migrator is not None
            and hasattr(self.migrator, "current_channel")
        ):
            return self.migrator.current_channel
        return None

    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of a file.

        Args:
            file_path: Path to the file

        Returns:
            MD5 hash of the file as a hexadecimal string
        """
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def pre_cache_folder_file_hashes(
        self, folder_id: str, shared_drive_id: Optional[str] = None
    ) -> int:
        """Pre-cache MD5 hashes of all files in a folder.

        This method queries all files in the specified folder and caches their
        MD5 hashes to avoid duplicate uploads later. This is especially useful
        for large migrations with many potential duplicate files.

        Args:
            folder_id: ID of the folder to cache files from
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            Number of files cached
        """
        # Skip if already pre-cached this folder
        if folder_id in self.folders_pre_cached:
            log_with_context(
                logging.DEBUG,
                f"Folder {folder_id} already pre-cached, skipping",
                channel=self._get_current_channel(),
            )
            return 0

        if self.dry_run:
            log_with_context(
                logging.INFO,
                f"[DRY RUN] Would pre-cache file hashes from folder {folder_id}",
                channel=self._get_current_channel(),
            )
            return 0

        try:
            log_with_context(
                logging.DEBUG,
                f"Pre-caching file hashes from folder {folder_id}",
                channel=self._get_current_channel(),
            )

            query = f"'{folder_id}' in parents and trashed=false"
            page_token = None
            files_cached = 0

            while True:
                # Build request parameters
                params: Dict[str, Any] = {
                    "q": query,
                    "fields": "nextPageToken, files(id, name, md5Checksum, webViewLink)",
                    "pageSize": 1000,  # Maximum allowed page size
                }

                if page_token:
                    params["pageToken"] = page_token

                # Add shared drive parameters if applicable
                if shared_drive_id:
                    params.update(
                        {
                            "spaces": "drive",
                            "corpora": "drive",
                            "driveId": shared_drive_id,
                            "includeItemsFromAllDrives": True,
                            "supportsAllDrives": True,
                        }
                    )

                response = self.drive_service.files().list(**params).execute()
                files = response.get("files", [])

                for file in files:
                    file_id = file.get("id")
                    file_hash = file.get("md5Checksum")
                    web_view_link = file.get("webViewLink")

                    if file_hash and file_id and web_view_link:
                        self.file_hash_cache[file_hash] = (file_id, web_view_link)
                        files_cached += 1

                page_token = response.get("nextPageToken")
                if not page_token:
                    break

            # Mark this folder as pre-cached
            self.folders_pre_cached.add(folder_id)

            log_with_context(
                logging.DEBUG,
                f"Successfully pre-cached {files_cached} files from folder {folder_id}",
            )
            return files_cached

        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to pre-cache file hashes from folder {folder_id}: {e}",
            )
            return 0

    def _find_file_by_hash(
        self,
        file_hash: str,
        filename: str,
        folder_id: str,
        shared_drive_id: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Find a file in Drive by its MD5 hash.

        Args:
            file_hash: MD5 hash of the file
            filename: Name of the file (for logging)
            folder_id: ID of the folder to search in
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            Tuple of (file_id, webViewLink) if found, (None, None) otherwise
        """
        try:
            # Check if we've seen this hash before in our cache
            if file_hash in self.file_hash_cache:
                cached_id, cached_url = self.file_hash_cache[file_hash]

                log_with_context(
                    logging.DEBUG,
                    f"Found cached file ID for hash {file_hash}: {cached_id}",
                    channel=self._get_current_channel(),
                )

                # Verify the file still exists
                try:
                    params: Dict[str, Any] = {
                        "fileId": cached_id,
                        "fields": "id,webViewLink",
                    }
                    if shared_drive_id:
                        params["supportsAllDrives"] = True

                    file = self.drive_service.files().get(**params).execute()
                    return cached_id, file.get("webViewLink")
                except Exception:
                    # File might have been deleted, continue with search
                    self.file_hash_cache.pop(file_hash, None)

            # Search for files with matching MD5 hash in the folder
            query = f"md5Checksum='{file_hash}' and trashed=false"

            # Add folder constraint if specified
            if folder_id:
                query += f" and '{folder_id}' in parents"

            log_with_context(
                logging.DEBUG,
                f"Searching for files with hash {file_hash} using query: {query}",
                channel=self._get_current_channel(),
            )

            # Build request parameters
            params: Dict[str, Any] = {
                "q": query,
                "fields": "files(id,name,webViewLink)",
            }

            # Add shared drive parameters if applicable
            if shared_drive_id:
                params.update(
                    {
                        "spaces": "drive",
                        "corpora": "drive",
                        "driveId": shared_drive_id,
                        "includeItemsFromAllDrives": True,
                        "supportsAllDrives": True,
                    }
                )

            response = self.drive_service.files().list(**params).execute()

            files = response.get("files", [])
            if files:
                file_id = files[0].get("id")
                web_view_link = files[0].get("webViewLink")

                # Cache the result for future lookups
                self.file_hash_cache[file_hash] = (file_id, web_view_link)

                log_with_context(
                    logging.DEBUG,
                    f"Found existing file with same hash: {filename} (ID: {file_id})",
                    channel=self._get_current_channel(),
                )
                return file_id, web_view_link

            return None, None

        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Error searching for file by hash: {e}",
                channel=self._get_current_channel(),
            )
            return None, None

    def upload_file_to_drive(
        self,
        file_path: str,
        filename: str,
        folder_id: str,
        shared_drive_id: Optional[str] = None,
        message_poster_email: Optional[str] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """Upload a file to Google Drive.

        Args:
            file_path: Path to the local file
            filename: Name for the uploaded file
            folder_id: ID of the Drive folder to upload to
            shared_drive_id: ID of the shared drive (if applicable)
            message_poster_email: Email of the user who will post the message with this attachment
                                 This user will get editor permissions on the file

        We rely on folder permissions for access control instead of setting individual
        file permissions. Only the message poster gets explicit editor access to the file.

        If a file with the same MD5 hash already exists in the folder, we'll reuse that file
        instead of uploading a duplicate.

        Returns:
            Tuple of (file_id, public_url) if successful, (None, None) otherwise
        """
        if self.dry_run:
            return (
                f"DRY_FILE_{filename}",
                f"https://drive.google.com/dry-run/{filename}",
            )

        try:
            # Get MIME type
            mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

            # Calculate the file's MD5 hash
            file_hash = self._calculate_file_hash(file_path)

            log_with_context(
                logging.DEBUG,
                f"Calculated MD5 hash for {filename}: {file_hash}",
                channel=self._get_current_channel(),
            )

            # Ensure the folder's files are pre-cached if we haven't done so already
            if folder_id not in self.folders_pre_cached:
                self.pre_cache_folder_file_hashes(folder_id, shared_drive_id)

            # Check if this file already exists in Drive by its hash
            existing_file_id, existing_url = self._find_file_by_hash(
                file_hash, filename, folder_id, shared_drive_id
            )

            # If the file already exists, reuse it instead of uploading again
            if existing_file_id and existing_url:
                log_with_context(
                    logging.DEBUG,
                    f"Reusing existing file with same hash: {filename} (ID: {existing_file_id})",
                    channel=self._get_current_channel(),
                )

                # Set permissions for the message poster on the existing file
                if message_poster_email:
                    self._set_message_poster_permission(
                        existing_file_id, message_poster_email, shared_drive_id
                    )

                return existing_file_id, existing_url

            # File doesn't exist yet, proceed with upload
            log_with_context(
                logging.DEBUG,
                f"Uploading file {filename} with MIME type {mime_type} to folder {folder_id}",
                channel=self._get_current_channel(),
            )

            file_metadata = {"name": filename, "parents": [folder_id]}

            media = MediaFileUpload(file_path, mimetype=mime_type)

            # Upload with appropriate parameters
            if shared_drive_id:
                file = (
                    self.drive_service.files()
                    .create(
                        body=file_metadata,
                        media_body=media,
                        fields="id,webViewLink",
                        supportsAllDrives=True,
                    )
                    .execute()
                )
            else:
                file = (
                    self.drive_service.files()
                    .create(
                        body=file_metadata, media_body=media, fields="id,webViewLink"
                    )
                    .execute()
                )

            file_id = file.get("id")
            public_url = file.get("webViewLink")

            # We only set editor permissions for the message poster
            # All other permissions are inherited from the folder
            if file_id and message_poster_email:
                self._set_message_poster_permission(
                    file_id, message_poster_email, shared_drive_id
                )

            log_with_context(
                logging.DEBUG,
                f"Successfully uploaded file {filename} to Drive (ID: {file_id})",
                channel=self._get_current_channel(),
            )

            return (file_id, public_url)

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to upload file {filename} to Drive: {e}",
                channel=self._get_current_channel(),
            )
            return (None, None)

    def _set_message_poster_permission(
        self,
        file_id: str,
        message_poster_email: str,
        shared_drive_id: Optional[str] = None,
    ) -> bool:
        """Set editor permission for the message poster on a file.

        Args:
            file_id: ID of the file
            message_poster_email: Email of the message poster
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            True if successful, False otherwise
        """
        log_with_context(
            logging.DEBUG,
            f"Setting editor permission for message poster {message_poster_email} on file {file_id}",
            channel=self._get_current_channel(),
        )
        try:
            permission = {
                "type": "user",
                "role": shared_drive_id
                and "writer"
                or "editor",  # 'writer' is used for shared drives
                "emailAddress": message_poster_email,
            }
            if shared_drive_id:
                self.drive_service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields="id",
                    sendNotificationEmail=False,
                    supportsAllDrives=True,
                ).execute()
            else:
                self.drive_service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields="id",
                    sendNotificationEmail=False,
                ).execute()

            log_with_context(
                logging.DEBUG,
                f"Successfully set editor permission for message poster {message_poster_email} on file {file_id}",
                channel=self._get_current_channel(),
            )
            return True
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to set editor permission for message poster on file {file_id}: {e}",
                channel=self._get_current_channel(),
            )
            return False

    def set_file_permissions_for_users(
        self,
        file_id: str,
        user_emails: list,
        message_poster_email: Optional[str] = None,
    ) -> bool:
        """Set file permissions for specific users only (not domain-wide).

        Args:
            file_id: ID of the file to set permissions on
            user_emails: List of user emails to grant access to
            message_poster_email: Optional email of the user who will post the message with this attachment
                                  This user will get editor permissions instead of reader

        Returns:
            True if successful, False otherwise
        """
        if not user_emails:
            log_with_context(
                logging.WARNING,
                f"No user emails provided for setting permissions on file {file_id}",
                channel=self._get_current_channel(),
            )
            return False

        success_count = 0
        failed_count = 0
        editor_set = False

        for email in user_emails:
            try:
                # Give editor permissions to the message poster, reader to everyone else
                role = "editor" if email == message_poster_email else "reader"
                if email == message_poster_email:
                    editor_set = True
                    log_with_context(
                        logging.DEBUG,
                        f"Granting editor permission to message poster {email} for file {file_id}",
                        channel=self._get_current_channel(),
                    )

                permission = {"type": "user", "role": role, "emailAddress": email}
                self.drive_service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields="id",
                    sendNotificationEmail=False,
                ).execute()

                success_count += 1

            except Exception as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to set permission for {email} on file {file_id}: {e}",
                    channel=self._get_current_channel(),
                )
                failed_count += 1

        # If the message poster wasn't in the user_emails list, add them separately with editor permission
        if (
            message_poster_email
            and not editor_set
            and message_poster_email not in user_emails
        ):
            try:
                permission = {
                    "type": "user",
                    "role": "editor",
                    "emailAddress": message_poster_email,
                }

                log_with_context(
                    logging.DEBUG,
                    f"Adding separate editor permission for message poster {message_poster_email} for file {file_id}",
                    channel=self._get_current_channel(),
                )
                self.drive_service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields="id",
                    sendNotificationEmail=False,
                ).execute()

                success_count += 1

            except Exception as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to set editor permission for message poster {message_poster_email} on file {file_id}: {e}",
                    channel=self._get_current_channel(),
                )
                failed_count += 1

        # Always add the service account with editor permissions if available
        if self.service_account_email:
            try:
                permission = {
                    "type": "user",
                    "role": "editor",
                    "emailAddress": self.service_account_email,
                }

                log_with_context(
                    logging.DEBUG,
                    f"Adding editor permission for service account {self.service_account_email} for file {file_id}",
                    channel=self._get_current_channel(),
                )
                self.drive_service.permissions().create(
                    fileId=file_id,
                    body=permission,
                    fields="id",
                    sendNotificationEmail=False,
                ).execute()

                success_count += 1

            except Exception as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to set editor permission for service account {self.service_account_email} on file {file_id}: {e}",
                    channel=self._get_current_channel(),
                )
                failed_count += 1

        log_with_context(
            logging.DEBUG if failed_count == 0 else logging.WARNING,
            f"Set file permissions for {file_id}: {success_count} successful, {failed_count} failed",
            channel=self._get_current_channel(),
        )

        return failed_count == 0

    def transfer_ownership(self, file_id: str, new_owner_email: str) -> bool:
        """Transfer ownership of a file to a new owner.

        Args:
            file_id: ID of the file to transfer
            new_owner_email: Email of the new owner

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            log_with_context(
                logging.DEBUG,
                f"[DRY RUN] Would transfer ownership of file {file_id} to {new_owner_email}",
                channel=self._get_current_channel(),
            )
            return True

        try:
            permission = {
                "type": "user",
                "role": "owner",
                "emailAddress": new_owner_email,
            }
            self.drive_service.permissions().create(
                fileId=file_id,
                body=permission,
                transferOwnership=True,
                sendNotificationEmail=False,
            ).execute()

            log_with_context(
                logging.DEBUG,
                f"Transferred ownership of file {file_id} to {new_owner_email}",
                channel=self._get_current_channel(),
            )

            return True

        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to transfer file ownership: {e}",
                channel=self._get_current_channel(),
            )
            return False
