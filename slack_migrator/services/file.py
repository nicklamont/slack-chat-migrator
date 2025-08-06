"""
File handling module for the Slack to Google Chat migration tool
"""

import hashlib
import logging
import mimetypes
import os
import tempfile
from typing import Any, Dict, Optional

import requests

from slack_migrator.services.chat import ChatFileUploader
from slack_migrator.services.drive import (
    DriveFileUploader,
    FolderManager,
    SharedDriveManager,
)
from slack_migrator.utils.logging import log_with_context


class FileHandler:
    """Handles file uploads and attachments during migration."""

    # MIME types suitable for direct upload to Google Chat (small images only)
    # For import mode, Google recommends Drive for most files, but small images can be direct
    DIRECT_UPLOAD_MIME_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}

    # Maximum file size for direct upload (in bytes) - 25MB for direct Chat uploads
    # Note: 200MB is Drive API limit, but Chat direct uploads are much smaller
    DIRECT_UPLOAD_MAX_SIZE = 25 * 1024 * 1024  # 25MB

    def __init__(
        self,
        drive_service,
        chat_service,
        folder_id: str,
        migrator,
        dry_run: bool = False,
    ):
        """Initialize the FileHandler.

        Args:
            drive_service: The Drive API service instance
            chat_service: The Chat API service instance
            folder_id: The ID of the root folder in Drive to store files (can be None for auto-creation)
            migrator: The parent Migrator instance
            dry_run: Whether to run in dry run mode
        """
        self.drive_service = drive_service
        self.chat_service = chat_service
        self.migrator = migrator
        self.dry_run = dry_run

        # Initialize the dictionary to track processed files
        self.processed_files = {}

        # Initialize cache to track which channel folders have already been shared
        self.shared_channel_folders = set()

        # Initialize file upload statistics
        self.file_stats = {
            "total_files": 0,
            "drive_uploads": 0,
            "direct_uploads": 0,
            "failed_uploads": 0,
            "external_user_files": 0,
            "ownership_transferred": 0,
            "ownership_transfer_failed": 0,
            "files_by_channel": {},
        }

        # Get workspace domain for permissions
        workspace_domain = getattr(migrator, "workspace_domain", None)

        # Initialize modular services
        self.shared_drive_manager = SharedDriveManager(
            drive_service, migrator.config, dry_run=dry_run
        )
        self.folder_manager = FolderManager(
            drive_service, workspace_domain, dry_run=dry_run
        )
        self.drive_uploader = DriveFileUploader(
            drive_service, workspace_domain, dry_run=dry_run
        )
        self.chat_uploader = ChatFileUploader(chat_service, dry_run=dry_run)

        # Set migrator reference on sub-services for channel context
        self.drive_uploader.migrator = migrator
        self.chat_uploader.migrator = migrator

        # Initialize the root folder and shared drive
        self._shared_drive_id = None
        self._root_folder_id = None
        self._drive_initialized = False

        # Don't initialize drive structures during construction - defer until needed
        # This avoids expensive operations before permission validation
        if folder_id:
            self._root_folder_id = folder_id

        if dry_run:
            self._root_folder_id = folder_id or "DRY_RUN_FOLDER"
            self._drive_initialized = True
            log_with_context(
                logging.DEBUG,
                "FileHandler initialized with verbose logging",
                channel=self._get_current_channel(),
            )

    def ensure_drive_initialized(self):
        """Ensure drive structures are initialized. Call this after permission validation."""
        if not self._drive_initialized and not self.dry_run:
            log_with_context(
                logging.INFO,
                "Initializing Google Drive structures (shared drive and folder hierarchy)...",
                channel=self._get_current_channel(),
            )
            self._initialize_shared_drive_and_folder()
            self._drive_initialized = True

    @property
    def folder_id(self):
        """Backward compatibility property for the root folder ID."""
        # Ensure drive is initialized when accessing folder_id
        self.ensure_drive_initialized()
        return self._root_folder_id

    def _get_current_channel(self):
        """Helper method to get the current channel from the migrator.

        Returns:
            Current channel name or None if not available
        """
        if hasattr(self, "migrator") and hasattr(self.migrator, "current_channel"):
            return self.migrator.current_channel
        return None

    @folder_id.setter
    def folder_id(self, value):
        """Backward compatibility setter for the root folder ID."""
        self._root_folder_id = value

    @property
    def shared_drive_id(self):
        """Property to access the shared drive ID."""
        return self._shared_drive_id

    def reset_shared_folder_cache(self):
        """Reset the cache of shared channel folders.

        This can be useful when starting a fresh migration or for testing.
        """
        self.shared_channel_folders.clear()
        log_with_context(
            logging.DEBUG,
            "Cleared shared channel folder cache",
        )

    def _initialize_shared_drive_and_folder(self) -> None:
        """Initialize the shared drive and root folder for attachments."""
        try:
            # Get shared drive configuration
            shared_drive_config = self.migrator.config.get("shared_drive", {})
            shared_drive_name = shared_drive_config.get("name")
            shared_drive_id = shared_drive_config.get("id")

            # If no shared drive specified, use default name
            if not shared_drive_name and not shared_drive_id:
                shared_drive_name = "Imported Slack Attachments"

            # Step 1: Find or create the shared drive using SharedDriveManager
            if shared_drive_id:
                # Use specified shared drive ID
                try:
                    # Validate the shared drive exists and is accessible
                    self.drive_service.drives().get(driveId=shared_drive_id).execute()
                    self._shared_drive_id = shared_drive_id
                    log_with_context(
                        logging.INFO,
                        f"Using configured shared drive ID: {shared_drive_id}",
                    )
                except Exception as e:
                    log_with_context(
                        logging.ERROR,
                        f"Configured shared drive ID {shared_drive_id} not accessible: {e}. Will create new one.",
                    )
                    shared_drive_id = None

            if not shared_drive_id and shared_drive_name:
                # Find existing shared drive by name or create new one
                self._shared_drive_id = (
                    self.shared_drive_manager.get_or_create_shared_drive()
                )

            # Step 2: Set the shared drive as the root folder
            if self._shared_drive_id:
                # Use the shared drive root directly - no need for an extra folder layer
                self._root_folder_id = self._shared_drive_id
                log_with_context(
                    logging.DEBUG,
                    f"Using shared drive root as attachment folder: {self._shared_drive_id}",
                )

                # Pre-cache file hashes from root folder to improve deduplication
                self._pre_cache_root_folder()
            else:
                # Fallback to regular Drive folder
                log_with_context(
                    logging.WARNING,
                    "Could not set up shared drive, falling back to regular Drive folder",
                )
                # For regular Drive, still create a root folder for organization
                self._root_folder_id = self.folder_manager.create_regular_drive_folder(
                    "Imported Slack Attachments"
                )

                # Pre-cache file hashes from root folder to improve deduplication
                self._pre_cache_root_folder()

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to initialize shared drive and folder: {e}. Using fallback.",
            )
            # Final fallback
            self._root_folder_id = self.folder_manager.create_regular_drive_folder(
                "Slack Attachments Fallback"
            )

    def _pre_cache_root_folder(self) -> None:
        """Pre-cache file hashes from the root folder and its subfolders.

        This helps improve deduplication by building a hash cache of all
        existing files before starting any channel uploads.
        """
        if not self._root_folder_id or self.dry_run:
            return

        try:
            log_with_context(
                logging.DEBUG,
                "Pre-caching file hashes from root folder to improve deduplication",
            )

            # First, pre-cache files directly in the root folder
            file_count = self.drive_uploader.pre_cache_folder_file_hashes(
                self._root_folder_id, self._shared_drive_id
            )

            log_with_context(
                logging.DEBUG, f"Pre-cached {file_count} files from root folder"
            )

            # Then, find all channel subfolders and pre-cache them as well
            # This is important for migrations that are being resumed
            try:
                # Query for all folders under the root folder
                query = f"'{self._root_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"

                params = {"q": query, "fields": "files(id,name)", "pageSize": 1000}

                if self._shared_drive_id:
                    params.update(
                        {
                            "spaces": "drive",
                            "corpora": "drive",
                            "driveId": self._shared_drive_id,
                            "includeItemsFromAllDrives": True,
                            "supportsAllDrives": True,
                        }
                    )

                response = self.drive_service.files().list(**params).execute()
                folders = response.get("files", [])

                total_subfolders = len(folders)
                folders_processed = 0
                total_files_cached = file_count

                for folder in folders:
                    folder_id = folder.get("id")
                    folder_name = folder.get("name")

                    if folder_id:
                        file_count = self.drive_uploader.pre_cache_folder_file_hashes(
                            folder_id, self._shared_drive_id
                        )
                        total_files_cached += file_count
                        folders_processed += 1

                        if folders_processed % 10 == 0:
                            log_with_context(
                                logging.DEBUG,
                                f"Pre-cached {folders_processed}/{total_subfolders} subfolders ({total_files_cached} total files)",
                            )

                log_with_context(
                    logging.DEBUG,
                    f"Completed pre-caching {total_files_cached} files from {folders_processed} channel folders",
                )

            except Exception as e:
                # Don't fail the entire process if subfolder caching fails
                log_with_context(
                    logging.WARNING,
                    f"Error pre-caching channel subfolders: {e}. Continuing with available cache.",
                )

        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to pre-cache file hashes from root folder: {e}. Continuing without pre-cache.",
            )

    def upload_attachment(
        self,
        file_obj: Dict,
        channel: Optional[str] = None,
        space: Optional[str] = None,
        user_service=None,
        sender_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upload a file using the most appropriate method based on file type.

        This method determines whether to use direct upload to Chat or Google Drive
        based on the file's MIME type and size.

        Args:
            file_obj: The file object from Slack
            channel: Optional channel name for context
            space: Optional space ID where the file will be used (e.g., "spaces/AAAAy2-BTIA")
            user_service: Optional user-specific Chat service to use for upload
            sender_email: Email address of the message sender (for permissions handling)

        Returns:
            Dict with upload details if successful, None otherwise
            Format: {
                'type': 'direct' or 'drive',
                'ref': reference object or None,
                'link': link to file (for Drive uploads),
                'name': file name
            }
        """
        try:
            # Ensure drive structures are initialized before processing files
            self.ensure_drive_initialized()

            file_id = file_obj.get("id", "unknown")
            name = file_obj.get("name", f"file_{file_id}")
            mime_type = file_obj.get("mimetype", "application/octet-stream")
            size = file_obj.get("size", 0)

            # Update statistics
            self.file_stats["total_files"] += 1
            if channel:
                if channel not in self.file_stats["files_by_channel"]:
                    self.file_stats["files_by_channel"][channel] = 0
                self.file_stats["files_by_channel"][channel] += 1

            # Check if user is external (if we have user info)
            username = file_obj.get("user", None)
            if (
                username
                and self.migrator
                and hasattr(self.migrator, "is_external_user")
            ):
                if self.migrator.is_external_user(username):
                    self.file_stats["external_user_files"] += 1

            log_with_context(
                logging.DEBUG,
                f"Processing file: {name} (MIME: {mime_type}, Size: {size})",
                channel=channel,
                file_id=file_id,
            )

            # Check if we've already processed this file
            if file_id in self.processed_files:
                cached_result = self.processed_files[file_id]
                log_with_context(
                    logging.DEBUG,
                    f"File {name} already processed, using cached result",
                    channel=channel,
                    file_id=file_id,
                )
                return cached_result

            # Download the file content
            file_content = self._download_file(file_obj)
            if not file_content:
                log_with_context(
                    logging.ERROR,
                    f"Failed to download file {name}, skipping attachment processing",
                    channel=channel,
                    file_id=file_id,
                    url_private=file_obj.get("url_private", "No URL")[:100],
                )
                self.file_stats["failed_uploads"] += 1
                return None

            # Check if this is a Google Docs file that should be skipped
            if file_content == b"__GOOGLE_DOCS_SKIP__":
                log_with_context(
                    logging.DEBUG,
                    f"Google Docs/Sheets file cannot be attached - will appear as link in message text: {name}",
                    channel=channel,
                    file_id=file_id,
                )
                # Return a special result indicating this was a Google Docs skip, not a failure
                return {
                    "type": "skip",
                    "reason": "google_docs_link",
                    "name": name,
                    "url": file_obj.get("url_private", ""),
                }

            # Check if this is a Google Drive file that should be referenced directly
            if file_content == b"__GOOGLE_DRIVE_FILE__":
                log_with_context(
                    logging.DEBUG,
                    f"Creating direct Google Drive reference for file: {name}",
                    channel=channel,
                    file_id=file_id,
                )
                return self._create_drive_reference(file_obj, channel)

            # Check for excessively large files that might cause memory issues
            MAX_FILE_SIZE = 200 * 1024 * 1024  # 200MB - Drive API limit
            if len(file_content) > MAX_FILE_SIZE:
                log_with_context(
                    logging.WARNING,
                    f"File {name} is very large ({len(file_content)} bytes), this may cause memory issues",
                    channel=channel,
                    file_id=file_id,
                )

            # Ensure we have a valid MIME type
            if not mime_type or mime_type == "null":
                guessed_type, _ = mimetypes.guess_type(name)
                mime_type = guessed_type if guessed_type else "application/octet-stream"

                log_with_context(
                    logging.DEBUG,
                    f"Using guessed MIME type {mime_type} for file {name}",
                    channel=channel,
                    file_id=file_id,
                )

            # Determine the best upload method based on file characteristics
            actual_size = len(file_content)

            # Decision logic for upload method:
            # 1. For import mode, Google generally recommends Drive for all attachments
            # 2. Small images can optionally use direct upload for better user experience
            # 3. All other files should use Drive upload for reliability and sharing
            # 4. Direct upload is limited to 25MB and specific MIME types

            use_direct_upload = (
                mime_type in self.DIRECT_UPLOAD_MIME_TYPES
                and actual_size <= self.DIRECT_UPLOAD_MAX_SIZE
                and not self.dry_run
                and self.chat_uploader.is_suitable_for_direct_upload(name, actual_size)
            )

            if use_direct_upload:
                log_with_context(
                    logging.DEBUG,
                    f"Attempting direct Chat upload for small image: {name} ({actual_size} bytes)",
                    channel=channel,
                    file_id=file_id,
                )

                # Try direct upload to Chat API first
                direct_result = self._upload_direct_to_chat(
                    file_obj, file_content, channel, space, user_service, sender_email
                )
                if direct_result:
                    # Store result for future reference and update stats
                    self.processed_files[file_id] = direct_result
                    self.file_stats["direct_uploads"] += 1
                    return direct_result
                else:
                    log_with_context(
                        logging.DEBUG,
                        f"Direct upload failed for {name}, falling back to Drive upload",
                        channel=channel,
                        file_id=file_id,
                    )
                    # Fall through to Drive upload

            # Use Google Drive upload (recommended for import mode and fallback)
            log_with_context(
                logging.DEBUG,
                f"Using Google Drive upload for file: {name} ({actual_size} bytes, {mime_type})",
                channel=channel,
                file_id=file_id,
            )

            # Get the user's email for setting file permissions and editor access
            username = file_obj.get("user")
            user_email = None
            if username and hasattr(self.migrator, "user_map"):
                user_email = self.migrator.user_map.get(username)
                log_with_context(
                    logging.DEBUG,
                    f"Found user email {user_email} for user ID {username}, will grant editor permission",
                    channel=channel,
                    file_id=file_id,
                )
                log_with_context(
                    logging.DEBUG,
                    f"Found user email {user_email} for user ID {username}, will grant editor permission",
                    channel=channel,
                    file_id=file_id,
                )

            drive_result = self._upload_to_drive(
                file_obj, file_content, channel, sender_email
            )
            if drive_result:
                # Store result for future reference and update stats
                self.processed_files[file_id] = drive_result
                self.file_stats["drive_uploads"] += 1

                log_with_context(
                    logging.DEBUG,
                    f"Successfully uploaded file {name} to Drive: {drive_result.get('link')}",
                    channel=channel,
                    file_id=file_id,
                    drive_file_id=drive_result.get("drive_id"),
                )

                return drive_result
            else:
                log_with_context(
                    logging.ERROR,
                    f"Failed to upload file {name}",
                    channel=channel,
                    file_id=file_id,
                )
                self.file_stats["failed_uploads"] += 1
                return None

        except Exception as e:
            self.file_stats["failed_uploads"] += 1
            log_with_context(
                logging.ERROR,
                f"Error uploading file: {str(e)}",
                channel=channel,
                file_id=file_obj.get("id", "unknown"),
                error=str(e),
            )
            return None

    def upload_file(
        self, file_obj: Dict, channel: Optional[str] = None
    ) -> Optional[str]:
        """Upload a file from Slack to Google Drive.

        This method is maintained for backward compatibility.
        For new code, use upload_attachment instead.

        Args:
            file_obj: The file object from Slack
            channel: Optional channel name for context

        Returns:
            The Drive file ID if successful, None otherwise
        """
        try:
            result = self.upload_attachment(file_obj, channel)
            if result and isinstance(result, dict) and result.get("type") == "drive":
                return result.get("drive_id")
            return None

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error uploading file: {str(e)}",
                channel=channel,
                file_id=file_obj.get("id", "unknown"),
                error=str(e),
            )
            return None

    def _upload_direct_to_chat(
        self,
        file_obj: Dict,
        file_content: bytes,
        channel: Optional[str] = None,
        space: Optional[str] = None,
        user_service=None,
        sender_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upload a file directly to Google Chat API.

        Args:
            file_obj: The file object from Slack
            file_content: The binary content of the file
            channel: Optional channel name for context
            space: Optional space ID where the file will be used (e.g., "spaces/AAAAy2-BTIA")
            user_service: Optional user-specific Chat service to use for upload
            sender_email: Email address of the message sender (for permissions handling)
            user_service: Optional user-specific Chat service to use for upload

        Returns:
            Dict with upload details if successful, None otherwise
        """
        try:
            file_id = file_obj.get("id", "unknown")
            name = file_obj.get("name", f"file_{file_id}")
            mime_type = file_obj.get("mimetype", "application/octet-stream")

            user_chat_uploader = None  # Ensure variable is always defined

            # Create a temporary file for the chat uploader
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=f"_{name}"
            ) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name

            try:
                # Use user-specific service if provided, otherwise use default chat uploader
                if user_service:
                    # Create a temporary chat uploader with the user's service
                    user_chat_uploader = ChatFileUploader(
                        user_service, dry_run=self.dry_run
                    )
                    # Set migrator reference for channel context logging
                    user_chat_uploader.migrator = self.migrator
                    upload_response, attachment_metadata = (
                        user_chat_uploader.upload_file_to_chat(
                            temp_file_path, name, space
                        )
                    )
                else:
                    # Use the default chat uploader (admin service)
                    user_chat_uploader = self.chat_uploader
                    upload_response, attachment_metadata = (
                        self.chat_uploader.upload_file_to_chat(
                            temp_file_path, name, space
                        )
                    )

                if upload_response and attachment_metadata:
                    # Create the attachment reference for Chat API
                    # According to API docs, use the complete upload response
                    attachment_ref = user_chat_uploader.create_attachment_for_message(
                        upload_response, attachment_metadata
                    )

                    result = {
                        "type": "direct",
                        "ref": attachment_ref,
                        "name": name,
                        "mime_type": mime_type,
                        "upload_response": upload_response,
                        "metadata": attachment_metadata,
                    }

                    log_with_context(
                        logging.DEBUG,
                        f"Successfully uploaded file {name} directly to Chat API",
                        channel=channel,
                        file_id=file_id,
                    )

                    return result
                else:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to get valid response from Chat API upload for {name}",
                        channel=channel,
                        file_id=file_id,
                    )
                    return None

            finally:
                # Clean up the temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception as cleanup_error:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to clean up temporary file: {cleanup_error}",
                        file_id=file_id,
                    )

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error in direct Chat upload for {file_obj.get('name', 'unknown')}: {e}",
                channel=channel,
                file_id=file_obj.get("id", "unknown"),
                error=str(e),
            )
            return None

    def _upload_to_drive(
        self,
        file_obj: Dict,
        file_content: bytes,
        channel: Optional[str] = None,
        sender_email: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upload a file to Google Drive.

        Args:
            file_obj: The file object from Slack
            file_content: The binary content of the file
            channel: Optional channel name for context
            sender_email: Email address of the message sender (for permissions handling)

        Returns:
            Dict with upload details if successful, None otherwise
            Format: {
                'type': 'drive',
                'drive_id': drive_file_id,
                'link': link to file,
                'name': file name
            }
        """
        try:
            file_id = file_obj.get("id", "unknown")
            name = file_obj.get("name", f"file_{file_id}")
            mime_type = file_obj.get("mimetype", "application/octet-stream")
            user_id = file_obj.get("user")
            url_private = file_obj.get("url_private", "")

            # Special handling for Google Docs/Sheets links to preserve correct MIME type
            is_google_docs_link = (
                "docs.google.com" in url_private
                or "drive.google.com" in url_private
                or "sheets.google.com" in url_private
                or "slides.google.com" in url_private
            )

            if is_google_docs_link:
                # Determine the correct MIME type based on the URL
                if "docs.google.com/document" in url_private:
                    mime_type = "application/vnd.google-apps.document"
                elif (
                    "docs.google.com/spreadsheets" in url_private
                    or "sheets.google.com" in url_private
                ):
                    mime_type = "application/vnd.google-apps.spreadsheet"
                elif "docs.google.com/presentation" in url_private:
                    mime_type = "application/vnd.google-apps.presentation"
                elif "drive.google.com" in url_private:
                    # Generic Google Drive file - keep existing MIME type or guess from name
                    if not mime_type or mime_type == "application/octet-stream":
                        guessed_type, _ = mimetypes.guess_type(name)
                        mime_type = (
                            guessed_type
                            if guessed_type
                            else "application/vnd.google-apps.document"
                        )

                log_with_context(
                    logging.DEBUG,
                    f"Detected Google Docs link, using MIME type {mime_type} for file {name}",
                    channel=channel,
                    file_id=file_id,
                    url=url_private[:100],
                )
            else:
                # Ensure we have a valid MIME type for regular files
                if not mime_type or mime_type == "null":
                    guessed_type, _ = mimetypes.guess_type(name)
                    mime_type = (
                        guessed_type if guessed_type else "application/octet-stream"
                    )

                    log_with_context(
                        logging.DEBUG,
                        f"Using guessed MIME type {mime_type} for file {name}",
                        channel=channel,
                        file_id=file_id,
                    )

            log_with_context(
                logging.DEBUG,
                f"Uploading file to Drive: {name} (Size: {len(file_content)} bytes, MIME: {mime_type})",
                channel=channel,
                file_id=file_id,
            )

            # Get the user's email for setting file ownership
            user_email = None
            if user_id and hasattr(self.migrator, "user_map"):
                user_email = self.migrator.user_map.get(user_id)

            # Get or create a folder for this channel
            folder_id = None

            if channel:
                folder_id = self.folder_manager.get_or_create_channel_folder(
                    channel, self._root_folder_id, self._shared_drive_id
                )

                # Mark this channel folder as processed to avoid redundant setup
                # Note: Channel folder permissions will be set later in add_regular_members()
                # after migration is complete and we have definitive member data
                channel_folder_key = f"{channel}_{folder_id}"
                if folder_id and channel_folder_key not in self.shared_channel_folders:
                    log_with_context(
                        logging.DEBUG,
                        f"Channel folder created for {channel}, permissions will be set after migration completes",
                        channel=channel,
                    )
                    # Mark this channel folder as processed to avoid redundant processing
                    self.shared_channel_folders.add(channel_folder_key)
                else:
                    log_with_context(
                        logging.DEBUG,
                        f"Channel folder for {channel} already processed",
                        channel=channel,
                    )

                # Pre-cache file hashes from this folder to avoid duplicate uploads
                if folder_id:
                    self.drive_uploader.pre_cache_folder_file_hashes(
                        folder_id, self._shared_drive_id
                    )

            # If we couldn't get or create a channel folder, use the root folder
            if not folder_id:
                folder_id = self._root_folder_id

            if not folder_id:
                log_with_context(
                    logging.ERROR,
                    f"No valid folder ID available for file upload",
                    channel=channel,
                    file_id=file_id,
                )
                return None

            # Write file content to a temporary file for upload
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=f"_{name}"
            ) as temp_file:
                temp_file.write(file_content)
                temp_file_path = temp_file.name

            try:
                # We now rely on folder permissions for access control
                # We only pass the message poster's email to grant them editor permissions
                # Use sender_email if provided, otherwise fall back to user_email
                message_poster_email = sender_email or user_email

                # Calculate hash of file content for logging and debugging
                content_hash = hashlib.md5(file_content).hexdigest()
                log_with_context(
                    logging.DEBUG,
                    f"File content hash: {content_hash}",
                    channel=channel,
                    file_id=file_id,
                )

                drive_file_id, public_url = self.drive_uploader.upload_file_to_drive(
                    temp_file_path,
                    name,
                    folder_id,
                    self._shared_drive_id,
                    message_poster_email=message_poster_email,
                )

                if not drive_file_id:
                    log_with_context(
                        logging.ERROR,
                        f"Failed to upload file {name} to Drive",
                        channel=channel,
                        file_id=file_id,
                    )
                    return None

                if message_poster_email:
                    log_with_context(
                        logging.DEBUG,
                        f"Gave editor permission to message poster {message_poster_email} for file {drive_file_id}",
                        channel=channel,
                        file_id=file_id,
                    )
                else:
                    log_with_context(
                        logging.WARNING,
                        f"No user email available for message poster, could not assign editor permissions",
                        channel=channel,
                        file_id=file_id,
                    )

                # Set ownership to the original poster if possible
                # Note: Ownership transfer only works for regular Drive folders, not shared drives
                # Also, external users cannot be made owners of files
                if (
                    user_email
                    and not self.migrator._is_external_user(user_email)
                    and not self._shared_drive_id
                ):
                    # For regular Drive folders, transfer ownership to the original internal poster
                    try:
                        self._transfer_file_ownership(drive_file_id, user_email)
                        self.file_stats["ownership_transferred"] += 1
                        log_with_context(
                            logging.DEBUG,
                            f"Transferred file ownership to original poster: {user_email}",
                            channel=channel,
                            file_id=file_id,
                            drive_file_id=drive_file_id,
                        )
                    except Exception as e:
                        self.file_stats["ownership_transfer_failed"] += 1
                        log_with_context(
                            logging.WARNING,
                            f"Could not transfer file ownership to {user_email}: {e}",
                            channel=channel,
                            file_id=file_id,
                        )
                elif user_email and self.migrator._is_external_user(user_email):
                    log_with_context(
                        logging.DEBUG,
                        f"External user {user_email} cannot be made file owner, using service account ownership",
                        channel=channel,
                        file_id=file_id,
                        drive_file_id=drive_file_id,
                    )
                elif self._shared_drive_id:
                    log_with_context(
                        logging.DEBUG,
                        f"Files in shared drives use inherited permissions, not individual ownership",
                        channel=channel,
                        file_id=file_id,
                        drive_file_id=drive_file_id,
                    )

                log_with_context(
                    logging.DEBUG,
                    f"Successfully uploaded file to Drive: {name}",
                    channel=channel,
                    file_id=file_id,
                    drive_file_id=drive_file_id,
                )

                # Return the Drive file information
                drive_result = {
                    "type": "drive",
                    "link": public_url
                    or f"https://drive.google.com/file/d/{drive_file_id}/view",
                    "drive_id": drive_file_id,
                    "name": name,
                    "mime_type": mime_type,
                }

                return drive_result

            finally:
                # Clean up temporary file
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error uploading file to Drive: {e}",
                channel=channel,
                file_id=file_obj.get("id", "unknown"),
                error=str(e),
            )
            return None

    def get_file_statistics(self) -> Dict[str, Any]:
        """Get detailed file upload statistics.

        Returns:
            Dictionary containing file upload statistics including counts
            by upload method, external user files, and ownership transfers.
        """
        return {
            "total_files_processed": self.file_stats["total_files"],
            "successful_uploads": self.file_stats["drive_uploads"]
            + self.file_stats["direct_uploads"],
            "failed_uploads": self.file_stats["failed_uploads"],
            "drive_uploads": self.file_stats["drive_uploads"],
            "direct_uploads": self.file_stats["direct_uploads"],
            "external_user_files": self.file_stats["external_user_files"],
            "ownership_transferred": self.file_stats["ownership_transferred"],
            "ownership_transfer_failed": self.file_stats["ownership_transfer_failed"],
            "files_by_channel": dict(self.file_stats["files_by_channel"]),
            "success_rate": (
                (self.file_stats["drive_uploads"] + self.file_stats["direct_uploads"])
                / max(1, self.file_stats["total_files"])
            )
            * 100,
        }

    def _transfer_file_ownership(self, file_id: str, new_owner_email: str) -> bool:
        """Transfer ownership of a file to a new owner.

        Args:
            file_id: ID of the file to transfer
            new_owner_email: Email of the new owner

        Returns:
            True if successful, False otherwise
        """
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
            )

            return True

        except Exception as e:
            log_with_context(logging.WARNING, f"Failed to transfer file ownership: {e}")
            return False

    def _download_file(self, file_obj: Dict) -> Optional[bytes]:
        """Download a file from Slack export or URL.

        Args:
            file_obj: The file object from Slack

        Returns:
            File content as bytes, or None if download failed
        """
        try:
            file_id = file_obj.get("id", "unknown")
            name = file_obj.get("name", f"file_{file_id}")
            url_private = file_obj.get("url_private")

            if not url_private:
                log_with_context(
                    logging.WARNING,
                    f"No URL found for file: {name}",
                    file_id=file_id,
                    channel=self._get_current_channel(),
                )
                return None

            # Create a hash of the URL to use as cache key
            file_hash = hashlib.md5(url_private.encode()).hexdigest()

            # Skip Google Docs links - these should not be processed as file attachments
            # Google Docs URLs in Slack messages are text links, not downloadable files
            # Only skip if they are actual Google Docs/Sheets/Slides documents
            is_google_docs = (
                ("docs.google.com/document" in url_private)
                or ("docs.google.com/spreadsheets" in url_private)
                or ("docs.google.com/presentation" in url_private)
                or ("sheets.google.com" in url_private and "/edit" in url_private)
                or ("slides.google.com" in url_private and "/edit" in url_private)
            )

            # Check if this is a Google Drive file that we should reference directly
            is_google_drive_file = (
                "drive.google.com/file/d/" in url_private
                or "drive.google.com/open?id=" in url_private
            )

            if is_google_docs:
                log_with_context(
                    logging.DEBUG,
                    f"Skipping Google Docs link - not a downloadable file: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                    file_id=file_id,
                    file_name=name,
                    channel=self._get_current_channel(),
                )
                return b"__GOOGLE_DOCS_SKIP__"

            if is_google_drive_file:
                log_with_context(
                    logging.DEBUG,
                    f"Google Drive file detected - will create direct reference instead of downloading: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                    file_id=file_id,
                    file_name=name,
                    channel=self._get_current_channel(),
                )
                # Return a special marker to indicate this is a Drive file
                return b"__GOOGLE_DRIVE_FILE__"

            log_with_context(
                logging.DEBUG,
                f"Downloading file from URL: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                file_id=file_id,
                file_name=name,
                channel=self._get_current_channel(),
            )

            # For files in the export, the URL might already contain a token
            # We'll try to download using requests with default headers
            headers = {}

            # Note: Slack token authentication removed as not needed
            # Export URLs already contain authentication tokens

            response = requests.get(url_private, headers=headers, stream=True)

            if response.status_code != 200:
                log_with_context(
                    logging.WARNING,
                    f"Failed to download file {name}: HTTP {response.status_code}",
                    file_id=file_id,
                    http_status=response.status_code,
                    channel=self._get_current_channel(),
                )
                # Raise an exception to trigger the retry
                response.raise_for_status()
                return None

            # Get content length if available
            content_length = response.headers.get("Content-Length")
            if content_length:
                log_with_context(
                    logging.DEBUG,
                    f"File size from headers: {content_length} bytes",
                    file_id=file_id,
                    channel=self._get_current_channel(),
                )

            # Return the actual file content
            content = response.content
            log_with_context(
                logging.DEBUG,
                f"Successfully downloaded file: {name} (Size: {len(content)} bytes)",
                file_id=file_id,
                channel=self._get_current_channel(),
            )
            return content

        except requests.exceptions.RequestException as e:
            # Check for authentication errors (401, 403) which are unlikely to be resolved by retrying
            if (
                hasattr(e, "response")
                and e.response
                and e.response.status_code in (401, 403)
            ):
                log_with_context(
                    logging.WARNING,
                    f"Authentication error downloading file, not retrying: {str(e)}",
                    file_id=file_obj.get("id", "unknown"),
                    file_name=file_obj.get("name", "unknown"),
                    error=str(e),
                    status_code=e.response.status_code,
                    channel=self._get_current_channel(),
                )
                # Return None instead of re-raising to prevent further retries for auth errors
                return None

            # For other network errors, log and re-raise to trigger retry
            log_with_context(
                logging.WARNING,
                f"Error downloading file: {str(e)}",
                file_id=file_obj.get("id", "unknown"),
                file_name=file_obj.get("name", "unknown"),
                error=str(e),
                channel=self._get_current_channel(),
            )
            raise  # Re-raise to trigger retry
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error downloading file: {str(e)}",
                file_id=file_obj.get("id", "unknown"),
                file_name=file_obj.get("name", "unknown"),
                error=str(e),
                channel=self._get_current_channel(),
            )
            return None

    def _create_drive_reference(
        self, file_obj: Dict, channel: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Create a direct reference to an existing Google Drive file.

        Args:
            file_obj: The file object from Slack containing Google Drive URL
            channel: Optional channel name for context

        Returns:
            Dict with drive reference details if successful, None otherwise
        """
        try:
            file_id = file_obj.get("id", "unknown")
            name = file_obj.get("name", f"file_{file_id}")
            url_private = file_obj.get("url_private", "")

            # Extract Google Drive file ID from the URL
            drive_file_id = None

            if "drive.google.com/file/d/" in url_private:
                # Format: https://drive.google.com/file/d/FILE_ID/view
                try:
                    drive_file_id = url_private.split("/file/d/")[1].split("/")[0]
                except IndexError:
                    pass
            elif "drive.google.com/open?id=" in url_private:
                # Format: https://drive.google.com/open?id=FILE_ID
                try:
                    drive_file_id = url_private.split("id=")[1].split("&")[0]
                except IndexError:
                    pass

            if not drive_file_id:
                log_with_context(
                    logging.WARNING,
                    f"Could not extract Drive file ID from URL: {url_private}",
                    channel=channel,
                    file_id=file_id,
                )
                return None

            log_with_context(
                logging.DEBUG,
                f"Created direct Drive reference for existing file: {name} (Drive ID: {drive_file_id})",
                channel=channel,
                file_id=file_id,
                drive_file_id=drive_file_id,
            )

            # Update statistics
            self.file_stats["drive_uploads"] += 1

            # Create the result format
            drive_result = {
                "type": "drive",
                "link": url_private,
                "drive_id": drive_file_id,
                "name": name,
                "mime_type": file_obj.get("mimetype", "application/octet-stream"),
                "is_reference": True,  # Flag to indicate this is a reference, not an upload
            }

            # Cache the result
            self.processed_files[file_id] = drive_result

            return drive_result

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error creating Drive reference: {e}",
                channel=channel,
                file_id=file_obj.get("id", "unknown"),
                error=str(e),
            )
            return None

    def share_file_with_members(self, drive_file_id: str, channel: str) -> bool:
        """Share a Drive file with all active members of a channel.

        If the file is already in a shared folder with proper permissions,
        this method will skip setting individual permissions.

        Args:
            drive_file_id: The ID of the Drive file to share
            channel: The channel name to get members from

        Returns:
            True if sharing was successful, False otherwise
        """
        try:
            # First check if this file is already in a shared folder
            if self._shared_drive_id:
                # For shared drives, check if file is in the shared drive
                file_info = (
                    self.drive_service.files()
                    .get(fileId=drive_file_id, fields="parents", supportsAllDrives=True)
                    .execute()
                )
            else:
                file_info = (
                    self.drive_service.files()
                    .get(fileId=drive_file_id, fields="parents")
                    .execute()
                )

            parent_folders = file_info.get("parents", [])

            # Check if any of the parent folders is our channel folder or shared drive
            for parent_id in parent_folders:
                try:
                    if self._shared_drive_id:
                        folder_info = (
                            self.drive_service.files()
                            .get(
                                fileId=parent_id, fields="name", supportsAllDrives=True
                            )
                            .execute()
                        )
                    else:
                        folder_info = (
                            self.drive_service.files()
                            .get(fileId=parent_id, fields="name")
                            .execute()
                        )

                    folder_name = folder_info.get("name", "")

                    # If this is our channel folder or shared drive, we don't need to set individual permissions
                    if folder_name == channel or parent_id == self._shared_drive_id:
                        log_with_context(
                            logging.DEBUG,
                            f"File {drive_file_id} is already in shared folder for channel {channel}, skipping individual permissions",
                            channel=channel,
                            file_id=drive_file_id,
                        )
                        return True
                except Exception:
                    # If we can't get folder info, continue checking other parents
                    continue

            # If we're here, the file is not in a channel folder, so we need to set individual permissions
            if (
                not hasattr(self.migrator, "active_users_by_channel")
                or channel not in self.migrator.active_users_by_channel
            ):
                log_with_context(
                    logging.WARNING,
                    f"No active users tracked for channel {channel}, can't share file",
                    channel=channel,
                )
                return False

            active_users = self.migrator.active_users_by_channel[channel]
            emails_to_share = []

            # Get emails for all active users (INCLUDING external users for channel folder access)
            for user_id in active_users:
                email = self.migrator.user_map.get(user_id)
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
                    if self._shared_drive_id:
                        self.drive_service.permissions().create(
                            fileId=drive_file_id,
                            body=permission,
                            sendNotificationEmail=False,
                            supportsAllDrives=True,
                        ).execute()
                    else:
                        self.drive_service.permissions().create(
                            fileId=drive_file_id,
                            body=permission,
                            sendNotificationEmail=False,
                        ).execute()

                except Exception as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to share file with {email}: {e}",
                        channel=channel,
                        file_id=drive_file_id,
                    )

            log_with_context(
                logging.DEBUG,
                f"Successfully shared Drive file with channel members",
                channel=channel,
                file_id=drive_file_id,
            )
            return True

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to share Drive file: {e}",
                channel=channel,
                file_id=drive_file_id,
            )
            # Re-raise to trigger retry
            raise
