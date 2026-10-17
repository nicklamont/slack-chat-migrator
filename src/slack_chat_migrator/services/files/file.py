"""
File handling module for the Slack to Google Chat migration tool
"""

from __future__ import annotations

import dataclasses
import logging
import mimetypes
from typing import TYPE_CHECKING, Any, ClassVar

import requests
from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import DIRECT_UPLOAD_MAX_BYTES
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.services.chat import ChatFileUploader
from slack_chat_migrator.services.drive import (
    DriveFileUploader,
    FolderManager,
    SharedDriveManager,
)
from slack_chat_migrator.services.files.file_download import (
    DownloadResult,
    DownloadStatus,
    create_drive_reference,
    download_file,
    google_link_status,
    unavailable_reason,
)
from slack_chat_migrator.services.files.file_permissions import (
    transfer_file_ownership,
)
from slack_chat_migrator.services.files.temp_files import (
    AttachmentTempDir,
    remove_temp_file,
    sweep_stale_temp_dirs,
)
from slack_chat_migrator.types import UploadResult
from slack_chat_migrator.utils.api import escape_drive_query_value
from slack_chat_migrator.utils.logging import log_with_context
from slack_chat_migrator.utils.mime import resolve_drive_mime_type

if TYPE_CHECKING:
    from slack_chat_migrator.services.chat_adapter import ChatAdapter
    from slack_chat_migrator.services.drive_adapter import DriveAdapter

logger = logging.getLogger("slack_chat_migrator")


#: ``UploadResult.skip_reason`` for files that can't be fetched (deleted,
#: hidden, external, sign-in page, ...).  The detailed reason is in
#: ``metadata["unavailable_reason"]``.  Such files are not failures.
SKIP_REASON_UNAVAILABLE = "unavailable"
SKIP_REASON_GOOGLE_DOCS_LINK = "google_docs_link"


class FileHandler:
    """Handles file uploads and attachments during migration."""

    # MIME types suitable for direct upload to Google Chat (small images only)
    # For import mode, Google recommends Drive for most files, but small images can be direct
    DIRECT_UPLOAD_MIME_TYPES: ClassVar[set[str]] = {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
    }

    def __init__(
        self,
        drive_service: DriveAdapter,
        chat_service: ChatAdapter,
        folder_id: str | None,
        config: MigrationConfig,
        workspace_domain: str,
        user_map: dict[str, str],
        user_resolver: Any,
        state: MigrationState,
        dry_run: bool = False,
    ) -> None:
        """Initialize the FileHandler.

        Args:
            drive_service: The Drive API service instance
            chat_service: The Chat API service instance
            folder_id: The ID of the root folder in Drive to store files (can be None for auto-creation)
            config: Migration configuration
            workspace_domain: Workspace domain for permission checks
            user_map: Slack user ID to Google email mapping
            user_resolver: User resolver for external user detection
            state: Mutable migration state
            dry_run: Whether to run in dry run mode
        """
        self.drive_service = drive_service
        self.chat_service = chat_service
        self.config = config
        self.workspace_domain = workspace_domain
        self.user_map = user_map
        self.user_resolver = user_resolver
        self.state = state
        self.dry_run = dry_run

        # Initialize the dictionary to track processed files
        self.processed_files: dict[str, Any] = {}

        # (drive_file_id, email) pairs that already received writer access
        self._granted_posters: set[tuple[str, str]] = set()

        # Initialize file upload statistics
        self.file_stats: dict[str, Any] = {
            "total_files": 0,
            "drive_uploads": 0,
            "direct_uploads": 0,
            "failed_uploads": 0,
            "unavailable_files": 0,
            "skipped_files": 0,
            "external_user_files": 0,
            "ownership_transferred": 0,
            "ownership_transfer_failed": 0,
            "files_by_channel": {},
        }

        # Downloads stream into a per-run temp dir; remove what killed runs
        # left behind before creating our own.
        self.temp_dir = AttachmentTempDir(lambda: self.state.context.output_dir)
        sweep_stale_temp_dirs(self.state.context.output_dir)

        # Initialize modular services
        self.shared_drive_manager = SharedDriveManager(drive_service, config)
        self.folder_manager = FolderManager(drive_service, workspace_domain)
        self.drive_uploader = DriveFileUploader(drive_service, workspace_domain)
        self.chat_uploader = ChatFileUploader(chat_service)

        # Initialize the root folder and shared drive
        self._shared_drive_id: str | None = None
        self._root_folder_id: str | None = None
        self._drive_initialized = False

        # Don't initialize drive structures during construction - defer until needed
        # This avoids expensive operations before permission validation
        if folder_id:
            self._root_folder_id = folder_id
            self._drive_initialized = True

    def ensure_drive_initialized(self) -> None:
        """Ensure drive structures are initialized. Call this after permission validation."""
        if not self._drive_initialized:
            log_with_context(
                logging.INFO,
                "Initializing Google Drive structures (shared drive and folder hierarchy)...",
                channel=self._get_current_channel(),
            )
            self._initialize_shared_drive_and_folder()
            self._drive_initialized = True

    @property
    def folder_id(self) -> str | None:
        """Backward compatibility property for the root folder ID.

        Returns:
            The root Google Drive folder ID, or None if not yet initialised.
        """
        # Ensure drive is initialized when accessing folder_id
        self.ensure_drive_initialized()
        return self._root_folder_id

    @folder_id.setter
    def folder_id(self, value: str | None) -> None:
        """Backward compatibility setter for the root folder ID.

        Args:
            value: The Drive folder ID to set, or None to clear.
        """
        self._root_folder_id = value

    def _get_current_channel(self) -> str | None:
        """Return the current channel name for logging context."""
        return self.state.context.current_channel

    @property
    def shared_drive_id(self) -> str | None:
        """Property to access the shared drive ID."""
        return self._shared_drive_id

    def _initialize_shared_drive_and_folder(self) -> None:
        """Initialize the shared drive and root folder for attachments.

        Delegates shared drive creation/validation to :class:`SharedDriveManager`
        (single source of truth), then sets the root folder ID and pre-caches
        file hashes for deduplication.
        """
        try:
            # SharedDriveManager handles: validate configured ID → find by
            # name → create new drive.  No need to duplicate that here.
            self._shared_drive_id = (
                self.shared_drive_manager.get_or_create_shared_drive()
            )

            if self._shared_drive_id:
                self._root_folder_id = self._shared_drive_id
                log_with_context(
                    logging.DEBUG,
                    f"Using shared drive root as attachment folder: {self._shared_drive_id}",
                )
            else:
                log_with_context(
                    logging.WARNING,
                    "Could not set up shared drive, falling back to regular Drive folder",
                )
                self._root_folder_id = self.folder_manager.create_regular_drive_folder(
                    "Imported Slack Attachments"
                )

            self._pre_cache_root_folder()

        except HttpError as e:
            log_with_context(
                logging.ERROR,
                f"Failed to initialize shared drive and folder: {e}. Using fallback.",
            )
            self._root_folder_id = self.folder_manager.create_regular_drive_folder(
                "Slack Attachments Fallback"
            )

    def _pre_cache_root_folder(self) -> None:
        """Pre-cache file hashes from the root folder and its subfolders.

        This helps improve deduplication by building a hash cache of all
        existing files before starting any channel uploads.
        """
        if not self._root_folder_id:
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
                safe_root = escape_drive_query_value(self._root_folder_id)
                query = f"'{safe_root}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"

                list_kwargs: dict[str, Any] = {
                    "q": query,
                    "fields": "files(id,name)",
                    "page_size": 1000,
                }

                if self._shared_drive_id:
                    list_kwargs.update(
                        {
                            "corpora": "drive",
                            "drive_id": self._shared_drive_id,
                            "include_items_from_all_drives": True,
                            "supports_all_drives": True,
                        }
                    )

                response = self.drive_service.list_files(**list_kwargs)
                folders = response.get("files", [])

                total_subfolders = len(folders)
                folders_processed = 0
                total_files_cached = file_count

                for folder in folders:
                    folder_id = folder.get("id")

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

            except HttpError as e:
                # Don't fail the entire process if subfolder caching fails
                log_with_context(
                    logging.WARNING,
                    f"Error pre-caching channel subfolders: {e}. Continuing with available cache.",
                )

        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to pre-cache file hashes from root folder: {e}. Continuing without pre-cache.",
            )

    def upload_attachment(
        self,
        file_obj: dict[str, Any],
        channel: str | None = None,
        space: str | None = None,
        user_service: ChatAdapter | None = None,
        sender_email: str | None = None,
    ) -> UploadResult:
        """Upload a file using the most appropriate method based on file type.

        Small images go straight to Chat when a space is given; everything
        else goes to Google Drive.

        Args:
            file_obj: The file object from Slack
            channel: Optional channel name for context
            space: Optional space ID where the file will be used
            user_service: Optional user-specific Chat service to use for upload
            sender_email: Email address of the message sender (for permissions handling)

        Returns:
            UploadResult with upload details. Check ``.success``, ``.skipped``,
            or ``.error`` to determine outcome.  Files that can't be fetched
            are skipped with ``skip_reason == "unavailable"``.
        """
        self._sync_channel_context()
        file_id = file_obj.get("id") or "unknown"
        name = file_obj.get("name") or f"file_{file_id}"

        try:
            self.ensure_drive_initialized()
            self._update_file_stats(file_obj, channel)

            log_with_context(
                logging.DEBUG,
                f"Processing file: {name} (MIME: {file_obj.get('mimetype')},"
                f" Size: {file_obj.get('size', 0)})",
                channel=channel,
                file_id=file_id,
            )

            found, cached = self._check_attachment_cache(file_id, name, channel)
            if found and cached is not None:
                return dataclasses.replace(cached, cached=True)

            result = self.process_file(
                file_obj, channel, space, user_service, sender_email
            )
            self._record_result(file_id, result)
            return result

        except (HttpError, requests.RequestException, OSError) as e:
            self._increment_stat("failed_uploads")
            log_with_context(
                logging.ERROR,
                f"Error uploading file: {e!s}",
                channel=channel,
                file_id=file_id,
                error=str(e),
            )
            return UploadResult(error=str(e), name=name)

    def process_file(
        self,
        file_obj: dict[str, Any],
        channel: str | None = None,
        space: str | None = None,
        user_service: ChatAdapter | None = None,
        sender_email: str | None = None,
    ) -> UploadResult:
        """Fetch one Slack file and upload it, without touching the stats.

        This is the per-file unit of work: download (streamed to a temp
        file), then direct Chat upload or Drive upload.  The temp file is
        deleted before returning.

        Args:
            file_obj: The file object from Slack.
            channel: Channel name, used for the Drive folder.
            space: Chat space for a direct upload, if any.
            user_service: Optional user-specific Chat service for uploads.
            sender_email: Email of the message sender.

        Returns:
            The UploadResult (success, skip or error).
        """
        file_id = file_obj.get("id") or "unknown"
        name = file_obj.get("name") or f"file_{file_id}"

        download = self._download_file(file_obj)
        try:
            outcome = self._handle_download_outcome(
                download, file_obj, name, channel, file_id
            )
            if outcome is not None:
                return outcome
            assert download.path is not None

            mime_type = self._resolve_mime_type(file_obj, name, channel, file_id)

            # Try direct Chat upload for eligible small images
            direct_result = self._try_direct_upload(
                file_obj,
                download.path,
                download.size,
                mime_type,
                channel,
                space,
                user_service,
                file_id,
                name,
            )
            if direct_result:
                return direct_result

            # Fall back to Drive upload, reusing the same temp file
            drive_result = self._try_drive_upload(
                file_obj,
                download.path,
                channel,
                sender_email,
                file_id,
                name,
                file_hash=download.md5,
            )
            if drive_result:
                return drive_result

            return UploadResult(error="Upload failed", name=name)
        finally:
            remove_temp_file(download.path)

    def _sync_channel_context(self) -> None:
        """Propagate current channel to sub-uploaders for logging context."""
        current_ch = self.state.context.current_channel
        self.drive_uploader.current_channel = current_ch
        self.chat_uploader.current_channel = current_ch

    def _increment_stat(self, key: str, amount: int = 1) -> None:
        """Add *amount* to a file statistics counter."""
        self.file_stats[key] += amount

    def _update_file_stats(self, file_obj: dict[str, Any], channel: str | None) -> None:
        """Update file processing statistics counters."""
        self._increment_stat("total_files")
        if channel:
            by_channel = self.file_stats["files_by_channel"]
            by_channel[channel] = by_channel.get(channel, 0) + 1

        username = file_obj.get("user", None)
        if username:
            user_email = self.user_map.get(username)
            if user_email and self.user_resolver.is_external_user(user_email):
                self._increment_stat("external_user_files")

    def _record_result(self, file_id: str, result: UploadResult) -> None:
        """Count a fresh result and cache it unless it is a retryable error."""
        if result.success:
            key = (
                "direct_uploads" if result.upload_type == "direct" else "drive_uploads"
            )
            self._increment_stat(key)
        elif result.skipped:
            if result.skip_reason == SKIP_REASON_UNAVAILABLE:
                self._increment_stat("unavailable_files")
            else:
                self._increment_stat("skipped_files")
        else:
            self._increment_stat("failed_uploads")
            return
        self.processed_files[file_id] = result

    def _check_attachment_cache(
        self, file_id: str, name: str, channel: str | None
    ) -> tuple[bool, UploadResult | None]:
        """Check if this file was already processed.

        Returns (found, cached_result). If found is False, cached_result is None.
        """
        if file_id in self.processed_files:
            cached_result: UploadResult | None = self.processed_files[file_id]
            log_with_context(
                logging.DEBUG,
                f"File {name} already processed, using cached result",
                channel=channel,
                file_id=file_id,
            )
            return True, cached_result
        return False, None

    def _handle_download_outcome(
        self,
        download: DownloadResult,
        file_obj: dict[str, Any],
        name: str,
        channel: str | None,
        file_id: str,
    ) -> UploadResult | None:
        """Turn every download outcome except ``OK`` into an UploadResult.

        Returns:
            The final result, or None when the content was downloaded and
            should be uploaded.
        """
        if download.status is DownloadStatus.OK:
            return None

        if download.status is DownloadStatus.GOOGLE_DOCS_LINK:
            log_with_context(
                logging.DEBUG,
                f"Google Docs/Sheets file cannot be attached - will appear as link in message text: {name}",
                channel=channel,
                file_id=file_id,
            )
            return UploadResult(
                upload_type="skip",
                skip_reason=SKIP_REASON_GOOGLE_DOCS_LINK,
                name=name,
                url=download.url or "",
            )

        if download.status is DownloadStatus.GOOGLE_DRIVE_FILE:
            log_with_context(
                logging.DEBUG,
                f"Creating direct Google Drive reference for file: {name}",
                channel=channel,
                file_id=file_id,
            )
            ref_result = self._create_drive_reference(file_obj, channel)
            if ref_result:
                return ref_result
            return UploadResult(error="Failed to create Drive reference", name=name)

        if download.status is DownloadStatus.UNAVAILABLE:
            return UploadResult(
                upload_type="skip",
                skip_reason=SKIP_REASON_UNAVAILABLE,
                name=name,
                url=download.url,
                metadata={"unavailable_reason": download.reason},
            )

        log_with_context(
            logging.ERROR,
            f"Failed to download file {name}, skipping attachment processing",
            channel=channel,
            file_id=file_id,
            reason=download.reason or "",
        )
        return UploadResult(error=f"Download failed: {download.reason}", name=name)

    def _resolve_mime_type(
        self,
        file_obj: dict[str, Any],
        name: str,
        channel: str | None,
        file_id: str,
    ) -> str:
        """Use Slack's MIME type, or guess one from the file name."""
        mime_type = file_obj.get("mimetype")
        if mime_type and mime_type != "null":
            return str(mime_type)
        guessed_type, _ = mimetypes.guess_type(name)
        mime_type = guessed_type or "application/octet-stream"
        log_with_context(
            logging.DEBUG,
            f"Using guessed MIME type {mime_type} for file {name}",
            channel=channel,
            file_id=file_id,
        )
        return mime_type

    def _try_direct_upload(
        self,
        file_obj: dict[str, Any],
        file_path: str,
        file_size: int,
        mime_type: str,
        channel: str | None,
        space: str | None,
        user_service: ChatAdapter | None,
        file_id: str,
        name: str,
    ) -> UploadResult | None:
        """Attempt direct Chat upload for eligible small images.

        Returns the UploadResult if successful, None to fall through to Drive.
        """
        use_direct = (
            space is not None
            and mime_type in self.DIRECT_UPLOAD_MIME_TYPES
            and file_size <= DIRECT_UPLOAD_MAX_BYTES
            and self.chat_uploader.is_suitable_for_direct_upload(
                name, file_size, mime_type
            )
        )
        if not use_direct:
            return None

        log_with_context(
            logging.DEBUG,
            f"Attempting direct Chat upload for small image: {name} ({file_size} bytes)",
            channel=channel,
            file_id=file_id,
        )

        direct_result = self._upload_direct_to_chat(
            file_obj, file_path, mime_type, channel, space, user_service
        )
        if direct_result:
            return direct_result

        log_with_context(
            logging.DEBUG,
            f"Direct upload failed for {name}, falling back to Drive upload",
            channel=channel,
            file_id=file_id,
        )
        return None

    def _try_drive_upload(
        self,
        file_obj: dict[str, Any],
        file_path: str,
        channel: str | None,
        sender_email: str | None,
        file_id: str,
        name: str,
        file_hash: str | None = None,
    ) -> UploadResult | None:
        """Upload file to Google Drive."""
        log_with_context(
            logging.DEBUG,
            f"Using Google Drive upload for file: {name}",
            channel=channel,
            file_id=file_id,
        )

        drive_result = self._upload_to_drive(
            file_obj, file_path, channel, sender_email, file_hash=file_hash
        )
        if drive_result:
            log_with_context(
                logging.DEBUG,
                f"Successfully uploaded file {name} to Drive: {drive_result.url}",
                channel=channel,
                file_id=file_id,
                drive_file_id=drive_result.drive_id,
            )
            return drive_result

        log_with_context(
            logging.ERROR,
            f"Failed to upload file {name}",
            channel=channel,
            file_id=file_id,
        )
        return None

    def _upload_direct_to_chat(
        self,
        file_obj: dict[str, Any],
        file_path: str,
        mime_type: str,
        channel: str | None = None,
        space: str | None = None,
        user_service: ChatAdapter | None = None,
    ) -> UploadResult | None:
        """Upload a downloaded file directly to the Google Chat API.

        Args:
            file_obj: The file object from Slack
            file_path: Temp file holding the content
            mime_type: MIME type to declare
            channel: Optional channel name for context
            space: Space ID where the file will be used (e.g., "spaces/AAAAy2-BTIA")
            user_service: Optional user-specific Chat service to use for upload

        Returns:
            UploadResult with direct upload details if successful, None otherwise
        """
        file_id = file_obj.get("id") or "unknown"
        name = file_obj.get("name") or f"file_{file_id}"
        try:
            # Use user-specific service if provided, otherwise the admin's
            if user_service:
                uploader = ChatFileUploader(user_service)
                uploader.current_channel = self.state.context.current_channel
            else:
                uploader = self.chat_uploader

            upload_response, attachment_metadata = uploader.upload_file_to_chat(
                file_path, name, space, mime_type=mime_type
            )
            if not (upload_response and attachment_metadata):
                log_with_context(
                    logging.WARNING,
                    f"Failed to get valid response from Chat API upload for {name}",
                    channel=channel,
                    file_id=file_id,
                )
                return None

            # According to API docs, use the complete upload response
            attachment_ref = uploader.create_attachment_for_message(
                upload_response, attachment_metadata
            )
            log_with_context(
                logging.DEBUG,
                f"Successfully uploaded file {name} directly to Chat API",
                channel=channel,
                file_id=file_id,
            )
            return UploadResult(
                upload_type="direct",
                attachment_ref=attachment_ref,
                name=name,
                mime_type=mime_type,
                metadata={
                    "upload_response": upload_response,
                    "attachment_metadata": attachment_metadata,
                },
            )

        except (HttpError, OSError) as e:
            log_with_context(
                logging.ERROR,
                f"Error in direct Chat upload for {name}: {e}",
                channel=channel,
                file_id=file_id,
                error=str(e),
            )
            return None

    def _upload_to_drive(
        self,
        file_obj: dict[str, Any],
        file_path: str,
        channel: str | None = None,
        sender_email: str | None = None,
        file_hash: str | None = None,
    ) -> UploadResult | None:
        """Upload a downloaded file to Google Drive.

        Args:
            file_obj: The file object from Slack
            file_path: Temp file holding the content
            channel: Optional channel name for context
            sender_email: Email address of the message sender (for permissions handling)
            file_hash: MD5 computed while downloading, so the file isn't
                read again just to hash it

        Returns:
            UploadResult with drive upload details if successful, None otherwise
        """
        file_id = file_obj.get("id") or "unknown"
        name = file_obj.get("name") or f"file_{file_id}"
        try:
            user_id = file_obj.get("user")

            mime_type = resolve_drive_mime_type(file_obj, name, channel, file_id)

            log_with_context(
                logging.DEBUG,
                f"Uploading file to Drive: {name} (MIME: {mime_type})",
                channel=channel,
                file_id=file_id,
            )

            user_email = self.user_map.get(user_id) if user_id else None

            folder_id = self._resolve_upload_folder(channel, file_id)
            if not folder_id:
                return None

            return self._execute_drive_upload(
                file_path,
                name,
                mime_type,
                folder_id,
                channel,
                file_id,
                user_email,
                sender_email,
                file_hash=file_hash,
            )

        except (HttpError, OSError) as e:
            log_with_context(
                logging.ERROR,
                f"Error uploading file to Drive: {e}",
                channel=channel,
                file_id=file_id,
                error=str(e),
            )
            return None

    def _resolve_upload_folder(self, channel: str | None, file_id: str) -> str | None:
        """Get or create the target Drive folder for an upload.

        Returns the folder ID, or None if no valid folder is available.
        """
        folder_id = None

        if channel and self._root_folder_id:
            folder_id = self.folder_manager.get_or_create_channel_folder(
                channel, self._root_folder_id, self._shared_drive_id
            )

            if folder_id:
                self.drive_uploader.pre_cache_folder_file_hashes(
                    folder_id, self._shared_drive_id
                )

        if not folder_id:
            folder_id = self._root_folder_id

        if not folder_id:
            log_with_context(
                logging.ERROR,
                "No valid folder ID available for file upload",
                channel=channel,
                file_id=file_id,
            )
            return None

        return folder_id

    def _execute_drive_upload(
        self,
        file_path: str,
        name: str,
        mime_type: str,
        folder_id: str,
        channel: str | None,
        file_id: str,
        user_email: str | None,
        sender_email: str | None,
        file_hash: str | None = None,
    ) -> UploadResult | None:
        """Upload a temp file to Drive and handle permissions.

        Args:
            file_path: Temp file holding the content.
            name: File name on Drive.
            mime_type: MIME type reported in the result.
            folder_id: Target Drive folder.
            channel: Channel name for logging context.
            file_id: Slack file ID for logging context.
            user_email: Mapped email of the Slack user who uploaded the file
                (the ownership-transfer target in regular-Drive mode).
            sender_email: Email of the message sender, who gets ``writer``
                access.  Callers pass None for external senders.
            file_hash: MD5 of the content, if known.

        Returns:
            The UploadResult or None on failure.
        """
        upload = self.drive_uploader.upload_file_to_drive(
            file_path,
            name,
            folder_id,
            self._shared_drive_id,
            mime_type=mime_type,
            file_hash=file_hash,
        )

        if upload is None:
            log_with_context(
                logging.ERROR,
                f"Failed to upload file {name} to Drive",
                channel=channel,
                file_id=file_id,
            )
            return None

        self._apply_file_permissions(
            upload.file_id, upload.reused, sender_email, user_email, channel, file_id
        )

        log_with_context(
            logging.DEBUG,
            f"Successfully uploaded file to Drive: {name}",
            channel=channel,
            file_id=file_id,
            drive_file_id=upload.file_id,
        )

        return UploadResult(
            upload_type="drive",
            url=upload.url or f"https://drive.google.com/file/d/{upload.file_id}/view",
            drive_id=upload.file_id,
            name=name,
            mime_type=mime_type,
            metadata={"reused": upload.reused},
        )

    def _is_internal_email(self, email: str | None) -> bool:
        """True for a mapped email that belongs to the workspace."""
        return bool(email) and not self.user_resolver.is_external_user(email)

    def _apply_file_permissions(
        self,
        drive_file_id: str,
        reused: bool,
        sender_email: str | None,
        owner_email: str | None,
        channel: str | None,
        file_id: str,
    ) -> None:
        """Grant the poster ``writer`` access and transfer ownership if allowed.

        Everyone else gets access through the channel folder.  External users
        never get a grant.  Ownership is transferred only for new uploads in
        regular-Drive mode, so a reused file keeps its owner.
        """
        poster = sender_email if self._is_internal_email(sender_email) else None
        owner = (
            owner_email
            if not reused
            and not self._shared_drive_id
            and self._is_internal_email(owner_email)
            else None
        )

        if sender_email and poster is None:
            log_with_context(
                logging.DEBUG,
                f"Not granting file access to external poster {sender_email}",
                channel=channel,
                file_id=file_id,
            )

        # The new owner needs no separate writer grant.
        if (
            poster
            and poster != owner
            and not self._already_granted(drive_file_id, poster)
        ):
            if self.drive_uploader.set_message_poster_permission(
                drive_file_id, poster, self._shared_drive_id
            ):
                self._mark_granted(drive_file_id, poster)

        if owner:
            transferred = self._handle_ownership_transfer(
                drive_file_id, owner, channel, file_id
            )
            if not transferred and poster == owner:
                # Keep the poster able to edit their file.
                if self.drive_uploader.set_message_poster_permission(
                    drive_file_id, poster, self._shared_drive_id
                ):
                    self._mark_granted(drive_file_id, poster)

    def _already_granted(self, drive_file_id: str, email: str) -> bool:
        """Whether *email* already got writer access to the file this run."""
        return (drive_file_id, email) in self._granted_posters

    def _mark_granted(self, drive_file_id: str, email: str) -> None:
        """Remember a successful poster grant."""
        self._granted_posters.add((drive_file_id, email))

    def _handle_ownership_transfer(
        self,
        drive_file_id: str,
        new_owner_email: str,
        channel: str | None,
        file_id: str,
    ) -> bool:
        """Transfer ownership of a new regular-Drive file to its Slack uploader.

        Returns:
            True if the transfer succeeded.  Both outcomes are counted.
        """
        if self._transfer_file_ownership(drive_file_id, new_owner_email):
            self._increment_stat("ownership_transferred")
            log_with_context(
                logging.DEBUG,
                f"Transferred file ownership to original poster: {new_owner_email}",
                channel=channel,
                file_id=file_id,
                drive_file_id=drive_file_id,
            )
            return True

        self._increment_stat("ownership_transfer_failed")
        log_with_context(
            logging.WARNING,
            f"Could not transfer file ownership to {new_owner_email}",
            channel=channel,
            file_id=file_id,
            drive_file_id=drive_file_id,
        )
        return False

    def get_file_statistics(self) -> dict[str, Any]:
        """Get detailed file upload statistics.

        Returns:
            Dictionary containing file upload statistics including counts
            by upload method, external user files, and ownership transfers.
            ``success_rate`` is the share of uploads that succeeded, out of
            those that succeeded or failed; unavailable and skipped files
            don't lower it.
        """
        successful = int(self.file_stats["drive_uploads"]) + int(
            self.file_stats["direct_uploads"]
        )
        failed = int(self.file_stats["failed_uploads"])
        return {
            "total_files_processed": self.file_stats["total_files"],
            "successful_uploads": successful,
            "failed_uploads": failed,
            "unavailable_files": self.file_stats["unavailable_files"],
            "skipped_files": self.file_stats["skipped_files"],
            "drive_uploads": self.file_stats["drive_uploads"],
            "direct_uploads": self.file_stats["direct_uploads"],
            "external_user_files": self.file_stats["external_user_files"],
            "ownership_transferred": self.file_stats["ownership_transferred"],
            "ownership_transfer_failed": self.file_stats["ownership_transfer_failed"],
            "files_by_channel": dict(self.file_stats["files_by_channel"]),
            "success_rate": successful / max(1, successful + failed) * 100,
        }

    def _transfer_file_ownership(self, file_id: str, new_owner_email: str) -> bool:
        """Transfer ownership of a file to a new owner.

        Delegates to :func:`file_permissions.transfer_file_ownership`.
        """
        return transfer_file_ownership(self.drive_service, file_id, new_owner_email)

    def _download_file(self, file_obj: dict[str, Any]) -> DownloadResult:
        """Download a Slack file into this run's temp directory.

        Delegates to :func:`file_download.download_file`.
        """
        needs_download = (
            google_link_status(file_obj) is None
            and unavailable_reason(file_obj) is None
        )
        # Don't create the temp directory when there is nothing to fetch.
        dest_dir = self.temp_dir.path() if needs_download else ""
        return download_file(
            file_obj,
            dest_dir,
            self._get_current_channel(),
            slack_token=self.config.slack_token,
        )

    def _create_drive_reference(
        self, file_obj: dict[str, Any], channel: str | None = None
    ) -> UploadResult | None:
        """Create a direct reference to an existing Google Drive file.

        Delegates to :func:`file_download.create_drive_reference`.
        """
        return create_drive_reference(file_obj, channel)

    def cleanup_temp_files(self) -> None:
        """Delete this run's attachment temp directory.

        Also runs automatically at interpreter exit.
        """
        self.temp_dir.cleanup()
