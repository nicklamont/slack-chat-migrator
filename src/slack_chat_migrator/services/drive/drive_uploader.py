"""
Google Drive file upload functionality.
"""

from __future__ import annotations

import hashlib
import logging
import mimetypes
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from slack_chat_migrator.services.drive_adapter import PermissionChange

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter

# Third-party imports
# pylint: disable=import-error
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from slack_chat_migrator.constants import (
    DRIVE_APP_PROPERTY_SLACK_CHANNEL,
    DRIVE_APP_PROPERTY_SLACK_FILE_ID,
    DRIVE_FILES_PAGE_SIZE,
    DRIVE_ROLE_WRITER,
    DRIVE_UPLOAD_CHUNK_BYTES,
    FILE_READ_CHUNK_BYTES,
)
from slack_chat_migrator.utils.api import escape_drive_query_value
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

logger = logging.getLogger("slack_chat_migrator")


#: Where a DriveUpload came from.
SOURCE_NEW = "new"  # uploaded now
SOURCE_HASH = "hash"  # identical content already in the folder
SOURCE_SLACK_ID = "slack_id"  # this Slack file, uploaded by an earlier run


@dataclass
class DriveUpload:
    """A Drive file holding an attachment.

    Attributes:
        file_id: Drive file ID.
        url: ``webViewLink`` of the file, if known.
        reused: True when an existing Drive file was linked instead of
            uploading a new one.  Ownership is never transferred for
            reused files.
        source: One of ``SOURCE_NEW``, ``SOURCE_HASH`` or ``SOURCE_SLACK_ID``.
    """

    file_id: str
    url: str | None = None
    reused: bool = False
    source: str = SOURCE_NEW


#: Cached Drive file: (file_id, webViewLink).
_CachedFile = tuple[str, str | None]

#: Drive limits each appProperties entry to 124 bytes of key plus value.
_APP_PROPERTY_MAX_BYTES = 124


def _fit_app_property(key: str, value: str) -> str:
    """Truncate *value* so ``key + value`` fits in an appProperties entry."""
    budget = _APP_PROPERTY_MAX_BYTES - len(key.encode("utf-8"))
    return value.encode("utf-8")[:budget].decode("utf-8", errors="ignore")


class DriveFileUploader:
    """Handles file uploads to Google Drive."""

    def __init__(
        self,
        drive_service: DriveAdapter,
        workspace_domain: str | None = None,
    ) -> None:
        """Initialize the DriveFileUploader.

        Args:
            drive_service: DriveAdapter instance wrapping the Google Drive API
            workspace_domain: The workspace domain for permissions
        """
        self.drive_service = drive_service
        self.workspace_domain = workspace_domain
        # Every cache is scoped to a folder: content found in one channel's
        # folder is never linked from another channel.
        # (folder_id, md5) -> file
        self.file_hash_cache: dict[tuple[str, str], _CachedFile] = {}
        # (folder_id, Slack file ID) -> file, from the appProperties tag
        self.slack_file_cache: dict[tuple[str, str], _CachedFile] = {}
        self.folders_pre_cached: set[str] = set()
        # Set by FileHandler before each channel is processed
        self.current_channel: str | None = None

    def _get_current_channel(self) -> str | None:
        """Return the current channel name for logging context."""
        return self.current_channel

    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of a file.

        Args:
            file_path: Path to the file

        Returns:
            MD5 hash of the file as a hexadecimal string
        """
        hash_md5 = hashlib.md5()  # noqa: S324 — not used for security
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(FILE_READ_CHUNK_BYTES), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def remember_file(
        self,
        folder_id: str,
        file_id: str,
        url: str | None,
        *,
        md5: str | None = None,
        slack_file_id: str | None = None,
    ) -> None:
        """Record a Drive file in the folder-scoped caches.

        Args:
            folder_id: Folder holding the file.
            file_id: Drive file ID.
            url: The file's ``webViewLink``.
            md5: Content MD5, if known (converted Google files have none).
            slack_file_id: Slack file ID the upload was tagged with.
        """
        entry: _CachedFile = (file_id, url)
        if md5:
            self.file_hash_cache.setdefault((folder_id, md5), entry)
        if slack_file_id:
            self.slack_file_cache.setdefault((folder_id, slack_file_id), entry)

    def pre_cache_folder_file_hashes(
        self, folder_id: str, shared_drive_id: str | None = None
    ) -> int:
        """Load a folder's files into the folder-scoped caches.

        Indexes each file by MD5 and by the Slack file ID in its
        ``appProperties``, so later uploads into the folder need no search
        and re-runs can link files without downloading them again.

        Args:
            folder_id: ID of the folder to cache files from
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            Number of files cached
        """
        # Skip if already pre-cached this folder
        if folder_id in self.folders_pre_cached:
            return 0

        try:
            log_with_context(
                logging.DEBUG,
                f"Pre-caching file hashes from folder {folder_id}",
                channel=self._get_current_channel(),
            )

            safe_folder = escape_drive_query_value(folder_id)
            query = f"'{safe_folder}' in parents and trashed=false"
            page_token = None
            files_cached = 0

            while True:
                # Build list_files call with explicit keyword args
                list_kwargs: dict[str, Any] = {
                    "q": query,
                    "fields": "nextPageToken, files(id, name, md5Checksum,"
                    " webViewLink, appProperties)",
                    "page_size": DRIVE_FILES_PAGE_SIZE,
                }

                if page_token:
                    list_kwargs["page_token"] = page_token

                # Add shared drive parameters if applicable
                if shared_drive_id:
                    list_kwargs.update(
                        {
                            "spaces": "drive",
                            "corpora": "drive",
                            "drive_id": shared_drive_id,
                            "include_items_from_all_drives": True,
                            "supports_all_drives": True,
                        }
                    )

                response = self.drive_service.list_files(**list_kwargs)

                for file in response.get("files", []):
                    file_id = file.get("id")
                    if not file_id:
                        continue
                    app_properties = file.get("appProperties") or {}
                    slack_file_id = app_properties.get(DRIVE_APP_PROPERTY_SLACK_FILE_ID)
                    md5 = file.get("md5Checksum")
                    if md5 or slack_file_id:
                        self.remember_file(
                            folder_id,
                            file_id,
                            file.get("webViewLink"),
                            md5=md5,
                            slack_file_id=slack_file_id,
                        )
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

        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to pre-cache file hashes from folder {folder_id}: {e}",
            )
            return 0

    def find_uploaded_slack_file(
        self, folder_id: str, slack_file_id: str
    ) -> DriveUpload | None:
        """Find a Slack file that an earlier run uploaded into *folder_id*.

        Only the folder cache is consulted, so pre-cache the folder first.

        Args:
            folder_id: Target folder.
            slack_file_id: Slack file ID.

        Returns:
            The existing upload (``source == SOURCE_SLACK_ID``), or None.
        """
        cached = self.slack_file_cache.get((folder_id, slack_file_id))
        if cached is None:
            return None
        return DriveUpload(cached[0], cached[1], reused=True, source=SOURCE_SLACK_ID)

    def _find_file_by_hash(
        self,
        file_hash: str,
        filename: str,
        folder_id: str,
        shared_drive_id: str | None = None,
    ) -> tuple[str | None, str | None]:
        """Find a file with this MD5 in *folder_id*.

        The folder cache is trusted.  Drive is searched only when the folder
        could not be pre-cached.

        Args:
            file_hash: MD5 hash of the file
            filename: Name of the file (for logging)
            folder_id: ID of the folder to search in
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            Tuple of (file_id, webViewLink) if found, (None, None) otherwise
        """
        cached = self.file_hash_cache.get((folder_id, file_hash))
        if cached is not None:
            log_with_context(
                logging.DEBUG,
                f"Found cached file ID for hash {file_hash}: {cached[0]}",
                channel=self._get_current_channel(),
            )
            return cached
        if folder_id in self.folders_pre_cached:
            return None, None

        try:
            safe_hash = escape_drive_query_value(file_hash)
            safe_folder_id = escape_drive_query_value(folder_id)
            query = (
                f"md5Checksum='{safe_hash}' and '{safe_folder_id}' in parents"
                " and trashed=false"
            )

            log_with_context(
                logging.DEBUG,
                f"Searching for files with hash {file_hash} using query: {query}",
                channel=self._get_current_channel(),
            )

            # Build list_files call with explicit keyword args
            search_kwargs: dict[str, Any] = {
                "q": query,
                "fields": "files(id,name,webViewLink)",
            }

            # Add shared drive parameters if applicable
            if shared_drive_id:
                search_kwargs.update(
                    {
                        "spaces": "drive",
                        "corpora": "drive",
                        "drive_id": shared_drive_id,
                        "include_items_from_all_drives": True,
                        "supports_all_drives": True,
                    }
                )

            response = self.drive_service.list_files(**search_kwargs)

            files = response.get("files", [])
            if files and files[0].get("id"):
                file_id = files[0]["id"]
                web_view_link = files[0].get("webViewLink")
                self.remember_file(folder_id, file_id, web_view_link, md5=file_hash)

                log_with_context(
                    logging.DEBUG,
                    f"Found existing file with same hash: {filename} (ID: {file_id})",
                    channel=self._get_current_channel(),
                )
                return file_id, web_view_link

            return None, None

        except HttpError as e:
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
        shared_drive_id: str | None = None,
        *,
        mime_type: str | None = None,
        file_hash: str | None = None,
        slack_file_id: str | None = None,
        channel: str | None = None,
        target_mime_type: str | None = None,
    ) -> DriveUpload | None:
        """Upload a file to Google Drive, or reuse an identical one.

        If a file with the same MD5 hash already exists in the folder, that
        file is returned (``reused=True``) instead of uploading a duplicate.
        New files are sent in a resumable session, in
        :data:`DRIVE_UPLOAD_CHUNK_BYTES` chunks, so memory use doesn't grow
        with the file size.  No permissions are set here; access comes from
        the folder, and the caller grants the poster's access.

        Args:
            file_path: Path to the local file
            filename: Name for the uploaded file
            folder_id: ID of the Drive folder to upload to
            shared_drive_id: ID of the shared drive (if applicable)
            mime_type: Content type (Slack's ``mimetype``); guessed from the
                file name when omitted
            file_hash: MD5 of the content, if already known; computed from
                the file otherwise
            slack_file_id: Slack file ID, stored in ``appProperties`` so a
                re-run can find the upload without downloading the file
            channel: Slack channel, stored in ``appProperties`` for reference
            target_mime_type: A Google Workspace type (e.g. a Google Doc) to
                convert the upload into.  Converted files skip MD5 dedup,
                since their content differs from the uploaded bytes.

        Returns:
            The DriveUpload, or None if the upload failed
        """
        try:
            if not mime_type:
                mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

            if not file_hash:
                file_hash = self._calculate_file_hash(file_path)

            log_with_context(
                logging.DEBUG,
                f"MD5 hash for {filename}: {file_hash}",
                channel=self._get_current_channel(),
            )

            # Ensure the folder's files are pre-cached if we haven't done so already
            if folder_id not in self.folders_pre_cached:
                self.pre_cache_folder_file_hashes(folder_id, shared_drive_id)

            if slack_file_id:
                existing = self.find_uploaded_slack_file(folder_id, slack_file_id)
                if existing is not None:
                    return existing

            # Check if this file already exists in the folder by its hash
            existing_file_id, existing_url = (
                self._find_file_by_hash(file_hash, filename, folder_id, shared_drive_id)
                if not target_mime_type
                else (None, None)
            )

            # If the file already exists, reuse it instead of uploading again
            if existing_file_id:
                log_with_context(
                    logging.DEBUG,
                    f"Reusing existing file with same hash: {filename} (ID: {existing_file_id})",
                    channel=self._get_current_channel(),
                )
                return DriveUpload(
                    existing_file_id, existing_url, reused=True, source=SOURCE_HASH
                )

            # File doesn't exist yet, proceed with upload
            log_with_context(
                logging.DEBUG,
                f"Uploading file {filename} with MIME type {mime_type} to folder {folder_id}",
                channel=self._get_current_channel(),
            )

            file_metadata: dict[str, Any] = {"name": filename, "parents": [folder_id]}
            if target_mime_type:
                file_metadata["mimeType"] = target_mime_type
            if slack_file_id:
                app_properties = {DRIVE_APP_PROPERTY_SLACK_FILE_ID: slack_file_id}
                if channel:
                    app_properties[DRIVE_APP_PROPERTY_SLACK_CHANNEL] = (
                        _fit_app_property(DRIVE_APP_PROPERTY_SLACK_CHANNEL, channel)
                    )
                file_metadata["appProperties"] = app_properties

            media = MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=True,
                chunksize=DRIVE_UPLOAD_CHUNK_BYTES,
            )

            # Upload with appropriate parameters
            file = self.drive_service.create_file(
                body=file_metadata,
                media_body=media,
                fields="id,webViewLink",
                supports_all_drives=bool(shared_drive_id),
            )

            file_id = file.get("id")
            if not file_id:
                return None
            url = file.get("webViewLink")
            self.remember_file(
                folder_id,
                file_id,
                url,
                md5=None if target_mime_type else file_hash,
                slack_file_id=slack_file_id,
            )

            log_with_context(
                logging.DEBUG,
                f"Successfully uploaded file {filename} to Drive (ID: {file_id})",
                channel=self._get_current_channel(),
            )

            return DriveUpload(file_id, url)

        except HttpError as e:
            log_with_context(
                logging.ERROR,
                f"Failed to upload file {filename} to Drive: {e}",
                channel=self._get_current_channel(),
            )
            return None

    @staticmethod
    def poster_permission_change(
        file_id: str, message_poster_email: str, shared_drive_id: str | None = None
    ) -> PermissionChange:
        """Describe the poster's ``writer`` grant, for batching."""
        return PermissionChange(
            file_id,
            body={
                "type": "user",
                "role": DRIVE_ROLE_WRITER,
                "emailAddress": message_poster_email,
            },
            supports_all_drives=bool(shared_drive_id),
        )

    def set_message_poster_permission(
        self,
        file_id: str,
        message_poster_email: str,
        shared_drive_id: str | None = None,
    ) -> bool:
        """Give the message poster ``writer`` access to a file.

        Args:
            file_id: ID of the file
            message_poster_email: Email of the message poster
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            True if successful, False otherwise
        """
        log_with_context(
            logging.DEBUG,
            f"Setting writer permission for message poster {message_poster_email} on file {file_id}",
            channel=self._get_current_channel(),
        )
        try:
            permission = {
                "type": "user",
                "role": DRIVE_ROLE_WRITER,
                "emailAddress": message_poster_email,
            }
            self.drive_service.create_permission(
                file_id=file_id,
                body=permission,
                fields="id",
                send_notification_email=False,
                supports_all_drives=bool(shared_drive_id),
            )

            log_with_context(
                logging.DEBUG,
                f"Successfully set writer permission for message poster {message_poster_email} on file {file_id}",
                channel=self._get_current_channel(),
            )
            return True
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to set writer permission for message poster on file {file_id}: {e}",
                channel=self._get_current_channel(),
            )
            return False
