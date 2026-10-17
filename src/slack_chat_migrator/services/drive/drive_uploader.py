"""
Google Drive file upload functionality.
"""

from __future__ import annotations

import enum
import hashlib
import logging
import mimetypes
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from slack_chat_migrator.services.drive.pending import PendingPermissions
from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    DRIVE_CALL_ERRORS,
    PermissionChange,
    http_status,
    is_drive_call_error,
    is_fatal_local_error,
)

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
from slack_chat_migrator.utils.api import (
    escape_drive_query_value,
)
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

logger = logging.getLogger("slack_chat_migrator")


class DriveUploadSource(str, enum.Enum):
    """Where a :class:`DriveUpload` came from."""

    NEW = "new"  # uploaded now
    HASH = "hash"  # identical content already in the folder
    #: This Slack file, tagged with its Slack ID: uploaded earlier in this
    #: run (for another space) or by an earlier run.
    SLACK_ID = "slack_id"


SOURCE_NEW = DriveUploadSource.NEW
SOURCE_HASH = DriveUploadSource.HASH
SOURCE_SLACK_ID = DriveUploadSource.SLACK_ID


@dataclass(frozen=True)
class DriveUpload:
    """A Drive file holding an attachment.

    Attributes:
        file_id: Drive file ID.
        url: ``webViewLink`` of the file, if known.
        source: Whether the file was uploaded now or an existing one is
            linked, and how it was found.
    """

    file_id: str
    url: str | None = None
    source: DriveUploadSource = DriveUploadSource.NEW

    @property
    def reused(self) -> bool:
        """True when an existing Drive file is linked instead of a new upload.

        A reused file keeps its owner, unless an earlier run's transfer of
        it failed (see ``FileHandler._apply_file_permissions``).
        """
        return self.source != DriveUploadSource.NEW


@dataclass(frozen=True)
class DriveUploadFailure:
    """Why :meth:`DriveFileUploader.upload_file_to_drive` has no file.

    Attributes:
        reason: The cause, for the log and the migration report.
        conversion_failed: Drive refused to convert the upload into the
            requested Google type (HTTP 400, 413, 415 or 500 with a target
            type).  A plain upload of the same file may still work.
    """

    reason: str
    conversion_failed: bool = False


#: Statuses that, for an upload with a target Google type, mean the
#: conversion failed rather than the upload: content Drive can't convert
#: (400, 415), too large to convert (413), or the converter failing (500,
#: still failing after the retries).
_CONVERSION_FAILURE_STATUSES = frozenset({400, 413, 415, 500})

#: Errors that leave an upload without a file: any Drive call error
#: (HttpError, network errors, credentials that can't be refreshed), or an
#: OSError reading the temp file.
_UPLOAD_ERRORS: tuple[type[BaseException], ...] = (OSError, *DRIVE_CALL_ERRORS)


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
        *,
        on_listing_failure: Callable[[str, BaseException], None] | None = None,
        on_flag_write_failure: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        """Initialize the DriveFileUploader.

        It makes grants only when FileHandler asks
        (:meth:`set_message_poster_permission`) and never decides who is
        internal; FileHandler does.

        Args:
            drive_service: DriveAdapter instance wrapping the Google Drive API
            on_listing_failure: Called with the folder ID and the error the
                first time a folder's files can't be listed, so the caller
                can count folders, not attempts.
            on_flag_write_failure: Called with the file ID and the error when
                a pending grant or transfer can't be recorded on the file.
        """
        self.drive_service = drive_service
        self._on_listing_failure = on_listing_failure or (lambda _folder, _e: None)
        # Folder ID -> failed listings; a folder that failed is listed again
        # on its next upload.
        self.listing_failures: dict[str, int] = {}
        # Every cache is scoped to a folder: content found in one channel's
        # folder is never linked from another channel.
        # (folder_id, md5) -> file
        self.file_hash_cache: dict[tuple[str, str], _CachedFile] = {}
        # (folder_id, Slack file ID) -> file, from the appProperties tag
        self.slack_file_cache: dict[tuple[str, str], _CachedFile] = {}
        self.folders_pre_cached: set[str] = set()
        # (folder_id, Slack file ID) searched for and not found, so a folder
        # that couldn't be listed is searched once per file, not twice.
        self._slack_id_misses: set[tuple[str, str]] = set()
        # Drive files created by this run.
        self.created_this_run: set[str] = set()
        #: Poster grants and transfers that failed, in this run or (read
        #: from the files' appProperties) an earlier one.
        self.pending = PendingPermissions(
            drive_service, on_write_failure=on_flag_write_failure
        )
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

        If the listing fails, the folder isn't marked as cached: the next
        upload into it lists it again, and until then each file is looked up
        with its own search (by Slack ID, then by MD5).  The first failure
        per folder is logged as a warning and reported to
        *on_listing_failure*; nothing is raised.

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
                    self.pending.read(file_id, app_properties)
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

        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            failures = self.listing_failures.get(folder_id, 0) + 1
            self.listing_failures[folder_id] = failures
            log_with_context(
                logging.WARNING if failures == 1 else logging.DEBUG,
                f"Could not list the files of Drive folder {folder_id} ({e});"
                " looking up each file on its own instead, and listing the"
                " folder again with the next upload",
                channel=self._get_current_channel(),
            )
            if failures == 1:
                self._on_listing_failure(folder_id, e)
            return 0

    def find_uploaded_slack_file(
        self,
        folder_id: str,
        slack_file_id: str,
        shared_drive_id: str | None = None,
    ) -> DriveUpload | None:
        """Find this Slack file's upload into *folder_id*.

        The upload was made earlier in this run or by an earlier run, and is
        tagged with the Slack file ID.  A pre-cached folder is answered from
        the cache.  For a folder whose listing failed, Drive is searched for
        the tag instead, so deduplication by Slack ID keeps working.

        Args:
            folder_id: Target folder.
            slack_file_id: Slack file ID.
            shared_drive_id: ID of the shared drive (if applicable).

        Returns:
            The existing upload (``source == SOURCE_SLACK_ID``), or None.

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` if the search fails, so the caller
            doesn't upload a second copy of a file that may already be there.
        """
        key = (folder_id, slack_file_id)
        cached = self.slack_file_cache.get(key)
        if (
            cached is None
            and folder_id not in self.folders_pre_cached
            and key not in self._slack_id_misses
        ):
            cached = self._search_slack_file(folder_id, slack_file_id, shared_drive_id)
        if cached is None:
            return None
        return DriveUpload(cached[0], cached[1], source=SOURCE_SLACK_ID)

    def _search_slack_file(
        self, folder_id: str, slack_file_id: str, shared_drive_id: str | None
    ) -> _CachedFile | None:
        """Search *folder_id* for a file tagged with *slack_file_id*.

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` (logged as a warning).
        """
        safe_id = escape_drive_query_value(slack_file_id)
        safe_folder = escape_drive_query_value(folder_id)
        query = (
            f"appProperties has {{ key='{DRIVE_APP_PROPERTY_SLACK_FILE_ID}'"
            f" and value='{safe_id}' }} and '{safe_folder}' in parents"
            " and trashed=false"
        )
        search_kwargs: dict[str, Any] = {
            "q": query,
            "fields": "files(id, webViewLink, appProperties)",
        }
        if shared_drive_id:
            search_kwargs.update(
                {
                    "corpora": "drive",
                    "drive_id": shared_drive_id,
                    "include_items_from_all_drives": True,
                    "supports_all_drives": True,
                }
            )
        try:
            files = self.drive_service.list_files(**search_kwargs).get("files", [])
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.WARNING,
                f"Could not search Drive folder {folder_id} for Slack file"
                f" {slack_file_id} ({e}); not uploading it, so no second copy"
                " is made. A re-run retries it.",
                channel=self._get_current_channel(),
            )
            raise
        found = next((f for f in files if f.get("id")), None)
        if found is None:
            self._slack_id_misses.add((folder_id, slack_file_id))
            return None
        self.pending.read(found["id"], found.get("appProperties") or {})
        url = found.get("webViewLink")
        self.remember_file(folder_id, found["id"], url, slack_file_id=slack_file_id)
        return found["id"], url

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

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` if the search fails.
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

        search_kwargs: dict[str, Any] = {
            "q": query,
            "fields": "files(id,name,webViewLink,appProperties)",
        }
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

        files = self.drive_service.list_files(**search_kwargs).get("files", [])
        if files and files[0].get("id"):
            file_id = files[0]["id"]
            web_view_link = files[0].get("webViewLink")
            self.pending.read(file_id, files[0].get("appProperties") or {})
            self.remember_file(folder_id, file_id, web_view_link, md5=file_hash)
            log_with_context(
                logging.DEBUG,
                f"Found existing file with same hash: {filename} (ID: {file_id})",
                channel=self._get_current_channel(),
            )
            return file_id, web_view_link
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
        pre_cache: bool = True,
    ) -> DriveUpload | DriveUploadFailure:
        """Upload a file to Google Drive, or reuse an identical one.

        If a file with the same MD5 hash already exists in the folder, that
        file is returned (``source=HASH``) instead of uploading a duplicate.
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
            pre_cache: List the folder into the caches first, unless it is
                cached already.  Pass False when the caller has just done
                so, so a folder whose listing failed isn't listed twice.

        Returns:
            The DriveUpload, or why there is none: any of
            ``DRIVE_CALL_ERRORS`` (an HttpError, a network error that
            outlasted the retries, credentials that can't be refreshed) from
            the dedup searches or the upload, an OSError reading the file,
            or Drive returning no file ID.  Nothing is raised; the caller
            logs and counts the failure.
        """
        try:
            mime_type = (
                mime_type
                or mimetypes.guess_type(filename)[0]
                or "application/octet-stream"
            )
            if not file_hash:
                file_hash = self._calculate_file_hash(file_path)

            log_with_context(
                logging.DEBUG,
                f"MD5 hash for {filename}: {file_hash}",
                channel=self._get_current_channel(),
            )

            if pre_cache and folder_id not in self.folders_pre_cached:
                self.pre_cache_folder_file_hashes(folder_id, shared_drive_id)

            existing = self._find_existing(
                filename,
                folder_id,
                shared_drive_id,
                slack_file_id=slack_file_id,
                file_hash=None if target_mime_type else file_hash,
            )
            if existing is not None:
                return existing
            return self._create(
                file_path,
                filename,
                folder_id,
                shared_drive_id,
                mime_type=mime_type,
                file_hash=file_hash,
                slack_file_id=slack_file_id,
                channel=channel,
                target_mime_type=target_mime_type,
            )

        except HttpError as e:
            status = http_status(e)
            return DriveUploadFailure(
                f"Drive refused the upload: {e}",
                conversion_failed=bool(target_mime_type)
                and status in _CONVERSION_FAILURE_STATUSES,
            )
        except _UPLOAD_ERRORS as e:
            if is_fatal_local_error(e):
                raise  # a full disk would fail every file in turn
            return DriveUploadFailure(f"{type(e).__name__}: {e}")

    def _find_existing(
        self,
        filename: str,
        folder_id: str,
        shared_drive_id: str | None,
        *,
        slack_file_id: str | None,
        file_hash: str | None,
    ) -> DriveUpload | DriveUploadFailure | None:
        """This Slack file's upload, or a file with the same content, if any.

        Returns a failure when a search fails: uploading anyway could make a
        second copy.
        """
        try:
            if slack_file_id:
                existing = self.find_uploaded_slack_file(
                    folder_id, slack_file_id, shared_drive_id
                )
                if existing is not None:
                    return existing
            if not file_hash:
                return None
            existing_file_id, existing_url = self._find_file_by_hash(
                file_hash, filename, folder_id, shared_drive_id
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            return DriveUploadFailure(
                "could not check the folder for an earlier upload"
                f" ({type(e).__name__}: {e})"
            )
        if not existing_file_id:
            return None
        log_with_context(
            logging.DEBUG,
            f"Reusing existing file with same hash: {filename} (ID: {existing_file_id})",
            channel=self._get_current_channel(),
        )
        return DriveUpload(existing_file_id, existing_url, source=SOURCE_HASH)

    def _create(
        self,
        file_path: str,
        filename: str,
        folder_id: str,
        shared_drive_id: str | None,
        *,
        mime_type: str,
        file_hash: str,
        slack_file_id: str | None,
        channel: str | None,
        target_mime_type: str | None,
    ) -> DriveUpload | DriveUploadFailure:
        """Upload a new file, tagged with its Slack file ID and channel.

        Raises:
            Any error from the upload; :meth:`upload_file_to_drive` turns
            them into a :class:`DriveUploadFailure`.
        """
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
                app_properties[DRIVE_APP_PROPERTY_SLACK_CHANNEL] = _fit_app_property(
                    DRIVE_APP_PROPERTY_SLACK_CHANNEL, channel
                )
            file_metadata["appProperties"] = app_properties

        media = MediaFileUpload(
            file_path,
            mimetype=mime_type,
            resumable=True,
            chunksize=DRIVE_UPLOAD_CHUNK_BYTES,
        )
        file = self.drive_service.create_file(
            body=file_metadata,
            media_body=media,
            fields="id,webViewLink",
            supports_all_drives=bool(shared_drive_id),
        )

        file_id = file.get("id")
        if not file_id:
            return DriveUploadFailure("Drive returned no file ID")
        url = file.get("webViewLink")
        self.created_this_run.add(file_id)
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
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.WARNING,
                f"Failed to set writer permission for message poster on file {file_id}: {e}",
                channel=self._get_current_channel(),
            )
            return False
