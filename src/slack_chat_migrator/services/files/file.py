"""
File handling module for the Slack to Google Chat migration tool
"""

from __future__ import annotations

import dataclasses
import logging
import mimetypes
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal, get_args

import requests
from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DIRECT_UPLOAD_MAX_BYTES,
    GOOGLE_DOCS_MIME_TYPE,
    GOOGLE_SHEETS_MIME_TYPE,
    SLACK_FILE_HOST_SUFFIXES,
)
from slack_chat_migrator.core.config import ConversationType, MigrationConfig
from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.exceptions import APIError, DriveQuotaError
from slack_chat_migrator.services.chat import ChatFileUploader
from slack_chat_migrator.services.drive import (
    DriveFileUploader,
    FolderManager,
    SharedDriveManager,
)
from slack_chat_migrator.services.drive.drive_uploader import (
    DriveUploadFailure,
    DriveUploadSource,
)
from slack_chat_migrator.services.drive.folder_manager import (
    FolderLookup,
    FolderSyncResult,
)
from slack_chat_migrator.services.drive.pending import Target
from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    DRIVE_CALL_ERRORS,
    is_drive_call_error,
    is_fatal_local_error,
    is_transient_error,
)
from slack_chat_migrator.services.files.file_download import (
    AUTH_FAILURE_REASONS,
    REASON_DISALLOWED_HOST,
    REASON_EXTERNAL,
    DownloadResult,
    DownloadStatus,
    create_drive_reference,
    download_file,
    expected_size,
    google_link_status,
    is_slack_file_host,
    unavailable_reason,
)
from slack_chat_migrator.services.files.file_permissions import (
    ownership_transfer_change,
    transfer_file_ownership,
)
from slack_chat_migrator.services.files.temp_files import (
    AttachmentTempDir,
    remove_temp_file,
    sweep_stale_temp_dirs,
)
from slack_chat_migrator.types import FileUploadStats, UploadResult
from slack_chat_migrator.utils.domains import (
    internal_domains,
    is_external_email,
)
from slack_chat_migrator.utils.logging import log_with_context
from slack_chat_migrator.utils.mime import (
    is_slack_canvas,
    is_slack_list,
    resolve_drive_mime_type,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.chat_adapter import ChatAdapter
    from slack_chat_migrator.services.drive_adapter import DriveAdapter

logger = logging.getLogger("slack_chat_migrator")

#: Errors that fail one file without stopping the run: any Drive call error,
#: a download error, or an OSError on the temp file, except the local faults
#: in ``FATAL_LOCAL_ERRNOS`` (a full disk), which are re-raised.
_FILE_ERRORS: tuple[type[BaseException], ...] = (
    requests.RequestException,
    OSError,
    *DRIVE_CALL_ERRORS,
)


#: ``UploadResult.skip_reason`` for files that no retry or token can fetch
#: (``DownloadStatus.UNAVAILABLE``: deleted, hidden by a plan limit,
#: external, not readable by the exporting workspace, no URL, 404/410).  The
#: detailed reason is in ``metadata["unavailable_reason"]``.  Such files are
#: not failures; refused downloads (401/403, sign-in pages) are.
SKIP_REASON_UNAVAILABLE = DownloadStatus.UNAVAILABLE.value
SKIP_REASON_GOOGLE_DOCS_LINK = "google_docs_link"

#: ``UploadResult.metadata`` key: True when only the conversion into a
#: Google type failed, so a plain upload may still work.  Written by
#: :meth:`FileHandler._execute_drive_upload`, read by
#: :func:`_conversion_failed`.
META_CONVERSION_FAILED = "conversion_failed"


def _normalize_email(email: str) -> str:
    return email.strip().lower()


def _conversion_failed(result: UploadResult) -> bool:
    """Whether *result* failed only because Drive refused the conversion."""
    return bool(result.metadata.get(META_CONVERSION_FAILED))


#: What a Slack canvas or list becomes; see :class:`SlackDocument`.
DocumentKind = Literal["canvas", "list"]


#: How attachments are stored; see :class:`DriveStatus`.
DriveMode = Literal["not_initialized", "folder", "shared_drive", "my_drive", "none"]

#: Modes without an attachment root folder.
_NO_ROOT_MODES: frozenset[DriveMode] = frozenset({"not_initialized", "none"})


@dataclass(frozen=True)
class DriveStatus:
    """Where attachments go on Drive, or why they can't go anywhere.

    Attributes:
        mode: ``shared_drive`` (the shared drive's root), ``my_drive`` (a
            regular Drive folder, because the account can't use or create
            shared drives), ``folder`` (a root folder given to FileHandler),
            ``not_initialized`` (before Drive is set up) or ``none`` (Drive
            couldn't be set up).
        root_folder_id: The attachment root; set exactly for the modes that
            have one (``shared_drive``, ``my_drive``, ``folder``).
        detail: One line describing the setup: what is used, and why a
            fallback was needed or what failed.
    """

    mode: DriveMode
    root_folder_id: str | None = None
    detail: str = ""

    def __post_init__(self) -> None:
        """Reject an unknown mode, and a root folder that doesn't fit the mode."""
        if self.mode not in get_args(DriveMode):
            raise ValueError(f"unknown DriveStatus mode {self.mode!r}")
        if (self.mode in _NO_ROOT_MODES) != (not self.root_folder_id):
            raise ValueError(
                f"a DriveStatus in mode {self.mode!r} must"
                f"{' not' if self.mode in _NO_ROOT_MODES else ''} have a root folder"
            )

    @property
    def ready(self) -> bool:
        """There is an attachment root folder, so uploads can be tried."""
        return bool(self.root_folder_id)

    @property
    def shared_drive_id(self) -> str | None:
        """The shared drive (its root is the attachment root), if one is used."""
        return self.root_folder_id if self.mode == "shared_drive" else None


@dataclass(frozen=True)
class SlackDocument:
    """How a Slack canvas or list is fetched and stored on Drive.

    Attributes:
        kind: A canvas or a list.
        source_mime: Type of the downloaded content (HTML or CSV).
        extension: File extension for a plain (unconverted) upload.
        target_mime: Google Docs/Sheets type to convert into, or None to
            upload the HTML/CSV file as is.
        url: Download URL to use instead of the usual choice (list CSV).
        title: File name without extension.
    """

    kind: DocumentKind
    source_mime: str
    extension: str
    target_mime: str | None
    url: str | None
    title: str

    @property
    def plain_name(self) -> str:
        """File name for an unconverted upload."""
        return self.title + self.extension


def _document_title(file_obj: dict[str, Any], extension: str) -> str:
    """A readable Drive name for a canvas or list: its title, tidied up."""
    raw = file_obj.get("title") or file_obj.get("name") or ""
    title = " ".join(str(raw).split())  # collapse whitespace, drop newlines
    title = "".join(ch for ch in title if ch.isprintable())
    if title.lower().endswith(extension):
        title = title[: -len(extension)].rstrip()
    return title[:200] or f"file_{file_obj.get('id') or 'unknown'}"


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
        channels_meta: Mapping[str, Any] | None = None,
    ) -> None:
        """Initialize the FileHandler.

        Args:
            drive_service: The Drive API service instance
            chat_service: The Chat API service instance
            folder_id: The ID of the root folder in Drive to store files (can be None for auto-creation)
            config: Migration configuration
            workspace_domain: The workspace admin's domain.  With
                ``config.workspace_domains`` it makes up the internal domains
                when there is no *user_resolver*.
            user_map: Slack user ID to Google email mapping
            user_resolver: Decides who is external
                (``is_external_user``); may be None.
            state: Mutable migration state
            dry_run: Whether to run in dry run mode
            channels_meta: Channel name -> ``channels.json`` record.  A
                channel listed there and not ``is_private`` counts as public;
                every other conversation (private channels, DMs, group DMs)
                gets a limited-access folder when
                ``config.restrict_private_channel_folders`` is set.  Use
                :meth:`set_conversation_type` to say it explicitly instead.
        """
        self.drive_service = drive_service
        self.chat_service = chat_service
        self.config = config
        self.workspace_domain = workspace_domain
        self._internal_domains = internal_domains(
            workspace_domain, config.workspace_domains
        )
        self.user_map = user_map
        self.user_resolver = user_resolver
        self.state = state
        self.dry_run = dry_run

        # Results by (Slack file ID, channel, space).  A file shared to
        # several channels gets its own upload in each channel's folder, and
        # a direct Chat upload (bound to one space) is never reused elsewhere.
        self.processed_files: dict[tuple[str, str, str], UploadResult] = {}

        # (drive_file_id, email) pairs that already received writer access
        self._granted_posters: set[tuple[str, str]] = set()

        # Conversation privacy, for limited-access folders
        self.channels_meta: Mapping[str, Any] = channels_meta or {}
        self._conversation_types: dict[str, ConversationType] = {}
        # Hosts whose downloads were refused (each is explained once).
        self._refused_hosts: set[str] = set()
        # Channels whose missing folder was logged as an error.
        self._folder_errors_logged: set[str | None] = set()

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
            "poster_grant_failures": 0,
            "pending_retried": 0,
            "pending_retry_failed": 0,
            "pending_flag_failures": 0,
            "pending_clear_failures": 0,
            "pending_sweep_failures": 0,
            "pending_skipped_unknown": 0,
            "pending_skipped_unmapped": 0,
            "pending_skipped_external": 0,
            "pending_skipped_transfers_off": 0,
            "pending_skipped_not_owner": 0,
            "folder_listing_failures": 0,
            "external_folder_grant_failures": 0,
            "folder_sync_failures": 0,
            "folder_grant_failures": 0,
            "folder_removal_failures": 0,
            "restricted_folder_failures": 0,
            "channel_folder_failures": 0,
            "download_auth_failures": 0,
            "files_by_channel": {},
        }

        # Downloads stream into a per-run temp dir; remove what killed runs
        # left behind before creating our own.
        self.temp_dir = AttachmentTempDir(lambda: self.state.context.output_dir)
        sweep_stale_temp_dirs(self.state.context.output_dir)

        # Initialize modular services
        self.shared_drive_manager = SharedDriveManager(drive_service, config)
        self.folder_manager = FolderManager(
            drive_service,
            workspace_domain,
            workspace_domains=config.workspace_domains,
            is_external=self._is_external_email,
            protected_emails=self._protected_emails,
            on_sync_result=self._count_folder_sync,
        )
        self.drive_uploader = DriveFileUploader(
            drive_service,
            on_listing_failure=self._count_listing_failure,
            on_flag_write_failure=self._count_flag_write_failure,
        )
        self.chat_uploader = ChatFileUploader(chat_service)

        # Where attachments go: the one source of truth for the root folder,
        # the shared drive and whether Drive was set up.  Drive is set up on
        # first use (after the permission checks), not here.
        self._drive_status = DriveStatus(
            "not_initialized", detail="Drive is not initialized yet"
        )
        if folder_id:
            self.folder_id = folder_id

    @property
    def _root_folder_id(self) -> str | None:
        """The attachment root folder (from :attr:`drive_status`)."""
        return self._drive_status.root_folder_id

    @property
    def _shared_drive_id(self) -> str | None:
        """The shared drive in use (from :attr:`drive_status`)."""
        return self._drive_status.shared_drive_id

    @property
    def _drive_initialized(self) -> bool:
        """Whether Drive has been set up (from :attr:`drive_status`)."""
        return self._drive_status.mode != "not_initialized"

    def ensure_drive_initialized(self) -> None:
        """Set up Drive for attachments (once), and check that it can be used.

        Call this after the permission checks; the migrate preflight does,
        so a run never starts without a place for its attachments.

        Raises:
            APIError: If Drive can't be used for attachments; the message
                gives the cause from :attr:`drive_status`.
            ConfigError: If the configured ``shared_drive.id`` can't be
                opened.
            DriveQuotaError: If a Drive quota is used up.
        """
        self._initialize_drive()
        if not self._drive_status.ready:
            detail = self._drive_status.detail.rstrip(".")
            raise APIError(
                f"Google Drive can't be used for attachments: {detail}."
                " Check network access and the workspace admin's Drive access,"
                " then run again."
            )

    def _initialize_drive(self) -> None:
        """Set up Drive once; a failure is recorded in :attr:`drive_status`.

        Unlike :meth:`ensure_drive_initialized` this doesn't raise when
        Drive can't be used: each upload then fails with the cause.

        Raises:
            ConfigError: If the configured ``shared_drive.id`` can't be
                opened; :attr:`drive_status` stays ``not_initialized``.  (A
                live run's preflight raises it first.)
            DriveQuotaError: If a Drive quota is used up (likewise).
        """
        if not self._drive_initialized:
            log_with_context(
                logging.INFO,
                "Initializing Google Drive structures (shared drive and folder hierarchy)...",
                channel=self._get_current_channel(),
            )
            self._initialize_shared_drive_and_folder()

    @property
    def folder_id(self) -> str | None:
        """Backward compatibility property for the root folder ID.

        Returns:
            The root Google Drive folder ID (Drive is set up first), or None
            if Drive couldn't be set up.
        """
        self._initialize_drive()
        return self._root_folder_id

    @folder_id.setter
    def folder_id(self, value: str | None) -> None:
        """Backward compatibility setter for the root folder ID.

        Args:
            value: The Drive folder ID to set, or None to clear.
        """
        self._drive_status = (
            DriveStatus("folder", value, f"attachment folder {value}")
            if value
            else DriveStatus("none", detail="no attachment folder is set")
        )

    @property
    def drive_status(self) -> DriveStatus:
        """Where attachments go, or why they can't go anywhere.

        Set when Drive is first set up (by :meth:`ensure_drive_initialized`,
        the first upload or the first read of :attr:`folder_id`) or when
        :attr:`folder_id` is assigned; reading it initializes nothing.
        """
        return self._drive_status

    @property
    def drive_ready(self) -> bool:
        """Whether Drive uploads can be tried (``drive_status.ready``)."""
        return self._drive_status.ready

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
        (single source of truth).  Only when Drive says this account can't
        use or create shared drives is a regular Drive folder used instead.
        A network error, credentials that can't be refreshed or any other
        failure never lead to that fallback: :attr:`drive_status` becomes
        ``none`` with the cause.  Then pre-caches the root folder for
        deduplication.

        Raises:
            ConfigError: If the configured ``shared_drive.id`` can't be
                opened.
            DriveQuotaError: If a Drive quota is used up.
        """
        fallback_name = "Imported Slack Attachments"
        try:
            # SharedDriveManager handles: validate configured ID → find by
            # name → create new drive.  No need to duplicate that here.
            shared_drive_id = self.shared_drive_manager.get_or_create_shared_drive()
        except APIError as e:
            self._drive_status = DriveStatus("none", detail=str(e))
            log_with_context(
                logging.ERROR, f"Drive is not available for attachments: {e}"
            )
            return

        if shared_drive_id:
            self._drive_status = DriveStatus(
                "shared_drive", shared_drive_id, f"shared drive {shared_drive_id}"
            )
            log_with_context(
                logging.DEBUG,
                f"Using shared drive root as attachment folder: {shared_drive_id}",
            )
        else:
            why = self.shared_drive_manager.last_error or "no shared drive"
            log_with_context(
                logging.WARNING,
                f"Shared drives can't be used ({why}); storing attachments in"
                f" the regular Drive folder {fallback_name!r} instead",
            )
            lookup = self.folder_manager.find_or_create_root_folder(fallback_name)
            self._drive_status = (
                DriveStatus(
                    "my_drive",
                    lookup.folder_id,
                    f"regular Drive folder {fallback_name!r} ({why})",
                )
                if lookup.folder_id
                else DriveStatus(
                    "none", detail=f"{why}; regular Drive folder: {lookup.error}"
                )
            )
            if not lookup.folder_id:
                log_with_context(
                    logging.ERROR,
                    "Drive is not available for attachments"
                    f" ({self._drive_status.detail})",
                )

        self._pre_cache_root_folder()

    def _pre_cache_root_folder(self) -> None:
        """Pre-cache the root folder's files and learn its channel folders.

        Every page of the subfolder listing is read, and each folder's ID is
        handed to the folder cache, so finding a channel's folder later
        needs no search.  A channel folder's own files are cached when the
        first upload goes into it.
        """
        if not self._root_folder_id:
            return

        # A failed listing is logged, counted and retried there.
        file_count = self.drive_uploader.pre_cache_folder_file_hashes(
            self._root_folder_id, self._shared_drive_id
        )
        log_with_context(
            logging.DEBUG, f"Pre-cached {file_count} files from root folder"
        )

        try:
            folders = self.folder_manager.list_child_folders(
                self._root_folder_id, self._shared_drive_id
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            # Don't fail the entire process if subfolder caching fails
            log_with_context(
                logging.WARNING,
                f"Error pre-caching channel subfolders: {e}. Continuing with available cache.",
            )
            return

        for folder in folders:
            self.folder_manager.remember_folder(
                self._root_folder_id,
                folder.get("name", ""),
                folder["id"],
                restricted=bool(folder.get("inheritedPermissionsDisabled")),
            )
        log_with_context(
            logging.DEBUG,
            f"Found {len(folders)} existing channel folders",
        )

    def upload_attachment(
        self,
        file_obj: dict[str, Any],
        channel: str | None = None,
        space: str | None = None,
        user_service: ChatAdapter | None = None,
        sender_email: str | None = None,
        *,
        sender_user_id: str | None = None,
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
            sender_user_id: Slack user ID of the message sender, stored with
                a failed grant so a later run can map the poster again.
                Without it, the file's uploader is used when it maps to
                *sender_email*, else the one user mapped to that address.

        Returns:
            UploadResult with upload details. Check ``.success``, ``.skipped``,
            or ``.error`` to determine outcome.  Files that can't be fetched
            are skipped with ``skip_reason == "unavailable"``.

        Raises:
            DriveQuotaError: If a Drive quota is used up (every later file
                would fail the same way); the file gets no result.
            OSError: For a local fault in ``FATAL_LOCAL_ERRNOS`` (a full
                disk), likewise.
        """
        self._sync_channel_context()
        file_id = file_obj.get("id") or "unknown"
        name = file_obj.get("name") or f"file_{file_id}"

        try:
            self._initialize_drive()
            self._update_file_stats(file_obj, channel)

            log_with_context(
                logging.DEBUG,
                f"Processing file: {name} (MIME: {file_obj.get('mimetype')},"
                f" Size: {file_obj.get('size', 0)})",
                channel=channel,
                file_id=file_id,
            )

            cache_key = self._processed_key(file_id, channel, space)
            cached = self._check_attachment_cache(cache_key, name, channel)
            if cached is not None:
                return dataclasses.replace(cached, cached=True)

            result = self.process_file(
                file_obj,
                channel,
                space,
                user_service,
                sender_email,
                sender_user_id=sender_user_id,
            )
            self._record_result(cache_key, result)
            return result

        except _FILE_ERRORS as e:
            if is_fatal_local_error(e):
                raise  # a full disk would fail every file in turn
            self._increment_stat("failed_uploads")
            log_with_context(
                logging.ERROR,
                f"Error uploading file: {e!s}",
                channel=channel,
                file_id=file_id,
                error=str(e),
            )
            return UploadResult(
                error=str(e), name=name, retryable=is_transient_error(e)
            )

    def process_file(
        self,
        file_obj: dict[str, Any],
        channel: str | None = None,
        space: str | None = None,
        user_service: ChatAdapter | None = None,
        sender_email: str | None = None,
        *,
        sender_user_id: str | None = None,
    ) -> UploadResult:
        """Fetch one Slack file and upload it: the per-file unit of work.

        Downloads (streamed to a temp file), then uploads directly to Chat
        or to Drive; the temp file is deleted before returning.

        The caller (:meth:`upload_attachment`) counts the outcome
        (``total_files``, ``files_by_channel``, ``external_user_files``,
        ``direct_uploads``, ``drive_uploads``, ``failed_uploads``,
        ``unavailable_files``, ``skipped_files``) and records it in
        :attr:`processed_files`.  What happens along the way is counted
        here: ``ownership_transferred``, ``ownership_transfer_failed``,
        ``poster_grant_failures``, ``pending_flag_failures``,
        ``pending_clear_failures``, ``restricted_folder_failures``,
        ``channel_folder_failures``, ``download_auth_failures`` and
        ``folder_listing_failures``.

        Args:
            file_obj: The file object from Slack.
            channel: Channel name, used for the Drive folder.
            space: Chat space for a direct upload, if any.
            user_service: Optional user-specific Chat service for uploads.
            sender_email: Email of the message sender.
            sender_user_id: Slack user ID of the message sender (see
                :meth:`upload_attachment`).

        Returns:
            The UploadResult (success, skip or error).
        """
        file_id = file_obj.get("id") or "unknown"
        name = file_obj.get("name") or f"file_{file_id}"
        poster_id = self._poster_user_id(file_obj, sender_email, sender_user_id)

        # A Drive-bound file already uploaded into this channel's folder
        # (earlier in this run or by an earlier run) is linked again without
        # downloading it.
        folder_id: str | None = None
        if self._is_drive_bound(file_obj, space):
            lookup = self._choose_upload_folder(channel, file_id)
            if lookup.folder_id is None:
                # Fail before downloading a file there's nowhere to put.
                return self._folder_failure(lookup, name, channel)
            folder_id = lookup.folder_id
            existing = self._find_existing_upload(
                file_obj, folder_id, name, channel, sender_email, poster_id=poster_id
            )
            if existing is not None:
                return existing

        document = self.slack_document(file_obj)
        if document is None:
            download = self._download_file(file_obj)
        else:
            # Canvases export as HTML and lists as CSV; their recorded
            # ``size`` is not the size of that rendering.
            download = self._download_file(
                file_obj,
                url=document.url,
                allow_html=document.kind == "canvas",
                check_size=False,
            )
        try:
            outcome = self._handle_download_outcome(
                download, file_obj, name, channel, file_id
            )
            if outcome is not None:
                return outcome
            file_path = download.file_path  # an OK result always has one

            if document is not None:
                return self._upload_document(
                    file_obj,
                    document,
                    download,
                    channel,
                    sender_email,
                    folder_id,
                    poster_id=poster_id,
                )

            mime_type = self._resolve_mime_type(file_obj, name, channel, file_id)

            # Try direct Chat upload for eligible small images
            direct_result = self._try_direct_upload(
                file_obj,
                file_path,
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
            return self._try_drive_upload(
                file_obj,
                file_path,
                channel,
                sender_email,
                file_id,
                name,
                file_hash=download.md5,
                folder_id=folder_id,
                poster_id=poster_id,
            )
        finally:
            remove_temp_file(download.path)

    def slack_document(self, file_obj: dict[str, Any]) -> SlackDocument | None:
        """Plan the upload of a Slack canvas or list; None for other files.

        With ``config.convert_canvases`` set, canvases become Google Docs and
        lists become Google Sheets; otherwise (and when conversion fails)
        they are uploaded as ``.html``/``.csv`` files.

        Args:
            file_obj: Slack file object.

        Returns:
            The plan, or None for ordinary files.
        """
        convert = self.config.convert_canvases
        if is_slack_canvas(file_obj):
            return SlackDocument(
                kind="canvas",
                source_mime="text/html",
                extension=".html",
                target_mime=GOOGLE_DOCS_MIME_TYPE if convert else None,
                url=None,
                title=_document_title(file_obj, ".html"),
            )
        if is_slack_list(file_obj):
            csv_url = file_obj.get("list_csv_download_url")
            return SlackDocument(
                kind="list",
                source_mime="text/csv",
                extension=".csv",
                target_mime=GOOGLE_SHEETS_MIME_TYPE if convert else None,
                url=csv_url if isinstance(csv_url, str) and csv_url else None,
                title=_document_title(file_obj, ".csv"),
            )
        return None

    def _upload_document(
        self,
        file_obj: dict[str, Any],
        document: SlackDocument,
        download: DownloadResult,
        channel: str | None,
        sender_email: str | None,
        folder_id: str | None,
        *,
        poster_id: str | None = None,
    ) -> UploadResult:
        """Upload a downloaded canvas or list, converting it when planned.

        Only a failed conversion falls back to a plain ``.html``/``.csv``
        upload; any other failure (no folder, a network error) is returned
        with its cause.
        """
        file_id = file_obj.get("id") or "unknown"
        file_path = download.file_path  # only called for OK results
        attempts: list[tuple[str, str | None]] = []
        if document.target_mime:
            attempts.append((document.title, document.target_mime))
        attempts.append((document.plain_name, None))

        for drive_name, target_mime in attempts:
            result = self._upload_to_drive(
                file_obj,
                file_path,
                channel,
                sender_email,
                file_hash=download.md5,
                folder_id=folder_id,
                name=drive_name,
                mime_type=document.source_mime,
                target_mime_type=target_mime,
                poster_id=poster_id,
            )
            if not (target_mime and _conversion_failed(result)):
                break
            log_with_context(
                logging.WARNING,
                f"Could not convert {document.kind} {document.title!r} to a"
                f" Google file ({result.error}); uploading it as"
                f" {document.extension.lstrip('.').upper()} instead",
                channel=channel,
                file_id=file_id,
            )
        if not result.success:
            log_with_context(
                logging.ERROR,
                f"Failed to upload {document.kind} {document.title!r}: {result.error}",
                channel=channel,
                file_id=file_id,
            )
        return result

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
            if user_email and self._is_external_email(user_email):
                self._increment_stat("external_user_files")

    @staticmethod
    def _processed_key(
        file_id: str, channel: str | None, space: str | None
    ) -> tuple[str, str, str]:
        """Key for :attr:`processed_files`: the file in this channel and space."""
        return (file_id, channel or "", space or "")

    def _is_drive_bound(self, file_obj: dict[str, Any], space: str | None) -> bool:
        """Whether the file will be downloaded and uploaded to Drive.

        Judged from Slack's metadata before downloading: small images with a
        target space may go straight to Chat instead, and files that won't
        be downloaded at all (Google links, unavailable files) aren't
        Drive-bound.
        """
        if google_link_status(file_obj) or unavailable_reason(file_obj):
            return False
        mime_type = file_obj.get("mimetype")
        size = expected_size(file_obj)
        direct_candidate = (
            space is not None
            and mime_type in self.DIRECT_UPLOAD_MIME_TYPES
            and (size is None or size <= DIRECT_UPLOAD_MAX_BYTES)
        )
        return not direct_candidate

    def _find_existing_upload(
        self,
        file_obj: dict[str, Any],
        folder_id: str | None,
        name: str,
        channel: str | None = None,
        sender_email: str | None = None,
        *,
        poster_id: str | None = None,
    ) -> UploadResult | None:
        """Return this Slack file's Drive upload into *folder_id*, if any.

        The upload was made earlier in this run (for another space) or by an
        earlier run.  Missing grants are made again, as for any reused file.
        If Drive can't be searched, the file fails as retryable rather than
        being uploaded a second time.
        """
        file_id = file_obj.get("id")
        if not folder_id or not file_id:
            return None
        try:
            existing = self.drive_uploader.find_uploaded_slack_file(
                folder_id, file_id, self._shared_drive_id
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            # Not knowing whether it's there, don't make a second copy.
            error = (
                "Could not check Drive for an earlier upload of this file:"
                f" {type(e).__name__}: {e}"
            )
            log_with_context(
                logging.ERROR,
                f"Not uploading {name}: {error}. It fails as retryable.",
                file_id=file_id,
            )
            return UploadResult(error=error, name=name, retryable=True)
        if existing is None:
            return None
        log_with_context(
            logging.DEBUG,
            f"File {name} was uploaded earlier; linking it without downloading",
            file_id=file_id,
            drive_file_id=existing.file_id,
        )
        user_id = file_obj.get("user")
        self._apply_file_permissions(
            existing.file_id,
            existing.source,
            sender_email,
            self.user_map.get(user_id) if user_id else None,
            channel,
            file_id,
            poster_id=poster_id,
            owner_id=user_id,
        )
        return UploadResult(
            upload_type="drive",
            url=existing.url
            or f"https://drive.google.com/file/d/{existing.file_id}/view",
            drive_id=existing.file_id,
            name=name,
            mime_type=file_obj.get("mimetype"),
            metadata={"reused": True, "source": existing.source.value},
        )

    def _record_result(self, key: tuple[str, str, str], result: UploadResult) -> None:
        """Count a fresh result and cache it unless it is a retryable error."""
        if result.success:
            stat = (
                "direct_uploads" if result.upload_type == "direct" else "drive_uploads"
            )
            self._increment_stat(stat)
        elif result.skipped:
            if result.skip_reason == SKIP_REASON_UNAVAILABLE:
                self._increment_stat("unavailable_files")
            else:
                self._increment_stat("skipped_files")
        else:
            self._increment_stat("failed_uploads")
            return
        self.processed_files[key] = result

    def _check_attachment_cache(
        self, key: tuple[str, str, str], name: str, channel: str | None
    ) -> UploadResult | None:
        """Return the result of processing this file here before, if any."""
        cached_result = self.processed_files.get(key)
        if cached_result is not None:
            log_with_context(
                logging.DEBUG,
                f"File {name} already processed, using cached result",
                channel=channel,
                file_id=key[0],
            )
        return cached_result

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
            # Only an external file's own link is worth keeping; Slack's
            # download URLs can carry an export token.
            keep_link = download.reason == REASON_EXTERNAL
            return UploadResult(
                upload_type="skip",
                skip_reason=SKIP_REASON_UNAVAILABLE,
                name=name,
                url=download.url if keep_link else None,
                metadata={"unavailable_reason": download.reason},
            )

        log_with_context(
            logging.ERROR,
            f"Failed to download file {name} ({download.reason}), skipping"
            " attachment processing",
            channel=channel,
            file_id=file_id,
            reason=download.reason or "",
        )
        if download.reason in AUTH_FAILURE_REASONS:
            self._note_auth_failure()
        elif download.reason == REASON_DISALLOWED_HOST:
            self._note_disallowed_host(download.refused_host or "unknown host")
        return UploadResult(
            error=f"Download failed: {download.reason}",
            name=name,
            retryable=download.retryable,
        )

    def _note_disallowed_host(self, host: str) -> None:
        """Say once per host why its downloads are refused and what to do.

        A Slack file host is refused only for a link (or redirect) that
        isn't HTTPS, so the message then names the scheme, not the host.
        """
        if host in self._refused_hosts:
            return
        self._refused_hosts.add(host)
        if is_slack_file_host(host):
            log_with_context(
                logging.ERROR,
                f"Refused to download attachments from {host}: the link (or a"
                " redirect) isn't HTTPS, and only HTTPS links are downloaded,"
                " so a token is never sent unencrypted. These files are counted"
                " as failed.",
            )
            return
        allowed = ", ".join(SLACK_FILE_HOST_SUFFIXES)
        log_with_context(
            logging.ERROR,
            f"Refused to download attachments from {host}: only HTTPS links on"
            f" Slack's file hosts ({allowed} and their subdomains) are"
            " downloaded, so a token is never sent anywhere else. These files"
            f" are counted as failed. If {host} is a real Slack file host for"
            " your workspace (for example on GovSlack), report it so it can be"
            " added.",
        )

    def _note_auth_failure(self) -> None:
        """Count a refused download and say how to fix it (first, then every 100)."""
        self._increment_stat("download_auth_failures")
        count = self.file_stats["download_auth_failures"]
        if count != 1 and count % 100:
            return
        if self.config.slack_token:
            fix = (
                "slack_token is set but was refused: check that it is a token"
                " of this workspace with the files:read scope"
            )
        else:
            fix = (
                "set slack_token in config.yaml (or the SLACK_TOKEN environment"
                " variable) to a Slack token with files:read, or re-export"
            )
        log_with_context(
            logging.ERROR,
            f"{count} attachment download(s) refused so far (HTTP 401/403 or a"
            f" sign-in page instead of the file): {fix}. These files are"
            " counted as failed.",
        )

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
        folder_id: str | None = None,
        poster_id: str | None = None,
    ) -> UploadResult:
        """Upload a file to Google Drive; a failure is logged with its cause."""
        log_with_context(
            logging.DEBUG,
            f"Using Google Drive upload for file: {name}",
            channel=channel,
            file_id=file_id,
        )

        drive_result = self._upload_to_drive(
            file_obj,
            file_path,
            channel,
            sender_email,
            file_hash=file_hash,
            folder_id=folder_id,
            poster_id=poster_id,
        )
        if drive_result.success:
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
            f"Failed to upload file {name}: {drive_result.error}",
            channel=channel,
            file_id=file_id,
        )
        return drive_result

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
        folder_id: str | None = None,
        name: str | None = None,
        mime_type: str | None = None,
        target_mime_type: str | None = None,
        poster_id: str | None = None,
    ) -> UploadResult:
        """Upload a downloaded file to Google Drive.

        Args:
            file_obj: The file object from Slack
            file_path: Temp file holding the content
            channel: Optional channel name for context
            sender_email: Email address of the message sender (for permissions handling)
            file_hash: MD5 computed while downloading, so the file isn't
                read again just to hash it
            folder_id: Target folder, if already resolved
            name: Drive file name, if not the Slack name
            mime_type: Content type, if not resolved from the Slack file
            target_mime_type: Google type to convert the upload into
            poster_id: Slack user ID of the message sender, if known

        Returns:
            The Drive upload, or an error result that says why it failed
            (:func:`_conversion_failed` when only the conversion did).
        """
        file_id = file_obj.get("id") or "unknown"
        name = name or file_obj.get("name") or f"file_{file_id}"
        try:
            user_id = file_obj.get("user")

            mime_type = mime_type or resolve_drive_mime_type(
                file_obj, name, channel, file_id
            )

            log_with_context(
                logging.DEBUG,
                f"Uploading file to Drive: {name} (MIME: {mime_type})",
                channel=channel,
                file_id=file_id,
            )

            user_email = self.user_map.get(user_id) if user_id else None

            if not folder_id:
                lookup = self._choose_upload_folder(channel, file_id)
                if lookup.folder_id is None:
                    return self._folder_failure(lookup, name, channel)
                folder_id = lookup.folder_id

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
                target_mime_type=target_mime_type,
                poster_id=poster_id,
                owner_id=user_id,
            )

        except _FILE_ERRORS as e:
            if is_fatal_local_error(e):
                raise  # a full disk would fail every file in turn
            return UploadResult(
                error=f"Drive upload failed: {type(e).__name__}: {e}",
                name=name,
                retryable=is_transient_error(e),
            )

    def _choose_upload_folder(self, channel: str | None, file_id: str) -> FolderLookup:
        """Get or create the target Drive folder for an upload, or say why not.

        A conversation's files go only into its own folder, never into the
        root folder, whose permissions don't include the channel's members.
        If the channel folder can't be found or created (any HttpError,
        network or auth error), or a private conversation's folder can't be
        made limited-access, there is no folder: the file fails with the
        cause, as retryable when the cause may go away.  Files without a
        channel go into the root.  The folder returned has been listed into
        the dedup caches (or listing it failed, and is retried with the next
        file), so the upload itself doesn't list it again.
        """
        root = self._root_folder_id
        if not root:
            return FolderLookup(
                None, f"Drive is not available ({self._drive_status.detail})"
            )
        if not channel:
            self.drive_uploader.pre_cache_folder_file_hashes(
                root, self._shared_drive_id
            )
            return FolderLookup(root)

        lookup = self.folder_manager.find_or_create_channel_folder(
            channel,
            root,
            self._shared_drive_id,
            restricted=self.should_restrict_folder(channel),
        )
        if lookup.folder_id:
            self.drive_uploader.pre_cache_folder_file_hashes(
                lookup.folder_id, self._shared_drive_id
            )
        return lookup

    def _folder_failure(
        self, lookup: FolderLookup, name: str, channel: str | None
    ) -> UploadResult:
        """The failed result for a file that has no usable Drive folder.

        Counted in ``restricted_folder_failures`` (a private folder that
        couldn't be made limited-access) or ``channel_folder_failures`` (a
        folder that couldn't be found or created), and in
        ``failed_uploads`` by the caller.  The result is retryable when the
        cause may go away (see :attr:`FolderLookup.retryable`); a file that
        failed because Drive isn't set up is retryable too.  A
        ``folder_unavailable`` failure is logged as an error once per
        channel (a restriction failure logs its own).
        """
        retryable = lookup.retryable
        if lookup.restriction_failed:
            self._increment_stat("restricted_folder_failures")
            cause = "folder_restricted"
        elif self._root_folder_id:
            self._increment_stat("channel_folder_failures")
            cause = "folder_unavailable"
        else:
            cause = "drive_unavailable"
            retryable = True
        error = f"No Drive folder ({cause}): {lookup.error or 'unknown error'}"
        if cause == "folder_unavailable" and channel not in self._folder_errors_logged:
            self._folder_errors_logged.add(channel)
            log_with_context(
                logging.ERROR,
                f"Files of {channel} are not uploaded: {error}"
                f" ({'retryable' if retryable else 'permanent'})",
                channel=channel,
            )
        return UploadResult(error=error, name=name, retryable=retryable)

    def set_conversation_type(
        self, channel: str, conversation_type: ConversationType
    ) -> None:
        """Record a conversation's type: public, private, mpim or dm.

        Overrides the guess from ``channels_meta``.

        Args:
            channel: Channel (folder) name.
            conversation_type: One of ``config.CONVERSATION_TYPES``.
        """
        self._conversation_types[channel] = conversation_type

    def is_public_conversation(self, channel: str) -> bool:
        """Whether *channel* is a public channel.

        An explicit :meth:`set_conversation_type` wins.  Otherwise a channel
        is public when ``channels.json`` lists it and it isn't private.
        """
        explicit = self._conversation_types.get(channel)
        if explicit is not None:
            return explicit == "public"
        meta = self.channels_meta.get(channel)
        return isinstance(meta, Mapping) and not meta.get("is_private", False)

    def should_restrict_folder(self, channel: str) -> bool:
        """Whether *channel*'s Drive folder should be a limited-access folder.

        True for non-public conversations when the run uses a shared drive
        and ``config.restrict_private_channel_folders`` is set.  In regular
        Drive mode the folders sit in the admin's unshared My Drive.
        """
        return bool(
            self.config.restrict_private_channel_folders
            and self._shared_drive_id
            and not self.is_public_conversation(channel)
        )

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
        target_mime_type: str | None = None,
        poster_id: str | None = None,
        owner_id: str | None = None,
    ) -> UploadResult:
        """Upload a temp file to Drive and handle permissions.

        Args:
            file_path: Temp file holding the content.
            name: File name on Drive.
            mime_type: MIME type reported in the result.
            folder_id: Target Drive folder.
            channel: Channel name for logging context.
            file_id: Slack file ID for logging context.
            user_email: Mapped email of the Slack user who uploaded the file
                (the ownership-transfer target in regular-Drive mode, when
                ``config.transfer_file_ownership`` is set).
            sender_email: Email of the message sender, who gets ``writer``
                access.  Callers pass None for external senders.
            file_hash: MD5 of the content, if known.
            target_mime_type: Google type to convert the upload into.
            poster_id: Slack user ID of the message sender, if known.
            owner_id: Slack user ID of the file's uploader, if known.

        Returns:
            The Drive upload, or an error result with the cause.
        """
        upload = self.drive_uploader.upload_file_to_drive(
            file_path,
            name,
            folder_id,
            self._shared_drive_id,
            mime_type=mime_type,
            file_hash=file_hash,
            slack_file_id=None if file_id == "unknown" else file_id,
            channel=channel,
            target_mime_type=target_mime_type,
            pre_cache=False,  # _choose_upload_folder listed the folder
        )

        if isinstance(upload, DriveUploadFailure):
            return UploadResult(
                error=f"Drive upload failed: {upload.reason}",
                name=name,
                metadata={META_CONVERSION_FAILED: upload.conversion_failed},
                retryable=upload.retryable,
            )

        # Never raises: a failed grant doesn't make the upload fail.
        self._apply_file_permissions(
            upload.file_id,
            upload.source,
            sender_email,
            user_email,
            channel,
            file_id,
            poster_id=poster_id,
            owner_id=owner_id,
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
            mime_type=target_mime_type or mime_type,
            metadata={
                "reused": upload.reused,
                "source": upload.source.value,
                **({"converted_to": target_mime_type} if target_mime_type else {}),
            },
        )

    def _is_internal_email(self, email: str | None) -> bool:
        """True for a mapped email that belongs to the workspace."""
        return bool(email) and not self._is_external_email(email or "")

    def _is_external_email(self, email: str) -> bool:
        """True for an email outside the workspace.

        Every internal/external decision in the files code comes here.  The
        user resolver decides (the admin's domain plus
        ``config.workspace_domains``); without one, the same domains are
        checked here.  External users never get per-file grants; as channel
        members they still get reader access to the channel folder.
        """
        if self.user_resolver is not None:
            return bool(self.user_resolver.is_external_user(email))
        return is_external_email(email, self._internal_domains)

    def _count_listing_failure(self, folder_id: str, error: BaseException) -> None:
        """Count a Drive folder whose files couldn't be listed (once per folder)."""
        self._increment_stat("folder_listing_failures")

    def _count_folder_sync(self, channel: str, result: FolderSyncResult) -> None:
        """Count what failed in a channel-folder permission sync."""
        if not result.ok:
            self._increment_stat("folder_sync_failures")
        self._increment_stat("folder_grant_failures", len(result.failed_grants))
        self._increment_stat("folder_removal_failures", len(result.failed_removals))
        self._increment_stat(
            "external_folder_grant_failures", len(result.external_failures)
        )

    def sync_channel_folder_permissions(
        self, channel: str, user_emails: list[str]
    ) -> FolderSyncResult:
        """Give a channel's current members, and only them, its folder.

        Looks the channel folder up under the attachment root (a channel
        without a folder needs no sync) and syncs its permissions.  Failures,
        including a failed lookup or a Drive that couldn't be set up, are
        counted in the file statistics.

        Args:
            channel: Channel name.
            user_emails: Emails of the channel's current members.

        Returns:
            See :class:`~slack_chat_migrator.services.drive.FolderSyncResult`.
        """
        root_folder_id = self.folder_id  # initializes Drive if needed
        if not root_folder_id:
            result = FolderSyncResult(
                lookup_error="Drive is not set up (no attachment folder)"
            )
            self._count_folder_sync(channel, result)
            return result
        return self.folder_manager.sync_channel_folder(
            channel, root_folder_id, user_emails, self._shared_drive_id
        )

    def _protected_emails(self) -> list[str]:
        """Accounts whose channel-folder access a permission sync never removes."""
        admin = getattr(self.user_resolver, "workspace_admin", None)
        return [admin] if isinstance(admin, str) and admin else []

    def _apply_file_permissions(
        self,
        drive_file_id: str,
        source: DriveUploadSource,
        sender_email: str | None,
        owner_email: str | None,
        channel: str | None,
        file_id: str,
        *,
        poster_id: str | None = None,
        owner_id: str | None = None,
    ) -> None:
        """Grant the poster ``writer`` access and transfer ownership if allowed.

        Everyone else, external members included, gets reader access through
        the channel folder.  External users never get a per-file grant.

        Ownership is transferred only when ``config.transfer_file_ownership``
        is set, only in regular-Drive mode and only to internal users: for a
        new upload, or for a file an earlier run uploaded (found by its
        Slack ID) whose transfer to this owner is still pending.  Any other
        reused file keeps its owner.

        A reused file gets the poster's grant unless this run already made
        it.  A file an earlier run uploaded (found by its Slack ID) gets it
        only if that run's grant to this poster failed: that run made its
        grants.  Failures are logged, counted per attempt
        (``poster_grant_failures``, ``ownership_transfer_failed``) and
        flagged per poster on the file (see
        :class:`~slack_chat_migrator.services.drive.pending.PendingPermissions`),
        never raised: the upload stays a success.  :meth:`retry_pending_permissions`
        retries the flagged ones at the end of the run.  A flag stores the
        Slack user ID (*poster_id* for the poster, *owner_id* for the new
        owner), so a later run can map that user again.
        """
        pending = self.drive_uploader.pending
        poster = sender_email if self._is_internal_email(sender_email) else None
        can_transfer = bool(
            self.config.transfer_file_ownership
            and not self._shared_drive_id
            and self._is_internal_email(owner_email)
        )
        from_earlier_run = (
            source is DriveUploadSource.SLACK_ID
            and drive_file_id not in self.drive_uploader.created_this_run
        )
        pending_owner = pending.owner(drive_file_id)
        owner = (
            owner_email
            if can_transfer
            and owner_email
            and (
                source is DriveUploadSource.NEW
                or (
                    from_earlier_run
                    and pending_owner is not None
                    and pending_owner.is_for(owner_id, owner_email)
                )
            )
            else None
        )
        if (
            owner
            and from_earlier_run
            and not self._repeat_transfer_needed(drive_file_id, owner)
        ):
            owner = None
        if from_earlier_run and poster and not pending.has_grant(drive_file_id, poster):
            poster = None  # the earlier run made this grant

        if sender_email and not self._is_internal_email(sender_email):
            log_with_context(
                logging.DEBUG,
                f"Not granting file access to external poster {sender_email}",
                channel=channel,
                file_id=file_id,
            )

        # The new owner needs no separate writer grant.
        grant_poster = bool(
            poster
            and poster != owner
            and not self._already_granted(drive_file_id, poster)
        )

        if grant_poster and owner and poster:
            # Two calls on one file: send them as one batch request.
            writer_error, transfer_error = self.drive_service.apply_permission_changes(
                [
                    self.drive_uploader.poster_permission_change(
                        drive_file_id, poster, self._shared_drive_id
                    ),
                    ownership_transfer_change(drive_file_id, owner),
                ]
            )
            if writer_error is not None:
                log_with_context(
                    logging.WARNING,
                    f"Failed to set writer permission for message poster on file"
                    f" {drive_file_id}: {writer_error}",
                    channel=channel,
                    file_id=file_id,
                )
            self._record_poster_grant(
                drive_file_id, poster, writer_error is None, user_id=poster_id
            )
            self._count_ownership_transfer(
                transfer_error is None,
                drive_file_id,
                owner,
                channel,
                file_id,
                error=transfer_error,
                user_id=owner_id,
            )
            return

        if grant_poster and poster:
            self._grant_poster(drive_file_id, poster, user_id=poster_id)

        if owner:
            transferred = self._handle_ownership_transfer(
                drive_file_id, owner, channel, file_id, user_id=owner_id
            )
            if not transferred and poster == owner:
                # Keep the poster able to edit their file.
                self._grant_poster(drive_file_id, poster, user_id=poster_id)

    def _grant_poster(
        self, drive_file_id: str, poster: str, *, user_id: str | None = None
    ) -> None:
        """Give the poster ``writer`` access (a failure is logged there)."""
        granted = self.drive_uploader.set_message_poster_permission(
            drive_file_id, poster, self._shared_drive_id
        )
        self._record_poster_grant(drive_file_id, poster, granted, user_id=user_id)

    def _record_poster_grant(
        self,
        drive_file_id: str,
        poster: str,
        granted: bool,
        *,
        user_id: str | None = None,
    ) -> None:
        """Remember a poster grant, or count and flag it as failed.

        The flag is per poster, so another poster's success never clears it.
        The end-of-run sweep retries flagged grants, as does linking the
        file again.
        """
        if granted:
            self._mark_granted(drive_file_id, poster)
        else:
            self._increment_stat("poster_grant_failures")
        self.drive_uploader.pending.set_grant(
            drive_file_id,
            poster,
            not granted,
            self._shared_drive_id,
            user_id=None if granted else user_id,
        )

    def _already_granted(self, drive_file_id: str, email: str) -> bool:
        """Whether *email* got writer access (or ownership) this run."""
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
        *,
        user_id: str | None = None,
    ) -> bool:
        """Transfer ownership of a regular-Drive file to its Slack uploader.

        The file is new, or an earlier run's transfer of it is pending;
        only called when ``config.transfer_file_ownership`` is set.

        Returns:
            True if the transfer succeeded.  Both outcomes are counted.
        """
        transferred = self._transfer_file_ownership(drive_file_id, new_owner_email)
        self._count_ownership_transfer(
            transferred,
            drive_file_id,
            new_owner_email,
            channel,
            file_id,
            user_id=user_id,
        )
        return transferred

    def _count_ownership_transfer(
        self,
        transferred: bool,
        drive_file_id: str,
        new_owner_email: str,
        channel: str | None,
        file_id: str,
        error: BaseException | None = None,
        *,
        user_id: str | None = None,
    ) -> None:
        """Count and log one ownership transfer attempt.

        A failed transfer is flagged on the file, so the end-of-run sweep
        (:meth:`retry_pending_permissions`) retries it, in this run and in
        later ones, as does a later run that links the file again.

        Args:
            transferred: Whether the transfer succeeded.
            drive_file_id: The Drive file.
            new_owner_email: The intended owner.
            channel: Channel name for logging context.
            file_id: Slack file ID for logging context.
            error: Why the transfer failed, when known.
            user_id: The new owner's Slack user ID, stored with the flag.
        """
        self.drive_uploader.pending.set_owner(
            drive_file_id,
            new_owner_email,
            not transferred,
            user_id=None if transferred else user_id,
        )
        if transferred:
            # The owner needs no writer grant when the file is linked again.
            self._mark_granted(drive_file_id, new_owner_email)
            self._increment_stat("ownership_transferred")
            log_with_context(
                logging.DEBUG,
                f"Transferred file ownership to original poster: {new_owner_email}",
                channel=channel,
                file_id=file_id,
                drive_file_id=drive_file_id,
            )
            return

        self._increment_stat("ownership_transfer_failed")
        cause = f": {error}" if error is not None else ""
        log_with_context(
            logging.WARNING,
            f"Could not transfer ownership of {drive_file_id} to"
            f" {new_owner_email}{cause}",
            channel=channel,
            file_id=file_id,
            drive_file_id=drive_file_id,
        )

    def retry_pending_permissions(self) -> None:
        """Retry the poster grants and ownership transfers that failed.

        Run when the migration's channel loop ends: also after an abort
        (``abort_on_error``) and with ``--resume`` (which doesn't link files
        of already-sent messages again), but not after Ctrl-C or SIGTERM.
        The flagged files are those this run flagged plus those whose
        appProperties flags an earlier run left: the whole shared drive is
        searched, or in regular Drive the attachment root and its channel
        folders.

        Each recipient is looked up again by its stored Slack user ID in
        today's user mapping (the stored email is used for flags without
        one).  Only internal recipients are retried; transfers only while
        ``transfer_file_ownership`` is still set and in regular Drive.  A
        success clears the flag.  ``pending_retried`` and
        ``pending_retry_failed`` count grant and transfer attempts;
        ``pending_skipped_*`` count flags kept without an attempt (unknown
        recipient, unmapped, external, transfers off, or a file the admin no
        longer owns).  A flagged transfer whose recipient already owns the
        file (its flag couldn't be cleared) is cleared and counted as
        retried, without a second transfer.

        Never raises a Drive error: a failed search is logged at ERROR and
        counted in ``pending_sweep_failures``, and this run's own flags are
        still retried.  A used-up Drive quota (DriveQuotaError) ends the
        sweep, logged and counted the same way; the flags still pending stay
        on their files.
        """
        root = self._root_folder_id
        if not root:
            return
        skipped: dict[str, list[str]] = {
            "unknown": [],
            "unmapped": [],
            "external": [],
            "not_owner": [],
        }
        tally: Counter[str] = Counter()
        try:
            self._find_flagged_files()
            self._retry_flagged_grants(skipped, tally)
            self._retry_flagged_transfers(skipped, tally)
        except DriveQuotaError as e:
            self._increment_stat("pending_sweep_failures")
            log_with_context(
                logging.ERROR,
                f"Stopped retrying pending Drive grants and transfers: {e} The"
                " flags still pending stay on their files for the next run.",
            )
        self._report_sweep_skips(skipped)
        retried, failed = tally["retried"], tally["failed"]
        if retried or failed:
            log_with_context(
                logging.WARNING if failed else logging.INFO,
                f"Retried {retried + failed} pending Drive grant(s) and"
                f" transfer(s): {retried} succeeded, {failed} still failing"
                " (their flags are kept, and the next run retries them)",
            )

    def count_sweep_failure(self) -> None:
        """Count a sweep that ended on an unexpected error (logged by the caller)."""
        self._increment_stat("pending_sweep_failures")

    def _find_flagged_files(self) -> None:
        """Read the flags earlier runs left on this migration's Drive files.

        A failed search is logged at ERROR and counted in
        ``pending_sweep_failures``; the sweep then retries the flags this
        run already knows about.
        """
        try:
            folder_ids = set() if self._shared_drive_id else self._migration_folders()
            self.drive_uploader.pending.find_flagged(self._shared_drive_id, folder_ids)
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            self._increment_stat("pending_sweep_failures")
            log_with_context(
                logging.ERROR,
                f"Could not look for Drive files with pending grants or"
                f" transfers ({e}); retrying only the ones this run already"
                " knows about. The next run looks again.",
            )

    def _count_retry(self, succeeded: bool, tally: Counter[str]) -> None:
        """Count one sweep attempt as it happens."""
        kind = "retried" if succeeded else "failed"
        tally[kind] += 1
        self._increment_stat("pending_retried" if succeeded else "pending_retry_failed")

    def _retry_flagged_grants(
        self, skipped: dict[str, list[str]], tally: Counter[str]
    ) -> None:
        """Retry every flagged poster grant (see :meth:`retry_pending_permissions`)."""
        pending = self.drive_uploader.pending
        for drive_file_id, targets in list(pending.grants.items()):
            for key, target in list(targets.items()):
                email = self._sweep_recipient(drive_file_id, target, skipped)
                if email is None:
                    continue
                granted = self.drive_uploader.set_message_poster_permission(
                    drive_file_id, email, self._shared_drive_id
                )
                if granted:
                    self._mark_granted(drive_file_id, email)
                    pending.clear_grant(drive_file_id, key, self._shared_drive_id)
                self._count_retry(granted, tally)

    def _retry_flagged_transfers(
        self, skipped: dict[str, list[str]], tally: Counter[str]
    ) -> None:
        """Retry every flagged ownership transfer, while transfers are on."""
        pending = self.drive_uploader.pending
        owners = list(pending.owners.items())
        if owners and not (
            self.config.transfer_file_ownership and not self._shared_drive_id
        ):
            self._increment_stat("pending_skipped_transfers_off", len(owners))
            log_with_context(
                logging.WARNING,
                f"Not retrying {len(owners)} pending ownership transfer(s):"
                " transfer_file_ownership is off or the files are on a shared"
                " drive. Their flags are kept for a run with it on.",
            )
            return
        for drive_file_id, target in owners:
            email = self._sweep_recipient(drive_file_id, target, skipped)
            if email is None:
                continue
            ownership = self._ownership(drive_file_id, email)
            if ownership == "recipient":
                self._transfer_found_done(drive_file_id, email)
                self._count_retry(True, tally)
                continue
            if ownership == "other":
                skipped["not_owner"].append(drive_file_id)
                self._increment_stat("pending_skipped_not_owner")
                continue
            transferred = self._transfer_file_ownership(drive_file_id, email)
            if transferred:
                self._mark_granted(drive_file_id, email)
                pending.set_owner(drive_file_id, email, False)
            self._count_retry(transferred, tally)

    def _ownership(
        self, drive_file_id: str, email: str
    ) -> Literal["admin", "recipient", "other", "unknown"]:
        """Who owns a file whose transfer to *email* is flagged.

        A transfer that went through but whose flag couldn't be cleared
        must not be sent again: Drive makes the previous owner a writer,
        and only the owner can transfer.

        Returns:
            ``recipient`` if *email* already owns the file, ``admin`` if the
            workspace admin still does, ``other`` if someone else does, and
            ``unknown`` if Drive couldn't say (the transfer is then tried).
        """
        try:
            file = self.drive_service.get_file(
                drive_file_id, fields="ownedByMe, owners(emailAddress)"
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            return "unknown"
        owners = {
            _normalize_email(owner["emailAddress"])
            for owner in file.get("owners") or []
            if isinstance(owner, Mapping) and isinstance(owner.get("emailAddress"), str)
        }
        if _normalize_email(email) in owners:
            return "recipient"
        if file.get("ownedByMe") is True:
            return "admin"
        if owners or file.get("ownedByMe") is False:
            return "other"
        return "unknown"

    def _transfer_found_done(self, drive_file_id: str, email: str) -> None:
        """Clear a pending transfer that an earlier attempt already made."""
        log_with_context(
            logging.DEBUG,
            f"{email} already owns Drive file {drive_file_id}; clearing its"
            " pending transfer",
            drive_file_id=drive_file_id,
        )
        self._mark_granted(drive_file_id, email)
        self.drive_uploader.pending.set_owner(drive_file_id, email, False)

    def _repeat_transfer_needed(self, drive_file_id: str, email: str) -> bool:
        """Whether a relinked file's pending transfer to *email* is still due.

        False if *email* already owns it (the flag is cleared) or someone
        other than the admin does (the flag is kept for the sweep).
        """
        ownership = self._ownership(drive_file_id, email)
        if ownership == "recipient":
            self._transfer_found_done(drive_file_id, email)
        return ownership in ("admin", "unknown")

    def _sweep_recipient(
        self, drive_file_id: str, target: Target, skipped: dict[str, list[str]]
    ) -> str | None:
        """Today's email for a flagged recipient, or None to keep the flag.

        A Slack user ID is mapped again through today's user mapping.  An
        unknown, unmapped or external recipient is recorded in *skipped*
        (by kind) and counted in ``pending_skipped_<kind>``.
        """
        email = target.email
        if target.user_id:
            today = self.user_map.get(target.user_id)
            if not today:
                skipped["unmapped"].append(target.user_id)
                self._increment_stat("pending_skipped_unmapped")
                return None
            if email and _normalize_email(today) != _normalize_email(email):
                log_with_context(
                    logging.INFO,
                    f"Slack user {target.user_id} is now mapped to {today}, not"
                    f" {email}; retrying the pending grant or transfer on Drive"
                    f" file {drive_file_id} for {today}",
                    drive_file_id=drive_file_id,
                )
            email = today
        if not email:
            skipped["unknown"].append(drive_file_id)
            self._increment_stat("pending_skipped_unknown")
            return None
        if not self._is_internal_email(email):
            skipped["external"].append(email)
            self._increment_stat("pending_skipped_external")
            return None
        return email

    @staticmethod
    def _report_sweep_skips(skipped: dict[str, list[str]]) -> None:
        """Log one warning per kind of flag the sweep kept without retrying."""
        reasons = {
            "unknown": (
                "the recipient's email was too long to store with the flag (a"
                " grant is still made if a later run links the file again for"
                " the same poster)",
                "Drive files",
            ),
            "unmapped": (
                "their Slack user is no longer mapped to a Google account",
                "Slack users",
            ),
            "external": ("their recipient is now outside the workspace", "emails"),
            "not_owner": (
                "the workspace admin no longer owns the file, and only its owner"
                " can transfer it",
                "Drive files",
            ),
        }
        for kind, items in skipped.items():
            if not items:
                continue
            reason, label = reasons[kind]
            names = sorted(set(items))
            shown = ", ".join(names[:10]) + (" ..." if len(names) > 10 else "")
            log_with_context(
                logging.WARNING,
                f"Kept {len(items)} pending Drive grant(s) or transfer(s)"
                f" without retrying them: {reason} ({label}: {shown}).",
            )

    def _poster_user_id(
        self,
        file_obj: Mapping[str, Any],
        sender_email: str | None,
        sender_user_id: str | None,
    ) -> str | None:
        """The message sender's Slack user ID, for a flag on a failed grant.

        The ID the caller gives wins.  Otherwise the file's uploader, when
        it maps to *sender_email*, and failing that the one user mapped to
        that address.  When several users share it, None: the flag then
        stores the email alone rather than maybe another user's ID.
        """
        if sender_user_id:
            return sender_user_id
        if not sender_email:
            return None
        uploader = file_obj.get("user")
        mapped = self.user_map.get(uploader) if isinstance(uploader, str) else None
        if mapped and _normalize_email(mapped) == _normalize_email(sender_email):
            return str(uploader)
        wanted = _normalize_email(sender_email)
        matches = [
            user_id
            for user_id, address in self.user_map.items()
            if isinstance(address, str) and _normalize_email(address) == wanted
        ]
        return matches[0] if len(matches) == 1 else None

    def _migration_folders(self) -> set[str]:
        """The attachment root and every channel folder in it (regular Drive).

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` if the folders can't be listed.
        """
        root = self._root_folder_id
        if not root:
            return set()
        children = self.folder_manager.list_child_folders(root, self._shared_drive_id)
        return {root} | {folder["id"] for folder in children}

    def _count_flag_write_failure(self, drive_file_id: str, clearing: bool) -> None:
        """Count a pending flag that couldn't be stored on, or cleared from, a file."""
        self._increment_stat(
            "pending_clear_failures" if clearing else "pending_flag_failures"
        )

    def get_file_statistics(self) -> FileUploadStats:
        """Get detailed file upload statistics.

        Returns:
            The counters described by
            :class:`~slack_chat_migrator.types.FileUploadStats`.
            ``success_rate`` is the share of uploads that succeeded, out of
            those that succeeded or failed; unavailable and skipped files
            don't lower it.
        """
        successful = int(self.file_stats["drive_uploads"]) + int(
            self.file_stats["direct_uploads"]
        )
        failed = int(self.file_stats["failed_uploads"])
        stats: FileUploadStats = {
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
            "poster_grant_failures": self.file_stats["poster_grant_failures"],
            "pending_retried": self.file_stats["pending_retried"],
            "pending_retry_failed": self.file_stats["pending_retry_failed"],
            "pending_flag_failures": self.file_stats["pending_flag_failures"],
            "pending_clear_failures": self.file_stats["pending_clear_failures"],
            "pending_sweep_failures": self.file_stats["pending_sweep_failures"],
            "pending_skipped_unknown": self.file_stats["pending_skipped_unknown"],
            "pending_skipped_unmapped": self.file_stats["pending_skipped_unmapped"],
            "pending_skipped_external": self.file_stats["pending_skipped_external"],
            "pending_skipped_transfers_off": self.file_stats[
                "pending_skipped_transfers_off"
            ],
            "pending_skipped_not_owner": self.file_stats["pending_skipped_not_owner"],
            "folder_listing_failures": self.file_stats["folder_listing_failures"],
            "external_folder_grant_failures": self.file_stats[
                "external_folder_grant_failures"
            ],
            "folder_sync_failures": self.file_stats["folder_sync_failures"],
            "folder_grant_failures": self.file_stats["folder_grant_failures"],
            "folder_removal_failures": self.file_stats["folder_removal_failures"],
            "restricted_folder_failures": self.file_stats["restricted_folder_failures"],
            "channel_folder_failures": self.file_stats["channel_folder_failures"],
            "download_auth_failures": self.file_stats["download_auth_failures"],
            "files_by_channel": dict(self.file_stats["files_by_channel"]),
            "success_rate": successful / max(1, successful + failed) * 100,
        }
        return stats

    def _transfer_file_ownership(self, file_id: str, new_owner_email: str) -> bool:
        """Transfer ownership of a file to a new owner.

        Delegates to :func:`file_permissions.transfer_file_ownership`.
        """
        return transfer_file_ownership(self.drive_service, file_id, new_owner_email)

    def _download_file(
        self,
        file_obj: dict[str, Any],
        *,
        url: str | None = None,
        allow_html: bool | None = None,
        check_size: bool = True,
    ) -> DownloadResult:
        """Download a Slack file into this run's temp directory.

        Delegates to :func:`file_download.download_file`; the keyword
        arguments are passed through.
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
            url=url,
            allow_html=allow_html,
            check_size=check_size,
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
