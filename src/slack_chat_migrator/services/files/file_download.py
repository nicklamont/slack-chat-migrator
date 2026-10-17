"""File download and content retrieval from Slack exports.

Downloads stream straight to a temp file through one pooled
:class:`requests.Session` that retries 429 and 5xx responses.  While
streaming, the MD5 and byte count are computed, so the content is never held
in memory and is hashed only once.

Each download returns a :class:`DownloadResult` whose status says whether the
file was fetched (``OK``), cannot be fetched and should not count as a
failure (``UNAVAILABLE``: deleted, hidden, external, sign-in page, 401/403/404,
no URL), failed after retries (``FAILED``), or is a Google Docs/Drive link
that needs no download.
"""

from __future__ import annotations

import enum
import hashlib
import logging
import os
import tempfile
import threading
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from slack_chat_migrator.constants import (
    DOWNLOAD_CONNECT_TIMEOUT_SECONDS,
    DOWNLOAD_MAX_REDIRECTS,
    DOWNLOAD_POOL_SIZE,
    DOWNLOAD_READ_TIMEOUT_SECONDS,
    DOWNLOAD_RETRY_BACKOFF_SECONDS,
    DOWNLOAD_RETRY_STATUSES,
    DOWNLOAD_RETRY_TOTAL,
    FILE_READ_CHUNK_BYTES,
    HTTP_OK,
    HTTP_RATE_LIMIT,
    HTTP_SERVER_ERROR_MIN,
    SLACK_FILE_HOST_SUFFIXES,
)
from slack_chat_migrator.services.files.temp_files import (
    remove_temp_file,
    safe_temp_suffix,
)
from slack_chat_migrator.types import UploadResult
from slack_chat_migrator.utils.logging import log_with_context

logger = logging.getLogger("slack_chat_migrator")


class DownloadStatus(str, enum.Enum):
    """Outcome of :func:`download_file`."""

    OK = "ok"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"
    GOOGLE_DOCS_LINK = "google_docs_link"
    GOOGLE_DRIVE_FILE = "google_drive_file"


# Reasons a file is UNAVAILABLE (not counted as a failed upload).
REASON_DELETED = "deleted"
REASON_HIDDEN_BY_LIMIT = "hidden_by_limit"
REASON_EXTERNAL = "external"
REASON_NO_URL = "no_url"
REASON_DISALLOWED_URL = "disallowed_url"
REASON_TOO_MANY_REDIRECTS = "too_many_redirects"
REASON_HTML_PAGE = "html_page"
REASON_SIZE_MISMATCH = "size_mismatch"

#: MIME types Slack uses for HTML content, including canvases.
_HTML_MIME_TYPES = frozenset(
    {"text/html", "application/xhtml+xml", "application/vnd.slack-docs"}
)
_HTML_FILETYPES = frozenset({"html", "quip", "canvas"})
_HTML_EXTENSIONS = (".html", ".htm", ".xhtml")


@dataclass
class DownloadResult:
    """Result of :func:`download_file`.

    Attributes:
        status: What happened (see :class:`DownloadStatus`).
        path: Temp file holding the content (``OK`` only).  The caller owns
            it and must delete it.
        md5: Hex MD5 of the content (``OK`` only).
        size: Number of bytes written.
        content_type: Response ``Content-Type`` header, if any.
        reason: Short machine-readable reason for ``UNAVAILABLE``/``FAILED``.
        url: The link to keep for files that are not downloaded (external
            files, Google links), else the URL that was fetched.
        http_status: Final HTTP status, when a response was received.
    """

    status: DownloadStatus
    path: str | None = None
    md5: str | None = None
    size: int = 0
    content_type: str | None = None
    reason: str | None = None
    url: str | None = None
    http_status: int | None = None

    @property
    def ok(self) -> bool:
        """True when the content was downloaded to :attr:`path`."""
        return self.status is DownloadStatus.OK


# ---------------------------------------------------------------------------
# Shared HTTP session
# ---------------------------------------------------------------------------

_session: requests.Session | None = None
_session_lock = threading.Lock()


def build_download_session(
    pool_size: int = DOWNLOAD_POOL_SIZE,
    retry_total: int = DOWNLOAD_RETRY_TOTAL,
    backoff_factor: float = DOWNLOAD_RETRY_BACKOFF_SECONDS,
) -> requests.Session:
    """Create a pooled session that retries transient download errors.

    GETs are retried on connection errors and on 429/5xx responses, honouring
    ``Retry-After``.  401, 403 and 404 are not retried.

    Args:
        pool_size: Connections kept per host.
        retry_total: Maximum retries per request.
        backoff_factor: Exponential backoff factor in seconds.

    Returns:
        A configured :class:`requests.Session`.
    """
    retry = Retry(
        total=retry_total,
        backoff_factor=backoff_factor,
        status_forcelist=DOWNLOAD_RETRY_STATUSES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        pool_connections=pool_size, pool_maxsize=pool_size, max_retries=retry
    )
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_download_session() -> requests.Session:
    """Return the module-wide download session, creating it on first use."""
    global _session
    with _session_lock:
        if _session is None:
            _session = build_download_session()
        return _session


# ---------------------------------------------------------------------------
# Classification helpers (pure)
# ---------------------------------------------------------------------------


def redact_url(url: str | None) -> str:
    """Return *url* without its query string, which may hold an auth token."""
    if not url:
        return ""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def is_allowed_file_url(url: str) -> bool:
    """Check that *url* is an https URL on a Slack file host.

    Args:
        url: Candidate download URL.

    Returns:
        True when the scheme is ``https`` and the host is, or is a subdomain
        of, one of :data:`SLACK_FILE_HOST_SUFFIXES`.
    """
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower().rstrip(".")
    except ValueError:
        return False
    if parsed.scheme != "https" or not host:
        return False
    return any(
        host == suffix or host.endswith("." + suffix)
        for suffix in SLACK_FILE_HOST_SUFFIXES
    )


def google_link_status(file_obj: dict[str, Any]) -> DownloadStatus | None:
    """Classify Google Docs and Drive links, which are never downloaded.

    Args:
        file_obj: Slack file object.

    Returns:
        ``GOOGLE_DOCS_LINK`` for Docs/Sheets/Slides documents,
        ``GOOGLE_DRIVE_FILE`` for Drive files, else None.
    """
    url = file_obj.get("url_private") or file_obj.get("url_private_download") or ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    host = (parsed.hostname or "").lower()
    path = parsed.path
    if host == "docs.google.com" and path.startswith(
        ("/document", "/spreadsheets", "/presentation")
    ):
        return DownloadStatus.GOOGLE_DOCS_LINK
    if host in ("sheets.google.com", "slides.google.com") and "/edit" in path:
        return DownloadStatus.GOOGLE_DOCS_LINK
    if host == "drive.google.com" and (
        path.startswith("/file/d/") or (path == "/open" and "id=" in parsed.query)
    ):
        return DownloadStatus.GOOGLE_DRIVE_FILE
    return None


def unavailable_reason(file_obj: dict[str, Any]) -> str | None:
    """Return why a file cannot be downloaded, judging by its metadata alone.

    Covers deleted (``tombstone``) and ``hidden_by_limit`` files, files
    hosted outside Slack (``mode: external``), and files without a URL.
    Google links are not covered; check :func:`google_link_status` first.

    Args:
        file_obj: Slack file object.

    Returns:
        One of the ``REASON_*`` constants, or None if a download can be tried.
    """
    mode = file_obj.get("mode")
    if mode == "tombstone":
        return REASON_DELETED
    if mode == "hidden_by_limit":
        return REASON_HIDDEN_BY_LIMIT
    if mode == "external" or file_obj.get("is_external"):
        return REASON_EXTERNAL
    if not select_download_url(file_obj):
        return REASON_NO_URL
    return None


def select_download_url(file_obj: dict[str, Any]) -> str | None:
    """Pick the URL to download: ``url_private_download``, else ``url_private``."""
    url = file_obj.get("url_private_download") or file_obj.get("url_private")
    return url if isinstance(url, str) and url else None


def external_link(file_obj: dict[str, Any]) -> str | None:
    """Return the link to keep in the message for a file that isn't downloaded."""
    for key in ("url_private", "permalink", "url_private_download"):
        value = file_obj.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def expects_html(file_obj: dict[str, Any]) -> bool:
    """Whether Slack says the file itself is HTML (so an HTML body is valid)."""
    mimetype = str(file_obj.get("mimetype") or "").lower()
    filetype = str(file_obj.get("filetype") or "").lower()
    name = str(file_obj.get("name") or "").lower()
    return (
        mimetype in _HTML_MIME_TYPES
        or filetype in _HTML_FILETYPES
        or name.endswith(_HTML_EXTENSIONS)
    )


def expected_size(file_obj: dict[str, Any]) -> int | None:
    """Return the byte size the export records for the file, if positive."""
    size = file_obj.get("size")
    if isinstance(size, bool):
        return None
    if isinstance(size, str) and size.isdigit():
        size = int(size)
    if isinstance(size, int) and size > 0:
        return size
    return None


def _is_html_content_type(content_type: str) -> bool:
    """True when a ``Content-Type`` header denotes an HTML document."""
    media_type = content_type.split(";", 1)[0].strip().lower()
    return media_type in ("text/html", "application/xhtml+xml")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


class _SizeMismatchError(Exception):
    """The body is larger than the size the export records."""


def _get_following_redirects(
    session: requests.Session,
    url: str,
    slack_token: str,
) -> tuple[requests.Response | None, str | None]:
    """GET *url*, following up to :data:`DOWNLOAD_MAX_REDIRECTS` redirects.

    Every hop is re-validated with :func:`is_allowed_file_url` before it is
    requested, so a redirect can't lead off Slack's file hosts.

    Returns:
        ``(response, None)`` on a non-redirect response, or
        ``(None, reason)`` when a hop is refused.
    """
    current = url
    for _hop in range(DOWNLOAD_MAX_REDIRECTS + 1):
        if not is_allowed_file_url(current):
            return None, REASON_DISALLOWED_URL
        headers = {"Authorization": f"Bearer {slack_token}"} if slack_token else {}
        response = session.get(
            current,
            headers=headers,
            stream=True,
            allow_redirects=False,
            timeout=(DOWNLOAD_CONNECT_TIMEOUT_SECONDS, DOWNLOAD_READ_TIMEOUT_SECONDS),
        )
        if not response.is_redirect:
            return response, None
        location = response.headers.get("Location", "")
        response.close()
        current = urljoin(current, location)
    return None, REASON_TOO_MANY_REDIRECTS


def _stream_to_file(
    response: requests.Response,
    dest_dir: str,
    name: str,
    max_size: int | None,
) -> tuple[str, str, int]:
    """Write the response body to a new temp file, hashing as it goes.

    Returns:
        ``(path, md5_hex, byte_count)``.

    Raises:
        _SizeMismatchError: If the body exceeds *max_size*.
        requests.RequestException: If the connection breaks mid-body.
        OSError: If the temp file can't be written.
    """
    fd, path = tempfile.mkstemp(
        dir=dest_dir, prefix="dl_", suffix=safe_temp_suffix(name)
    )
    md5 = hashlib.md5()  # noqa: S324 — content fingerprint, not security
    written = 0
    try:
        with os.fdopen(fd, "wb") as out:
            for chunk in response.iter_content(chunk_size=FILE_READ_CHUNK_BYTES):
                if not chunk:
                    continue
                written += len(chunk)
                if max_size is not None and written > max_size:
                    raise _SizeMismatchError
                md5.update(chunk)
                out.write(chunk)
    except BaseException:
        remove_temp_file(path)
        raise
    return path, md5.hexdigest(), written


def _status_result(status_code: int, url: str) -> DownloadResult:
    """Classify a non-200, non-redirect HTTP status."""
    reason = f"http_{status_code}"
    if status_code == HTTP_RATE_LIMIT or status_code >= HTTP_SERVER_ERROR_MIN:
        return DownloadResult(
            DownloadStatus.FAILED, reason=reason, url=url, http_status=status_code
        )
    return DownloadResult(
        DownloadStatus.UNAVAILABLE, reason=reason, url=url, http_status=status_code
    )


def download_file(
    file_obj: dict[str, Any],
    dest_dir: str,
    channel: str | None = None,
    *,
    slack_token: str = "",
    url: str | None = None,
    allow_html: bool | None = None,
    check_size: bool = True,
    session: requests.Session | None = None,
) -> DownloadResult:
    """Download a Slack export file into a temp file in *dest_dir*.

    Args:
        file_obj: The Slack file object.
        dest_dir: Directory for the temp file.
        channel: Channel name for logging context.
        slack_token: Optional Slack token, sent as ``Authorization: Bearer``.
            It only ever goes to Slack file hosts.
        url: URL to fetch instead of :func:`select_download_url`'s choice.
        allow_html: Accept an HTML body.  Defaults to :func:`expects_html`.
        check_size: Reject a body whose length differs from the export's
            ``size``.
        session: Session to use instead of the shared one (tests).

    Returns:
        A :class:`DownloadResult`.  On ``OK`` the caller must delete
        ``result.path``.
    """
    file_id = file_obj.get("id", "unknown")
    name = file_obj.get("name") or f"file_{file_id}"

    google = google_link_status(file_obj)
    if google is not None:
        link = file_obj.get("url_private") or file_obj.get("url_private_download")
        return DownloadResult(google, url=link)

    reason = unavailable_reason(file_obj)
    if reason == REASON_NO_URL and url:
        reason = None  # the caller supplied the URL
    if reason is not None:
        log_with_context(
            logging.INFO if reason == REASON_EXTERNAL else logging.WARNING,
            f"File {name} is unavailable ({reason}); it will not be uploaded",
            channel=channel,
            file_id=file_id,
        )
        return DownloadResult(
            DownloadStatus.UNAVAILABLE, reason=reason, url=external_link(file_obj)
        )

    target = url or select_download_url(file_obj)
    if not target:
        return DownloadResult(DownloadStatus.UNAVAILABLE, reason=REASON_NO_URL)
    if allow_html is None:
        allow_html = expects_html(file_obj)
    max_size = expected_size(file_obj) if check_size else None
    session = session or get_download_session()

    log_with_context(
        logging.DEBUG,
        f"Downloading file {name} from {redact_url(target)}",
        channel=channel,
        file_id=file_id,
    )

    try:
        response, refusal = _get_following_redirects(session, target, slack_token)
    except requests.RequestException as e:
        status_code = e.response.status_code if e.response is not None else None
        log_with_context(
            logging.WARNING,
            f"Failed to download file {name} after retries: {type(e).__name__}",
            channel=channel,
            file_id=file_id,
            error=str(e),
        )
        return DownloadResult(
            DownloadStatus.FAILED,
            reason=f"network_error:{type(e).__name__}",
            url=target,
            http_status=status_code,
        )

    if response is None:
        log_with_context(
            logging.WARNING,
            f"Refused to download file {name}: {refusal} ({redact_url(target)})",
            channel=channel,
            file_id=file_id,
        )
        return DownloadResult(DownloadStatus.UNAVAILABLE, reason=refusal, url=target)

    with response:
        if response.status_code != HTTP_OK:
            result = _status_result(response.status_code, target)
            log_with_context(
                logging.WARNING,
                f"Failed to download file {name}: HTTP {response.status_code}"
                f" ({result.status.value})",
                channel=channel,
                file_id=file_id,
                http_status=response.status_code,
            )
            return result

        content_type = response.headers.get("Content-Type", "")
        if _is_html_content_type(content_type) and not allow_html:
            log_with_context(
                logging.WARNING,
                f"Download of {name} returned an HTML page instead of the file"
                " (expired export link or missing Slack token?)",
                channel=channel,
                file_id=file_id,
            )
            return DownloadResult(
                DownloadStatus.UNAVAILABLE,
                reason=REASON_HTML_PAGE,
                url=target,
                content_type=content_type,
                http_status=response.status_code,
            )

        try:
            path, md5, size = _stream_to_file(response, dest_dir, name, max_size)
        except _SizeMismatchError:
            return _size_mismatch(name, channel, file_id, target, max_size, None)
        except requests.RequestException as e:
            log_with_context(
                logging.WARNING,
                f"Download of {name} was interrupted: {type(e).__name__}",
                channel=channel,
                file_id=file_id,
                error=str(e),
            )
            return DownloadResult(
                DownloadStatus.FAILED,
                reason=f"network_error:{type(e).__name__}",
                url=target,
            )
        except OSError as e:
            log_with_context(
                logging.ERROR,
                f"Could not write download of {name} to disk: {e}",
                channel=channel,
                file_id=file_id,
            )
            return DownloadResult(
                DownloadStatus.FAILED, reason="write_error", url=target
            )

    if max_size is not None and size != max_size:
        remove_temp_file(path)
        return _size_mismatch(name, channel, file_id, target, max_size, size)

    log_with_context(
        logging.DEBUG,
        f"Downloaded file {name} ({size} bytes)",
        channel=channel,
        file_id=file_id,
    )
    return DownloadResult(
        DownloadStatus.OK,
        path=path,
        md5=md5,
        size=size,
        content_type=content_type or None,
        url=target,
        http_status=HTTP_OK,
    )


def _size_mismatch(
    name: str,
    channel: str | None,
    file_id: str,
    url: str,
    expected: int | None,
    actual: int | None,
) -> DownloadResult:
    """Log and build the result for a body whose length differs from ``size``."""
    got = "more" if actual is None else str(actual)
    log_with_context(
        logging.WARNING,
        f"Download of {name} has the wrong length (expected {expected} bytes,"
        f" got {got}); not uploading it",
        channel=channel,
        file_id=file_id,
    )
    return DownloadResult(
        DownloadStatus.UNAVAILABLE, reason=REASON_SIZE_MISMATCH, url=url
    )


def create_drive_reference(
    file_obj: dict[str, Any],
    channel: str | None,
) -> UploadResult | None:
    """Create a direct reference to an existing Google Drive file.

    Instead of downloading and re-uploading, this links the file already
    hosted on Google Drive.

    Args:
        file_obj: The file object from Slack containing a Google Drive URL.
        channel: Optional channel name for context.

    Returns:
        UploadResult with drive reference details if successful, None otherwise.
    """
    file_id = file_obj.get("id", "unknown")
    name = file_obj.get("name") or f"file_{file_id}"
    url = file_obj.get("url_private") or file_obj.get("url_private_download") or ""

    drive_file_id = None
    try:
        parsed = urlparse(url)
        if parsed.path.startswith("/file/d/"):
            # Format: https://drive.google.com/file/d/FILE_ID/view
            drive_file_id = parsed.path[len("/file/d/") :].split("/")[0]
        elif "id=" in parsed.query:
            # Format: https://drive.google.com/open?id=FILE_ID
            drive_file_id = parsed.query.split("id=", 1)[1].split("&")[0]
    except ValueError:
        drive_file_id = None

    if not drive_file_id:
        log_with_context(
            logging.WARNING,
            f"Could not extract Drive file ID from URL: {redact_url(url)}",
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

    return UploadResult(
        upload_type="drive",
        url=url,
        drive_id=drive_file_id,
        name=name,
        mime_type=file_obj.get("mimetype") or "application/octet-stream",
        metadata={"is_reference": True},
    )
