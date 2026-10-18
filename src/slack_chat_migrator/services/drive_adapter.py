"""Typed adapter for the Google Drive API.

Replaces raw ``drive.files().create(...).execute()`` chains with explicit
method calls that are easier to mock, test, and type-check.

The adapter delegates to a pre-built (and retry-wrapped) Drive service
object, so it does **not** add its own retry logic.  The one exception is
:meth:`DriveAdapter.apply_permission_changes`, which re-sends some calls that
failed inside a batch request; its docstring states the rule.

Every other method raises what the service raises once its retries are
spent: ``HttpError``, ``RefreshError`` (credentials that can't be refreshed)
or a network error.  :data:`DRIVE_CALL_ERRORS` names them all.  Callers that
handle a failed call catch :data:`DRIVE_CALL_CANDIDATES` and re-raise what
:func:`is_drive_call_error` rejects, so a network-down ``OSError`` fails the
call while a local fault (a full disk) still stops the run.

A 403 that says a Drive quota is used up (:data:`QUOTA_REASONS`) is raised
as :class:`~slack_chat_migrator.exceptions.DriveQuotaError` instead, by
every method: it isn't a Drive call error, so it passes those handlers and
stops the run.
"""

from __future__ import annotations

import errno
import socket
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DRIVE_BATCH_MAX_REQUESTS,
    HTTP_FORBIDDEN,
    HTTP_RATE_LIMIT,
    HTTP_SERVER_ERROR_MIN,
)
from slack_chat_migrator.exceptions import DriveQuotaError
from slack_chat_migrator.utils.api import (
    HTTP_REQUEST_TIMEOUT,
    RETRYABLE_NETWORK_ERRORS,
    http_error_reasons,
    http_status,
    is_rate_limit_error,
)

#: Network failures with a class of their own.  Built from the shared retry
#: tuple, so classes added there (e.g. ``http.client.HTTPException``) count
#: here too; DNS failures are plain ``socket.gaierror``/``herror``.
_NETWORK_ERRORS: tuple[type[BaseException], ...] = (
    *RETRYABLE_NETWORK_ERRORS,
    socket.gaierror,
    socket.herror,
)

#: Errors a Drive call raises once retries are spent: API errors, credentials
#: that can't be refreshed and network errors.  apply_permission_changes
#: returns them instead of raising.
DRIVE_CALL_ERRORS: tuple[type[BaseException], ...] = (
    HttpError,
    RefreshError,
    *_NETWORK_ERRORS,
)

#: ``errno`` values of a plain OSError that mean the network is down.
#: httplib2 and googleapiclient pass these on as plain OSErrors, so they
#: aren't in :data:`DRIVE_CALL_ERRORS` by class.
NETWORK_ERRNOS: frozenset[int] = frozenset(
    {
        errno.ENETUNREACH,
        errno.EHOSTUNREACH,
        errno.ENETDOWN,
        errno.EHOSTDOWN,
        errno.ENETRESET,
        errno.EADDRNOTAVAIL,
        errno.ECONNREFUSED,
        errno.ECONNRESET,
        errno.ECONNABORTED,
        errno.ENOTCONN,
        errno.ENOBUFS,
        errno.ETIMEDOUT,
        errno.EPIPE,
    }
)

#: ``errno`` values of a local fault that would hit every file in turn (no
#: space, quota, read-only file system, too many open files).  These must
#: stop the run instead of failing one file.
FATAL_LOCAL_ERRNOS: frozenset[int] = frozenset(
    code
    for code in (
        errno.ENOSPC,
        getattr(errno, "EDQUOT", None),
        errno.EROFS,
        errno.EMFILE,
        errno.ENFILE,
    )
    if code is not None
)

#: What to catch around a Drive call: :data:`DRIVE_CALL_ERRORS` plus OSError,
#: so that network-down errors without a class of their own are caught too.
#: Handlers re-raise what :func:`is_drive_call_error` rejects.
DRIVE_CALL_CANDIDATES: tuple[type[BaseException], ...] = (*DRIVE_CALL_ERRORS, OSError)


#: 403 reasons that mean a Drive quota is used up: the project's API limits,
#: the storage the attachments count against, or a shared drive's item
#: limit.  Every later Drive call would fail the same way, so they stop the
#: run (:func:`drive_quota_error`).  Rate limits (``userRateLimitExceeded``,
#: ``rateLimitExceeded``) aren't among them: the retry wrapper waits them
#: out.  ``quotaExceeded`` is waited out first too, then stops the run.
QUOTA_REASONS = frozenset(
    {
        "dailyLimitExceeded",
        "quotaExceeded",
        "storageQuotaExceeded",
        "teamDriveFileLimitExceeded",
    }
)

#: What to do about each of :data:`QUOTA_REASONS`.
QUOTA_FIXES: Mapping[str, str] = MappingProxyType(
    {
        "dailyLimitExceeded": (
            "the project's daily Drive API limit is used up. Wait for it to"
            " reset, or raise it in the Google Cloud console (IAM & Admin >"
            " Quotas)"
        ),
        "quotaExceeded": (
            "a Drive API quota of the project is used up. Wait for it to"
            " reset, or raise it in the Google Cloud console (IAM & Admin >"
            " Quotas)"
        ),
        "storageQuotaExceeded": (
            "the Drive storage the attachments count against is full (in My"
            " Drive, the workspace admin's own storage). Free space or add"
            " storage, or store the attachments on a shared drive"
        ),
        "teamDriveFileLimitExceeded": (
            "the shared drive has reached Drive's limit on the number of"
            " items, which can't be raised. Set shared_drive.id or"
            " shared_drive.name to another shared drive"
        ),
    }
)


def quota_reason(error: BaseException | None) -> str | None:
    """The quota reason of *error* (see :data:`QUOTA_REASONS`), if any."""
    if not isinstance(error, HttpError):
        return None
    reasons = sorted(http_error_reasons(error) & QUOTA_REASONS)
    return reasons[0] if reasons else None


def drive_quota_error(
    error: BaseException | None, action: str | None = None
) -> DriveQuotaError | None:
    """The DriveQuotaError for *error*, or None if it isn't a quota error.

    Args:
        error: Any exception (or None).
        action: What was being done, e.g. ``"upload a file"``.

    Returns:
        An error naming the quota and the fix.
    """
    reason = quota_reason(error)
    if reason is None:
        return None
    during = f" while trying to {action}" if action else ""
    return DriveQuotaError(
        f"Drive quota reached ({reason}){during}: {QUOTA_FIXES[reason]}."
        " Then run the migration again: `migrate --resume` continues where"
        " this run stopped.",
        reason,
    )


def is_network_error(error: BaseException) -> bool:
    """Whether *error* means the network failed, not the local machine."""
    if isinstance(error, _NETWORK_ERRORS):
        return True
    return isinstance(error, OSError) and error.errno in NETWORK_ERRNOS


def is_drive_call_error(error: BaseException) -> bool:
    """Whether *error* should fail one Drive call rather than the run.

    True for :data:`DRIVE_CALL_ERRORS` and network-down OSErrors; False for
    local OSErrors such as a full disk, which handlers re-raise.
    """
    return isinstance(error, DRIVE_CALL_ERRORS) or is_network_error(error)


def is_transient_error(error: BaseException) -> bool:
    """Whether a later attempt may succeed where *error* failed.

    True for network errors (including network-down OSErrors), credentials
    that couldn't be refreshed (a later run with working credentials can
    succeed), and HTTP 408, 429, 5xx, rate-limit 403s and quota 403s (which
    the adapter raises as DriveQuotaError, stopping the run).  False for other
    HTTP errors (400, 404, a 403 that isn't a rate limit), an HttpError
    without a response (a ``BatchError`` raised while building a batch) and
    local errors.
    """
    if isinstance(error, HttpError):
        status = http_status(error)
        if status is None:
            return False
        return (
            status in (HTTP_REQUEST_TIMEOUT, HTTP_RATE_LIMIT)
            or status >= HTTP_SERVER_ERROR_MIN
            or is_rate_limit_error(error)
            or quota_reason(error) is not None
        )
    return isinstance(error, RefreshError) or is_network_error(error)


def is_fatal_local_error(error: BaseException) -> bool:
    """Whether *error* is a local fault that must stop the run (see
    :data:`FATAL_LOCAL_ERRNOS`)."""
    return (
        isinstance(error, OSError)
        and not is_network_error(error)
        and error.errno in FATAL_LOCAL_ERRNOS
    )


@dataclass(frozen=True)
class PermissionChange:
    """One permission to create (``body`` set) or delete (``permission_id`` set).

    Exactly one of ``body`` and ``permission_id`` is set, and
    ``transfer_ownership`` only goes with a ``body``.
    """

    file_id: str
    body: dict[str, Any] | None = None
    permission_id: str | None = None
    supports_all_drives: bool = False
    transfer_ownership: bool = False

    def __post_init__(self) -> None:
        """Reject changes that are neither, or both, a grant and a removal."""
        if (self.body is None) == (self.permission_id is None):
            raise ValueError(
                "PermissionChange needs exactly one of body (a grant) or"
                " permission_id (a removal)"
            )
        if self.transfer_ownership and self.body is None:
            raise ValueError("transfer_ownership only applies to a grant (body)")


def _retry_individually(error: BaseException) -> bool:
    """Whether a call that failed inside a batch is re-sent on its own.

    See :meth:`DriveAdapter.apply_permission_changes` for the rule.
    """
    if isinstance(error, HttpError):
        status = http_status(error) or 0
        return (
            status in (HTTP_FORBIDDEN, HTTP_REQUEST_TIMEOUT, HTTP_RATE_LIMIT)
            or status >= HTTP_SERVER_ERROR_MIN
        )
    # Credentials that can't be refreshed won't work on a second try.
    return not isinstance(error, RefreshError)


class DriveAdapter:
    """Thin typed wrapper around the Google Drive API service."""

    def __init__(self, service: Any) -> None:
        self._svc = service

    @staticmethod
    def _execute(request: Any, action: str) -> Any:
        """Run *request*; a quota 403 is raised as DriveQuotaError."""
        try:
            return request.execute()
        except HttpError as e:
            quota = drive_quota_error(e, action)
            if quota is not None:
                raise quota from e
            raise

    # -- Files ----------------------------------------------------------------

    def list_files(
        self,
        q: str | None = None,
        page_size: int = 100,
        fields: str = "files(id, name)",
        page_token: str | None = None,
        *,
        spaces: str = "drive",
        corpora: str | None = None,
        drive_id: str | None = None,
        include_items_from_all_drives: bool = False,
        supports_all_drives: bool = False,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        """List or search for files.

        Args:
            q: Search query string (Drive API ``q`` parameter).
            page_size: Maximum files per page.
            fields: Response field mask.
            page_token: Continuation token.
            spaces: Drive spaces to search (default ``"drive"``).
            corpora: Search scope (e.g. ``"drive"`` for shared drives).
            drive_id: Shared drive ID when ``corpora="drive"``.
            include_items_from_all_drives: Include shared-drive items.
            supports_all_drives: Enable shared-drive support.
            order_by: Sort order (e.g. ``"createdTime desc"``).

        Returns:
            Raw API response dict with ``files`` and ``nextPageToken`` keys.
        """
        kwargs: dict[str, Any] = {
            "pageSize": page_size,
            "fields": fields,
            "spaces": spaces,
        }
        if q is not None:
            kwargs["q"] = q
        if page_token is not None:
            kwargs["pageToken"] = page_token
        if corpora is not None:
            kwargs["corpora"] = corpora
        if drive_id is not None:
            kwargs["driveId"] = drive_id
        if include_items_from_all_drives:
            kwargs["includeItemsFromAllDrives"] = True
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        if order_by is not None:
            kwargs["orderBy"] = order_by
        result: dict[str, Any] = self._execute(
            self._svc.files().list(**kwargs), "list or search files"
        )
        return result

    def create_file(
        self,
        body: dict[str, Any],
        media_body: Any = None,
        fields: str = "id",
        *,
        supports_all_drives: bool = False,
    ) -> dict[str, Any]:
        """Create (upload) a file.

        Args:
            body: File metadata dict.
            media_body: Optional ``MediaFileUpload`` or ``MediaIoBaseUpload``.
            fields: Response field mask.
            supports_all_drives: Enable shared-drive support.

        Returns:
            Created file resource dict.
        """
        kwargs: dict[str, Any] = {"body": body, "fields": fields}
        if media_body is not None:
            kwargs["media_body"] = media_body
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        result: dict[str, Any] = self._execute(
            self._svc.files().create(**kwargs), "create a file"
        )
        return result

    def get_file(
        self,
        file_id: str,
        fields: str | None = None,
        *,
        supports_all_drives: bool = False,
    ) -> dict[str, Any]:
        """Get file metadata.

        Args:
            file_id: Drive file ID.
            fields: Optional response field mask.
            supports_all_drives: Enable shared-drive support.

        Returns:
            File resource dict.
        """
        kwargs: dict[str, Any] = {"fileId": file_id}
        if fields is not None:
            kwargs["fields"] = fields
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        result: dict[str, Any] = self._execute(
            self._svc.files().get(**kwargs), "read a file"
        )
        return result

    def update_file(
        self,
        file_id: str,
        body: dict[str, Any],
        fields: str = "id",
        *,
        supports_all_drives: bool = False,
    ) -> dict[str, Any]:
        """Update file metadata.

        Args:
            file_id: Drive file ID.
            body: Fields to change.
            fields: Response field mask.
            supports_all_drives: Enable shared-drive support.

        Returns:
            Updated file resource dict.
        """
        kwargs: dict[str, Any] = {"fileId": file_id, "body": body, "fields": fields}
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        result: dict[str, Any] = self._execute(
            self._svc.files().update(**kwargs), "update a file"
        )
        return result

    def delete_file(self, file_id: str) -> dict[str, Any]:
        """Delete a file.

        Args:
            file_id: Drive file ID.

        Returns:
            API response dict (typically empty on success).
        """
        result: dict[str, Any] = self._execute(
            self._svc.files().delete(fileId=file_id), "delete a file"
        )
        return result

    # -- Permissions ----------------------------------------------------------

    def create_permission(
        self,
        file_id: str,
        body: dict[str, Any],
        fields: str = "id",
        *,
        send_notification_email: bool = False,
        supports_all_drives: bool = False,
        transfer_ownership: bool = False,
    ) -> dict[str, Any]:
        """Create a permission on a file.

        Args:
            file_id: Drive file ID.
            body: Permission body (``role``, ``type``, ``emailAddress``).
            fields: Response field mask.
            send_notification_email: Send email to grantee.  Ignored for an
                ownership transfer, where Drive always notifies the new owner.
            supports_all_drives: Enable shared-drive support.
            transfer_ownership: Transfer ownership to the grantee.

        Returns:
            Created permission resource dict.
        """
        kwargs: dict[str, Any] = {"fileId": file_id, "body": body, "fields": fields}
        if transfer_ownership:
            # Drive: sendNotificationEmail "must not be disabled for ownership
            # transfers", so it is left at its default (true).
            kwargs["transferOwnership"] = True
        else:
            kwargs["sendNotificationEmail"] = send_notification_email
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        result: dict[str, Any] = self._execute(
            self._svc.permissions().create(**kwargs), "share a file"
        )
        return result

    def list_permissions(
        self,
        file_id: str,
        *,
        supports_all_drives: bool = False,
        fields: str = "nextPageToken, permissions(id, type, role, emailAddress,"
        " permissionDetails)",
    ) -> list[dict[str, Any]]:
        """List every permission on a file or folder (all pages).

        Args:
            file_id: Drive file ID.
            supports_all_drives: Enable shared-drive support.
            fields: Response field mask; must include ``nextPageToken``.

        Returns:
            Permission resource dicts.
        """
        permissions: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            kwargs: dict[str, Any] = {"fileId": file_id, "fields": fields}
            if supports_all_drives:
                kwargs["supportsAllDrives"] = True
            if page_token:
                kwargs["pageToken"] = page_token
            result: dict[str, Any] = self._execute(
                self._svc.permissions().list(**kwargs), "list permissions"
            )
            permissions.extend(result.get("permissions", []))
            page_token = result.get("nextPageToken")
            if not page_token:
                return permissions

    def delete_permission(
        self,
        file_id: str,
        permission_id: str,
        *,
        supports_all_drives: bool = False,
    ) -> dict[str, Any]:
        """Delete a permission from a file.

        Args:
            file_id: Drive file ID.
            permission_id: Permission ID to delete.
            supports_all_drives: Enable shared-drive support.

        Returns:
            API response dict (empty on success).
        """
        kwargs: dict[str, Any] = {"fileId": file_id, "permissionId": permission_id}
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        result: dict[str, Any] = self._execute(
            self._svc.permissions().delete(**kwargs), "remove a permission"
        )
        return result or {}

    def _permission_request(self, change: PermissionChange) -> Any:
        """Build (without executing) the request for one PermissionChange."""
        kwargs: dict[str, Any] = {"fileId": change.file_id}
        if change.supports_all_drives:
            kwargs["supportsAllDrives"] = True
        if change.permission_id is not None:
            kwargs["permissionId"] = change.permission_id
            return self._svc.permissions().delete(**kwargs)
        kwargs.update({"body": change.body, "fields": "id"})
        if change.transfer_ownership:
            # The notification can't be disabled for ownership transfers.
            kwargs["transferOwnership"] = True
        else:
            kwargs["sendNotificationEmail"] = False
        return self._svc.permissions().create(**kwargs)

    def apply_permission_changes(
        self, changes: list[PermissionChange]
    ) -> list[BaseException | None]:
        """Create and delete permissions, batching several into one request.

        A single change is sent on its own, through the retry-wrapped
        service.  Several are sent as Drive batch requests of up to
        :data:`DRIVE_BATCH_MAX_REQUESTS` calls.

        Re-send rule: a call that failed inside a batch is sent once more on
        its own, through the retry-wrapped service, when its error is an
        ``HttpError`` with status 403, 408, 429 or 5xx (403 because Drive
        reports rate limits as 403), or any non-HTTP failure such as a
        dropped connection, except a ``RefreshError``.  When the whole batch
        request fails, every call in it gets that error and the same rule
        applies.  Other ``HttpError`` statuses (400, 404, ...) and
        ``RefreshError`` are not re-sent.

        Args:
            changes: The permissions to create or delete.

        Returns:
            One entry per change, in order: None on success, else the error.
            ``HttpError``, ``RefreshError`` and network errors (including
            network-down OSErrors) that outlast the retries are returned,
            never raised; a local fault such as a full disk is raised, and so
            is a used-up quota (DriveQuotaError, once every change was tried).
        """
        requests = [self._permission_request(change) for change in changes]
        results: list[BaseException | None] = [None] * len(requests)
        raw_service = getattr(self._svc, "_wrapped_obj", self._svc)
        new_batch = getattr(raw_service, "new_batch_http_request", None)

        if len(requests) > 1 and new_batch is not None:
            for start in range(0, len(requests), DRIVE_BATCH_MAX_REQUESTS):

                def callback(
                    request_id: str,
                    _response: Any,
                    exception: BaseException | None,
                    _offset: int = start,
                ) -> None:
                    if exception is not None:
                        results[_offset + int(request_id)] = exception

                batch = new_batch(callback=callback)
                chunk = requests[start : start + DRIVE_BATCH_MAX_REQUESTS]
                for index, request in enumerate(chunk):
                    # Batches take the raw request, not the retry wrapper.
                    batch.add(
                        getattr(request, "_wrapped_obj", request),
                        request_id=str(index),
                    )
                try:
                    batch.execute()
                except DRIVE_CALL_CANDIDATES as e:
                    if not is_drive_call_error(e):
                        raise
                    # The whole batch request failed: each call in it gets the
                    # error, then the re-send rule applies.
                    for offset in range(len(chunk)):
                        results[start + offset] = e
            pending = [
                i
                for i, error in enumerate(results)
                if error is not None and _retry_individually(error)
            ]
        else:
            pending = list(range(len(requests)))

        for index in pending:
            try:
                requests[index].execute()
                results[index] = None
            except DRIVE_CALL_CANDIDATES as e:
                if not is_drive_call_error(e):
                    raise
                results[index] = e
        for error in results:
            quota = drive_quota_error(error, "change permissions")
            if quota is not None:
                raise quota from error
        return results

    def update_permission(
        self,
        file_id: str,
        permission_id: str,
        body: dict[str, Any],
        *,
        supports_all_drives: bool = False,
        transfer_ownership: bool = False,
    ) -> dict[str, Any]:
        """Update a permission on a file.

        Args:
            file_id: Drive file ID.
            permission_id: Permission ID to update.
            body: Fields to update (e.g. ``{"role": "owner"}``).
            supports_all_drives: Enable shared-drive support.
            transfer_ownership: Transfer ownership to the grantee.

        Returns:
            Updated permission resource dict.
        """
        kwargs: dict[str, Any] = {
            "fileId": file_id,
            "permissionId": permission_id,
            "body": body,
        }
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        if transfer_ownership:
            kwargs["transferOwnership"] = True
        result: dict[str, Any] = self._execute(
            self._svc.permissions().update(**kwargs), "change a permission"
        )
        return result

    # -- Shared Drives --------------------------------------------------------

    def get_drive(self, drive_id: str, *, fields: str | None = None) -> dict[str, Any]:
        """Get shared drive metadata.

        Args:
            drive_id: Shared drive ID.
            fields: Optional response field mask, e.g.
                ``"id, name, capabilities(canAddChildren)"``.

        Returns:
            Drive resource dict.
        """
        kwargs: dict[str, Any] = {"driveId": drive_id}
        if fields is not None:
            kwargs["fields"] = fields
        result: dict[str, Any] = self._execute(
            self._svc.drives().get(**kwargs), "open a shared drive"
        )
        return result

    def list_drives(
        self,
        page_size: int = 100,
        page_token: str | None = None,
        *,
        q: str | None = None,
        fields: str | None = None,
    ) -> dict[str, Any]:
        """List shared drives the caller is a member of (one page).

        Args:
            page_size: Maximum drives per page (the API allows 1-100 and
                returns at most 10 when it is omitted).
            page_token: Continuation token from ``nextPageToken``.
            q: Optional search query, e.g. ``name = 'Attachments'``.
            fields: Optional response field mask.

        Returns:
            Raw API response dict with ``drives`` and ``nextPageToken`` keys.
        """
        kwargs: dict[str, Any] = {"pageSize": page_size}
        if page_token is not None:
            kwargs["pageToken"] = page_token
        if q is not None:
            kwargs["q"] = q
        if fields is not None:
            kwargs["fields"] = fields
        result: dict[str, Any] = self._execute(
            self._svc.drives().list(**kwargs), "list shared drives"
        )
        return result

    def create_drive(
        self,
        body: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        """Create a shared drive.

        Args:
            body: Drive metadata (must include ``name``).
            request_id: Idempotency key.

        Returns:
            Created drive resource dict.
        """
        result: dict[str, Any] = self._execute(
            self._svc.drives().create(body=body, requestId=request_id),
            "create a shared drive",
        )
        return result
