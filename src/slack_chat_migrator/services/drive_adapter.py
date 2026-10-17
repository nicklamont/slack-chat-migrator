"""Typed adapter for the Google Drive API.

Replaces raw ``drive.files().create(...).execute()`` chains with explicit
method calls that are easier to mock, test, and type-check.

The adapter delegates to a pre-built (and retry-wrapped) Drive service
object, so it does **not** add its own retry logic.  The one exception is
:meth:`DriveAdapter.apply_permission_changes`: calls that fail inside a batch
request are re-sent one by one through the retry-wrapped service.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DRIVE_BATCH_MAX_REQUESTS,
    HTTP_FORBIDDEN,
    HTTP_RATE_LIMIT,
    HTTP_SERVER_ERROR_MIN,
)


@dataclass(frozen=True)
class PermissionChange:
    """One permission to create (``body`` set) or delete (``permission_id`` set)."""

    file_id: str
    body: dict[str, Any] | None = None
    permission_id: str | None = None
    supports_all_drives: bool = False
    transfer_ownership: bool = False


def _retry_individually(error: HttpError) -> bool:
    """Whether a call that failed inside a batch is worth sending again alone."""
    status = int(getattr(error.resp, "status", 0) or 0)
    return (
        status in (HTTP_FORBIDDEN, HTTP_RATE_LIMIT) or status >= HTTP_SERVER_ERROR_MIN
    )


class DriveAdapter:
    """Thin typed wrapper around the Google Drive API service."""

    def __init__(self, service: Any) -> None:
        self._svc = service

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
        result: dict[str, Any] = self._svc.files().list(**kwargs).execute()
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
        result: dict[str, Any] = self._svc.files().create(**kwargs).execute()
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
        result: dict[str, Any] = self._svc.files().get(**kwargs).execute()
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
        result: dict[str, Any] = self._svc.files().update(**kwargs).execute()
        return result

    def delete_file(self, file_id: str) -> dict[str, Any]:
        """Delete a file.

        Args:
            file_id: Drive file ID.

        Returns:
            API response dict (typically empty on success).
        """
        result: dict[str, Any] = self._svc.files().delete(fileId=file_id).execute()
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
            send_notification_email: Send email to grantee.
            supports_all_drives: Enable shared-drive support.
            transfer_ownership: Transfer ownership to the grantee.

        Returns:
            Created permission resource dict.
        """
        kwargs: dict[str, Any] = {
            "fileId": file_id,
            "body": body,
            "fields": fields,
            "sendNotificationEmail": send_notification_email,
        }
        if supports_all_drives:
            kwargs["supportsAllDrives"] = True
        if transfer_ownership:
            kwargs["transferOwnership"] = True
        result: dict[str, Any] = self._svc.permissions().create(**kwargs).execute()
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
            result: dict[str, Any] = self._svc.permissions().list(**kwargs).execute()
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
        result: dict[str, Any] = self._svc.permissions().delete(**kwargs).execute()
        return result or {}

    def _permission_request(self, change: PermissionChange) -> Any:
        """Build (without executing) the request for one PermissionChange."""
        kwargs: dict[str, Any] = {"fileId": change.file_id}
        if change.supports_all_drives:
            kwargs["supportsAllDrives"] = True
        if change.permission_id is not None:
            kwargs["permissionId"] = change.permission_id
            return self._svc.permissions().delete(**kwargs)
        kwargs.update(
            {"body": change.body, "fields": "id", "sendNotificationEmail": False}
        )
        if change.transfer_ownership:
            kwargs["transferOwnership"] = True
        return self._svc.permissions().create(**kwargs)

    def apply_permission_changes(
        self, changes: list[PermissionChange]
    ) -> list[HttpError | None]:
        """Create and delete permissions, batching several into one request.

        A single change is sent on its own.  Several are sent as Drive batch
        requests of up to :data:`DRIVE_BATCH_MAX_REQUESTS` calls; calls that
        fail with a rate-limit or server error are then re-sent one by one
        through the retry-wrapped service.

        Args:
            changes: The permissions to create or delete.

        Returns:
            One entry per change, in order: None on success, else the error.
        """
        requests = [self._permission_request(change) for change in changes]
        results: list[HttpError | None] = [None] * len(requests)
        raw_service = getattr(self._svc, "_wrapped_obj", self._svc)
        new_batch = getattr(raw_service, "new_batch_http_request", None)

        if len(requests) > 1 and new_batch is not None:
            for start in range(0, len(requests), DRIVE_BATCH_MAX_REQUESTS):

                def callback(
                    request_id: str,
                    _response: Any,
                    exception: Exception | None,
                    _offset: int = start,
                ) -> None:
                    if isinstance(exception, HttpError):
                        results[_offset + int(request_id)] = exception

                batch = new_batch(callback=callback)
                for index, request in enumerate(
                    requests[start : start + DRIVE_BATCH_MAX_REQUESTS]
                ):
                    # Batches take the raw request, not the retry wrapper.
                    batch.add(
                        getattr(request, "_wrapped_obj", request),
                        request_id=str(index),
                    )
                batch.execute()
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
            except HttpError as e:
                results[index] = e
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
        result: dict[str, Any] = self._svc.permissions().update(**kwargs).execute()
        return result

    # -- Shared Drives --------------------------------------------------------

    def get_drive(self, drive_id: str) -> dict[str, Any]:
        """Get shared drive metadata.

        Args:
            drive_id: Shared drive ID.

        Returns:
            Drive resource dict.
        """
        result: dict[str, Any] = self._svc.drives().get(driveId=drive_id).execute()
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
        result: dict[str, Any] = self._svc.drives().list(**kwargs).execute()
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
        result: dict[str, Any] = (
            self._svc.drives().create(body=body, requestId=request_id).execute()
        )
        return result
