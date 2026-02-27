"""Typed adapter for the Google Drive API.

Replaces raw ``drive.files().create(...).execute()`` chains with explicit
method calls that are easier to mock, test, and type-check.

The adapter delegates to a pre-built (and retry-wrapped) Drive service
object, so it does **not** add its own retry logic.
"""

from __future__ import annotations

from typing import Any


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

    def list_drives(self) -> dict[str, Any]:
        """List shared drives.

        Returns:
            Raw API response dict with ``drives`` key.
        """
        result: dict[str, Any] = self._svc.drives().list().execute()
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
