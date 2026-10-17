"""No-op Google Drive API service for dry-run mode.

Mirrors the method-chain interface of the real Drive API service
(e.g. ``drive.files().create(body=X).execute()``) but logs instead of
making real API calls.  Return values match the shapes that callers
actually read from real responses.

Phase 7 of the DI refactoring injects this in place of the real service
when ``dry_run=True``, eliminating scattered ``if dry_run`` checks.
"""

from __future__ import annotations

import logging
from typing import Any

from slack_chat_migrator.utils.logging import log_with_context

# ---------------------------------------------------------------------------
# Leaf request object — every chain terminates with .execute()
# ---------------------------------------------------------------------------


class DryRunRequest:
    """Mock API request that returns preset data on ``execute()``."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def execute(self) -> dict[str, Any]:
        return self._data


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


class DryRunFiles:
    """Stub for ``files()``."""

    def __init__(self) -> None:
        self._counter = 0

    def list(self, **kwargs: Any) -> DryRunRequest:
        return DryRunRequest({"files": [], "nextPageToken": ""})

    def get(self, **kwargs: Any) -> DryRunRequest:
        self._counter += 1
        file_id = kwargs.get("fileId", f"dry-run-file-{self._counter}")
        return DryRunRequest(
            {
                "id": file_id,
                "webViewLink": f"https://drive.google.com/dry-run/{file_id}",
            }
        )

    def create(self, **kwargs: Any) -> DryRunRequest:
        self._counter += 1
        file_id = f"dry-run-file-{self._counter}"
        body = kwargs.get("body", {})
        name = body.get("name", "unknown")
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would create Drive file '{name}'",
        )
        return DryRunRequest(
            {
                "id": file_id,
                "webViewLink": f"https://drive.google.com/dry-run/{file_id}",
            }
        )

    def update(self, **kwargs: Any) -> DryRunRequest:
        file_id = kwargs.get("fileId", "unknown")
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would update Drive file {file_id}",
        )
        return DryRunRequest({"id": file_id, **kwargs.get("body", {})})

    def delete(self, **kwargs: Any) -> DryRunRequest:
        file_id = kwargs.get("fileId", "unknown")
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would delete Drive file {file_id}",
        )
        return DryRunRequest({})


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


class DryRunPermissions:
    """Stub for ``permissions()``."""

    def create(self, **kwargs: Any) -> DryRunRequest:
        file_id = kwargs.get("fileId", "unknown")
        body = kwargs.get("body", {})
        email = body.get("emailAddress", "unknown")
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would create permission on {file_id} for {email}",
        )
        return DryRunRequest({"id": "dry-run-permission"})

    def list(self, **kwargs: Any) -> DryRunRequest:
        return DryRunRequest({"permissions": []})

    def delete(self, **kwargs: Any) -> DryRunRequest:
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would delete permission {kwargs.get('permissionId')}"
            f" on {kwargs.get('fileId', 'unknown')}",
        )
        return DryRunRequest({})


class DryRunBatch:
    """Stub for ``new_batch_http_request()``: runs each request in order."""

    def __init__(self, callback: Any = None) -> None:
        self._callback = callback
        self._requests: list[tuple[str, DryRunRequest]] = []

    def add(self, request: DryRunRequest, request_id: str | None = None) -> None:
        self._requests.append((request_id or str(len(self._requests)), request))

    def execute(self) -> None:
        for request_id, request in self._requests:
            response = request.execute()
            if self._callback is not None:
                self._callback(request_id, response, None)


# ---------------------------------------------------------------------------
# Drives (shared drives)
# ---------------------------------------------------------------------------


class DryRunDrives:
    """Stub for ``drives()``."""

    def __init__(self) -> None:
        self._counter = 0

    def get(self, *, driveId: str = "") -> DryRunRequest:
        return DryRunRequest(
            {
                "id": driveId,
                "name": "dry-run-drive",
            }
        )

    def list(self, **kwargs: Any) -> DryRunRequest:
        return DryRunRequest({"drives": []})

    def create(
        self,
        *,
        body: dict[str, Any] | None = None,
        requestId: str = "",
    ) -> DryRunRequest:
        self._counter += 1
        name = (body or {}).get("name", "unknown")
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would create shared drive '{name}'",
        )
        return DryRunRequest({"id": f"dry-run-drive-{self._counter}"})


# ---------------------------------------------------------------------------
# Top-level service
# ---------------------------------------------------------------------------


class DryRunDriveService:
    """No-op Drive API service for dry-run mode.

    Implements the same method-chain interface as the Google Drive API
    (``service.files().create(body=X).execute()``) but returns canned
    responses instead of making real API calls.
    """

    def files(self) -> DryRunFiles:
        return DryRunFiles()

    def permissions(self) -> DryRunPermissions:
        return DryRunPermissions()

    def drives(self) -> DryRunDrives:
        return DryRunDrives()

    def new_batch_http_request(self, callback: Any = None) -> DryRunBatch:
        return DryRunBatch(callback)
