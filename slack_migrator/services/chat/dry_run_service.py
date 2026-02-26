"""No-op Google Chat API service for dry-run mode.

Mirrors the method-chain interface of the real Chat API service
(e.g. ``chat.spaces().create(body=X).execute()``) but logs instead of
making real API calls.  Return values match the shapes that callers
actually read from real responses.

Phase 7 of the DI refactoring injects this in place of the real service
when ``dry_run=True``, eliminating scattered ``if dry_run`` checks.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.state import MigrationState


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
# Reactions
# ---------------------------------------------------------------------------


class DryRunReactions:
    """Stub for ``spaces().messages().reactions()``."""

    def create(
        self, *, parent: str = "", body: dict[str, Any] | None = None
    ) -> DryRunRequest:
        log_with_context(logging.DEBUG, f"[DRY RUN] Would create reaction on {parent}")
        return DryRunRequest({})


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class DryRunMessages:
    """Stub for ``spaces().messages()``."""

    def __init__(self, state: MigrationState) -> None:
        self._state = state
        self._counter = 0

    def create(
        self,
        *,
        parent: str = "",
        body: dict[str, Any] | None = None,
        messageId: str = "",
        messageReplyOption: str = "",
    ) -> DryRunRequest:
        self._counter += 1
        thread_name = f"{parent}/threads/dry-run-{self._counter}"
        msg_name = f"{parent}/messages/dry-run-{self._counter}"
        log_with_context(logging.DEBUG, f"[DRY RUN] Would send message to {parent}")
        return DryRunRequest(
            {
                "name": msg_name,
                "thread": {"name": thread_name},
            }
        )

    def list(
        self,
        *,
        parent: str = "",
        pageSize: int = 100,
        orderBy: str = "",
    ) -> DryRunRequest:
        return DryRunRequest({"messages": []})

    def reactions(self) -> DryRunReactions:
        return DryRunReactions()


# ---------------------------------------------------------------------------
# Members
# ---------------------------------------------------------------------------


class DryRunMembers:
    """Stub for ``spaces().members()``."""

    def __init__(self, state: MigrationState) -> None:
        self._state = state
        self._counter = 0

    def create(
        self, *, parent: str = "", body: dict[str, Any] | None = None
    ) -> DryRunRequest:
        self._counter += 1
        member_name = f"{parent}/members/dry-run-{self._counter}"
        log_with_context(logging.DEBUG, f"[DRY RUN] Would add member to {parent}")
        return DryRunRequest({"name": member_name})

    def list(
        self,
        *,
        parent: str = "",
        pageSize: int = 100,
        pageToken: str | None = None,
    ) -> DryRunRequest:
        return DryRunRequest({"memberships": [], "nextPageToken": ""})

    def delete(self, *, name: str = "") -> DryRunRequest:
        log_with_context(logging.DEBUG, f"[DRY RUN] Would delete member {name}")
        return DryRunRequest({})


# ---------------------------------------------------------------------------
# Media
# ---------------------------------------------------------------------------


class DryRunMedia:
    """Stub for ``media()``."""

    def __init__(self) -> None:
        self._counter = 0

    def upload(
        self,
        *,
        parent: str = "",
        media_body: Any = None,
        body: dict[str, Any] | None = None,
    ) -> DryRunRequest:
        self._counter += 1
        filename = (body or {}).get("filename", "unknown")
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would upload media {filename} to {parent}",
        )
        return DryRunRequest(
            {
                "attachmentDataRef": {
                    "resourceName": f"dry-run-media-{self._counter}",
                },
            }
        )


# ---------------------------------------------------------------------------
# Spaces
# ---------------------------------------------------------------------------


class DryRunSpaces:
    """Stub for ``spaces()``."""

    def __init__(self, state: MigrationState) -> None:
        self._state = state
        self._counter = 0

    def create(self, *, body: dict[str, Any] | None = None) -> DryRunRequest:
        self._counter += 1
        display_name = (body or {}).get("displayName", "unknown")
        space_name = f"spaces/dry-run-{self._counter}"
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would create space '{display_name}'",
        )
        return DryRunRequest({"name": space_name})

    def list(
        self,
        *,
        pageSize: int = 100,
        pageToken: str | None = None,
    ) -> DryRunRequest:
        return DryRunRequest({"spaces": [], "nextPageToken": ""})

    def get(self, *, name: str = "") -> DryRunRequest:
        return DryRunRequest(
            {
                "name": name,
                "displayName": "dry-run-space",
                "importMode": False,
                "externalUserAllowed": False,
                "createTime": "",
                "spaceType": "SPACE",
            }
        )

    def patch(
        self,
        *,
        name: str = "",
        updateMask: str = "",
        body: dict[str, Any] | None = None,
    ) -> DryRunRequest:
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would patch space {name} (mask={updateMask})",
        )
        return DryRunRequest({})

    def delete(self, *, name: str = "") -> DryRunRequest:
        log_with_context(logging.DEBUG, f"[DRY RUN] Would delete space {name}")
        return DryRunRequest({})

    def completeImport(self, *, name: str = "") -> DryRunRequest:
        log_with_context(
            logging.DEBUG,
            f"[DRY RUN] Would complete import for {name}",
        )
        return DryRunRequest({})

    def members(self) -> DryRunMembers:
        return DryRunMembers(self._state)

    def messages(self) -> DryRunMessages:
        return DryRunMessages(self._state)


# ---------------------------------------------------------------------------
# Top-level service
# ---------------------------------------------------------------------------


class DryRunChatService:
    """No-op Chat API service for dry-run mode.

    Implements the same method-chain interface as the Google Chat API
    (``service.spaces().create(body=X).execute()``) but returns canned
    responses instead of making real API calls.
    """

    def __init__(self, state: MigrationState) -> None:
        self._state = state

    def spaces(self) -> DryRunSpaces:
        return DryRunSpaces(self._state)

    def media(self) -> DryRunMedia:
        return DryRunMedia()
