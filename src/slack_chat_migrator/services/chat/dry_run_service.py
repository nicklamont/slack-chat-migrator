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

from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_chat_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_chat_migrator.core.state import MigrationState


# ---------------------------------------------------------------------------
# Leaf request objects — every chain terminates with .execute()
# ---------------------------------------------------------------------------


class DryRunRequest:
    """Mock API request that returns preset data on ``execute()``."""

    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def execute(self) -> dict[str, Any]:
        return self._data


class DryRunErrorRequest:
    """Mock API request that raises ``HttpError`` on ``execute()``."""

    def __init__(self, status_code: int) -> None:
        self._status_code = status_code

    def execute(self) -> dict[str, Any]:
        resp = Response({"status": str(self._status_code)})
        raise HttpError(resp, b"Injected dry-run error")


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

    def __init__(
        self,
        state: MigrationState,
        error_schedule: dict[int, int] | None = None,
    ) -> None:
        self._state = state
        self._counter = 0
        self.captured_calls: list[dict[str, Any]] = []
        self._error_schedule: dict[int, int] = error_schedule or {}

    def create(
        self,
        *,
        parent: str = "",
        body: dict[str, Any] | None = None,
        messageId: str = "",
        messageReplyOption: str = "",
    ) -> DryRunRequest | DryRunErrorRequest:
        self._counter += 1

        # Capture the call for test inspection
        self.captured_calls.append(
            {
                "parent": parent,
                "body": body,
                "messageId": messageId,
                "messageReplyOption": messageReplyOption,
            }
        )

        # Check error schedule
        if self._counter in self._error_schedule:
            status_code = self._error_schedule[self._counter]
            log_with_context(
                logging.DEBUG,
                f"[DRY RUN] Injecting HTTP {status_code} error for message #{self._counter}",
            )
            return DryRunErrorRequest(status_code)

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
    """Stub for ``spaces()``.

    Holds singleton ``_messages`` and ``_members`` sub-objects so that
    state (counters, captured calls) accumulates correctly across the
    entire migration run.
    """

    def __init__(
        self,
        state: MigrationState,
        message_error_schedule: dict[int, int] | None = None,
    ) -> None:
        self._state = state
        self._counter = 0
        self._messages = DryRunMessages(state, error_schedule=message_error_schedule)
        self._members = DryRunMembers(state)

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
        return self._members

    def messages(self) -> DryRunMessages:
        return self._messages


# ---------------------------------------------------------------------------
# Top-level service
# ---------------------------------------------------------------------------


class DryRunChatService:
    """No-op Chat API service for dry-run mode.

    Implements the same method-chain interface as the Google Chat API
    (``service.spaces().create(body=X).execute()``) but returns canned
    responses instead of making real API calls.

    Holds singleton ``_spaces`` and ``_media`` sub-objects so that state
    (counters, captured calls, error schedules) persists across the
    entire migration run.
    """

    def __init__(
        self,
        state: MigrationState,
        message_error_schedule: dict[int, int] | None = None,
    ) -> None:
        self._state = state
        self._spaces = DryRunSpaces(
            state, message_error_schedule=message_error_schedule
        )
        self._media = DryRunMedia()

    def spaces(self) -> DryRunSpaces:
        return self._spaces

    def media(self) -> DryRunMedia:
        return self._media

    @property
    def captured_messages(self) -> list[dict[str, Any]]:
        """All ``create()`` calls recorded by the messages stub."""
        return self._spaces.messages().captured_calls
