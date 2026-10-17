"""Poster grants and ownership transfers that failed and must be made again.

A poster's ``writer`` grant or an ownership transfer can fail after the file
was uploaded (a network error, a sharing policy).  The upload still counts.
The failure is recorded here, and on the Drive file itself in its
``appProperties``, so the sweep at the end of every run can retry it, also
for files that a later run never links again (``--resume`` skips messages
it already sent):

- ``slackGrantPending = "true"`` while any poster's grant on the file is
  pending, plus one ``slackGrant.<hash> = <email>`` per such poster.  The
  hash is the first 8 hex digits of the SHA-256 of the lowercased email,
  so one poster's success never clears another's pending grant.
- ``slackOwnerPending = "true"`` and ``slackOwner = <email>`` while the
  file's ownership transfer is pending.

The markers have a fixed value so that one ``appProperties has {...}`` query
finds every flagged file.  Drive limits an entry to 124 bytes of key plus
value; an email that doesn't fit is stored as ``"-"``.  This run still knows
it and retries it, but a later run can't, and its sweep says so.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Collection
from typing import TYPE_CHECKING, Any

from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    is_drive_call_error,
)
from slack_chat_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter

GRANT_MARKER = "slackGrantPending"
OWNER_MARKER = "slackOwnerPending"
GRANT_TARGET_PREFIX = "slackGrant."
OWNER_TARGET = "slackOwner"
MARKER_VALUE = "true"
#: Stored instead of an email too long for an appProperties entry.
UNKNOWN_TARGET = "-"

#: Drive limits each appProperties entry to 124 bytes of key plus value.
_APP_PROPERTY_MAX_BYTES = 124
#: The largest page Drive serves for files.list.
_SWEEP_PAGE_SIZE = 1000


def _normalize(email: str) -> str:
    return email.strip().lower()


def grant_key(email: str) -> str:
    """The appProperties key that holds *email*'s pending grant."""
    digest = hashlib.sha256(_normalize(email).encode("utf-8")).hexdigest()
    return f"{GRANT_TARGET_PREFIX}{digest[:8]}"


def _stored_value(key: str, email: str) -> str:
    """*email*, or :data:`UNKNOWN_TARGET` if it doesn't fit next to *key*."""
    size = len(key.encode("utf-8")) + len(email.encode("utf-8"))
    return email if size <= _APP_PROPERTY_MAX_BYTES else UNKNOWN_TARGET


class PendingPermissions:
    """Which files still need a poster grant or an ownership transfer.

    The state lives in memory for this run and in each file's
    ``appProperties`` for later runs; see the module docstring.
    """

    def __init__(
        self,
        drive_service: DriveAdapter,
        *,
        on_write_failure: Callable[[str, BaseException], None] | None = None,
    ) -> None:
        """Initialize the record.

        Args:
            drive_service: Typed Drive API adapter, for storing the flags.
            on_write_failure: Called with the file ID and the error when a
                flag can't be stored on the file, so the caller can count it.
        """
        self.drive_service = drive_service
        self._on_write_failure = on_write_failure or (lambda _file, _e: None)
        #: Drive file ID -> {grant key: poster email, or UNKNOWN_TARGET}.
        self.grants: dict[str, dict[str, str]] = {}
        #: Drive file ID -> new owner's email, or UNKNOWN_TARGET.
        self.owners: dict[str, str] = {}

    def read(self, file_id: str, app_properties: dict[str, Any]) -> None:
        """Learn a file's pending grants and transfer from its appProperties.

        What this run already knows about the file is kept: it may hold an
        email that was stored as ``"-"``.
        """
        if app_properties.get(GRANT_MARKER) == MARKER_VALUE:
            targets = self.grants.setdefault(file_id, {})
            for key, value in app_properties.items():
                if key.startswith(GRANT_TARGET_PREFIX) and isinstance(value, str):
                    targets.setdefault(key, value)
            if not targets:
                del self.grants[file_id]
        if app_properties.get(OWNER_MARKER) == MARKER_VALUE:
            owner = app_properties.get(OWNER_TARGET)
            self.owners.setdefault(
                file_id, owner if isinstance(owner, str) else UNKNOWN_TARGET
            )

    def has_grant(self, file_id: str, email: str) -> bool:
        """Whether *email*'s grant on the file is pending."""
        return grant_key(email) in self.grants.get(file_id, {})

    def owner(self, file_id: str) -> str | None:
        """The pending new owner of the file (maybe UNKNOWN_TARGET), if any."""
        return self.owners.get(file_id)

    def set_grant(
        self,
        file_id: str,
        email: str,
        pending: bool,
        shared_drive_id: str | None = None,
    ) -> None:
        """Flag *email*'s grant on the file as pending, or clear the flag.

        Nothing is sent when the flag doesn't change.  Storing it is best
        effort: a failure is logged as a warning and reported to
        *on_write_failure*, and only this run remembers the flag.
        """
        key = grant_key(email)
        targets = dict(self.grants.get(file_id, {}))
        if pending == (key in targets):
            return
        if pending:
            targets[key] = email
            props: dict[str, str | None] = {
                GRANT_MARKER: MARKER_VALUE,
                key: _stored_value(key, email),
            }
        else:
            del targets[key]
            props = {key: None}
            if not targets:
                props[GRANT_MARKER] = None
        if targets:
            self.grants[file_id] = targets
        else:
            self.grants.pop(file_id, None)
        self._write(file_id, props, shared_drive_id)

    def set_owner(self, file_id: str, email: str, pending: bool) -> None:
        """Flag the file's ownership transfer to *email* as pending, or clear it.

        See :meth:`set_grant`.  Transfers only happen in regular Drive.
        """
        if pending == (file_id in self.owners):
            return
        if pending:
            self.owners[file_id] = email
            props: dict[str, str | None] = {
                OWNER_MARKER: MARKER_VALUE,
                OWNER_TARGET: _stored_value(OWNER_TARGET, email),
            }
        else:
            del self.owners[file_id]
            props = {OWNER_MARKER: None, OWNER_TARGET: None}
        self._write(file_id, props, None)

    def _write(
        self,
        file_id: str,
        props: dict[str, str | None],
        shared_drive_id: str | None,
    ) -> None:
        """Store *props* on the file; a failure is logged and reported."""
        try:
            self.drive_service.update_file(
                file_id,
                {"appProperties": props},
                supports_all_drives=bool(shared_drive_id),
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.WARNING,
                f"Could not record a pending grant or transfer on Drive file"
                f" {file_id} ({e}); this run's sweep still retries it, but"
                " later runs won't know about it",
                drive_file_id=file_id,
            )
            self._on_write_failure(file_id, e)

    def find_flagged(
        self,
        shared_drive_id: str | None,
        folder_ids: Collection[str],
    ) -> int:
        """Read every flagged file of this migration into the record.

        With a shared drive, the whole drive is searched.  In regular Drive,
        the admin's files are searched and only those in *folder_ids* (the
        attachment root and its channel folders) are kept.

        Args:
            shared_drive_id: The shared drive, or None for regular Drive.
            folder_ids: Folders whose files belong to this migration.

        Returns:
            How many flagged files were found.

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` if a page can't be listed.
        """
        found = 0
        for marker in (GRANT_MARKER, OWNER_MARKER):
            list_kwargs: dict[str, Any] = {
                "q": (
                    f"appProperties has {{ key='{marker}' and"
                    f" value='{MARKER_VALUE}' }} and trashed=false"
                ),
                "fields": "nextPageToken, files(id, parents, appProperties)",
                "page_size": _SWEEP_PAGE_SIZE,
            }
            if shared_drive_id:
                list_kwargs.update(
                    {
                        "corpora": "drive",
                        "drive_id": shared_drive_id,
                        "include_items_from_all_drives": True,
                        "supports_all_drives": True,
                    }
                )
            page_token: str | None = None
            while True:
                response = self.drive_service.list_files(
                    **list_kwargs, page_token=page_token
                )
                for file in response.get("files", []):
                    file_id = file.get("id")
                    if not file_id:
                        continue
                    parents = set(file.get("parents") or [])
                    if not shared_drive_id and not parents & set(folder_ids):
                        continue
                    self.read(file_id, file.get("appProperties") or {})
                    found += 1
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        return found
