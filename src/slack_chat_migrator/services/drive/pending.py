"""Poster grants and ownership transfers that failed and must be made again.

A poster's ``writer`` grant or an ownership transfer can fail after the file
was uploaded (a network error, a sharing policy).  The upload still counts.
The failure is recorded here, and on the Drive file itself in its
``appProperties``, so the sweep at the end of every run can retry it, also
for files that a later run never links again (``--resume`` skips messages
it already sent):

- ``slackGrantPending = "true"`` while any poster's grant on the file is
  pending, plus one ``slackGrant.<hash> = <target>`` per such poster.  The
  hash is the first 8 hex digits of the SHA-256 of the lowercased email,
  so one poster's success never clears another's pending grant.
- ``slackOwnerPending = "true"`` and ``slackOwner = <target>`` while the
  file's ownership transfer is pending.

A target is ``"<Slack user ID> <email>"``, so a later run can look the user
up in its own user mapping: the email may have changed, or the user may now
be unmapped or external.  Drive limits an entry to 124 bytes of key plus
value (UTF-8); when both don't fit, only the Slack user ID is stored, and
without one only the email.  ``"-"`` means neither fit or was known: this
run still knows the recipient and retries it, a later run's sweep can't
(although a grant stored as ``"-"`` is still made when a later run links
the file again for the same poster).  Values written by earlier versions
hold the email alone and are read as such.

The markers have a fixed value so that one ``appProperties has {...}`` query
per kind (grants, transfers) finds every flagged file.  Drive also allows at most 30 private properties
per file and app; with the upload's three tags and this module's three
fixed keys, at most :data:`MAX_PENDING_GRANTS` poster grants can be flagged
on one file.  A further one is kept for this run's sweep only.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from types import MappingProxyType
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
#: Stored when neither the Slack user ID nor the email fits (or is known).
UNKNOWN_TARGET = "-"

#: Drive limits each appProperties entry to 124 bytes of key plus value.
APP_PROPERTY_MAX_BYTES = 124
#: Drive allows this many private properties per file and app.
MAX_APP_PROPERTIES = 30
#: Keys a flagged upload has besides its grant entries: the upload's tags
#: (slackFileId, slackChannel, slackMd5), GRANT_MARKER, OWNER_MARKER and
#: OWNER_TARGET.
FIXED_APP_PROPERTIES = 6
#: The most poster grants that can be flagged on one file.
MAX_PENDING_GRANTS = MAX_APP_PROPERTIES - FIXED_APP_PROPERTIES
#: The largest page Drive serves for files.list.
_SWEEP_PAGE_SIZE = 1000


def _normalize(email: str) -> str:
    return email.strip().lower()


@dataclass(frozen=True)
class Target:
    """Who a pending grant or transfer is for.

    Attributes:
        user_id: The Slack user ID, when known; a later run maps it again.
        email: The email the flagging run used, when known.
    """

    user_id: str | None = None
    email: str | None = None

    def merged(self, other: Target) -> Target:
        """This target, with what only *other* knows filled in."""
        return Target(self.user_id or other.user_id, self.email or other.email)

    def is_for(self, user_id: str | None, email: str | None) -> bool:
        """Whether this is the Slack user *user_id* (else the address *email*)."""
        if self.user_id and user_id:
            return self.user_id == user_id
        return bool(
            self.email and email and _normalize(self.email) == _normalize(email)
        )


def grant_key(email: str) -> str:
    """The appProperties key that holds *email*'s pending grant."""
    digest = hashlib.sha256(_normalize(email).encode("utf-8")).hexdigest()
    return f"{GRANT_TARGET_PREFIX}{digest[:8]}"


def _fits(key: str, value: str) -> bool:
    size = len(key.encode("utf-8")) + len(value.encode("utf-8"))
    return size <= APP_PROPERTY_MAX_BYTES


def encode_target(key: str, target: Target) -> str:
    """The value stored under *key* for *target* (see the module docstring)."""
    both = (
        f"{target.user_id} {target.email}" if target.user_id and target.email else None
    )
    for value in (both, target.user_id, target.email):
        if value and _fits(key, value):
            return value
    return UNKNOWN_TARGET


def decode_target(value: object) -> Target:
    """Read a stored target; an email alone is how earlier versions stored it."""
    if not isinstance(value, str):
        return Target()
    value = value.strip()
    if not value or value == UNKNOWN_TARGET:
        return Target()
    if " " in value:
        user_id, email = value.split(" ", 1)
        return Target(user_id or None, email.strip() or None)
    if "@" in value:
        return Target(email=value)
    return Target(user_id=value)


class PendingPermissions:
    """Which files still need a poster grant or an ownership transfer.

    The state lives in memory for this run and in each file's
    ``appProperties`` for later runs; see the module docstring.
    """

    def __init__(
        self,
        drive_service: DriveAdapter,
        *,
        on_write_failure: Callable[[str, bool], None] | None = None,
    ) -> None:
        """Initialize the record.

        Args:
            drive_service: Typed Drive API adapter, for storing the flags.
            on_write_failure: Called with the file ID and whether the write
                was clearing a flag, when a flag can't be stored on (or
                cleared from) the file, so the caller can count it.
        """
        self.drive_service = drive_service
        self._on_write_failure = on_write_failure or (lambda _file, _clearing: None)
        # Drive file ID -> {grant key: target}.
        self._grants: dict[str, dict[str, Target]] = {}
        # Drive file ID -> new owner.
        self._owners: dict[str, Target] = {}
        # (file ID, grant key) pending in memory only (too many on the file).
        self._unstored: set[tuple[str, str]] = set()

    @property
    def grants(self) -> Mapping[str, Mapping[str, Target]]:
        """Drive file ID -> {grant key: target} (a read-only view)."""
        return MappingProxyType(self._grants)

    @property
    def owners(self) -> Mapping[str, Target]:
        """Drive file ID -> pending new owner (a read-only view)."""
        return MappingProxyType(self._owners)

    def read(self, file_id: str, app_properties: Mapping[str, Any]) -> None:
        """Learn a file's pending grants and transfer from its appProperties.

        What this run already knows about a target is kept: it may hold an
        email or a Slack user ID that the file doesn't.
        """
        if app_properties.get(GRANT_MARKER) == MARKER_VALUE:
            targets = self._grants.setdefault(file_id, {})
            for key, value in app_properties.items():
                if key.startswith(GRANT_TARGET_PREFIX):
                    stored = decode_target(value)
                    known = targets.get(key)
                    targets[key] = known.merged(stored) if known else stored
                    self._unstored.discard((file_id, key))
            if not targets:
                del self._grants[file_id]
        if app_properties.get(OWNER_MARKER) == MARKER_VALUE:
            stored = decode_target(app_properties.get(OWNER_TARGET))
            known = self._owners.get(file_id)
            self._owners[file_id] = known.merged(stored) if known else stored

    def has_grant(self, file_id: str, email: str) -> bool:
        """Whether *email*'s grant on the file is pending."""
        return grant_key(email) in self._grants.get(file_id, {})

    def owner(self, file_id: str) -> Target | None:
        """The pending new owner of the file, if any."""
        return self._owners.get(file_id)

    def set_grant(
        self,
        file_id: str,
        email: str,
        pending: bool,
        shared_drive_id: str | None = None,
        *,
        user_id: str | None = None,
    ) -> None:
        """Flag *email*'s grant on the file as pending, or clear the flag.

        Nothing is sent when the flag doesn't change; a flag that is already
        set only learns *user_id* and *email*.  Storing it is best effort: a
        failure is logged as a warning and reported to *on_write_failure*,
        and only this run remembers the flag.

        Args:
            file_id: The Drive file.
            email: The poster's email.
            pending: Whether the grant is pending (else it was made).
            shared_drive_id: The shared drive, if the file is in one.
            user_id: The poster's Slack user ID, when known.
        """
        key = grant_key(email)
        if not pending:
            self.clear_grant(file_id, key, shared_drive_id)
            return
        target = Target(user_id, email)
        targets = self._grants.setdefault(file_id, {})
        known = targets.get(key)
        if known is not None:
            targets[key] = known.merged(target)
            return
        stored = sum(1 for k in targets if (file_id, k) not in self._unstored)
        targets[key] = target
        if stored >= MAX_PENDING_GRANTS:
            self._unstored.add((file_id, key))
            log_with_context(
                logging.WARNING,
                f"Could not record the pending grant for {email} on Drive file"
                f" {file_id}: {MAX_PENDING_GRANTS} poster grants are already"
                " flagged on it, the most Drive's property limit allows. This"
                " run's sweep still retries it, but later runs won't know"
                " about it.",
                drive_file_id=file_id,
            )
            self._on_write_failure(file_id, False)
            return
        props: dict[str, str | None] = {
            GRANT_MARKER: MARKER_VALUE,
            key: encode_target(key, target),
        }
        self._write(file_id, props, shared_drive_id, clearing=False)

    def clear_grant(
        self, file_id: str, key: str, shared_drive_id: str | None = None
    ) -> None:
        """Clear the pending grant stored under *key* (see :func:`grant_key`).

        The marker is cleared with the file's last pending grant.
        """
        targets = self._grants.get(file_id, {})
        if key not in targets:
            return
        del targets[key]
        if not targets:
            del self._grants[file_id]
        props: dict[str, str | None] = {}
        if (file_id, key) in self._unstored:
            self._unstored.discard((file_id, key))
        else:
            props[key] = None
        if not targets:
            props[GRANT_MARKER] = None
        if props:
            self._write(file_id, props, shared_drive_id, clearing=True)

    def set_owner(
        self,
        file_id: str,
        email: str,
        pending: bool,
        *,
        user_id: str | None = None,
    ) -> None:
        """Flag the file's ownership transfer to *email* as pending, or clear it.

        See :meth:`set_grant`.  Transfers only happen in regular Drive.  A
        pending transfer to someone else replaces the stored one.  Clearing
        removes the new owner from the file too.
        """
        known = self._owners.get(file_id)
        if not pending:
            if known is None:
                return
            del self._owners[file_id]
            self._write(
                file_id, {OWNER_MARKER: None, OWNER_TARGET: None}, None, clearing=True
            )
            return
        target = Target(user_id, email)
        if known is not None and known.is_for(user_id, email):
            self._owners[file_id] = known.merged(target)
            return
        self._owners[file_id] = target
        props: dict[str, str | None] = {
            OWNER_MARKER: MARKER_VALUE,
            OWNER_TARGET: encode_target(OWNER_TARGET, target),
        }
        self._write(file_id, props, None, clearing=False)

    def _write(
        self,
        file_id: str,
        props: dict[str, str | None],
        shared_drive_id: str | None,
        *,
        clearing: bool,
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
            if clearing:
                message = (
                    f"Could not clear the pending flag on Drive file {file_id}"
                    f" after its grant or transfer succeeded ({e}); the next"
                    " run's sweep clears it (a grant is made again, which is"
                    " harmless; a transfer is found done)"
                )
            else:
                message = (
                    f"Could not record a pending grant or transfer on Drive file"
                    f" {file_id} ({e}); this run's sweep still retries it, but"
                    " later runs won't know about it"
                )
            log_with_context(logging.WARNING, message, drive_file_id=file_id)
            self._on_write_failure(file_id, clearing)

    def find_flagged(
        self,
        shared_drive_id: str | None,
        folder_ids: Collection[str],
    ) -> int:
        """Read every flagged file of this migration into the record.

        With a shared drive, the whole drive is searched.  In regular Drive,
        the admin's files are searched and only those in *folder_ids* (the
        attachment root and its channel folders) are kept.  Every page is
        read.

        Args:
            shared_drive_id: The shared drive, or None for regular Drive.
            folder_ids: Folders whose files belong to this migration.

        Returns:
            How many flagged files were found; a file with both a pending
            grant and a pending transfer counts once.

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` if a page can't be listed.
        """
        found: set[str] = set()
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
                    found.add(file_id)
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
        return len(found)
