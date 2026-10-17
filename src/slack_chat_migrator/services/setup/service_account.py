"""Service account creation, key download, and checks of existing key files.

:func:`check_existing_key` decides whether a key file from an earlier run
can be reused.  It reports a key it couldn't check (unreadable file, IAM
API or network failure, unparseable timestamp) as such, never as inactive,
so the wizard never offers to replace a key that may still work.
:func:`download_key` reserves the output file before it asks IAM for a new
key, and deletes the new key again if it can't be saved.

Uses the REST-based discovery API (google-api-python-client) instead of the
gRPC client to avoid billing routing through the ADC quota project.

No project-level IAM roles are needed — the service account gets all its
permissions via domain-wide delegation scopes configured in the Google
Workspace Admin Console.
"""

from __future__ import annotations

import base64
import codecs
import http.client
import json
import logging
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError
from httplib2 import HttpLib2Error

from slack_chat_migrator.utils.api import RETRYABLE_NETWORK_ERRORS, http_status
from slack_chat_migrator.utils.logging import log_with_context


def _build_iam_service(credentials: Any) -> Any:
    """Build an IAM API client via discovery."""
    from googleapiclient.discovery import build

    return build("iam", "v1", credentials=credentials)


def list_service_accounts(
    credentials: Any,
    project_id: str,
) -> list[dict[str, str]]:
    """List the user-created service accounts in the project.

    Returns:
        List of dicts with 'email' and 'display_name' for each account;
        empty, with a logged warning, if the accounts can't be listed.
    """
    try:
        service = _build_iam_service(credentials)
        response = (
            service.projects()
            .serviceAccounts()
            .list(name=f"projects/{project_id}")
            .execute()
        )
    except _LIST_ERRORS as e:
        log_with_context(
            logging.WARNING,
            f"Couldn't list the service accounts of project {project_id}"
            f" ({type(e).__name__}: {e}); you can still create one or enter"
            " an existing one's email.",
        )
        return []
    # Only show user-created SAs (in the project's IAM domain)
    iam_domain = f"@{project_id}.iam.gserviceaccount.com"
    return [
        {
            "email": sa["email"],
            "display_name": sa.get("displayName", ""),
        }
        for sa in response.get("accounts", [])
        if str(sa.get("email", "")).endswith(iam_domain)
    ]


def create_service_account(
    credentials: Any,
    project_id: str,
    account_id: str,
    display_name: str,
) -> dict[str, str]:
    """Create a service account in the project.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: GCP project ID.
        account_id: Short ID for the service account (e.g. 'slack-migrator').
        display_name: Human-readable name.

    Returns:
        Dict with 'email' and 'name' of the created account.
    """
    service = _build_iam_service(credentials)
    sa = (
        service.projects()
        .serviceAccounts()
        .create(
            name=f"projects/{project_id}",
            body={
                "accountId": account_id,
                "serviceAccount": {"displayName": display_name},
            },
        )
        .execute()
    )
    return {"email": sa["email"], "name": sa["name"]}


def _key_name(service_account_email: str) -> str:
    """Return the IAM resource name of a service account."""
    return f"projects/-/serviceAccounts/{service_account_email}"


class KeyStatus(str, Enum):
    """Outcome of checking an existing key file against IAM."""

    #: The file holds a usable key of the service account.
    ACTIVE = "active"
    #: The file holds an active key of the account but starts with a UTF-8
    #: byte order mark, which google-auth can't read.  Rewriting the file
    #: without the BOM (:func:`rewrite_without_bom`) makes it usable; no new
    #: key is needed.
    HAS_BOM = "has_bom"
    #: The file holds a key of the account that is deleted, disabled or
    #: expired.
    INACTIVE = "inactive"
    #: The file was read but isn't a key of this service account (not JSON,
    #: not a key file, or another account's key).
    NOT_A_KEY = "not_a_key"
    #: The file can't be read (a directory, permission denied, an I/O
    #: error), so nothing is known about what it holds.
    UNREADABLE = "unreadable"
    #: The key couldn't be checked (IAM API not enabled, missing
    #: permission, service account not found, network error, unparseable
    #: timestamp).
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class KeyCheck:
    """Result of :func:`check_existing_key`.

    Attributes:
        status: What the check found.
        reason: Why the key isn't usable, or why it couldn't be checked;
            empty exactly when *status* is ``ACTIVE``, and otherwise a
            phrase that isn't just whitespace.
    """

    status: KeyStatus
    reason: str = ""

    def __post_init__(self) -> None:
        """Enforce "reason is empty iff the key is active".

        Raises:
            ValueError: If *status* isn't a :class:`KeyStatus` or *reason*
                isn't a string, an ``ACTIVE`` result has a reason, or any
                other result has none (or only whitespace).
        """
        if not isinstance(self.status, KeyStatus):
            raise ValueError(f"KeyCheck.status must be a KeyStatus: {self.status!r}")
        if not isinstance(self.reason, str):
            raise ValueError(f"KeyCheck.reason must be a string: {self.reason!r}")
        if self.status is KeyStatus.ACTIVE:
            invalid = self.reason != ""
        else:
            invalid = not self.reason.strip()
        if invalid:
            raise ValueError(
                "KeyCheck.reason must be empty for an ACTIVE key and given"
                f" otherwise (status={self.status.value}, reason={self.reason!r})"
            )


#: Errors that mean "the keys couldn't be listed", not "the key is bad":
#: HTTP errors, credential errors and network failures.  Anything else (a
#: bug) propagates.
_LIST_ERRORS: tuple[type[BaseException], ...] = (
    HttpError,
    GoogleAuthError,
    HttpLib2Error,
    OSError,
    http.client.HTTPException,
    *RETRYABLE_NETWORK_ERRORS,
)

_FRACTION_RE = re.compile(r"([Tt ]\d{2}:\d{2}:\d{2})\.(\d+)")


def _parse_key_time(value: str) -> datetime:
    """Parse an IAM timestamp such as ``2026-01-31T12:00:00.123456789Z``.

    A trailing ``Z``/``z`` becomes ``+00:00`` and the seconds fraction is
    padded or cut to 6 digits; a timestamp without an offset counts as UTC.

    Raises:
        ValueError: If :meth:`datetime.fromisoformat` can't parse it after
            those fix-ups.
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    # Before Python 3.11, fromisoformat() accepts only 0, 3 or 6 fractional
    # digits, and IAM sends 0, 3, 6 or 9: pad or cut the fraction to 6.
    text = _FRACTION_RE.sub(
        lambda m: f"{m.group(1)}.{(m.group(2) + '000000')[:6]}", text
    )
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _assess_key(key: Mapping[str, Any] | None, now: datetime | None = None) -> KeyCheck:
    """Assess a listed IAM ``ServiceAccountKey``.

    ``validAfterTime`` is not checked: for user-managed keys it is the
    creation time, and a local clock that runs behind IAM's must not make a
    working key look unusable.

    Args:
        key: The key resource from ``keys.list``, or None when the key isn't
            listed (it was deleted).
        now: The current time (for tests); defaults to now, in UTC.

    Returns:
        ``ACTIVE``; ``INACTIVE`` when the key is deleted, disabled or
        expired; ``UNKNOWN`` when its expiry time can't be parsed.

    Raises:
        TypeError: If *now* is a naive datetime (a programming error).
    """
    if key is None:
        return KeyCheck(KeyStatus.INACTIVE, "it was deleted")
    if key.get("disabled") is True:
        return KeyCheck(KeyStatus.INACTIVE, "it is disabled")
    raw_expiry = key.get("validBeforeTime")
    if raw_expiry in (None, ""):
        return KeyCheck(KeyStatus.ACTIVE)
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:  # a caller's bug, not something IAM sent
        raise TypeError("now must be a timezone-aware datetime")
    try:
        expiry = _parse_key_time(str(raw_expiry))
    except ValueError:
        return KeyCheck(
            KeyStatus.UNKNOWN,
            f"its expiry time {raw_expiry!r} from IAM couldn't be parsed",
        )
    if expiry <= now:
        return KeyCheck(KeyStatus.INACTIVE, f"it expired on {raw_expiry}")
    return KeyCheck(KeyStatus.ACTIVE)


def _find_key(
    credentials: Any, service_account_email: str, key_id: str
) -> dict[str, Any] | None:
    """Return the user-managed key *key_id* of the account, or None.

    Raises:
        HttpError: If the IAM API refuses the request.
        GoogleAuthError: If the credentials can't be used.
        OSError: On a network failure (also ``httplib2.HttpLib2Error`` and
            ``http.client.HTTPException``).
    """
    service = _build_iam_service(credentials)
    response = (
        service.projects()
        .serviceAccounts()
        .keys()
        .list(name=_key_name(service_account_email), keyTypes="USER_MANAGED")
        .execute()
    )
    for key in response.get("keys", []):
        if str(key.get("name", "")).rsplit("/", 1)[-1] == key_id:
            return dict(key)
    return None


def _describe_list_error(error: BaseException, service_account_email: str) -> str:
    """Explain why listing the account's keys failed."""
    if isinstance(error, HttpError):
        status = http_status(error)
        detail = getattr(error, "reason", "") or ""
        # keys.list addresses the account as projects/-/serviceAccounts/...,
        # so a missing account is a 403; 404 is only a fallback.
        if status == 404:
            hint = (
                f" The service account {service_account_email} doesn't exist;"
                f" check the email chosen in the service account step{_RESUME_HINT}."
            )
        elif status in (400, 401, 403):
            hint = (
                " Check that the IAM API is enabled, that your account may list"
                " the service account's keys (iam.serviceAccountKeys.list), and"
                f" that {service_account_email} exists (IAM answers 403, not 404,"
                f" for a missing account here){_RESUME_HINT}."
            )
        else:
            hint = ""
        return (
            f"listing the keys failed with HTTP {status}"
            + (f" ({detail})" if detail else "")
            + "."
            + hint
        )
    if isinstance(error, GoogleAuthError) and not isinstance(
        error, RETRYABLE_NETWORK_ERRORS
    ):
        return (
            f"your Google credentials couldn't be used ({type(error).__name__}:"
            f" {error}). Run 'gcloud auth application-default login' and try"
            " again."
        )
    return f"the IAM API couldn't be reached ({type(error).__name__}: {error})."


def check_existing_key(
    credentials: Any,
    service_account_email: str,
    key_path: Path,
    *,
    now: datetime | None = None,
) -> KeyCheck:
    """Check whether *key_path* holds a usable key of the service account.

    Used to reuse a key file instead of minting a new key on every run (a
    service account can have at most 10 keys).  A file that can't be read
    is ``UNREADABLE``, and a key that can't be checked (the keys can't be
    listed, or its expiry can't be parsed) is ``UNKNOWN``; neither is ever
    reported as an inactive key, so a working key file isn't overwritten
    because of a permission or network problem.  Only a file google-auth
    can load is ``ACTIVE``: an active key in a file with a UTF-8 byte order
    mark is ``HAS_BOM``.

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        key_path: Path to an existing JSON key file.
        now: The current time (for tests); defaults to now, in UTC.

    Returns:
        The :class:`KeyCheck` result.
    """
    try:
        raw = key_path.read_bytes()
    except OSError as e:
        return KeyCheck(KeyStatus.UNREADABLE, f"it can't be read ({e.strerror or e})")
    if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        # google-auth reads key files as UTF-8 only; say why it can't be used.
        return KeyCheck(
            KeyStatus.NOT_A_KEY,
            "it is saved as UTF-16, so it can't be read as a key file (if it"
            " holds a key of this account, that key stays active in IAM)",
        )
    # google-auth rejects a UTF-8 byte order mark too ("Unexpected UTF-8
    # BOM"), so a file with one is never reported ACTIVE: the key inside is
    # checked as usual, and an active one is HAS_BOM (fixable by rewriting).
    has_bom = raw.startswith(codecs.BOM_UTF8)
    if has_bom:
        raw = raw[len(codecs.BOM_UTF8) :]
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return KeyCheck(KeyStatus.NOT_A_KEY, "it isn't UTF-8 text")
    check = _check_key_text(credentials, service_account_email, text, now)
    if not has_bom:
        return check
    if check.status is KeyStatus.ACTIVE:
        return KeyCheck(
            KeyStatus.HAS_BOM,
            "it starts with a UTF-8 byte order mark, which google-auth can't"
            " read (the key itself is active)",
        )
    return KeyCheck(
        check.status, f"{check.reason} (and it starts with a UTF-8 byte order mark)"
    )


def _check_key_text(
    credentials: Any, service_account_email: str, text: str, now: datetime | None
) -> KeyCheck:
    """Check the decoded text of a key file (see :func:`check_existing_key`)."""
    try:
        data = json.loads(text)
    except ValueError as e:
        return KeyCheck(KeyStatus.NOT_A_KEY, f"it isn't valid JSON ({e})")
    if not isinstance(data, dict) or not data.get("private_key_id"):
        return KeyCheck(KeyStatus.NOT_A_KEY, "it isn't a service account key file")
    owner = data.get("client_email")
    if owner != service_account_email:
        return KeyCheck(KeyStatus.NOT_A_KEY, f"it is a key of {owner or 'unknown'}")
    try:
        key = _find_key(credentials, service_account_email, str(data["private_key_id"]))
    except _LIST_ERRORS as e:
        return KeyCheck(
            KeyStatus.UNKNOWN, _describe_list_error(e, service_account_email)
        )
    return _assess_key(key, now)


def rewrite_without_bom(key_path: Path) -> Path:
    """Rewrite a key file without its UTF-8 byte order mark, atomically.

    The same key is kept (no new key is created in IAM).  A symlink is
    followed and its target rewritten, so the key stays in one file.  The
    new content is written to a private (``0600``) temporary file next to
    that file, with its owner and group, which then replaces it, so the old
    file stays intact if anything fails.

    Args:
        key_path: A key file that starts with a UTF-8 BOM.

    Returns:
        The file that was rewritten: *key_path*, or the file it links to.

    Raises:
        ValueError: If the file doesn't start with a UTF-8 BOM, has more
            than one hard link (the other names would keep the BOM and a
            second copy of the key), or belongs to another user whom the
            rewritten file can't be given (only root can).
        OSError: If the file can't be read or rewritten.
    """
    real = Path(os.path.realpath(key_path))
    raw = real.read_bytes()
    if not raw.startswith(codecs.BOM_UTF8):
        raise ValueError(f"{real} doesn't start with a UTF-8 byte order mark")
    original = real.stat()
    if original.st_nlink > 1:
        raise ValueError(
            f"{real} has {original.st_nlink} hard links, which would keep the"
            " byte order mark (and a copy of the key) if only this name were"
            " rewritten; re-save the file as UTF-8 without a byte order mark"
        )
    temp = real.with_name(f".{real.name}.{os.getpid()}.tmp")
    fd = os.open(str(temp), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            _keep_owner(f.fileno(), original, real)
            f.write(raw[len(codecs.BOM_UTF8) :])
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, real)
    except BaseException:
        _remove_quietly(temp)
        raise
    return real


def _keep_owner(fd: int, original: os.stat_result, path: Path) -> None:
    """Give the open file *fd* the owner and group of *path* (*original*).

    Raises:
        ValueError: If *path* belongs to another user and *fd* can't be
            given that owner (only root can).
    """
    if not hasattr(os, "fchown"):  # Windows: no POSIX owners
        return
    current = os.fstat(fd)
    if (current.st_uid, current.st_gid) == (original.st_uid, original.st_gid):
        return
    try:
        os.fchown(fd, original.st_uid, original.st_gid)
    except PermissionError:
        if current.st_uid != original.st_uid:
            raise ValueError(
                f"{path} belongs to another user (uid {original.st_uid}), and"
                " only root can give the rewritten file that owner; run setup"
                " as that user"
            ) from None
        # Only the group differs: the file is 0600, so its group can't read
        # it either way.


def _rebuild_error(
    cls: type[BaseException], args: tuple[Any, ...], state: dict[str, Any]
) -> BaseException:
    """Recreate an exception with keyword-only attributes (for pickle)."""
    error = cls.__new__(cls)
    BaseException.__init__(error, *args)
    error.__dict__.update(state)
    return error


class _PicklableError(BaseException):
    """Pickles an exception through ``args`` plus its attributes.

    The default pickling calls ``cls(*args)``, which fails for exceptions
    with required keyword-only attributes.
    """

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild_error, (type(self), self.args, self.__dict__))


class KeyNotSavedError(_PicklableError, Exception):
    """A new key was created in IAM but couldn't be written to its file.

    Attributes:
        key_name: The IAM resource name of the new key ("" if IAM didn't
            return one).
        deleted: True if the key was deleted again, so no key is left in
            IAM; False if deleting it failed, so it may still be active and
            should be deleted by hand.
    """

    def __init__(self, message: str, key_name: str, *, deleted: bool) -> None:
        super().__init__(message)
        self.key_name = key_name
        self.deleted = deleted


class KeyCreateError(_PicklableError, Exception):
    """``keys.create`` failed, so no key file was written.

    Attributes:
        maybe_created: True when the outcome is unknown (a network failure,
            a timeout, HTTP 408 or 5xx): IAM may have created the key.
            False when IAM refused the request or the credentials failed
            before it was sent, so this request created no key.
        status: The HTTP status, or None when there was no response.
    """

    def __init__(
        self, message: str, *, maybe_created: bool, status: int | None = None
    ) -> None:
        super().__init__(message)
        self.maybe_created = maybe_created
        self.status = status


class KeySetupInterrupted(_PicklableError, KeyboardInterrupt):
    """Ctrl-C while a new key was being created, saved or cleaned up.

    It is still a :class:`KeyboardInterrupt`, so the run stops as for any
    Ctrl-C; its message says what the interruption left behind: a key that
    may still be active in IAM (and how to find and delete it), or an
    earlier key file that was emptied for the new key.

    Attributes:
        key_name: The new key's IAM resource name, or "" when it isn't
            known (Ctrl-C during ``keys.create``).
        deleted: True if the new key was deleted again, so no key is left
            in IAM.
    """

    def __init__(
        self,
        message: str = "Interrupted while setting up a service account key.",
        key_name: str = "",
        *,
        deleted: bool = False,
    ) -> None:
        super().__init__(message)
        self.key_name = key_name
        self.deleted = deleted


def _list_keys_command(service_account_email: str) -> str:
    return (
        "gcloud iam service-accounts keys list"
        f" --iam-account={service_account_email} --managed-by=user"
    )


def _delete_key_command(key_name: str, service_account_email: str) -> str:
    key_id = key_name.rsplit("/", 1)[-1] if key_name else "<KEY_ID>"
    return (
        f"gcloud iam service-accounts keys delete {key_id}"
        f" --iam-account={service_account_email}"
    )


def _created_around(requested_at: datetime) -> str:
    """How to recognise a key created by a request sent at *requested_at*."""
    return (
        "CREATED_AT is about"
        f" {requested_at.astimezone(timezone.utc):%Y-%m-%d %H:%M} UTC"
    )


def _maybe_created_advice(service_account_email: str, requested_at: datetime) -> str:
    """Say how to find, and delete, only the key this request may have made.

    The account may have other keys that are in use elsewhere (a CI system,
    another machine), so the advice never says "delete keys you have no
    file for".
    """
    return (
        "IAM may have created the key anyway. List the account's keys with"
        f" '{_list_keys_command(service_account_email)}': a key created by this"
        f" request is one whose {_created_around(requested_at)}. Delete only"
        " that key, with"
        f" '{_delete_key_command('', service_account_email)}'; don't delete"
        " the account's other keys, which may be in use elsewhere."
    )


#: How to choose another service account on a resumed setup run.
_RESUME_HINT = (
    " (to choose another account, re-run setup and answer No to 'Resume from"
    " previous session?')"
)

#: Where Google explains how to exempt a project from the key-creation policy.
_ALLOW_KEY_CREATION_URL = (
    "https://cloud.google.com/iam/docs/keys-create-delete#allow-creation"
)

#: What to do when IAM refuses to create a key, by HTTP status.  A 400 that
#: comes from the key-creation org policy gets :func:`_policy_advice`
#: instead.  ``keys.create`` addresses the account as
#: ``projects/-/serviceAccounts/<email>``, and with ``-`` IAM answers 403,
#: not 404, for an account that doesn't exist; 404 is only a fallback.
_CREATE_REFUSED_ADVICE = {
    400: "If {email} already has 10 keys (the most IAM allows), delete one you"
    " know is no longer used: list them with '{list}', then run 'gcloud iam"
    " service-accounts keys delete <KEY_ID> --iam-account={email}'. Otherwise"
    " see the HTTP error for the cause.",
    401: "Your Google credentials were rejected: run 'gcloud auth"
    " application-default login' and try again.",
    403: "Your account may not create keys for {email} (it needs"
    " iam.serviceAccountKeys.create, for example through the Service Account"
    " Key Admin role), or {email} doesn't exist (IAM answers 403, not 404, for"
    " a missing account here){resume}.",
    404: "The service account {email} doesn't exist; check the email chosen in"
    " the service account step{resume}.",
    429: "IAM's quota or rate limit was hit: wait a minute and try again.",
}


#: The org-policy constraints that forbid creating keys: the legacy one and
#: its managed successor, which new organizations enforce by default.
_KEY_CREATION_CONSTRAINT = re.compile(
    r"iam\.(?:managed\.)?disableServiceAccountKeyCreation"
)


def _key_creation_policy(error: HttpError) -> str | None:
    """Return the org-policy constraint that refused ``keys.create``, if any.

    IAM refuses under these policies with a 400 FAILED_PRECONDITION, like
    the 10-key limit, so the refusals are told apart by what names the
    constraint:

    - ``iam.disableServiceAccountKeyCreation`` (legacy) names itself in a
      ``PreconditionFailure`` violation, with the message "Key creation is
      not allowed on this service account.";
    - ``iam.managed.disableServiceAccountKeyCreation`` refuses with
      "Operation denied by org policy …" and names itself in an
      ``ErrorInfo``'s ``metadata.customConstraints``.

    The error details are read first, then the message.

    Returns:
        The constraint as the error names it, without ``constraints/``;
        ``iam.disableServiceAccountKeyCreation`` when only the legacy
        message says so; None if no key-creation policy is named.
    """
    texts: list[str] = []
    details = getattr(error, "error_details", None)
    for detail in details if isinstance(details, list) else []:
        if not isinstance(detail, Mapping):
            continue
        violations = detail.get("violations")
        for violation in violations if isinstance(violations, list) else []:
            if isinstance(violation, Mapping):
                texts.append(str(violation.get("type", "")))
        metadata = detail.get("metadata")
        if isinstance(metadata, Mapping):
            texts.append(str(metadata.get("customConstraints", "")))
    content = error.content.decode("utf-8", "replace") if error.content else ""
    message = f"{getattr(error, 'reason', '')} {content}"
    for text in (*texts, message):
        match = _KEY_CREATION_CONSTRAINT.search(text)
        if match:
            return match.group(0)
    if "Key creation is not allowed" in message:
        return "iam.disableServiceAccountKeyCreation"
    return None


def _policy_advice(constraint: str, service_account_email: str) -> str:
    return (
        f"An organization policy ({constraint}) forbids creating service"
        " account keys in this project; organizations created since May 2024"
        " enforce it by default. Ask an Organization Policy Administrator"
        " (roles/orgpolicy.policyAdmin) to exempt this project (see"
        f" {_ALLOW_KEY_CREATION_URL}), or reuse an existing key file of"
        f" {service_account_email} if you have one."
    )


def _create_failure(
    error: BaseException, service_account_email: str, requested_at: datetime
) -> BaseException | None:
    """Turn a ``keys.create`` failure into the error to raise.

    Args:
        error: What ``keys.create().execute()`` raised.
        service_account_email: The account the key was for.
        requested_at: When the request was sent, to identify a key it may
            have created.

    Returns:
        A :class:`KeyCreateError` that says whether a key may exist, a
        :class:`KeySetupInterrupted` for Ctrl-C, or None for anything else
        (a bug), which the caller re-raises as it is.
    """
    email = service_account_email
    if isinstance(error, KeyboardInterrupt):
        return KeySetupInterrupted(
            "Interrupted while IAM was creating a key; "
            + _maybe_created_advice(email, requested_at)
        )
    if isinstance(error, HttpError):
        status = http_status(error)
        detail = f"HTTP {status if status is not None else 'error'}" + (
            f": {error.reason}" if getattr(error, "reason", "") else ""
        )
        # Without a status nothing says IAM refused, so a key may exist.
        if status is None or status == 408 or status >= 500:
            return KeyCreateError(
                f"Creating a key failed ({detail}). {_maybe_created_advice(email, requested_at)}",
                maybe_created=True,
                status=status,
            )
        constraint = _key_creation_policy(error) if status == 400 else None
        if constraint is not None:
            advice = _policy_advice(constraint, email)
        else:
            advice = _CREATE_REFUSED_ADVICE.get(
                status, "See the HTTP error for the cause, then try again."
            ).format(email=email, list=_list_keys_command(email), resume=_RESUME_HINT)
        return KeyCreateError(
            f"IAM refused to create a key ({detail}); no key was created. {advice}",
            maybe_created=False,
            status=status,
        )
    if isinstance(error, GoogleAuthError) and not isinstance(
        error, RETRYABLE_NETWORK_ERRORS
    ):
        # The token refresh fails before the request is sent.
        return KeyCreateError(
            f"Your Google credentials couldn't be used ({type(error).__name__}:"
            f" {error}); no key was created. Run 'gcloud auth"
            " application-default login' and try again.",
            maybe_created=False,
        )
    if isinstance(error, _LIST_ERRORS):
        return KeyCreateError(
            f"The request to create a key failed ({type(error).__name__}:"
            f" {error}). {_maybe_created_advice(email, requested_at)}",
            maybe_created=True,
        )
    return None


def download_key(
    credentials: Any,
    service_account_email: str,
    output_path: Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Create and download a JSON key for the service account.

    The output file is opened (or created) first, so a path that can't be
    written fails before any key exists in IAM.  If the key can't be saved
    after it was created, it is deleted again.  The key file is written
    with mode ``0600`` (also when an existing file is overwritten).

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        output_path: Where to write the JSON key file.
        overwrite: Replace *output_path* if it already exists.

    Returns:
        Path to the written key file.

    Raises:
        FileExistsError: If *output_path* exists and *overwrite* is False.
        OSError: If *output_path* can't be opened for writing, or the IAM
            client can't be built; no key is created then.
        KeyCreateError: If ``keys.create`` failed; ``maybe_created`` says
            whether IAM may have created the key anyway.
        KeyNotSavedError: If the new key couldn't be written; it says
            whether the key was deleted again or must be deleted by hand.
        KeySetupInterrupted: On Ctrl-C while a key may be left in IAM; it
            names the key (when known) and how to delete it.
    """
    existed = output_path.exists()
    if existed and not overwrite:
        raise FileExistsError(f"Key file already exists: {output_path}")

    # Reserve the file before minting a key: O_EXCL also closes the race
    # with another process creating it.  An existing file is truncated only
    # once the key exists.
    flags = os.O_WRONLY | os.O_CREAT | (0 if overwrite else os.O_EXCL)
    fd = os.open(str(output_path), flags, 0o600)
    try:
        out = os.fdopen(fd, "w", encoding="utf-8")
    except BaseException:
        os.close(fd)
        raise

    def discard_file() -> bool:
        """Close the output file, and remove it if this call created it.

        Never raises, so it can't pre-empt deleting or reporting a key.

        Returns:
            True if Ctrl-C arrived while closing the file.
        """
        interrupted = False
        try:
            out.close()
        except BaseException as close_error:
            interrupted = isinstance(close_error, KeyboardInterrupt)
            log_with_context(
                logging.DEBUG, f"Couldn't close {output_path}: {close_error!r}"
            )
        if not existed:
            _remove_quietly(output_path)
        return interrupted

    try:
        # O_CREAT's mode only applies to new files; tighten an existing one.
        os.chmod(output_path, 0o600)
        service = _build_iam_service(credentials)
    except BaseException:
        discard_file()
        raise
    requested_at = datetime.now(timezone.utc)
    try:
        key = (
            service.projects()
            .serviceAccounts()
            .keys()
            .create(
                name=_key_name(service_account_email),
                body={"keyAlgorithm": "KEY_ALG_RSA_2048"},
            )
            .execute()
        )
    except BaseException as e:
        interrupted = discard_file()
        failure = _create_failure(e, service_account_email, requested_at)
        if failure is None:
            raise
        if interrupted and isinstance(failure, KeyCreateError):
            if not failure.maybe_created:
                raise KeyboardInterrupt from e  # nothing was created
            failure = KeySetupInterrupted(str(failure))
        raise failure from e

    truncated = False  # whether an existing file was emptied for the new key
    try:
        key_data = json.loads(base64.b64decode(key["privateKeyData"]))
        out.truncate(0)
        truncated = existed
        json.dump(key_data, out, indent=2)
        out.close()
    except BaseException as e:
        # Delete (or name) the key first; closing and removing the file
        # comes after and can't stop that.
        outcome = _unsaved_key_outcome(
            service, key, output_path, e, service_account_email, truncated, requested_at
        )
        if discard_file() and isinstance(outcome, KeyNotSavedError):
            outcome = KeySetupInterrupted(
                str(outcome), outcome.key_name, deleted=outcome.deleted
            )
        if outcome is e:
            raise
        raise outcome from e
    return output_path


def _remove_quietly(path: Path) -> None:
    """Delete *path*, best effort (a failure is logged at DEBUG)."""
    try:
        path.unlink()
    except OSError as e:
        log_with_context(logging.DEBUG, f"Couldn't remove {path}: {e}")


def _unsaved_key_outcome(
    service: Any,
    key: Mapping[str, Any],
    output_path: Path,
    error: BaseException,
    service_account_email: str,
    truncated: bool,
    requested_at: datetime,
) -> BaseException:
    """Delete a new key that couldn't be saved, and say what happened.

    Never raises: a failed or interrupted delete is part of the outcome.

    Args:
        service: The IAM client that created the key.
        key: The ``keys.create`` response.
        output_path: Where the key should have been written.
        error: Why writing it failed.
        service_account_email: The key's service account.
        truncated: True if an earlier file at *output_path* was already
            emptied for the new key.
        requested_at: When ``keys.create`` was sent, to identify the key
            when IAM didn't return its name.

    Returns:
        The exception to raise: *error* itself (a plain Ctrl-C, when the
        key was deleted and no earlier file was lost);
        :class:`KeySetupInterrupted` on Ctrl-C (while saving or while
        deleting) with the key possibly still active or an earlier file
        lost; otherwise :class:`KeyNotSavedError`, whose ``deleted`` says
        whether the key is gone.
    """
    key_name = str(key.get("name", ""))
    delete_error: BaseException | None = None
    try:
        if not key_name:
            raise ValueError("IAM didn't return the new key's name")
        service.projects().serviceAccounts().keys().delete(name=key_name).execute()
    except BaseException as e:  # Ctrl-C included: the key must still be named
        delete_error = e

    interrupted = isinstance(error, KeyboardInterrupt) or isinstance(
        delete_error, KeyboardInterrupt
    )
    cause = f"{type(error).__name__}: {error}" if str(error) else type(error).__name__
    lost_file = (
        f" {output_path} no longer holds its earlier content (it was emptied"
        " for the new key); restore it or remove it."
        if truncated
        else ""
    )
    if delete_error is None:
        if isinstance(error, KeyboardInterrupt) and not truncated:
            return error  # nothing is left behind: a plain Ctrl-C
        message = (
            f"The new key couldn't be saved to {output_path} ({cause}); it was"
            f" deleted again, so no key is left in IAM.{lost_file}"
        )
        if interrupted:
            return KeySetupInterrupted(message, key_name, deleted=True)
        return KeyNotSavedError(message, key_name, deleted=True)

    if isinstance(delete_error, KeyboardInterrupt):
        how = "deleting it was interrupted"
    else:
        how = f"deleting it failed ({type(delete_error).__name__}: {delete_error})"
    message = (
        f"A new key was created but couldn't be saved to {output_path}"
        f" ({cause}), and {how}. The key may still be active: delete it with"
        f" '{_delete_key_command(key_name, service_account_email)}'"
        + (
            ""
            if key_name
            else (
                f" (find it with '{_list_keys_command(service_account_email)}':"
                f" it is the key whose {_created_around(requested_at)}; don't"
                " delete the account's other keys)"
            )
        )
        + " or in the Cloud Console (IAM > Service accounts > Keys)."
        + lost_file
    )
    if interrupted:
        return KeySetupInterrupted(message, key_name, deleted=False)
    return KeyNotSavedError(message, key_name, deleted=False)
