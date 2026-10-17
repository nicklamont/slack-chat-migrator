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
import http.client
import json
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

from slack_chat_migrator.utils.api import RETRYABLE_NETWORK_ERRORS


def _build_iam_service(credentials: Any) -> Any:
    """Build an IAM API client via discovery."""
    from googleapiclient.discovery import build

    return build("iam", "v1", credentials=credentials)


def list_service_accounts(
    credentials: Any,
    project_id: str,
) -> list[dict[str, str]]:
    """List service accounts in the project.

    Returns:
        List of dicts with 'email' and 'display_name' for each account.
    """
    service = _build_iam_service(credentials)
    try:
        response = (
            service.projects()
            .serviceAccounts()
            .list(name=f"projects/{project_id}")
            .execute()
        )
        # Only show user-created SAs (in the project's IAM domain)
        iam_domain = f"@{project_id}.iam.gserviceaccount.com"
        return [
            {
                "email": sa["email"],
                "display_name": sa.get("displayName", ""),
            }
            for sa in response.get("accounts", [])
            if sa["email"].endswith(iam_domain)
        ]
    except Exception:
        return []


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
    #: permission, network error, unparseable timestamp).
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class KeyCheck:
    """Result of :func:`check_existing_key`.

    Attributes:
        status: What the check found.
        reason: Why the key isn't usable, or why it couldn't be checked;
            empty exactly when *status* is ``ACTIVE``.
    """

    status: KeyStatus
    reason: str = ""

    def __post_init__(self) -> None:
        """Enforce "reason is empty iff the key is active".

        Raises:
            ValueError: If an ``ACTIVE`` result has a reason, or any other
                result has none.
        """
        if not isinstance(self.status, KeyStatus):
            raise ValueError(f"KeyCheck.status must be a KeyStatus: {self.status!r}")
        if (self.status is KeyStatus.ACTIVE) == bool(self.reason):
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

_FRACTION_RE = re.compile(r"(T\d{2}:\d{2}:\d{2})\.(\d+)")


def _parse_key_time(value: str) -> datetime:
    """Parse an IAM timestamp such as ``2026-01-31T12:00:00.123456789Z``.

    Raises:
        ValueError: If *value* isn't an RFC 3339 timestamp.
    """
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    # Before Python 3.11, fromisoformat() accepts only 0, 3 or 6 fractional
    # digits, and IAM sends 0, 3, 6 or 9: pad or cut every fraction to 6.
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
    """
    if key is None:
        return KeyCheck(KeyStatus.INACTIVE, "it was deleted")
    if key.get("disabled") is True:
        return KeyCheck(KeyStatus.INACTIVE, "it is disabled")
    raw_expiry = key.get("validBeforeTime")
    if raw_expiry in (None, ""):
        return KeyCheck(KeyStatus.ACTIVE)
    try:
        expiry = _parse_key_time(str(raw_expiry))
    except ValueError:
        return KeyCheck(
            KeyStatus.UNKNOWN,
            f"its expiry time {raw_expiry!r} from IAM couldn't be parsed",
        )
    if expiry <= (now or datetime.now(timezone.utc)):
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
        status = error.resp.status
        detail = getattr(error, "reason", "") or ""
        if status == 404:
            hint = (
                f" The service account {service_account_email} doesn't exist;"
                " check the email chosen in the service account step."
            )
        elif status in (400, 401, 403):
            hint = (
                " Check that the IAM API is enabled and that your account may"
                " list the service account's keys (iam.serviceAccountKeys.list)."
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
    because of a permission or network problem.

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        key_path: Path to an existing JSON key file.
        now: The current time (for tests); defaults to now, in UTC.

    Returns:
        The :class:`KeyCheck` result.
    """
    try:
        text = key_path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return KeyCheck(KeyStatus.NOT_A_KEY, "it isn't a text file")
    except OSError as e:
        return KeyCheck(KeyStatus.UNREADABLE, f"it can't be read ({e.strerror or e})")
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


class KeyNotSavedError(Exception):
    """A new key was created in IAM but couldn't be written to its file.

    Attributes:
        key_name: The IAM resource name of the new key.
        deleted: True if the key was deleted again, so nothing is left
            behind; False if it is still active and must be deleted by hand.
    """

    def __init__(self, message: str, key_name: str, deleted: bool) -> None:
        super().__init__(message)
        self.key_name = key_name
        self.deleted = deleted


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
        OSError: If *output_path* can't be opened for writing (no key is
            created then).
        KeyNotSavedError: If the new key couldn't be written; it says
            whether the key was deleted again or must be deleted by hand.
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
    try:
        # O_CREAT's mode only applies to new files; tighten an existing one.
        os.chmod(output_path, 0o600)
        service = _build_iam_service(credentials)
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
    except BaseException:
        out.close()
        if not existed:
            _remove_quietly(output_path)
        raise

    try:
        key_data = json.loads(base64.b64decode(key["privateKeyData"]))
        out.truncate(0)
        json.dump(key_data, out, indent=2)
        out.close()
    except BaseException as e:
        out.close()
        if not existed:
            _remove_quietly(output_path)
        unsaved = _delete_unsaved_key(service, key, output_path, e)
        # Ctrl-C stays Ctrl-C once the key is gone; otherwise say what's left.
        if isinstance(e, Exception) or not unsaved.deleted:
            raise unsaved from e
        raise
    return output_path


def _remove_quietly(path: Path) -> None:
    """Delete *path*, ignoring errors (best-effort cleanup)."""
    try:
        path.unlink()
    except OSError:
        pass


def _delete_unsaved_key(
    service: Any, key: Mapping[str, Any], output_path: Path, error: BaseException
) -> KeyNotSavedError:
    """Delete a new key that couldn't be saved.

    Args:
        service: The IAM client that created the key.
        key: The ``keys.create`` response.
        output_path: Where the key should have been written.
        error: Why writing it failed.

    Returns:
        The error to report: ``deleted`` says whether the key is gone, and
        the message names the key when it has to be deleted by hand.
    """
    key_name = str(key.get("name", ""))
    try:
        if not key_name:
            raise ValueError("IAM didn't return the new key's name")
        service.projects().serviceAccounts().keys().delete(name=key_name).execute()
    except Exception as delete_error:
        return KeyNotSavedError(
            f"A new key was created but couldn't be saved to {output_path}"
            f" ({type(error).__name__}: {error}), and deleting it failed"
            f" ({type(delete_error).__name__}: {delete_error}). The key is"
            f" still active: delete {key_name or 'the newest key'} in the Cloud"
            " Console (IAM > Service accounts > Keys) or with 'gcloud iam"
            " service-accounts keys delete'.",
            key_name,
            deleted=False,
        )
    return KeyNotSavedError(
        f"The new key couldn't be saved to {output_path}"
        f" ({type(error).__name__}: {error}); it was deleted again, so no key"
        " was left behind.",
        key_name,
        deleted=True,
    )
