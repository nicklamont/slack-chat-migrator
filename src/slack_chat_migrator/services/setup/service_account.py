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
from typing import Any, NoReturn

from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError
from httplib2 import HttpLib2Error

from slack_chat_migrator.utils.api import RETRYABLE_NETWORK_ERRORS
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
        if (self.status is KeyStatus.ACTIVE) == bool(self.reason.strip()):
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
        expired = expiry <= (now or datetime.now(timezone.utc))
    except (ValueError, TypeError):  # TypeError: naive against aware time
        return KeyCheck(
            KeyStatus.UNKNOWN,
            f"its expiry time {raw_expiry!r} from IAM couldn't be parsed",
        )
    if expired:
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
                " check the email chosen in the service account step (to"
                " choose it again, re-run setup and answer No to 'Resume from"
                " previous session?')."
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
        raw = key_path.read_bytes()
    except OSError as e:
        return KeyCheck(KeyStatus.UNREADABLE, f"it can't be read ({e.strerror or e})")
    if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
        # google-auth can't load it either; say why rather than "not text".
        return KeyCheck(
            KeyStatus.NOT_A_KEY,
            "it is saved as UTF-16, not UTF-8 (if it holds a key of this"
            " account, that key stays active in IAM)",
        )
    try:
        text = raw.decode("utf-8-sig")  # tolerate a UTF-8 byte order mark
    except UnicodeDecodeError:
        return KeyCheck(KeyStatus.NOT_A_KEY, "it isn't UTF-8 text")
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


def _rebuild_error(
    cls: type[BaseException], args: tuple[Any, ...], state: dict[str, Any]
) -> BaseException:
    """Recreate an exception with keyword-only attributes (for pickle)."""
    error = cls.__new__(cls)
    BaseException.__init__(error, *args)
    error.__dict__.update(state)
    return error


class KeyNotSavedError(Exception):
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

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild_error, (type(self), self.args, self.__dict__))


class KeyCreateError(Exception):
    """``keys.create`` failed, so no key file was written.

    Attributes:
        maybe_created: True when the outcome is unknown (a network failure,
            a timeout, HTTP 408 or 5xx): IAM may have created the key.
            False when IAM refused the request, so no key exists.
        status: The HTTP status, or None when there was no response.
    """

    def __init__(
        self, message: str, *, maybe_created: bool, status: int | None = None
    ) -> None:
        super().__init__(message)
        self.maybe_created = maybe_created
        self.status = status

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild_error, (type(self), self.args, self.__dict__))


class KeySetupInterrupted(KeyboardInterrupt):
    """Ctrl-C while a new key was being created or cleaned up.

    It is still a :class:`KeyboardInterrupt`, so the run stops as for any
    Ctrl-C; its message says which key may be left active in IAM and how to
    delete it.

    Attributes:
        key_name: The new key's IAM resource name, or "" when it isn't
            known (Ctrl-C during ``keys.create``).
    """

    def __init__(self, message: str, key_name: str = "") -> None:
        super().__init__(message)
        self.key_name = key_name

    def __reduce__(self) -> tuple[Any, ...]:
        return (_rebuild_error, (type(self), self.args, self.__dict__))


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


def _maybe_created_advice(service_account_email: str) -> str:
    return (
        "IAM may have created the key anyway. Check with"
        f" '{_list_keys_command(service_account_email)}' and delete any key"
        " you have no file for with"
        f" '{_delete_key_command('', service_account_email)}'."
    )


#: What to do when IAM refuses to create a key, by HTTP status.
_CREATE_REFUSED_ADVICE = {
    400: "A service account can have at most 10 keys: delete unused ones"
    " ('{list}', then 'gcloud iam service-accounts keys delete <KEY_ID>"
    " --iam-account={email}') and try again.",
    401: "Your Google credentials were rejected: run 'gcloud auth"
    " application-default login' and try again.",
    403: "Your account may not create keys for {email} (it needs"
    " iam.serviceAccountKeys.create, for example through the Service Account"
    " Key Admin role), or an organization policy"
    " (iam.disableServiceAccountKeyCreation) forbids new keys.",
    404: "The service account {email} doesn't exist; check the email chosen in"
    " the service account step.",
    429: "IAM's quota or rate limit was hit: wait a minute and try again. If"
    " the account already has 10 keys, delete unused ones ('{list}').",
}


def _create_failure(
    error: BaseException, service_account_email: str
) -> BaseException | None:
    """Turn a ``keys.create`` failure into the error to raise.

    Returns:
        A :class:`KeyCreateError` that says whether a key may exist, a
        :class:`KeySetupInterrupted` for Ctrl-C, or None for anything else
        (a bug), which the caller re-raises as it is.
    """
    email = service_account_email
    if isinstance(error, KeyboardInterrupt):
        return KeySetupInterrupted(
            "Interrupted while IAM was creating a key; " + _maybe_created_advice(email)
        )
    if isinstance(error, HttpError):
        status = error.resp.status
        detail = f"HTTP {status}" + (
            f": {error.reason}" if getattr(error, "reason", "") else ""
        )
        if status == 408 or status >= 500:
            return KeyCreateError(
                f"Creating a key failed ({detail}). {_maybe_created_advice(email)}",
                maybe_created=True,
                status=status,
            )
        advice = _CREATE_REFUSED_ADVICE.get(
            status, "Check the error above and try again."
        ).format(email=email, list=_list_keys_command(email))
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
            f" {error}). {_maybe_created_advice(email)}",
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

    def discard_file() -> None:
        out.close()
        if not existed:
            _remove_quietly(output_path)

    try:
        # O_CREAT's mode only applies to new files; tighten an existing one.
        os.chmod(output_path, 0o600)
        service = _build_iam_service(credentials)
    except BaseException:
        discard_file()
        raise
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
        discard_file()
        failure = _create_failure(e, service_account_email)
        if failure is None:
            raise
        raise failure from e

    try:
        key_data = json.loads(base64.b64decode(key["privateKeyData"]))
        out.truncate(0)
        json.dump(key_data, out, indent=2)
        out.close()
    except BaseException as e:
        discard_file()
        _handle_unsaved_key(
            service, key, output_path, e, service_account_email, existed
        )
    return output_path


def _remove_quietly(path: Path) -> None:
    """Delete *path*, best effort (a failure is logged at DEBUG)."""
    try:
        path.unlink()
    except OSError as e:
        log_with_context(logging.DEBUG, f"Couldn't remove {path}: {e}")


def _handle_unsaved_key(
    service: Any,
    key: Mapping[str, Any],
    output_path: Path,
    error: BaseException,
    service_account_email: str,
    overwrote: bool,
) -> NoReturn:
    """Delete a new key that couldn't be saved, then raise what happened.

    Args:
        service: The IAM client that created the key.
        key: The ``keys.create`` response.
        output_path: Where the key should have been written.
        error: Why writing it failed.
        service_account_email: The key's service account.
        overwrote: True if *output_path* held an earlier file, which is now
            truncated.

    Raises:
        KeyboardInterrupt: The original Ctrl-C, when the key was deleted
            and no earlier file was lost.
        KeySetupInterrupted: On Ctrl-C (while saving or while deleting)
            with the key possibly still active, or an earlier file lost.
        KeyNotSavedError: Otherwise; ``deleted`` says whether the key is
            gone.
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
        f" {output_path} no longer holds its earlier content (it was truncated"
        " for the new key); restore it or remove it."
        if overwrote
        else ""
    )
    if delete_error is None:
        if isinstance(error, KeyboardInterrupt) and not overwrote:
            raise error  # nothing is left behind: a plain Ctrl-C
        message = (
            f"The new key couldn't be saved to {output_path} ({cause}); it was"
            f" deleted again, so no key is left in IAM.{lost_file}"
        )
        if interrupted:
            raise KeySetupInterrupted(message, key_name) from error
        raise KeyNotSavedError(message, key_name, deleted=True) from error

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
            else f" (find it with '{_list_keys_command(service_account_email)}')"
        )
        + " or in the Cloud Console (IAM > Service accounts > Keys)."
        + lost_file
    )
    if interrupted:
        raise KeySetupInterrupted(message, key_name) from error
    raise KeyNotSavedError(message, key_name, deleted=False) from error
