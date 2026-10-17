"""Service account creation and key download.

Uses the REST-based discovery API (google-api-python-client) instead of the
gRPC client to avoid billing routing through the ADC quota project.

No project-level IAM roles are needed — the service account gets all its
permissions via domain-wide delegation scopes configured in the Google
Workspace Admin Console.
"""

from __future__ import annotations

import base64
import json
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
    #: The file isn't a key of this service account (unreadable, not a key
    #: file, or another account's key).
    NOT_A_KEY = "not_a_key"
    #: The account's keys couldn't be listed (IAM API not enabled, missing
    #: permission, network error), so nothing is known about the key.
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class KeyCheck:
    """Result of :func:`check_existing_key`.

    Attributes:
        status: What the check found.
        reason: Why the key isn't usable, or why it couldn't be checked;
            empty when *status* is ``ACTIVE``.
    """

    status: KeyStatus
    reason: str = ""

    @property
    def active(self) -> bool:
        """True when the key file can be reused."""
        return self.status is KeyStatus.ACTIVE


def _parse_key_time(value: Any) -> datetime | None:
    """Parse an IAM timestamp such as ``2026-01-31T12:00:00Z``."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Python 3.10's fromisoformat() accepts at most 6 fractional digits.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def key_inactive_reason(
    key: Mapping[str, Any] | None, now: datetime | None = None
) -> str | None:
    """Say why an IAM ``ServiceAccountKey`` can't be used, or None if it can.

    Args:
        key: The key resource from ``keys.list``, or None when the key isn't
            listed (it was deleted).
        now: The current time (for tests); defaults to now, in UTC.

    Returns:
        A short reason ("it was deleted", "it is disabled", "it expired on
        ...", "it isn't valid until ..."), or None when the key is active.
    """
    if key is None:
        return "it was deleted"
    if key.get("disabled") is True:
        return "it is disabled"
    now = now or datetime.now(timezone.utc)
    valid_before = _parse_key_time(key.get("validBeforeTime"))
    if valid_before is not None and valid_before <= now:
        return f"it expired on {key.get('validBeforeTime')}"
    valid_after = _parse_key_time(key.get("validAfterTime"))
    if valid_after is not None and valid_after > now:
        return f"it isn't valid until {key.get('validAfterTime')}"
    return None


def _find_key(
    credentials: Any, service_account_email: str, key_id: str
) -> dict[str, Any] | None:
    """Return the user-managed key *key_id* of the account, or None.

    Raises:
        HttpError: If the keys can't be listed.
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


def is_key_active(
    credentials: Any,
    service_account_email: str,
    key_id: str,
    *,
    now: datetime | None = None,
) -> bool:
    """Return True if *key_id* is a usable user-managed key of the account.

    A key is usable when it is still listed (not deleted), isn't disabled,
    hasn't expired (``validBeforeTime``) and is already valid
    (``validAfterTime``).

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        key_id: The key's ``private_key_id``.
        now: The current time (for tests); defaults to now, in UTC.

    Returns:
        True when the key can be used to authenticate.

    Raises:
        HttpError: If the account's keys can't be listed.
    """
    key = _find_key(credentials, service_account_email, key_id)
    return key_inactive_reason(key, now) is None


def _describe_list_error(error: Exception) -> str:
    """Explain why listing the account's keys failed."""
    if isinstance(error, HttpError):
        status = error.resp.status
        detail = getattr(error, "reason", "") or ""
        hint = (
            " Check that the IAM API is enabled and that your account may list"
            " the service account's keys (iam.serviceAccountKeys.list)."
            if status in (400, 401, 403, 404)
            else ""
        )
        return (
            f"listing the keys failed with HTTP {status}"
            + (f" ({detail})" if detail else "")
            + "."
            + hint
        )
    if isinstance(error, GoogleAuthError):
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
    service account can have at most 10 keys).  A failure to list the keys
    is reported as ``UNKNOWN``, never as an inactive key, so a working key
    file isn't overwritten because of a permission or network problem.

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        key_path: Path to an existing JSON key file.
        now: The current time (for tests); defaults to now, in UTC.

    Returns:
        The :class:`KeyCheck` result.
    """
    try:
        data = json.loads(key_path.read_text())
    except (OSError, ValueError) as e:
        return KeyCheck(KeyStatus.NOT_A_KEY, f"it can't be read as JSON ({e})")
    if not isinstance(data, dict) or not data.get("private_key_id"):
        return KeyCheck(KeyStatus.NOT_A_KEY, "it isn't a service account key file")
    owner = data.get("client_email")
    if owner != service_account_email:
        return KeyCheck(KeyStatus.NOT_A_KEY, f"it is a key of {owner or 'unknown'}")
    try:
        key = _find_key(credentials, service_account_email, str(data["private_key_id"]))
    except (HttpError, GoogleAuthError, HttpLib2Error, OSError) as e:
        return KeyCheck(KeyStatus.UNKNOWN, _describe_list_error(e))
    reason = key_inactive_reason(key, now)
    if reason is not None:
        return KeyCheck(KeyStatus.INACTIVE, reason)
    return KeyCheck(KeyStatus.ACTIVE)


def download_key(
    credentials: Any,
    service_account_email: str,
    output_path: Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Create and download a JSON key for the service account.

    The key file is written with mode ``0600`` (also when an existing file is
    overwritten).

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        output_path: Where to write the JSON key file.
        overwrite: Replace *output_path* if it already exists.

    Returns:
        Path to the written key file.

    Raises:
        FileExistsError: If *output_path* exists and *overwrite* is False.
    """
    import os

    if output_path.exists() and not overwrite:
        raise FileExistsError(f"Key file already exists: {output_path}")

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
    key_data = json.loads(base64.b64decode(key["privateKeyData"]))
    fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    # O_CREAT's mode only applies to new files; tighten an overwritten one too.
    os.chmod(output_path, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(key_data, f, indent=2)
    return output_path
