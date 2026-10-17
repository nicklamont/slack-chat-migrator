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
from pathlib import Path
from typing import Any


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


def is_key_active(credentials: Any, service_account_email: str, key_id: str) -> bool:
    """Return True if *key_id* is a current user-managed key of the account.

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        key_id: The key's ``private_key_id``.

    Returns:
        True when the key still exists (it hasn't been deleted).
    """
    service = _build_iam_service(credentials)
    response = (
        service.projects()
        .serviceAccounts()
        .keys()
        .list(name=_key_name(service_account_email), keyTypes="USER_MANAGED")
        .execute()
    )
    return any(
        str(key.get("name", "")).rsplit("/", 1)[-1] == key_id
        for key in response.get("keys", [])
    )


def existing_key_is_valid(
    credentials: Any, service_account_email: str, key_path: Path
) -> bool:
    """Return True if *key_path* holds an active key of the service account.

    Used to reuse a key file instead of minting a new key on every run
    (a service account can have at most 10 keys).

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        key_path: Path to an existing JSON key file.

    Returns:
        True if the file is a key for this account and the key still exists.
    """
    try:
        data = json.loads(key_path.read_text())
    except (OSError, ValueError):
        return False
    if not isinstance(data, dict):
        return False
    key_id = data.get("private_key_id")
    if data.get("client_email") != service_account_email or not key_id:
        return False
    try:
        return is_key_active(credentials, service_account_email, str(key_id))
    except Exception:
        return False


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
