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


def download_key(
    credentials: Any,
    service_account_email: str,
    output_path: Path,
) -> Path:
    """Create and download a JSON key for the service account.

    Args:
        credentials: Google OAuth2 credentials.
        service_account_email: Full email of the service account.
        output_path: Where to write the JSON key file.

    Returns:
        Path to the written key file.
    """
    import os

    service = _build_iam_service(credentials)
    sa_name = f"projects/-/serviceAccounts/{service_account_email}"
    key = (
        service.projects()
        .serviceAccounts()
        .keys()
        .create(
            name=sa_name,
            body={"keyAlgorithm": "KEY_ALG_RSA_2048"},
        )
        .execute()
    )
    key_data = json.loads(base64.b64decode(key["privateKeyData"]))
    fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(key_data, f, indent=2)
    return output_path
