"""Test domain-wide delegation by impersonating the admin user."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def test_delegation(
    creds_path: Path,
    workspace_admin: str,
) -> dict[str, Any]:
    """Verify delegation works by making a test API call as the admin.

    Args:
        creds_path: Path to the service account JSON key file.
        workspace_admin: Email of the workspace admin to impersonate.

    Returns:
        Dict with 'success' bool and 'detail' message.
    """
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/chat.spaces.readonly"]
        sa_creds = service_account.Credentials.from_service_account_file(
            str(creds_path), scopes=scopes
        )
        delegated = sa_creds.with_subject(workspace_admin)
        service = build("chat", "v1", credentials=delegated)
        service.spaces().list(pageSize=1).execute()
        return {"success": True, "detail": "Delegation verified successfully."}
    except Exception as e:
        return {"success": False, "detail": str(e)}
