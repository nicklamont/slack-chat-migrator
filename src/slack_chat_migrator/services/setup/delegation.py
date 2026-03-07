"""Test domain-wide delegation by impersonating the admin user."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _read_key_info(creds_path: Path) -> dict[str, str]:
    """Extract diagnostic fields from the service account key file."""
    try:
        data = json.loads(creds_path.read_text())
        return {
            "client_id": data.get("client_id", ""),
            "client_email": data.get("client_email", ""),
            "project_id": data.get("project_id", ""),
        }
    except Exception:
        return {}


def verify_chat_app(
    creds_path: Path,
    workspace_admin: str,
) -> dict[str, Any]:
    """Check if the Chat app is configured by making a test API call.

    Uses the service account with delegation to make a Chat API call.
    If the Chat app isn't configured, the API returns a specific 404.

    Returns:
        Dict with 'configured' bool and 'detail' message.
    """
    if not workspace_admin or "@" not in workspace_admin:
        return {
            "configured": True,
            "detail": "Skipped — no workspace admin email provided.",
        }
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
        return {"configured": True, "detail": "Chat app is configured."}
    except Exception as e:
        error_str = str(e)
        if "Chat app not found" in error_str:
            return {"configured": False, "detail": error_str}
        # Other errors (e.g. delegation not set up) — can't determine
        # Chat app status, so skip this check
        return {"configured": True, "detail": f"Could not verify: {error_str}"}


def test_delegation(
    creds_path: Path,
    workspace_admin: str,
) -> dict[str, Any]:
    """Verify delegation works by making a test API call as the admin.

    Args:
        creds_path: Path to the service account JSON key file.
        workspace_admin: Email of the workspace admin to impersonate.

    Returns:
        Dict with 'success' bool, 'detail' message, and 'key_info' diagnostics.
    """
    key_info = _read_key_info(creds_path)

    if not workspace_admin or "@" not in workspace_admin:
        return {
            "success": False,
            "detail": "A valid workspace admin email is required for delegation test.",
            "key_info": key_info,
        }

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
        return {
            "success": True,
            "detail": "Delegation verified successfully.",
            "key_info": key_info,
        }
    except Exception as e:
        error_str = str(e)
        # A 404 "Chat app not found" means delegation worked (the
        # impersonated credential was accepted) but the Chat API
        # hasn't been configured with a Chat app yet.
        if "Chat app not found" in error_str or (
            "chat.googleapis.com" in error_str and "404" in error_str
        ):
            return {
                "success": True,
                "detail": "Delegation verified (Chat app configuration pending).",
                "key_info": key_info,
                "chat_app_missing": True,
            }
        return {"success": False, "detail": error_str, "key_info": key_info}
