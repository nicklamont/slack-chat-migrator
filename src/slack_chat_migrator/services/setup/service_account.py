"""Service account creation, key download, and role grants."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any


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
    from google.cloud import iam_admin_v1  # type: ignore[import-untyped]

    client = iam_admin_v1.IAMClient(credentials=credentials)
    sa = client.create_service_account(
        request={
            "name": f"projects/{project_id}",
            "account_id": account_id,
            "service_account": {"display_name": display_name},
        }
    )
    return {"email": sa.email, "name": sa.name}


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
    from google.cloud import iam_admin_v1  # type: ignore[import-untyped]

    client = iam_admin_v1.IAMClient(credentials=credentials)
    sa_name = f"projects/-/serviceAccounts/{service_account_email}"
    key = client.create_service_account_key(
        request={"name": sa_name, "key_algorithm": "KEY_ALG_RSA_2048"}
    )
    key_data = json.loads(base64.b64decode(key.private_key_data))
    import os

    fd = os.open(str(output_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(key_data, f, indent=2)
    return output_path


def grant_roles(
    credentials: Any,
    project_id: str,
    service_account_email: str,
    roles: list[str] | None = None,
) -> list[str]:
    """Grant IAM roles to the service account on the project.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: GCP project ID.
        service_account_email: Full email of the service account.
        roles: Roles to grant. Defaults to Chat Admin role.

    Returns:
        List of roles granted.
    """
    from google.cloud import resourcemanager_v3  # type: ignore[import-untyped]
    from google.iam.v1 import iam_policy_pb2, policy_pb2  # type: ignore[import-untyped]

    if roles is None:
        roles = ["roles/chat.admin"]

    client = resourcemanager_v3.ProjectsClient(credentials=credentials)
    resource = f"projects/{project_id}"

    policy = client.get_iam_policy(
        request=iam_policy_pb2.GetIamPolicyRequest(resource=resource)
    )

    member = f"serviceAccount:{service_account_email}"
    for role in roles:
        existing = next((b for b in policy.bindings if b.role == role), None)
        if existing:
            if member not in existing.members:
                existing.members.append(member)
        else:
            binding = policy_pb2.Binding(role=role, members=[member])
            policy.bindings.append(binding)

    client.set_iam_policy(
        request=iam_policy_pb2.SetIamPolicyRequest(resource=resource, policy=policy)
    )
    return roles
