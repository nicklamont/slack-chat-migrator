"""GCP project creation and selection.

Uses the REST-based discovery API (google-api-python-client) instead of the
gRPC client to avoid billing routing through the ADC quota project.
"""

from __future__ import annotations

from typing import Any


def _build_crm_service(credentials: Any) -> Any:
    """Build a Cloud Resource Manager v1 API client via discovery."""
    from googleapiclient.discovery import build

    return build("cloudresourcemanager", "v1", credentials=credentials)


def list_projects(credentials: Any) -> list[dict[str, str]]:
    """List accessible GCP projects.

    Args:
        credentials: Google OAuth2 credentials.

    Returns:
        List of dicts with 'project_id' and 'display_name' keys.
    """
    service = _build_crm_service(credentials)
    projects: list[dict[str, str]] = []
    request = service.projects().list(filter="lifecycleState:ACTIVE")
    while request is not None:
        response = request.execute()
        for p in response.get("projects", []):
            projects.append(
                {
                    "project_id": p["projectId"],
                    "display_name": p.get("name", p["projectId"]),
                }
            )
        request = service.projects().list_next(request, response)
    return projects


def check_project_id(credentials: Any, project_id: str) -> str | None:
    """Check if a project ID is already in use and why.

    Returns:
        A human-readable reason string, or None if the ID is available.
    """
    service = _build_crm_service(credentials)
    try:
        project = service.projects().get(projectId=project_id).execute()
        lifecycle = project.get("lifecycleState", "UNKNOWN")
        if lifecycle == "DELETE_REQUESTED":
            return (
                f"Project [bold]{project_id}[/bold] is pending deletion in your account.\n"
                "GCP keeps deleted projects for 30 days. You can restore it at:\n"
                f"  https://console.cloud.google.com/cloud-resource-manager?project={project_id}\n"
                "Or wait for it to be fully deleted, then reuse the ID."
            )
        return f"Project [bold]{project_id}[/bold] already exists (state: {lifecycle})."
    except Exception:
        # 403/404 means we can't see it — it's owned by someone else or doesn't exist.
        # If creation then fails with 409, it's globally taken by another user.
        return None


def create_project(credentials: Any, project_id: str, display_name: str) -> str:
    """Create a new GCP project.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: Desired project ID.
        display_name: Human-readable project name.

    Returns:
        The created project ID.
    """
    service = _build_crm_service(credentials)
    operation = (
        service.projects()
        .create(body={"projectId": project_id, "name": display_name})
        .execute()
    )
    # Poll until operation completes
    if not operation.get("done"):
        import time

        op_name = operation["name"]
        crm_v1 = service
        for _ in range(30):
            result = crm_v1.operations().get(name=op_name).execute()
            if result.get("done"):
                if "error" in result:
                    raise RuntimeError(
                        result["error"].get("message", "Project creation failed")
                    )
                break
            time.sleep(2)
        else:
            raise RuntimeError(f"Operation {op_name} timed out")
    return project_id
