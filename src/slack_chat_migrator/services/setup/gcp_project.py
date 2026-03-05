"""GCP project creation and selection."""

from __future__ import annotations

from typing import Any


def list_projects(credentials: Any) -> list[dict[str, str]]:
    """List accessible GCP projects.

    Args:
        credentials: Google OAuth2 credentials.

    Returns:
        List of dicts with 'project_id' and 'display_name' keys.
    """
    from google.cloud import resourcemanager_v3  # type: ignore[import-untyped]

    client = resourcemanager_v3.ProjectsClient(credentials=credentials)
    projects = []
    for project in client.search_projects():
        if project.state.name == "ACTIVE":
            projects.append(
                {
                    "project_id": project.project_id,
                    "display_name": project.display_name,
                }
            )
    return projects


def create_project(credentials: Any, project_id: str, display_name: str) -> str:
    """Create a new GCP project.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: Desired project ID.
        display_name: Human-readable project name.

    Returns:
        The created project ID.
    """
    from google.cloud import resourcemanager_v3  # type: ignore[import-untyped]

    client = resourcemanager_v3.ProjectsClient(credentials=credentials)
    operation = client.create_project(
        project=resourcemanager_v3.Project(
            project_id=project_id,
            display_name=display_name,
        )
    )
    result = operation.result()
    return str(result.project_id)
