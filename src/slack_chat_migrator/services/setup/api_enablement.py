"""Enable required Google APIs on a GCP project."""

from __future__ import annotations

from typing import Any

REQUIRED_APIS = [
    "chat.googleapis.com",
    "drive.googleapis.com",
    "admin.googleapis.com",
]


def get_enabled_apis(credentials: Any, project_id: str) -> set[str]:
    """Return the set of already-enabled API service names.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: GCP project ID.

    Returns:
        Set of enabled API service names.
    """
    from google.cloud import servicemanagement_v1  # type: ignore[import-untyped]

    client = servicemanagement_v1.ServiceManagerClient(credentials=credentials)
    enabled: set[str] = set()
    for service in client.list_services(
        request={
            "producer_project_id": project_id,
            "consumer_id": f"project:{project_id}",
        }
    ):
        enabled.add(service.service_name)
    return enabled


def enable_api(credentials: Any, project_id: str, service_name: str) -> None:
    """Enable a single API on the project.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: GCP project ID.
        service_name: API service name (e.g. 'chat.googleapis.com').
    """
    from google.cloud import servicemanagement_v1  # type: ignore[import-untyped]

    client = servicemanagement_v1.ServiceManagerClient(credentials=credentials)
    operation = client.enable_service(
        request={
            "service_name": service_name,
            "consumer_id": f"project:{project_id}",
        }
    )
    operation.result()


def enable_required_apis(
    credentials: Any,
    project_id: str,
    on_progress: Any | None = None,
) -> list[str]:
    """Enable all required APIs, skipping already-enabled ones.

    Args:
        credentials: Google OAuth2 credentials.
        project_id: GCP project ID.
        on_progress: Optional callback(service_name, status) for progress.

    Returns:
        List of newly enabled API names.
    """
    already_enabled = get_enabled_apis(credentials, project_id)
    newly_enabled: list[str] = []

    for api in REQUIRED_APIS:
        if api in already_enabled:
            if on_progress:
                on_progress(api, "already_enabled")
            continue
        enable_api(credentials, project_id, api)
        newly_enabled.append(api)
        if on_progress:
            on_progress(api, "enabled")

    return newly_enabled
