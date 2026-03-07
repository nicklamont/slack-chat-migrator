"""Enable required Google APIs on a GCP project.

Uses the REST-based discovery API (google-api-python-client) for API
enablement. On fresh projects the Service Usage API itself may not be
enabled, so we bootstrap it via ``gcloud`` first.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from typing import Any

log = logging.getLogger(__name__)

REQUIRED_APIS = [
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "chat.googleapis.com",
    "drive.googleapis.com",
    "admin.googleapis.com",
]


def _build_service(credentials: Any) -> Any:
    """Build a Service Usage API client via discovery."""
    from googleapiclient.discovery import build

    return build("serviceusage", "v1", credentials=credentials)


def _bootstrap_service_usage(project_id: str) -> None:
    """Enable the Service Usage API via gcloud (bootstrap).

    On a fresh GCP project, the Service Usage API isn't enabled, so the
    REST client can't be used to enable other APIs.  Since the user already
    has ``gcloud`` installed (they ran ``gcloud auth application-default
    login``), we shell out to bootstrap this single API.

    Raises:
        RuntimeError: If gcloud is not available or the command fails.
    """
    gcloud = shutil.which("gcloud")
    if not gcloud:
        raise RuntimeError(
            "The Service Usage API is not enabled on this project and 'gcloud' "
            "was not found on PATH.\n"
            "Enable it manually:\n"
            f"  gcloud services enable serviceusage.googleapis.com "
            f"--project={project_id}"
        )
    cmd = [
        gcloud,
        "services",
        "enable",
        "serviceusage.googleapis.com",
        f"--project={project_id}",
    ]
    log.debug("Bootstrapping Service Usage API: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to bootstrap Service Usage API:\n{result.stderr.strip()}"
        )


def get_enabled_apis(credentials: Any, project_id: str) -> set[str]:
    """Return the set of already-enabled API service names.

    Returns an empty set if listing fails (e.g. permissions, API not yet
    enabled on the project).
    """
    service = _build_service(credentials)
    enabled: set[str] = set()
    try:
        request = service.services().list(
            parent=f"projects/{project_id}",
            filter="state:ENABLED",
        )
        while request is not None:
            response = request.execute()
            for svc in response.get("services", []):
                name = svc.get("config", {}).get("name")
                if name:
                    enabled.add(name)
            request = service.services().list_next(request, response)
    except Exception as e:
        log.debug("Could not list enabled APIs: %s", e)
    return enabled


def enable_api(credentials: Any, project_id: str, service_name: str) -> None:
    """Enable a single API on the project."""
    service = _build_service(credentials)
    request = service.services().enable(
        name=f"projects/{project_id}/services/{service_name}",
    )
    operation = request.execute()
    # Wait for the operation to complete if it's long-running
    if operation.get("done") is not True:
        op_name = operation.get("name", "")
        if op_name:
            _wait_for_operation(service, op_name)


def _wait_for_operation(service: Any, operation_name: str) -> None:
    """Poll an operation until it completes."""
    import time

    for _ in range(30):
        result = service.operations().get(name=operation_name).execute()
        if result.get("done"):
            if "error" in result:
                raise RuntimeError(result["error"].get("message", "Operation failed"))
            return
        time.sleep(2)
    raise RuntimeError(f"Operation {operation_name} timed out")


def enable_required_apis(
    credentials: Any,
    project_id: str,
    on_progress: Any | None = None,
) -> list[str]:
    """Enable all required APIs, skipping already-enabled ones.

    Automatically bootstraps the Service Usage API via ``gcloud`` if it
    is not yet enabled on the project.

    Returns:
        List of newly enabled API names.
    """
    already_enabled = get_enabled_apis(credentials, project_id)
    newly_enabled: list[str] = []

    # Bootstrap: ensure Service Usage API is enabled first via gcloud,
    # since the REST client can't enable APIs if this API is disabled.
    if "serviceusage.googleapis.com" not in already_enabled:
        log.debug("Service Usage API not detected; bootstrapping via gcloud")
        _bootstrap_service_usage(project_id)
        if on_progress:
            on_progress("serviceusage.googleapis.com", "enabled")

    for api in REQUIRED_APIS:
        if api in already_enabled:
            if on_progress:
                on_progress(api, "already_enabled")
            continue
        if api == "serviceusage.googleapis.com":
            # Already bootstrapped above
            newly_enabled.append(api)
            continue
        enable_api(credentials, project_id, api)
        newly_enabled.append(api)
        if on_progress:
            on_progress(api, "enabled")

    return newly_enabled
