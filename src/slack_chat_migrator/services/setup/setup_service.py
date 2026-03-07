"""Orchestrator for the GCP setup wizard with persistent state."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class StepStatus(str, Enum):
    """Status of an individual setup step."""

    PENDING = "pending"
    COMPLETE = "complete"
    SKIPPED = "skipped"


@dataclass
class SetupState:
    """Persistent state for the setup wizard."""

    project_id: str | None = None
    apis_enabled: list[str] = field(default_factory=list)
    service_account_email: str | None = None
    key_path: str | None = None
    workspace_admin: str | None = None
    delegation_verified: bool = False
    steps: dict[str, str] = field(default_factory=dict)

    def step_status(self, name: str) -> StepStatus:
        """Get the status of a named step."""
        return StepStatus(self.steps.get(name, StepStatus.PENDING))

    def mark_step(self, name: str, status: StepStatus) -> None:
        """Mark a step as complete or skipped."""
        self.steps[name] = status.value


_STATE_DIR = Path.home() / ".config" / "slack-chat-migrator"
_STATE_FILE = _STATE_DIR / "setup-state.json"


def load_state(path: Path | None = None) -> SetupState | None:
    """Load setup state from disk.

    Args:
        path: Override state file path (useful for testing).

    Returns:
        The loaded state, or None if no valid state exists.
    """
    state_path = path or _STATE_FILE
    if not state_path.exists():
        return None
    try:
        text = state_path.read_text().strip()
        if not text:
            return None
        data = json.loads(text)
        if not isinstance(data, dict):
            return None
        return SetupState(**data)
    except (json.JSONDecodeError, TypeError, OSError):
        return None


def save_state(state: SetupState, path: Path | None = None) -> None:
    """Persist setup state to disk.

    Args:
        state: The state to save.
        path: Override state file path (useful for testing).
    """
    from dataclasses import asdict

    state_path = path or _STATE_FILE
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(asdict(state), indent=2))


def get_credentials() -> Any:
    """Get default application credentials for the setup wizard.

    Returns:
        Google OAuth2 credentials.

    Raises:
        RuntimeError: If no credentials are available.
    """
    try:
        import google.auth

        credentials, _ = google.auth.default()
        return credentials
    except Exception as e:
        raise RuntimeError(
            "No Google credentials found. Run 'gcloud auth application-default login' first."
        ) from e


def get_adc_quota_project() -> str | None:
    """Read the quota_project_id from the ADC file.

    Returns:
        The quota project ID, or None if not set or the file is missing.
    """
    import json
    from pathlib import Path

    adc_path = (
        Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    )
    if not adc_path.exists():
        return None
    try:
        data = json.loads(adc_path.read_text())
        result: str | None = data.get("quota_project_id")
        return result
    except Exception:
        return None


def verify_quota_project(credentials: Any, project_id: str) -> tuple[str, str] | None:
    """Check whether the quota project is active.

    Uses the REST discovery API to avoid gRPC dependency issues.

    Returns:
        None if the project is active, or a (severity, message) tuple.
        severity is "error" for definite problems, "warning" for transient issues.
    """
    from googleapiclient.discovery import build

    try:
        service = build("cloudresourcemanager", "v1", credentials=credentials)
        project = service.projects().get(projectId=project_id).execute()
        lifecycle = project.get("lifecycleState", "UNKNOWN")
        if lifecycle != "ACTIVE":
            return (
                "error",
                f"Quota project '{project_id}' is {lifecycle}",
            )
        return None
    except Exception as e:
        error_msg = str(e)
        if "has been deleted" in error_msg or "NOT_FOUND" in error_msg:
            return (
                "error",
                f"Quota project '{project_id}' is deleted or not found",
            )
        # Network errors, timeouts, etc. — don't block, just warn
        return (
            "warning",
            f"Could not verify quota project '{project_id}': {error_msg}",
        )
