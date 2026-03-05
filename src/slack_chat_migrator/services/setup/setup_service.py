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


def load_state(path: Path | None = None) -> SetupState:
    """Load setup state from disk.

    Args:
        path: Override state file path (useful for testing).

    Returns:
        The loaded state, or a fresh state if none exists.
    """
    state_path = path or _STATE_FILE
    if state_path.exists():
        data = json.loads(state_path.read_text())
        return SetupState(**data)
    return SetupState()


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
