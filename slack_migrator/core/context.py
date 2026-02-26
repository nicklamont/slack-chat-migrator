"""Immutable migration context.

MigrationContext is a frozen dataclass that holds all configuration and
loaded data for a migration run.  It is created once during migrator
initialization and shared (read-only) with every service function that
needs configuration, paths, or user/channel metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from slack_migrator.core.config import MigrationConfig


@dataclass(frozen=True)
class MigrationContext:
    """Immutable context for a migration run. Created once, shared everywhere."""

    # Paths
    export_root: Path
    creds_path: str

    # Workspace identity
    workspace_admin: str
    workspace_domain: str

    # Mode flags
    dry_run: bool
    update_mode: bool
    verbose: bool
    debug_api: bool

    # Loaded configuration
    config: MigrationConfig

    # User data (populated from users.json + config overrides)
    user_map: dict[str, str]  # slack_user_id -> google_email
    users_without_email: list[dict[str, Any]]

    # Channel metadata (from channels.json)
    channels_meta: dict[str, Any]  # channel_name -> channel data
    channel_id_to_name: dict[str, str]
    channel_name_to_id: dict[str, str]

    @property
    def import_mode(self) -> bool:
        """True when running in import mode (the default, opposite of update mode)."""
        return not self.update_mode

    @property
    def progress_file(self) -> Path:
        """Path to the migration progress tracking file."""
        return self.export_root / ".migration_progress.json"

    @property
    def log_prefix(self) -> str:
        """Mode-aware log prefix.

        Returns a string like ``"[DRY RUN] "`` or ``"[UPDATE MODE] "`` that
        can be prepended to log messages.  Eliminates the many scattered
        prefix computations throughout the codebase.
        """
        if self.dry_run and self.update_mode:
            return "[DRY RUN] [UPDATE MODE] "
        if self.dry_run:
            return "[DRY RUN] "
        if self.update_mode:
            return "[UPDATE MODE] "
        return ""
