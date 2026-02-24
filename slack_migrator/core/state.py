"""
Migration state container for the Slack to Google Chat migration.

Extracts mutable tracking state from SlackToChatMigrator into a dedicated
dataclass, making the migrator's runtime state inspectable and testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MigrationState:
    """Holds all mutable tracking state for a migration run.

    Grouped by concern:
    - Space/channel mapping and tracking
    - Thread and message tracking
    - File and drive caching
    - User validation caching
    - Migration progress and statistics
    - Error and issue tracking
    - Current operation context
    """

    # --- Space/channel mapping ---
    space_cache: dict[str, str] = field(default_factory=dict)
    created_spaces: dict[str, str] = field(default_factory=dict)
    channel_to_space: dict[str, str] = field(default_factory=dict)
    channel_id_to_space_id: dict[str, str] = field(default_factory=dict)
    channel_handlers: dict[str, Any] = field(default_factory=dict)

    # --- Thread and message tracking ---
    thread_map: dict[str, str] = field(default_factory=dict)
    sent_messages: set[str] = field(default_factory=set)
    message_id_map: dict[str, str] = field(default_factory=dict)
    failed_messages: list[dict[str, Any]] = field(default_factory=list)
    failed_messages_by_channel: dict[str, list[str]] = field(default_factory=dict)

    # --- File and drive caching ---
    drive_files_cache: dict[str, Any] = field(default_factory=dict)

    # --- User validation caching ---
    chat_delegates: dict[str, Any] = field(default_factory=dict)
    valid_users: dict[str, bool] = field(default_factory=dict)
    external_users: set[str] = field(default_factory=set)
    skipped_reactions: list[dict[str, str]] = field(default_factory=list)

    # --- Migration progress and statistics ---
    migration_summary: dict[str, Any] = field(default_factory=dict)
    last_processed_timestamps: dict[str, float] = field(default_factory=dict)
    channel_stats: dict[str, dict[str, int]] = field(default_factory=dict)
    spaces_with_external_users: dict[str, bool] = field(default_factory=dict)
    active_users_by_channel: dict[str, set[str]] = field(default_factory=dict)

    # --- Error and issue tracking ---
    high_failure_rate_channels: dict[str, float] = field(default_factory=dict)
    incomplete_import_spaces: list[tuple[str, str]] = field(default_factory=list)
    channel_conflicts: set[str] = field(default_factory=set)
    migration_issues: dict[str, Any] = field(default_factory=dict)

    # --- Migration lifecycle (initialized during migrate()) ---
    migration_errors: list[Any] = field(default_factory=list)
    channels_with_errors: list[str] = field(default_factory=list)
    channel_error_count: int = 0
    first_channel_processed: bool = False

    # --- Current operation context ---
    current_channel: str | None = None
    current_space: str | None = None
    current_message_ts: str | None = None
    output_dir: str | None = None
