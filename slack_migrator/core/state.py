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
    space_mapping: dict[str, str] = field(default_factory=dict)
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

    def __post_init__(self) -> None:
        """Validate invariants after initialization."""
        # Validate numeric counters are non-negative
        if self.channel_error_count < 0:
            raise ValueError(
                f"channel_error_count must be non-negative, got {self.channel_error_count}"
            )

        # Validate collection fields are the correct types
        dict_fields = [
            "space_mapping",
            "space_cache",
            "created_spaces",
            "channel_to_space",
            "channel_id_to_space_id",
            "channel_handlers",
            "thread_map",
            "message_id_map",
            "failed_messages_by_channel",
            "drive_files_cache",
            "chat_delegates",
            "valid_users",
            "migration_summary",
            "last_processed_timestamps",
            "channel_stats",
            "spaces_with_external_users",
            "active_users_by_channel",
            "high_failure_rate_channels",
            "migration_issues",
        ]
        for name in dict_fields:
            value = getattr(self, name)
            if not isinstance(value, dict):
                raise TypeError(f"{name} must be a dict, got {type(value).__name__}")

        list_fields = [
            "failed_messages",
            "skipped_reactions",
            "incomplete_import_spaces",
            "migration_errors",
            "channels_with_errors",
        ]
        for name in list_fields:
            value = getattr(self, name)
            if not isinstance(value, list):
                raise TypeError(f"{name} must be a list, got {type(value).__name__}")

        set_fields = ["sent_messages", "external_users", "channel_conflicts"]
        for name in set_fields:
            value = getattr(self, name)
            if not isinstance(value, set):
                raise TypeError(f"{name} must be a set, got {type(value).__name__}")

    def reset_for_run(self) -> None:
        """Reset per-run state at the start of a new migration run.

        Consolidates the scattered state resets that were previously done
        inline in the migrate() method.
        """
        self.channel_handlers = {}
        self.thread_map = {}
        self.migration_summary = {
            "channels_processed": [],
            "spaces_created": 0,
            "messages_created": 0,
            "reactions_created": 0,
            "files_created": 0,
        }
        self.migration_errors = []
        self.channels_with_errors = []
        self.channel_error_count = 0
        self.first_channel_processed = False

    @property
    def has_errors(self) -> bool:
        """Return True if any migration errors or channel errors were recorded."""
        return bool(self.migration_errors) or bool(self.channels_with_errors)

    @property
    def total_messages_attempted(self) -> int:
        """Return total messages attempted (created + failed)."""
        created: int = self.migration_summary.get("messages_created", 0)
        failed = len(self.failed_messages)
        return created + failed

    @property
    def success_rate(self) -> float:
        """Return percentage of successful messages out of total attempted.

        Returns 100.0 if no messages were attempted.
        """
        total = self.total_messages_attempted
        if total == 0:
            return 100.0
        created: int = self.migration_summary.get("messages_created", 0)
        return (created / total) * 100.0
