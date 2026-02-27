"""
Migration state container for the Slack to Google Chat migration.

Mutable tracking state for a migration run, separated from immutable
configuration (MigrationContext) for clear ownership boundaries.

State is organized into typed sub-state dataclasses by concern area.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from slack_migrator.types import FailedMessage, MigrationSummary, SkippedReaction


def _default_migration_summary() -> MigrationSummary:
    """Return a fresh MigrationSummary with zeroed counters."""
    return MigrationSummary(
        channels_processed=[],
        spaces_created=0,
        messages_created=0,
        reactions_created=0,
        files_created=0,
    )


# ---------------------------------------------------------------------------
# Sub-state dataclasses
# ---------------------------------------------------------------------------


@dataclass
class SpaceState:
    """Space and channel mapping state."""

    space_mapping: dict[str, str] = field(default_factory=dict)
    space_cache: dict[str, str] = field(default_factory=dict)
    created_spaces: dict[str, str] = field(default_factory=dict)
    channel_to_space: dict[str, str] = field(default_factory=dict)
    channel_id_to_space_id: dict[str, str] = field(default_factory=dict)
    channel_handlers: dict[str, Any] = field(default_factory=dict)


@dataclass
class MessageState:
    """Thread and message tracking state."""

    thread_map: dict[str, str] = field(default_factory=dict)
    sent_messages: set[str] = field(default_factory=set)
    message_id_map: dict[str, str] = field(default_factory=dict)
    failed_messages: list[FailedMessage] = field(default_factory=list)
    failed_messages_by_channel: dict[str, list[str]] = field(default_factory=dict)


@dataclass
class UserState:
    """User validation and delegation caching."""

    chat_delegates: dict[str, Any] = field(default_factory=dict)
    valid_users: dict[str, bool] = field(default_factory=dict)
    external_users: set[str] = field(default_factory=set)
    skipped_reactions: list[SkippedReaction] = field(default_factory=list)


@dataclass
class ProgressState:
    """Migration progress and statistics."""

    migration_summary: MigrationSummary = field(
        default_factory=_default_migration_summary
    )
    last_processed_timestamps: dict[str, float] = field(default_factory=dict)
    channel_stats: dict[str, dict[str, int]] = field(default_factory=dict)
    spaces_with_external_users: dict[str, bool] = field(default_factory=dict)
    active_users_by_channel: dict[str, set[str]] = field(default_factory=dict)


@dataclass
class ErrorState:
    """Error and issue tracking."""

    high_failure_rate_channels: dict[str, float] = field(default_factory=dict)
    incomplete_import_spaces: list[tuple[str, str]] = field(default_factory=list)
    channel_conflicts: set[str] = field(default_factory=set)
    migration_issues: dict[str, Any] = field(default_factory=dict)
    migration_errors: list[Any] = field(default_factory=list)
    channels_with_errors: list[str] = field(default_factory=list)
    channel_error_count: int = 0


@dataclass
class ContextState:
    """Current operation context."""

    current_channel: str | None = None
    current_space: str | None = None
    current_message_ts: str | None = None
    output_dir: str | None = None
    first_channel_processed: bool = False


# ---------------------------------------------------------------------------
# Composed MigrationState
# ---------------------------------------------------------------------------


@dataclass
class MigrationState:
    """Holds all mutable tracking state for a migration run.

    Composed of typed sub-state dataclasses:
    - ``spaces``: Space/channel mapping and tracking
    - ``messages``: Thread and message tracking
    - ``users``: User validation caching
    - ``progress``: Migration progress and statistics
    - ``errors``: Error and issue tracking
    - ``context``: Current operation context
    """

    spaces: SpaceState = field(default_factory=SpaceState)
    messages: MessageState = field(default_factory=MessageState)
    users: UserState = field(default_factory=UserState)
    progress: ProgressState = field(default_factory=ProgressState)
    errors: ErrorState = field(default_factory=ErrorState)
    context: ContextState = field(default_factory=ContextState)

    # File and drive caching (standalone — doesn't fit neatly in a sub-state)
    drive_files_cache: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate invariants after initialization."""
        if self.errors.channel_error_count < 0:
            raise ValueError(
                f"channel_error_count must be non-negative, got {self.errors.channel_error_count}"
            )

    def reset_for_run(self) -> None:
        """Reset per-run state at the start of a new migration run."""
        self.spaces.channel_handlers = {}
        self.messages.thread_map = {}
        self.progress.migration_summary = _default_migration_summary()
        self.errors.migration_errors = []
        self.errors.channels_with_errors = []
        self.errors.channel_error_count = 0
        self.context.first_channel_processed = False

    @property
    def has_errors(self) -> bool:
        """Return True if any migration errors or channel errors were recorded."""
        return bool(self.errors.migration_errors) or bool(
            self.errors.channels_with_errors
        )

    @property
    def total_messages_attempted(self) -> int:
        """Return total messages attempted (created + failed)."""
        created: int = self.progress.migration_summary["messages_created"]
        failed = len(self.messages.failed_messages)
        return created + failed

    @property
    def success_rate(self) -> float:
        """Return percentage of successful messages out of total attempted.

        Returns 100.0 if no messages were attempted.
        """
        total = self.total_messages_attempted
        if total == 0:
            return 100.0
        created: int = self.progress.migration_summary["messages_created"]
        return (created / total) * 100.0
