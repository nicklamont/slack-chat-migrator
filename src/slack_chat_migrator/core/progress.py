"""Progress tracking via an event-emitter pattern.

Migration code emits structured ``ProgressEvent`` objects through a
``ProgressTracker``.  Renderers (Rich, plain text, etc.) subscribe to
receive these events without the migration core knowing about UI details.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Types of progress events emitted during migration."""

    CHANNEL_START = auto()
    CHANNEL_COMPLETE = auto()
    MESSAGE_SENT = auto()
    MESSAGE_FAILED = auto()
    FILE_UPLOADED = auto()
    REACTION_ADDED = auto()
    SPACE_CREATED = auto()
    MEMBER_ADDED = auto()
    PHASE_CHANGE = auto()


@dataclass
class ProgressEvent:
    """A single progress event emitted by the migration pipeline.

    Attributes:
        event_type: The kind of event.
        channel: The channel name this event relates to (if any).
        detail: Human-readable detail string (e.g. error message).
        count: Current progress count within a phase.
        total: Total items expected in this phase.
        timestamp: Unix timestamp when the event was created.
    """

    event_type: EventType
    channel: str | None = None
    detail: str | None = None
    count: int | None = None
    total: int | None = None
    timestamp: float = field(default_factory=time.time)


# Type alias for subscriber callbacks.
Subscriber = Callable[[ProgressEvent], Any]


class ProgressTracker:
    """Event emitter for migration progress.

    Subscribers register via :meth:`subscribe` and receive every
    :class:`ProgressEvent` that is emitted.
    """

    def __init__(self) -> None:
        self._subscribers: list[Subscriber] = []

    def subscribe(self, callback: Subscriber) -> None:
        """Register a callback to receive progress events."""
        self._subscribers.append(callback)

    def emit(self, event: ProgressEvent) -> None:
        """Broadcast *event* to all subscribers.

        A failing subscriber is logged and skipped so that renderer errors
        never halt the migration.
        """
        for callback in self._subscribers:
            try:
                callback(event)
            except Exception:
                logger.debug(
                    "Subscriber %r failed for %s",
                    callback,
                    event.event_type,
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    # Convenience helpers — thin wrappers around ``emit()``
    # ------------------------------------------------------------------

    def channel_start(
        self,
        channel: str,
        total_messages: int | None = None,
    ) -> None:
        """Signal that processing of *channel* has begun."""
        self.emit(
            ProgressEvent(
                event_type=EventType.CHANNEL_START,
                channel=channel,
                total=total_messages,
            )
        )

    def channel_complete(self, channel: str) -> None:
        """Signal that processing of *channel* has finished."""
        self.emit(
            ProgressEvent(
                event_type=EventType.CHANNEL_COMPLETE,
                channel=channel,
            )
        )

    def message_sent(
        self,
        channel: str,
        count: int | None = None,
        total: int | None = None,
    ) -> None:
        """Record a successfully sent message."""
        self.emit(
            ProgressEvent(
                event_type=EventType.MESSAGE_SENT,
                channel=channel,
                count=count,
                total=total,
            )
        )

    def message_failed(
        self,
        channel: str,
        detail: str | None = None,
    ) -> None:
        """Record a message that failed to send."""
        self.emit(
            ProgressEvent(
                event_type=EventType.MESSAGE_FAILED,
                channel=channel,
                detail=detail,
            )
        )

    def file_uploaded(self, channel: str) -> None:
        """Record a file upload."""
        self.emit(
            ProgressEvent(
                event_type=EventType.FILE_UPLOADED,
                channel=channel,
            )
        )

    def reaction_added(self, channel: str) -> None:
        """Record a reaction addition."""
        self.emit(
            ProgressEvent(
                event_type=EventType.REACTION_ADDED,
                channel=channel,
            )
        )

    def space_created(self, channel: str) -> None:
        """Record a Chat space creation."""
        self.emit(
            ProgressEvent(
                event_type=EventType.SPACE_CREATED,
                channel=channel,
            )
        )

    def member_added(self, channel: str) -> None:
        """Record a member addition."""
        self.emit(
            ProgressEvent(
                event_type=EventType.MEMBER_ADDED,
                channel=channel,
            )
        )

    def phase_change(self, phase: str) -> None:
        """Signal a migration phase transition (e.g. 'validation' → 'migration')."""
        self.emit(
            ProgressEvent(
                event_type=EventType.PHASE_CHANGE,
                detail=phase,
            )
        )
