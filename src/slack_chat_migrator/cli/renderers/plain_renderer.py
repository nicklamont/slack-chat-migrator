"""Plain-text progress renderer for non-TTY output (pipes, CI)."""

from __future__ import annotations

import sys
import time
from typing import TextIO

from slack_chat_migrator.core.progress import EventType, ProgressEvent, ProgressTracker


class PlainProgressRenderer:
    """Renders migration progress as periodic text status lines.

    Used when stdout is not a TTY (e.g. piped to a file or running in CI).
    Prints a status line every *interval* seconds or on phase changes.
    """

    def __init__(
        self,
        tracker: ProgressTracker,
        output: TextIO | None = None,
        interval: float = 5.0,
        dry_run: bool = False,
    ) -> None:
        self._tracker = tracker
        self._output = output or sys.stderr
        self._interval = interval
        self._dry_run = dry_run
        self._start_time: float = 0.0
        self._last_print: float = 0.0

        # Counters
        self._messages_sent = 0
        self._messages_failed = 0
        self._files_uploaded = 0
        self._members_added = 0
        self._channels_complete = 0
        self._current_phase = "Initializing"
        self._current_channel: str | None = None

        tracker.subscribe(self.handle_event)

    def start(self) -> None:
        """Record start time and print initial status."""
        self._start_time = time.time()
        self._print("Migration started")

    def stop(self) -> None:
        """Print final status line."""
        self._print(
            f"Migration finished — "
            f"{self._messages_sent} sent, "
            f"{self._messages_failed} failed, "
            f"{self._files_uploaded} files, "
            f"{self._members_added} members, "
            f"{self._channels_complete} channels"
        )

    def handle_event(self, event: ProgressEvent) -> None:
        """Process a progress event."""
        if event.event_type == EventType.PHASE_CHANGE:
            self._current_phase = event.detail or "Unknown"
            self._print(f"Phase: {self._current_phase}")
        elif event.event_type == EventType.CHANNEL_START:
            self._current_channel = event.channel
            self._print(f"Processing channel: {event.channel}")
        elif event.event_type == EventType.CHANNEL_COMPLETE:
            self._channels_complete += 1
            self._print(f"Completed channel: {event.channel}")
        elif event.event_type == EventType.MESSAGE_SENT:
            self._messages_sent += 1
            self._maybe_print_status()
        elif event.event_type == EventType.MESSAGE_FAILED:
            self._messages_failed += 1
            self._print(
                f"Message failed in {event.channel}: {event.detail or 'unknown error'}"
            )
        elif event.event_type == EventType.FILE_UPLOADED:
            self._files_uploaded += 1
        elif event.event_type == EventType.MEMBER_ADDED:
            self._members_added += 1
            self._maybe_print_status()
        elif event.event_type == EventType.MEMBER_PHASE_START:
            self._print(
                f"Adding members to #{event.channel} ({event.total or 0} users)"
            )
        elif event.event_type == EventType.MESSAGE_PHASE_START:
            self._print(
                f"Sending messages in #{event.channel} ({event.total or 0} messages)"
            )

    def _maybe_print_status(self) -> None:
        """Print a status line if enough time has elapsed since the last one."""
        now = time.time()
        if now - self._last_print >= self._interval:
            elapsed = now - self._start_time if self._start_time else 0
            throughput = ""
            if elapsed > 0 and self._messages_sent > 0:
                rate = self._messages_sent / elapsed
                throughput = f" ({rate:.0f}/sec)"
            self._print(
                f"Messages: {self._messages_sent}{throughput}, "
                f"Files: {self._files_uploaded}, "
                f"Errors: {self._messages_failed}, "
                f"Channels: {self._channels_complete}"
            )

    def _print(self, message: str) -> None:
        """Write a timestamped line to the output stream."""
        elapsed = time.time() - self._start_time if self._start_time else 0
        minutes, seconds = divmod(int(elapsed), 60)
        prefix = "[DRY RUN] " if self._dry_run else ""
        self._output.write(f"[{minutes:02d}:{seconds:02d}] {prefix}{message}\n")
        self._output.flush()
        self._last_print = time.time()
