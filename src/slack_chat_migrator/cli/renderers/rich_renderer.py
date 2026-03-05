"""Rich-based live progress renderer for TTY output."""

from __future__ import annotations

import logging
import time

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TaskID, TextColumn
from rich.table import Table
from rich.text import Text

from slack_chat_migrator.core.progress import (
    EventType,
    ProgressEvent,
    ProgressTracker,
)


class RichProgressRenderer:
    """Renders migration progress using Rich Live display.

    Subscribes to a :class:`ProgressTracker` and maintains a Rich
    ``Live`` context with progress bars and a stats table.

    Usage::

        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, total_channels=47)
        renderer.start()
        # ... run migration (tracker emits events) ...
        renderer.stop()
    """

    def __init__(
        self,
        tracker: ProgressTracker,
        console: Console | None = None,
        total_channels: int = 0,
    ) -> None:
        self._tracker = tracker
        self._console = console or Console()
        self._live: Live | None = None
        self._start_time: float = 0.0

        # Stats counters
        self._messages_sent = 0
        self._messages_failed = 0
        self._files_uploaded = 0
        self._reactions_added = 0
        self._spaces_created = 0
        self._members_added = 0
        self._channels_complete = 0
        self._total_channels = total_channels

        # Current state
        self._current_phase = "Initializing"
        self._current_channel: str | None = None
        self._channel_msg_done = 0
        self._channel_msg_total = 0

        self._saved_console_level: int | None = None

        # Rich Progress bars (embedded in layout, NOT standalone)
        self._progress = Progress(
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("{task.percentage:>3.0f}%"),
            TextColumn("{task.completed}/{task.total}"),
            console=self._console,
        )
        self._overall_task: TaskID | None = None
        self._message_task: TaskID | None = None

        tracker.subscribe(self.handle_event)

    def start(self) -> None:
        """Begin the Rich Live display."""
        self._suppress_console_logging()
        self._start_time = time.time()

        # Create overall channel progress bar
        if self._total_channels > 0:
            self._overall_task = self._progress.add_task(
                "Overall", total=self._total_channels
            )

        self._live = Live(
            self._build_layout(),
            console=self._console,
            refresh_per_second=4,
        )
        self._live.start()

    def stop(self) -> None:
        """Stop the Rich Live display."""
        if self._live is not None:
            self._live.stop()
            self._live = None
        self._restore_console_logging()

    def _suppress_console_logging(self) -> None:
        """Raise the console handler level so log lines don't clash with Live."""
        logger = logging.getLogger("slack_chat_migrator")
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, logging.FileHandler
            ):
                self._saved_console_level = handler.level
                handler.setLevel(logging.CRITICAL + 1)
                break

    def _restore_console_logging(self) -> None:
        """Restore the console handler to its original level."""
        if self._saved_console_level is None:
            return
        logger = logging.getLogger("slack_chat_migrator")
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, logging.FileHandler
            ):
                handler.setLevel(self._saved_console_level)
                break
        self._saved_console_level = None

    def handle_event(self, event: ProgressEvent) -> None:
        """Process a progress event and update the display."""
        handler = _EVENT_HANDLERS.get(event.event_type)
        if handler:
            handler(self, event)
        self._refresh()

    def _refresh(self) -> None:
        """Update the Live display with current state."""
        if self._live is not None:
            self._live.update(self._build_layout())

    def _elapsed_str(self) -> str:
        """Format the elapsed time as HH:MM:SS or MM:SS."""
        elapsed = time.time() - self._start_time if self._start_time else 0
        minutes, seconds = divmod(int(elapsed), 60)
        hours, minutes = divmod(minutes, 60)
        if hours:
            return f"{hours}h{minutes:02d}m{seconds:02d}s"
        return f"{minutes}m{seconds:02d}s"

    def _build_layout(self) -> Layout:
        """Construct the Rich layout from current state."""
        layout = Layout()
        layout.split_column(
            Layout(self._build_header_panel(), name="header", size=3),
            Layout(self._build_progress_section(), name="progress", size=7),
            Layout(self._build_stats_table(), name="stats", size=10),
        )
        return layout

    def _build_header_panel(self) -> Panel:
        """Build the header panel showing phase name."""
        return Panel(
            Text(self._current_phase, style="bold"),
            title="Migrating Slack \u2192 Google Chat",
            subtitle=f"Elapsed: {self._elapsed_str()}",
            border_style="blue",
        )

    def _build_progress_section(self) -> Panel:
        """Build the progress bars section."""
        channel_label = (
            f"Current: #{self._current_channel}" if self._current_channel else ""
        )
        return Panel(
            Group(
                self._progress,
                Text(channel_label, style="dim"),
            ),
            border_style="dim",
        )

    def _build_stats_table(self) -> Table:
        """Build the stats summary table."""
        table = Table(expand=True, show_header=False, box=None, padding=(0, 2))
        table.add_column("Metric", style="cyan", min_width=18)
        table.add_column("Count", justify="right", style="green", min_width=10)

        table.add_row("Spaces created", f"{self._spaces_created:,}")
        table.add_row("Messages sent", f"{self._messages_sent:,}")
        if self._messages_failed > 0:
            table.add_row(
                Text("Messages failed", style="red"),
                Text(f"{self._messages_failed:,}", style="red"),
            )
        table.add_row("Files uploaded", f"{self._files_uploaded:,}")
        table.add_row("Reactions added", f"{self._reactions_added:,}")
        table.add_row("Members added", f"{self._members_added:,}")
        if self._channels_complete > 0 or self._total_channels > 0:
            table.add_row(
                "Channels complete",
                f"{self._channels_complete}/{self._total_channels}",
            )

        return table

    # ------------------------------------------------------------------
    # Per-event-type handlers
    # ------------------------------------------------------------------

    def _on_channel_start(self, event: ProgressEvent) -> None:
        self._current_channel = event.channel
        self._channel_msg_done = 0
        self._channel_msg_total = event.total or 0

        # Reset or create message progress bar for this channel
        if self._message_task is not None:
            self._progress.remove_task(self._message_task)
        if self._channel_msg_total > 0:
            self._message_task = self._progress.add_task(
                "Messages", total=self._channel_msg_total
            )
        else:
            self._message_task = None

    def _on_channel_complete(self, event: ProgressEvent) -> None:
        self._channels_complete += 1
        if self._overall_task is not None:
            self._progress.update(self._overall_task, completed=self._channels_complete)
        # Remove message bar for completed channel
        if self._message_task is not None:
            self._progress.remove_task(self._message_task)
            self._message_task = None

    def _on_message_sent(self, event: ProgressEvent) -> None:
        self._messages_sent += 1
        self._channel_msg_done += 1
        if self._message_task is not None:
            self._progress.update(self._message_task, completed=self._channel_msg_done)

    def _on_message_failed(self, event: ProgressEvent) -> None:
        self._messages_failed += 1

    def _on_file_uploaded(self, event: ProgressEvent) -> None:
        self._files_uploaded += 1

    def _on_reaction_added(self, event: ProgressEvent) -> None:
        self._reactions_added += 1

    def _on_space_created(self, event: ProgressEvent) -> None:
        self._spaces_created += 1

    def _on_member_added(self, event: ProgressEvent) -> None:
        self._members_added += 1

    def _on_phase_change(self, event: ProgressEvent) -> None:
        self._current_phase = event.detail or "Unknown"


# Dispatch table mapping event types to handler methods.
_EVENT_HANDLERS = {
    EventType.CHANNEL_START: RichProgressRenderer._on_channel_start,
    EventType.CHANNEL_COMPLETE: RichProgressRenderer._on_channel_complete,
    EventType.MESSAGE_SENT: RichProgressRenderer._on_message_sent,
    EventType.MESSAGE_FAILED: RichProgressRenderer._on_message_failed,
    EventType.FILE_UPLOADED: RichProgressRenderer._on_file_uploaded,
    EventType.REACTION_ADDED: RichProgressRenderer._on_reaction_added,
    EventType.SPACE_CREATED: RichProgressRenderer._on_space_created,
    EventType.MEMBER_ADDED: RichProgressRenderer._on_member_added,
    EventType.PHASE_CHANGE: RichProgressRenderer._on_phase_change,
}
