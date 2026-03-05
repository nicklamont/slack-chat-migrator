"""Rich-based live progress renderer for TTY output."""

from __future__ import annotations

import time

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from slack_chat_migrator.core.progress import EventType, ProgressEvent, ProgressTracker


class RichProgressRenderer:
    """Renders migration progress using Rich Live display.

    Subscribes to a :class:`ProgressTracker` and maintains a Rich
    ``Live`` context with an overall progress bar, per-channel status,
    and a stats table.

    Usage::

        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)
        renderer.start()
        # ... run migration (tracker emits events) ...
        renderer.stop()
    """

    def __init__(
        self,
        tracker: ProgressTracker,
        console: Console | None = None,
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
        self._total_channels = 0

        # Current state
        self._current_phase = "Initializing"
        self._current_channel: str | None = None
        self._channel_progress: dict[str, tuple[int, int]] = {}  # ch -> (done, total)

        tracker.subscribe(self.handle_event)

    def start(self) -> None:
        """Begin the Rich Live display."""
        self._start_time = time.time()
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

    def _build_layout(self) -> Layout:
        """Construct the Rich layout from current state."""
        layout = Layout()
        layout.split_column(
            Layout(self._build_header_panel(), size=3),
            Layout(self._build_stats_table(), size=10),
        )
        return layout

    def _build_header_panel(self) -> Panel:
        """Build the header panel with phase and elapsed time."""
        elapsed = time.time() - self._start_time if self._start_time else 0
        minutes, seconds = divmod(int(elapsed), 60)
        hours, minutes = divmod(minutes, 60)
        time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        channel_info = (
            f" | Channel: {self._current_channel}" if self._current_channel else ""
        )
        progress_info = ""
        if self._total_channels > 0:
            progress_info = (
                f" | {self._channels_complete}/{self._total_channels} channels"
            )

        return Panel(
            f"[bold]{self._current_phase}[/bold]{channel_info}{progress_info}"
            f" | Elapsed: {time_str}",
            title="Slack Chat Migrator",
            border_style="blue",
        )

    def _build_stats_table(self) -> Table:
        """Build the stats summary table."""
        table = Table(title="Migration Progress", expand=True)
        table.add_column("Metric", style="cyan")
        table.add_column("Count", justify="right", style="green")
        table.add_row("Messages sent", str(self._messages_sent))
        table.add_row("Messages failed", str(self._messages_failed))
        table.add_row("Files uploaded", str(self._files_uploaded))
        table.add_row("Reactions added", str(self._reactions_added))
        table.add_row("Spaces created", str(self._spaces_created))
        table.add_row("Members added", str(self._members_added))
        table.add_row("Channels complete", str(self._channels_complete))
        return table

    # ------------------------------------------------------------------
    # Per-event-type handlers
    # ------------------------------------------------------------------

    def _on_channel_start(self, event: ProgressEvent) -> None:
        self._current_channel = event.channel
        if event.total is not None and event.channel:
            self._channel_progress[event.channel] = (0, event.total)

    def _on_channel_complete(self, event: ProgressEvent) -> None:
        self._channels_complete += 1
        if event.channel and event.channel in self._channel_progress:
            del self._channel_progress[event.channel]

    def _on_message_sent(self, event: ProgressEvent) -> None:
        self._messages_sent += 1
        if event.channel and event.channel in self._channel_progress:
            done, total = self._channel_progress[event.channel]
            self._channel_progress[event.channel] = (done + 1, total)

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
