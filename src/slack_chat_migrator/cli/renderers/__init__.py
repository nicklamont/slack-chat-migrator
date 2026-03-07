"""CLI renderers for migration progress output."""

from __future__ import annotations

import sys
from typing import Union

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

from slack_chat_migrator.cli.renderers.plain_renderer import PlainProgressRenderer
from slack_chat_migrator.cli.renderers.rich_renderer import RichProgressRenderer
from slack_chat_migrator.core.progress import ProgressTracker

ProgressRenderer = Union[RichProgressRenderer, PlainProgressRenderer]


def create_renderer(
    tracker: ProgressTracker,
    total_channels: int = 0,
    dry_run: bool = False,
) -> ProgressRenderer:
    """Create the appropriate renderer based on terminal capabilities.

    Returns a :class:`RichProgressRenderer` when stdout is a TTY,
    otherwise a :class:`PlainProgressRenderer`.
    """
    if sys.stdout.isatty():
        return RichProgressRenderer(
            tracker, total_channels=total_channels, dry_run=dry_run
        )
    return PlainProgressRenderer(tracker, dry_run=dry_run)


# ------------------------------------------------------------------
# Shared Rich helpers for consistent CLI output across commands
# ------------------------------------------------------------------

_console: Console | None = None


def get_console() -> Console:
    """Return a shared Console singleton for CLI output."""
    global _console
    if _console is None:
        _console = Console()
    return _console


def success_panel(title: str, body: str) -> Panel:
    """Green-bordered panel for success messages."""
    return Panel(body, title=title, border_style="green")


def warning_panel(title: str, body: str) -> Panel:
    """Yellow-bordered panel for warning messages."""
    return Panel(body, title=title, border_style="yellow")


def error_panel(title: str, body: str) -> Panel:
    """Red-bordered panel for error messages."""
    return Panel(body, title=title, border_style="red")


def next_step_panel(command: str) -> Panel:
    """Hint panel showing the next command to run."""
    return Panel(
        Text.from_markup(f"  [bold]{command}[/bold]"),
        title="Next step",
        border_style="cyan",
    )
