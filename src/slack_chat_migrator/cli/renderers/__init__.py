"""CLI renderers for migration progress output."""

from __future__ import annotations

import sys
from typing import Union

from slack_chat_migrator.cli.renderers.plain_renderer import PlainProgressRenderer
from slack_chat_migrator.cli.renderers.rich_renderer import RichProgressRenderer
from slack_chat_migrator.core.progress import ProgressTracker

ProgressRenderer = Union[RichProgressRenderer, PlainProgressRenderer]


def create_renderer(tracker: ProgressTracker) -> ProgressRenderer:
    """Create the appropriate renderer based on terminal capabilities.

    Returns a :class:`RichProgressRenderer` when stdout is a TTY,
    otherwise a :class:`PlainProgressRenderer`.
    """
    if sys.stdout.isatty():
        return RichProgressRenderer(tracker)
    return PlainProgressRenderer(tracker)
