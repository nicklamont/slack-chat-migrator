"""Inspect a Slack export directory for structure, stats, and issues.

Pure file I/O — no API calls. Reused by ``init`` and ``validate``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from slack_chat_migrator.utils.logging import log_with_context

logger = logging.getLogger(__name__)


class ExportInspector:
    """Read-only inspector for a Slack export directory.

    Args:
        export_path: Root of the Slack export (contains channels.json,
            users.json, and per-channel subdirectories).
    """

    def __init__(self, export_path: Path) -> None:
        self.export_path = export_path
        self._channels_data: list[dict[str, Any]] | None = None
        self._users_data: list[dict[str, Any]] | None = None

    # ------------------------------------------------------------------
    # Lazy loaders
    # ------------------------------------------------------------------

    def _load_channels(self) -> list[dict[str, Any]]:
        if self._channels_data is None:
            channels_file = self.export_path / "channels.json"
            if channels_file.exists():
                try:
                    with open(channels_file, encoding="utf-8") as f:
                        self._channels_data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    self._channels_data = []
            else:
                self._channels_data = []
        return self._channels_data

    def _load_users(self) -> list[dict[str, Any]]:
        if self._users_data is None:
            users_file = self.export_path / "users.json"
            if users_file.exists():
                try:
                    with open(users_file, encoding="utf-8") as f:
                        self._users_data = json.load(f)
                except (json.JSONDecodeError, OSError):
                    self._users_data = []
            else:
                self._users_data = []
        return self._users_data

    # ------------------------------------------------------------------
    # Public accessors
    # ------------------------------------------------------------------

    def get_channel_dirs(self) -> list[Path]:
        """Return sorted list of channel subdirectories.

        Filters out hidden directories (starting with ``.``) and common
        non-channel directories like ``__MACOSX``.
        """
        return sorted(
            d
            for d in self.export_path.iterdir()
            if d.is_dir() and not d.name.startswith((".", "__"))
        )

    def get_channel_count(self) -> int:
        """Number of channel subdirectories in the export."""
        return len(self.get_channel_dirs())

    def get_user_count(self) -> int:
        """Number of users in users.json."""
        return len(self._load_users())

    def get_message_counts(self) -> dict[str, int]:
        """Return ``{channel_name: message_count}`` for each channel dir."""
        counts: dict[str, int] = {}
        for ch_dir in self.get_channel_dirs():
            total = 0
            for jf in ch_dir.glob("*.json"):
                try:
                    with open(jf, encoding="utf-8") as f:
                        msgs = json.load(f)
                    total += sum(1 for m in msgs if m.get("type") == "message")
                except (OSError, ValueError):
                    log_with_context(
                        logging.WARNING,
                        f"Could not read {jf.name} in {ch_dir.name}",
                    )
            counts[ch_dir.name] = total
        return counts

    def get_total_message_count(self) -> int:
        """Total messages across all channels."""
        return sum(self.get_message_counts().values())

    def get_total_file_count(self) -> int:
        """Count file references across all messages."""
        total = 0
        for ch_dir in self.get_channel_dirs():
            for jf in ch_dir.glob("*.json"):
                try:
                    with open(jf, encoding="utf-8") as f:
                        msgs = json.load(f)
                    for m in msgs:
                        files = m.get("files", [])
                        if isinstance(files, list):
                            total += len(files)
                except (OSError, ValueError):
                    pass
        return total

    def get_export_date_range(self) -> tuple[str, str] | None:
        """Return (earliest_date, latest_date) from JSON filenames.

        Slack exports name message files like ``2024-01-15.json``.
        Returns None if no date-formatted files are found.
        """
        dates: list[str] = []
        for ch_dir in self.get_channel_dirs():
            for jf in ch_dir.glob("*.json"):
                stem = jf.stem
                # Quick check: YYYY-MM-DD format
                if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
                    dates.append(stem)
        if not dates:
            return None
        dates.sort()
        return dates[0], dates[-1]

    def get_users_without_email(self) -> list[dict[str, Any]]:
        """Return users that lack a profile email."""
        return [u for u in self._load_users() if not u.get("profile", {}).get("email")]

    def get_bot_users(self) -> list[dict[str, Any]]:
        """Return users flagged as bots."""
        return [
            u for u in self._load_users() if u.get("is_bot") or u.get("is_app_user")
        ]

    def get_structure_issues(self) -> list[str]:
        """Check export structure and return a list of human-readable issues."""
        issues: list[str] = []

        if not self.export_path.is_dir():
            issues.append(f"Export path is not a directory: {self.export_path}")
            return issues

        if not (self.export_path / "users.json").exists():
            issues.append("Missing users.json (required for user mapping)")

        if not (self.export_path / "channels.json").exists():
            issues.append("Missing channels.json (channel metadata)")

        channel_dirs = self.get_channel_dirs()
        if not channel_dirs:
            issues.append("No channel subdirectories found")

        for ch_dir in channel_dirs:
            json_files = list(ch_dir.glob("*.json"))
            if not json_files:
                issues.append(f"Channel '{ch_dir.name}' has no JSON message files")

        return issues
