"""Integration test fixtures: export builders and migrator factory.

These tests exercise the full migration pipeline in dry-run mode (no
Google API credentials needed).  The ``DryRunChatService`` and
``DryRunDriveService`` are injected automatically when ``dry_run=True``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from slack_chat_migrator.core.migrator import SlackToChatMigrator

# ---------------------------------------------------------------------------
# Low-level builders
# ---------------------------------------------------------------------------


def build_export(
    tmp_path: Path,
    users: list[dict[str, Any]],
    channels: list[dict[str, Any]],
    messages_by_channel: dict[str, list[dict[str, Any]]] | None = None,
) -> Path:
    """Write a complete Slack export directory and return its path.

    Parameters
    ----------
    tmp_path:
        Root directory for the export.
    users:
        List of Slack user dicts (written to ``users.json``).
    channels:
        List of Slack channel dicts (written to ``channels.json``).
    messages_by_channel:
        Mapping of ``channel_name -> [message_dicts]``.  Messages are
        grouped by date (derived from ``ts``) into daily JSON files,
        matching the real Slack export layout.
    """
    (tmp_path / "users.json").write_text(json.dumps(users))
    (tmp_path / "channels.json").write_text(json.dumps(channels))

    for ch in channels:
        ch_dir = tmp_path / ch["name"]
        ch_dir.mkdir(exist_ok=True)

    if messages_by_channel:
        for channel_name, messages in messages_by_channel.items():
            write_channel_messages(tmp_path, channel_name, messages)

    return tmp_path


def write_channel_messages(
    export_path: Path,
    channel_name: str,
    messages: list[dict[str, Any]],
) -> None:
    """Write messages into per-day JSON files inside a channel directory.

    Slack exports group messages by date — ``2021-01-01.json`` etc.
    This helper derives the date from each message's ``ts`` field.
    """
    ch_dir = export_path / channel_name
    ch_dir.mkdir(exist_ok=True)

    by_date: dict[str, list[dict[str, Any]]] = {}
    for msg in messages:
        ts = float(msg.get("ts", "0"))
        date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        by_date.setdefault(date_str, []).append(msg)

    for date_str, day_messages in by_date.items():
        (ch_dir / f"{date_str}.json").write_text(json.dumps(day_messages))


# ---------------------------------------------------------------------------
# Migrator factory
# ---------------------------------------------------------------------------

# Minimal config YAML that disables nothing and has no overrides.
_MINIMAL_CONFIG = """\
exclude_channels: []
include_channels: []
"""


def make_migrator(
    export_path: Path,
    *,
    admin_email: str = "admin@example.com",
    config_text: str = _MINIMAL_CONFIG,
) -> SlackToChatMigrator:
    """Create a ``SlackToChatMigrator`` in dry-run mode, ready to ``migrate()``.

    The migrator is fully initialised — API services (dry-run stubs) are
    **not** yet wired because ``migrate()`` does that lazily.  The output
    directory is placed *outside* the export root (as a sibling) so the
    migrator doesn't mistake it for a channel directory.
    """
    config_path = export_path / "config.yaml"
    config_path.write_text(config_text)

    m = SlackToChatMigrator(
        creds_path="fake_creds.json",
        export_path=str(export_path),
        workspace_admin=admin_email,
        config_path=str(config_path),
        dry_run=True,
    )

    # Place output directory as a sibling of the export root so it doesn't
    # get picked up as a channel directory by migrate().
    output_dir = export_path.parent / "migration_output"
    output_dir.mkdir(exist_ok=True)
    (output_dir / "channel_logs").mkdir(exist_ok=True)
    m.state.context.output_dir = str(output_dir)

    return m


# ---------------------------------------------------------------------------
# Reusable user / channel data
# ---------------------------------------------------------------------------

USERS = [
    {
        "id": "U001",
        "name": "alice",
        "real_name": "Alice Smith",
        "profile": {"email": "alice@example.com", "real_name": "Alice Smith"},
        "is_bot": False,
        "deleted": False,
    },
    {
        "id": "U002",
        "name": "bob",
        "real_name": "Bob Jones",
        "profile": {"email": "bob@example.com", "real_name": "Bob Jones"},
        "is_bot": False,
        "deleted": False,
    },
    {
        "id": "B001",
        "name": "testbot",
        "real_name": "Test Bot",
        "profile": {},
        "is_bot": True,
        "deleted": False,
    },
]

GENERAL_CHANNEL = {
    "id": "C001",
    "name": "general",
    "members": ["U001", "U002"],
    "purpose": {"value": "General discussion"},
    "topic": {"value": "Welcome"},
}

RANDOM_CHANNEL = {
    "id": "C002",
    "name": "random",
    "members": ["U001"],
    "purpose": {"value": "Random stuff"},
    "topic": {"value": ""},
}


def make_messages(
    count: int,
    user: str = "U001",
    start_ts: float = 1609459200.0,
) -> list[dict[str, Any]]:
    """Generate *count* simple text messages starting at *start_ts*."""
    return [
        {
            "type": "message",
            "user": user,
            "text": f"Message {i + 1}",
            "ts": f"{start_ts + i:.6f}",
        }
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# Composite fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def single_channel_export(tmp_path: Path) -> Path:
    """Export with one channel (``general``) containing 3 messages."""
    return build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL],
        messages_by_channel={"general": make_messages(3)},
    )


@pytest.fixture()
def multi_channel_export(tmp_path: Path) -> Path:
    """Export with two channels, each containing messages."""
    return build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
        messages_by_channel={
            "general": make_messages(3, user="U001"),
            "random": make_messages(2, user="U002"),
        },
    )


@pytest.fixture()
def empty_channel_export(tmp_path: Path) -> Path:
    """Export with one channel that has zero messages (directory exists but empty)."""
    return build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL],
        messages_by_channel={},
    )


@pytest.fixture()
def bot_messages_export(tmp_path: Path) -> Path:
    """Export where all messages come from a bot user."""
    bot_messages = [
        {
            "type": "message",
            "user": "B001",
            "text": "Bot message 1",
            "ts": "1609459200.000000",
        },
        {
            "type": "message",
            "user": "B001",
            "text": "Bot message 2",
            "ts": "1609459201.000000",
        },
    ]
    return build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL],
        messages_by_channel={"general": bot_messages},
    )
