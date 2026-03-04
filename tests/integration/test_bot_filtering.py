"""Tests for bot message filtering in the dry-run pipeline.

Uses ``ignore_bots: true`` config to verify that bot messages and
bot reactions are properly excluded from migration counts.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_message_with_reactions,
    make_messages,
    make_migrator,
)

pytestmark = pytest.mark.integration

_BOT_FILTERING_CONFIG = """\
exclude_channels: []
include_channels: []
ignore_bots: true
"""


class TestBotSubtypeFiltered:
    """Messages with ``subtype: "bot_message"`` are excluded."""

    def test_bot_subtype_filtered(self, tmp_path: Path) -> None:
        messages = [
            make_messages(1, user="U001", start_ts=1609459200.0)[0],
            {
                "type": "message",
                "subtype": "bot_message",
                "user": "B001",
                "text": "Bot says hello",
                "ts": "1609459201.000000",
                "username": "testbot",
            },
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export, config_text=_BOT_FILTERING_CONFIG)
        m.migrate()

        summary = m.state.progress.migration_summary
        # Only the regular message is counted; bot_message is filtered
        assert summary["messages_created"] == 1


class TestBotUserFiltered:
    """Messages from a user with ``is_bot: True`` are excluded."""

    def test_bot_user_filtered(self, tmp_path: Path) -> None:
        messages = [
            make_messages(1, user="U001", start_ts=1609459200.0)[0],
            {
                "type": "message",
                "user": "B001",
                "text": "I am a bot user",
                "ts": "1609459201.000000",
            },
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export, config_text=_BOT_FILTERING_CONFIG)
        m.migrate()

        summary = m.state.progress.migration_summary
        # B001 is is_bot: True in USERS; filtered when ignore_bots is enabled
        assert summary["messages_created"] == 1


class TestNonBotMessagesStillCounted:
    """Regular messages still pass through when bot filtering is enabled."""

    def test_non_bot_messages_still_counted(self, tmp_path: Path) -> None:
        messages = make_messages(3, user="U001")
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export, config_text=_BOT_FILTERING_CONFIG)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 3


class TestBotReactionsExcluded:
    """Bot reactions are excluded from ``reactions_created``."""

    def test_bot_reactions_excluded(self, tmp_path: Path) -> None:
        messages = [
            make_message_with_reactions(
                user="U001",
                ts="1609459200.000000",
                reactions=[
                    # U001 (human) + B001 (bot) react; only U001 should count
                    {"name": "thumbsup", "users": ["U001", "B001"], "count": 2},
                ],
            ),
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export, config_text=_BOT_FILTERING_CONFIG)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 1
        # B001 is is_bot: True, so only U001's reaction counts
        assert summary["reactions_created"] == 1
