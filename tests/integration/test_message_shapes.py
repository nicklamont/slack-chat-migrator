"""Tests for realistic message shapes flowing through the dry-run pipeline.

These tests verify that rich_text blocks, threads, reactions, files,
and system subtypes are correctly handled when the full send pipeline
runs in dry-run mode.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_message_with_files,
    make_message_with_reactions,
    make_messages,
    make_migrator,
    make_rich_text_message,
    make_subtype_message,
    make_thread_messages,
)

pytestmark = pytest.mark.integration


class TestRichTextBlocks:
    """Messages with ``blocks`` containing ``rich_text`` are parsed and counted."""

    def test_rich_text_blocks_processed(self, tmp_path: Path) -> None:
        messages = [
            make_rich_text_message(user="U001", ts="1609459200.000000", text="Hello"),
            make_rich_text_message(user="U002", ts="1609459201.000000", text="World"),
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 2


class TestThreadMessages:
    """Thread parent + replies are all processed."""

    def test_thread_messages_counted(self, tmp_path: Path) -> None:
        messages = make_thread_messages(
            user="U001", parent_ts="1609459200.000000", reply_count=3
        )
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        summary = m.state.progress.migration_summary
        # 1 parent + 3 replies = 4
        assert summary["messages_created"] == 4


class TestReactions:
    """Reactions are counted in the summary."""

    def test_reactions_counted_in_summary(self, tmp_path: Path) -> None:
        messages = [
            make_message_with_reactions(
                user="U001",
                ts="1609459200.000000",
                reactions=[
                    {"name": "thumbsup", "users": ["U001", "U002"], "count": 2},
                    {"name": "heart", "users": ["U001"], "count": 1},
                ],
            ),
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 1
        # 2 users on thumbsup + 1 user on heart = 3 reactions
        assert summary["reactions_created"] == 3


class TestFiles:
    """File counts appear in the summary."""

    def test_files_counted_in_summary(self, tmp_path: Path) -> None:
        messages = [
            make_message_with_files(user="U001", ts="1609459200.000000", file_count=2),
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        summary = m.state.progress.migration_summary
        assert summary["messages_created"] == 1
        assert summary["files_created"] == 2


class TestSystemSubtypes:
    """System subtype messages are excluded from messages_created."""

    def test_system_subtypes_skipped(self, tmp_path: Path) -> None:
        messages = [
            make_messages(1, user="U001", start_ts=1609459200.0)[0],
            make_subtype_message(
                user="U001", ts="1609459201.000000", subtype="channel_join"
            ),
            make_subtype_message(
                user="U002", ts="1609459202.000000", subtype="channel_leave"
            ),
            make_messages(1, user="U002", start_ts=1609459203.0)[0],
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        summary = m.state.progress.migration_summary
        # 2 regular messages; channel_join + channel_leave are skipped
        assert summary["messages_created"] == 2


class TestChannelStats:
    """Per-channel stats are populated."""

    def test_channel_stats_populated(self, tmp_path: Path) -> None:
        messages = [
            make_messages(1, user="U001", start_ts=1609459200.0)[0],
            make_message_with_reactions(
                user="U001",
                ts="1609459201.000000",
                reactions=[{"name": "wave", "users": ["U001"], "count": 1}],
            ),
            make_message_with_files(user="U002", ts="1609459202.000000", file_count=1),
        ]
        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        stats = m.state.progress.channel_stats
        assert "general" in stats
        assert stats["general"]["message_count"] == 3
        assert stats["general"]["reaction_count"] == 1
        assert stats["general"]["file_count"] == 1


class TestMixedMessageTypes:
    """Export with all message types — totals are consistent."""

    def test_mixed_message_types(self, tmp_path: Path) -> None:
        messages = [
            # Regular message
            make_messages(1, user="U001", start_ts=1609459200.0)[0],
            # Rich text
            make_rich_text_message(user="U002", ts="1609459201.000000"),
            # System subtype (should be skipped)
            make_subtype_message(
                user="U001", ts="1609459202.000000", subtype="channel_join"
            ),
            # Message with reactions
            make_message_with_reactions(
                user="U001",
                ts="1609459203.000000",
                reactions=[{"name": "thumbsup", "users": ["U001"], "count": 1}],
            ),
            # Message with file
            make_message_with_files(user="U002", ts="1609459204.000000", file_count=1),
        ]
        # Thread messages (parent + 2 replies)
        thread_msgs = make_thread_messages(
            user="U001", parent_ts="1609459205.000000", reply_count=2
        )
        messages.extend(thread_msgs)

        export = build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": messages},
        )
        m = make_migrator(export)
        m.migrate()

        summary = m.state.progress.migration_summary
        # 1 regular + 1 rich_text + 1 reactions + 1 file + 1 thread parent + 2 replies = 7
        # channel_join is skipped
        assert summary["messages_created"] == 7
        assert summary["reactions_created"] == 1
        assert summary["files_created"] == 1
