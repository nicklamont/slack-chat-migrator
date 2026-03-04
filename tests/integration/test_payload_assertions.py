"""Payload assertion tests for the dry-run migration pipeline.

Uses ``migrator._dry_run_chat_service.captured_messages`` to inspect
the exact payloads that ``ChatAdapter.create_message`` would send to
the real Google Chat API.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_messages,
    make_migrator,
    make_rich_text_message,
    make_thread_messages,
)

pytestmark = pytest.mark.integration


def _captured(tmp_path: Path, **kwargs: Any) -> list[dict[str, Any]]:
    """Run a migration and return the captured message payloads."""
    m = make_migrator(tmp_path, **kwargs)
    m.migrate()
    assert m._dry_run_chat_service is not None
    return m._dry_run_chat_service.captured_messages


class TestSimpleMessagePayload:
    """A plain text message produces the expected payload structure."""

    def test_simple_message_payload_structure(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(1)},
        )
        calls = _captured(tmp_path)
        assert len(calls) == 1

        body = calls[0]["body"]
        assert "createTime" in body
        assert "text" in body
        assert "sender" in body
        assert "name" in body["sender"]
        assert "thread" in body
        # Non-reply messages use thread_key
        assert "thread_key" in body["thread"]
        # No attachment on a plain text message
        assert "attachment" not in body


class TestRichTextPayload:
    """Rich text blocks produce formatted text with bold and link markers."""

    def test_rich_text_produces_formatted_text(self, tmp_path: Path) -> None:
        msg = make_rich_text_message(
            user="U001",
            ts="1609459200.000000",
            text="Hello world",
        )
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": [msg]},
        )
        calls = _captured(tmp_path)
        assert len(calls) == 1

        text = calls[0]["body"]["text"]
        # Bold formatting wraps the "bold part" text
        assert "*bold part*" in text
        # Link from the rich_text_section
        assert "https://example.com" in text


class TestThreadReplyPayload:
    """Thread replies reference the parent via ``thread.name`` (not ``thread_key``)."""

    def test_thread_reply_references_parent(self, tmp_path: Path) -> None:
        thread_msgs = make_thread_messages(
            user="U001",
            parent_ts="1609459200.000000",
            reply_count=1,
        )
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": thread_msgs},
        )
        calls = _captured(tmp_path)
        assert len(calls) == 2  # parent + 1 reply

        # Parent uses thread_key
        parent_body = calls[0]["body"]
        assert "thread_key" in parent_body["thread"]

        # Reply uses thread.name (set after parent was created)
        reply_body = calls[1]["body"]
        assert "name" in reply_body["thread"]
        assert calls[1]["messageReplyOption"] == "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD"


class TestSenderResolution:
    """Sender email matches the user map."""

    def test_sender_uses_correct_email(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(1, user="U001")},
        )
        calls = _captured(tmp_path)
        body = calls[0]["body"]
        assert body["sender"]["name"] == "users/alice@example.com"


class TestCreateTimeMapping:
    """``createTime`` is the correct RFC 3339 for the Slack ts."""

    def test_create_time_matches_slack_ts(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(1)},
        )
        calls = _captured(tmp_path)
        body = calls[0]["body"]
        # ts=1609459200.000000 → 2021-01-01T00:00:00.000000Z
        assert body["createTime"] == "2021-01-01T00:00:00.000000Z"


class TestEditedMessage:
    """Edited messages include an ``_(edited at ...)_`` indicator."""

    def test_edited_message_has_indicator(self, tmp_path: Path) -> None:
        edited_msg = {
            "type": "message",
            "user": "U001",
            "text": "Updated text",
            "ts": "1609459200.000000",
            "edited": {"user": "U001", "ts": "1609459300.000000"},
        }
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": [edited_msg]},
        )
        calls = _captured(tmp_path)
        text = calls[0]["body"]["text"]
        assert "_(edited at " in text


class TestUnmappedUserFallback:
    """Unmapped user IDs fall back to the admin sender."""

    def test_unmapped_user_uses_admin_sender(self, tmp_path: Path) -> None:
        unmapped_msg = {
            "type": "message",
            "user": "U_UNKNOWN",
            "text": "ghost message",
            "ts": "1609459200.000000",
        }
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": [unmapped_msg]},
        )
        calls = _captured(tmp_path)
        body = calls[0]["body"]
        assert body["sender"]["name"] == "users/admin@example.com"


class TestMessageOrdering:
    """Captured payloads arrive in ascending ``createTime`` order."""

    def test_messages_arrive_in_ts_order(self, tmp_path: Path) -> None:
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": make_messages(5)},
        )
        calls = _captured(tmp_path)
        create_times = [c["body"]["createTime"] for c in calls]
        assert create_times == sorted(create_times)
