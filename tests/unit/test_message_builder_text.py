"""Tests for the message text that build_message_payload() produces."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.core.context import MigrationContext
from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.services.messages.message_builder import (
    build_message_payload,
)

USER_MAP = {"U001": "alice@example.com"}


def _ctx(**config: Any) -> MigrationContext:
    return MigrationContext(
        export_root=Path("/fake/export"),
        creds_path=None,
        workspace_admin="admin@example.com",
        workspace_domain="example.com",
        dry_run=True,
        update_mode=False,
        verbose=False,
        debug_api=False,
        config=MigrationConfig(**config),
        user_map=USER_MAP,
        users_without_email=[],
        bot_user_ids=frozenset(),
        channels_meta={},
        channel_id_to_name={"C0123": "general"},
        channel_name_to_id={"general": "C0123"},
    )


def _payload_text(
    message: dict[str, Any],
    ctx: MigrationContext | None = None,
    is_edited: bool = False,
    edited_ts: str = "",
) -> str:
    """Build a payload for *message* and return its text."""
    resolver = MagicMock()
    resolver.get_internal_email.side_effect = lambda uid, email: email
    resolver.is_external_user.return_value = False
    state = MigrationState()
    state.context.current_channel = "general"
    payload, _, _, _ = build_message_payload(
        ctx or _ctx(),
        state,
        resolver,
        message,
        message["ts"],
        message.get("user", ""),
        None,
        "general",
        is_edited,
        edited_ts,
        USER_MAP,
    )
    return str(payload["text"])


class TestChannelReferences:
    """Channel tokens without a label are resolved from channels.json."""

    def test_unlabelled_channel_tokens_use_channel_names(self) -> None:
        text = _payload_text(
            {
                "ts": "1700000000.000100",
                "user": "U001",
                "text": "see <#C0123|> and <#C0123> or <#C9999|>",
            }
        )
        assert text == "see #general and #general or #C9999"


class TestEmojiInPayloadText:
    """Emoji shortcodes use config.custom_emoji_map and skip code."""

    def test_custom_emoji_map_is_used(self) -> None:
        ctx = _ctx(custom_emoji_map={"partyparrot": "\U0001f99c"})
        text = _payload_text(
            {
                "ts": "1700000000.000100",
                "user": "U001",
                "text": ":partyparrot: `key:ok:value` :tada:",
            },
            ctx,
        )
        assert text == "\U0001f99c `key:ok:value` \U0001f389"


class TestEditMarker:
    """The edit marker is in UTC, whatever the host's time zone."""

    @pytest.mark.skipif(not hasattr(time, "tzset"), reason="needs time.tzset()")
    @pytest.mark.parametrize("zone", ["UTC", "America/Los_Angeles", "Asia/Tokyo"])
    def test_edit_time_is_utc(self, monkeypatch: pytest.MonkeyPatch, zone: str) -> None:
        monkeypatch.setenv("TZ", zone)
        time.tzset()
        try:
            text = _payload_text(
                {"ts": "1699999000.000100", "user": "U001", "text": "fixed"},
                is_edited=True,
                edited_ts="1700000000.000000",
            )
        finally:
            monkeypatch.undo()
            time.tzset()
        assert text == "fixed\n\n_(edited at 2023-11-14 22:13:20 UTC)_"

    def test_invalid_edit_ts_is_shown_raw(self) -> None:
        text = _payload_text(
            {"ts": "1699999000.000100", "user": "U001", "text": "fixed"},
            is_edited=True,
            edited_ts="garbage",
        )
        assert text == "fixed\n\n_(edited at garbage)_"


class TestAttachmentText:
    """Legacy attachments reach the payload text."""

    def test_attachment_only_bot_post(self) -> None:
        text = _payload_text(
            {
                "ts": "1700000000.000100",
                "subtype": "bot_message",
                "bot_id": "B1",
                "text": "",
                "attachments": [{"title": "CPU high", "text": "host-1 at 99%"}],
            }
        )
        assert "*CPU high*\nhost-1 at 99%" in text
