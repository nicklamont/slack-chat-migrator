"""Tests for the message text that build_message_payload() produces."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

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
