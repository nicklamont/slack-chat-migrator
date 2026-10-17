"""Tests for reaction emoji conversion (Slack name to Chat ``emoji.unicode``)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.core.context import MigrationContext
from slack_chat_migrator.core.state import MigrationState, _default_migration_summary
from slack_chat_migrator.services.messages.reaction_processor import (
    convert_reaction_emoji,
    process_reactions_batch,
)


def _ctx(dry_run: bool, custom_emoji_map: dict[str, str] | None = None) -> Any:
    return MigrationContext(
        export_root=Path("/fake/export"),
        creds_path="/fake/creds.json",
        workspace_admin="admin@example.com",
        workspace_domain="example.com",
        dry_run=dry_run,
        update_mode=False,
        verbose=False,
        debug_api=False,
        config=MigrationConfig(custom_emoji_map=custom_emoji_map or {}),
        user_map={"U001": "user1@example.com"},
        users_without_email=[],
        bot_user_ids=frozenset(),
        channels_meta={},
        channel_id_to_name={},
        channel_name_to_id={},
    )


def _state() -> MigrationState:
    state = MigrationState()
    state.context.current_channel = "general"
    state.progress.migration_summary = _default_migration_summary()
    return state


def _resolver(delegate: MagicMock) -> MagicMock:
    resolver = MagicMock()
    resolver.get_internal_email.side_effect = lambda uid, email: email
    resolver.is_external_user.return_value = False
    resolver.get_delegate.return_value = delegate
    return resolver


class TestConvertReactionEmoji:
    """convert_reaction_emoji() returns valid Unicode or None."""

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("thumbsup::skin-tone-2", "\U0001f44d\U0001f3fb"),
            ("+1::skin-tone-5", "\U0001f44d\U0001f3fe"),
            ("man-shrugging", "\U0001f937\u200d\u2642\ufe0f"),
            ("flag-us", "\U0001f1fa\U0001f1f8"),
            ("simple_smile", "\U0001f642"),
            ("white_check_mark", "\u2705"),
        ],
    )
    def test_valid_unicode(self, name: str, expected: str) -> None:
        assert convert_reaction_emoji(name) == expected

    @pytest.mark.parametrize("name", ["partyparrot", "squirrel", "nope-not-real"])
    def test_custom_or_unknown_is_none(self, name: str) -> None:
        assert convert_reaction_emoji(name) is None

    def test_custom_emoji_map(self) -> None:
        mapping = {"partyparrot": "\U0001f99c"}
        assert convert_reaction_emoji("partyparrot", mapping) == "\U0001f99c"


class TestReactionsUseConvertedEmoji:
    """process_reactions_batch() sends converted emoji and skips custom ones."""

    def test_skin_tone_sent_as_valid_unicode(self) -> None:
        delegate = MagicMock()
        batch = MagicMock()
        delegate.new_batch_http_request.return_value = batch
        process_reactions_batch(
            _ctx(dry_run=False),
            _state(),
            MagicMock(),
            _resolver(delegate),
            "spaces/S/messages/M",
            [{"name": "thumbsup::skin-tone-2", "users": ["U001"]}],
            "M",
        )
        body = delegate.build_create_reaction_request.call_args.kwargs["body"]
        assert body == {"emoji": {"unicode": "\U0001f44d\U0001f3fb"}}

    def test_custom_emoji_is_not_sent_or_counted(self) -> None:
        delegate = MagicMock()
        state = _state()
        process_reactions_batch(
            _ctx(dry_run=False),
            state,
            MagicMock(),
            _resolver(delegate),
            "spaces/S/messages/M",
            [{"name": "partyparrot", "users": ["U001"]}],
            "M",
        )
        delegate.build_create_reaction_request.assert_not_called()
        delegate.create_reaction.assert_not_called()
        assert state.progress.migration_summary["reactions_created"] == 0

    def test_mapped_custom_emoji_is_sent(self) -> None:
        delegate = MagicMock()
        process_reactions_batch(
            _ctx(dry_run=False, custom_emoji_map={"partyparrot": ":tada:"}),
            _state(),
            MagicMock(),
            _resolver(delegate),
            "spaces/S/messages/M",
            [{"name": "partyparrot", "users": ["U001"]}],
            "M",
        )
        body = delegate.build_create_reaction_request.call_args.kwargs["body"]
        assert body == {"emoji": {"unicode": "\U0001f389"}}
