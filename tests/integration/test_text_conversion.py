"""End-to-end text conversion through the dry-run migration pipeline.

The messages below follow the shape of real Slack export day files
(``client_msg_id``, ``team``, ``user_profile``, ``blocks`` with
``block_id``), so the whole path is exercised: block parsing, mrkdwn
token conversion, emoji, attachments and the payload sent to Chat.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    RANDOM_CHANNEL,
    USERS,
    build_export,
    make_migrator,
)

pytestmark = pytest.mark.integration

ZWSP = "\N{ZERO WIDTH SPACE}"


def _msg(ts: str, text: str, blocks: list[dict[str, Any]] | None = None, **extra: Any):
    """A user message in Slack export shape."""
    message: dict[str, Any] = {
        "client_msg_id": f"00000000-0000-0000-0000-{ts.replace('.', '')[-12:]}",
        "type": "message",
        "text": text,
        "user": "U001",
        "ts": ts,
        "team": "T0001",
        "user_profile": {"real_name": "Alice Smith", "display_name": "alice"},
    }
    if blocks is not None:
        message["blocks"] = blocks
    message.update(extra)
    return message


def _rich(*elements: dict[str, Any]) -> list[dict[str, Any]]:
    return [{"type": "rich_text", "block_id": "Ab1Cd", "elements": list(elements)}]


def _section(*elements: dict[str, Any]) -> dict[str, Any]:
    return {"type": "rich_text_section", "elements": list(elements)}


def _texts(tmp_path: Path, messages: list[dict[str, Any]]) -> dict[str, str]:
    """Run a dry-run migration and return payload text by Slack ts."""
    build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
        messages_by_channel={"general": messages, "random": []},
    )
    migrator = make_migrator(tmp_path)
    migrator.migrate()
    assert migrator._dry_run_chat_service is not None
    by_ts = {m["ts"]: m for m in messages}
    texts: dict[str, str] = {}
    for call in migrator._dry_run_chat_service.captured_messages:
        body = call["body"]
        thread_key = body.get("thread", {}).get("thread_key")
        if thread_key in by_ts:
            texts[thread_key] = body["text"]
    return texts


class TestRealisticMessages:
    """Modern rich-text messages keep every word, with Chat markup."""

    def test_mentions_channels_links_and_emoji(self, tmp_path: Path) -> None:
        ts = "1700000000.000100"
        message = _msg(
            ts,
            "<!here> deploy blocked by <#C002|> - see "
            "<https://ci.example/run/42?x=1&amp;y=2|run 42> :rotating_light: "
            "cc <@U002> <!subteam^S0123|@oncall>",
            _rich(
                _section(
                    {"type": "broadcast", "range": "here"},
                    {"type": "text", "text": " deploy blocked by "},
                    {"type": "channel", "channel_id": "C002"},
                    {"type": "text", "text": " - see "},
                    {
                        "type": "link",
                        "url": "https://ci.example/run/42?x=1&y=2",
                        "text": "run 42",
                    },
                    {"type": "text", "text": " "},
                    {"type": "emoji", "name": "rotating_light", "unicode": "1f6a8"},
                    {"type": "text", "text": " cc "},
                    {"type": "user", "user_id": "U002"},
                    {"type": "text", "text": " "},
                    {"type": "usergroup", "usergroup_id": "S0123"},
                )
            ),
        )
        texts = _texts(tmp_path, [message])
        assert texts[ts] == (
            "<users/all> deploy blocked by #random - see "
            "<https://ci.example/run/42?x=1&y=2|run 42> \N{POLICE CARS REVOLVING LIGHT} "
            "cc <users/bob@example.com> @oncall"
        )

    def test_code_is_untouched(self, tmp_path: Path) -> None:
        ts = "1700000001.000100"
        message = _msg(
            ts,
            "use `redis-cli GET session:id:42` :tada:\n```fe80::a:b:1\nkey:ok:value &lt;x&gt;```",
            _rich(
                _section(
                    {"type": "text", "text": "use "},
                    {
                        "type": "text",
                        "text": "redis-cli GET session:id:42",
                        "style": {"code": True},
                    },
                    {"type": "text", "text": " "},
                    {"type": "emoji", "name": "tada", "unicode": "1f389"},
                ),
                {
                    "type": "rich_text_preformatted",
                    "border": 0,
                    "elements": [
                        {"type": "text", "text": "fe80::a:b:1\nkey:ok:value <x>"}
                    ],
                },
            ),
        )
        texts = _texts(tmp_path, [message])
        assert texts[ts] == (
            "use `redis-cli GET session:id:42` \N{PARTY POPPER}\n\n"
            "```\nfe80::a:b:1\nkey:ok:value <x>\n```"
        )

    def test_nested_lists_and_quote(self, tmp_path: Path) -> None:
        ts = "1700000002.000100"
        message = _msg(
            ts,
            "&gt; ship it?\nPlan:\n1. build\n    \N{BULLET} lint\n2. deploy",
            _rich(
                {
                    "type": "rich_text_quote",
                    "elements": [{"type": "text", "text": "ship it?"}],
                },
                _section({"type": "text", "text": "Plan:\n"}),
                {
                    "type": "rich_text_list",
                    "style": "ordered",
                    "indent": 0,
                    "border": 0,
                    "elements": [_section({"type": "text", "text": "build"})],
                },
                {
                    "type": "rich_text_list",
                    "style": "bullet",
                    "indent": 1,
                    "border": 0,
                    "elements": [_section({"type": "text", "text": "lint"})],
                },
                {
                    "type": "rich_text_list",
                    "style": "ordered",
                    "indent": 0,
                    "offset": 1,
                    "border": 0,
                    "elements": [_section({"type": "text", "text": "deploy"})],
                },
            ),
        )
        texts = _texts(tmp_path, [message])
        assert texts[ts] == "> ship it?\n\nPlan:\n\n1. build\n    * lint\n2. deploy"

    def test_typed_markup_is_not_a_link_or_mention(self, tmp_path: Path) -> None:
        ts = "1700000003.000100"
        message = _msg(
            ts,
            "&lt;https://evil.example|https://bank.example&gt; &lt;@U002&gt;",
            _rich(
                _section(
                    {
                        "type": "text",
                        "text": "<https://evil.example|https://bank.example> <@U002>",
                    }
                )
            ),
        )
        text = _texts(tmp_path, [message])[ts]
        assert "users/" not in text
        assert "<https://evil.example|" not in text
        assert text.replace(ZWSP, "") == (
            "<https://evil.example|https://bank.example> <@U002>"
        )

    def test_edit_marker_is_utc(self, tmp_path: Path) -> None:
        ts = "1700000004.000100"
        message = _msg(
            ts,
            "fixed typo",
            edited={"user": "U001", "ts": "1700000600.000000"},
        )
        assert _texts(tmp_path, [message])[ts] == (
            "fixed typo\n\n_(edited at 2023-11-14 22:23:20 UTC)_"
        )


class TestAttachments:
    """Bot attachments and forwarded messages reach the payload."""

    def test_attachment_only_bot_post_is_sent(self, tmp_path: Path) -> None:
        ts = "1700000010.000100"
        message = {
            "type": "message",
            "subtype": "bot_message",
            "text": "",
            "ts": ts,
            "bot_id": "B0BOT",
            "username": "GitHub",
            "attachments": [
                {
                    "id": 1,
                    "color": "36a64f",
                    "fallback": "[acme/app] Pull request opened: #1 Fix login",
                    "pretext": "[acme/app] Pull request opened by alice",
                    "title": "#1 Fix login",
                    "title_link": "https://github.example/acme/app/pull/1",
                    "text": "Fixes the crash on *empty* passwords",
                    "fields": [{"title": "Reviewers", "value": "bob", "short": True}],
                    "footer": "GitHub",
                }
            ],
        }
        text = _texts(tmp_path, [message])[ts]
        assert text.endswith(
            "[acme/app] Pull request opened by alice\n"
            "<https://github.example/acme/app/pull/1|#1 Fix login>\n"
            "Fixes the crash on *empty* passwords\n"
            "*Reviewers*: bob\n"
            "GitHub"
        )

    def test_forwarded_message_time_is_utc(self, tmp_path: Path) -> None:
        ts = "1700000011.000100"
        message = _msg(
            ts,
            "FYI",
            attachments=[
                {
                    "is_share": True,
                    "author_name": "Bob Jones",
                    "text": "the original",
                    "ts": "1700000000.000100",
                    "channel_id": "C002",
                }
            ],
        )
        assert _texts(tmp_path, [message])[ts] == (
            "FYI\n\n*Forwarded message from Bob Jones"
            " (originally sent November 14, 2023 at 10:13 PM UTC):*\nthe original"
        )


class TestReactionEmoji:
    """Only reactions with a valid Unicode emoji are counted."""

    def test_custom_reaction_is_not_counted(self, tmp_path: Path) -> None:
        message = _msg(
            "1700000020.000100",
            "nice",
            reactions=[
                {"name": "thumbsup::skin-tone-2", "users": ["U001"], "count": 1},
                {"name": "partyparrot", "users": ["U002"], "count": 1},
            ],
        )
        build_export(
            tmp_path,
            users=USERS,
            channels=[GENERAL_CHANNEL],
            messages_by_channel={"general": [message]},
        )
        migrator = make_migrator(tmp_path)
        migrator.migrate()
        assert migrator.state.progress.migration_summary["reactions_created"] == 1
