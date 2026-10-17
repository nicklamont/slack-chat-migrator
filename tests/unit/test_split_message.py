"""Tests for split_message_text() (Chat's 32,000-byte message limit)."""

from __future__ import annotations

import pytest

from slack_chat_migrator.constants import MESSAGE_MAX_BYTES
from slack_chat_migrator.utils.emoji import is_emoji_modifier_or_joiner
from slack_chat_migrator.utils.formatting import split_message_text

ZWJ = "\N{ZERO WIDTH JOINER}"


def _size(text: str) -> int:
    return len(text.encode("utf-8"))


def _assert_fits(parts: list[str], max_bytes: int) -> None:
    for part in parts:
        assert _size(part) <= max_bytes
        part.encode("utf-8")  # every part is valid text


class TestNoSplit:
    """Text that fits is returned unchanged."""

    def test_default_limit_is_32000(self) -> None:
        assert MESSAGE_MAX_BYTES == 32000

    def test_short_text(self) -> None:
        assert split_message_text("hello") == ["hello"]

    def test_empty_text(self) -> None:
        assert split_message_text("") == [""]

    def test_exactly_at_limit(self) -> None:
        text = "a" * 32000
        assert split_message_text(text) == [text]

    def test_budget_too_small(self) -> None:
        with pytest.raises(ValueError, match="max_bytes"):
            split_message_text("x" * 100, max_bytes=10)


class TestSplitPoints:
    """Parts break at paragraphs, then lines, then whitespace."""

    def test_forty_thousand_two_byte_chars(self) -> None:
        """The audit repro: 40,000 chars of e-acute is 80,000 bytes."""
        text = "\N{LATIN SMALL LETTER E WITH ACUTE}" * 40_000
        parts = split_message_text(text)
        assert len(parts) >= 3
        _assert_fits(parts, MESSAGE_MAX_BYTES)
        assert "".join(parts) == text

    def test_splits_at_paragraphs(self) -> None:
        paragraphs = [f"paragraph {i} " + "x" * 300 for i in range(20)]
        text = "\n\n".join(paragraphs)
        parts = split_message_text(text, max_bytes=1000)
        _assert_fits(parts, 1000)
        assert "\n\n".join(parts) == text
        assert all(part.startswith("paragraph") for part in parts)

    def test_splits_at_lines(self) -> None:
        lines = [f"line {i} " + "y" * 90 for i in range(50)]
        text = "\n".join(lines)
        parts = split_message_text(text, max_bytes=500)
        _assert_fits(parts, 500)
        assert "\n".join(parts) == text

    def test_splits_at_whitespace(self) -> None:
        text = " ".join(f"word{i}" for i in range(500))
        parts = split_message_text(text, max_bytes=300)
        _assert_fits(parts, 300)
        assert " ".join(parts) == text
        assert all(not part.startswith(" ") for part in parts)

    def test_four_byte_characters_are_never_split(self) -> None:
        text = "\N{MUSICAL SYMBOL G CLEF}" * 1000
        parts = split_message_text(text, max_bytes=101)
        _assert_fits(parts, 101)
        assert "".join(parts) == text

    def test_emoji_sequences_stay_together(self) -> None:
        technologist = "\N{WOMAN}" + ZWJ + "\N{PERSONAL COMPUTER}"
        toned = "\N{THUMBS UP SIGN}\N{EMOJI MODIFIER FITZPATRICK TYPE-4}"
        text = (technologist + toned) * 200
        parts = split_message_text(text, max_bytes=100)
        _assert_fits(parts, 100)
        assert "".join(parts) == text
        for part in parts:
            assert not is_emoji_modifier_or_joiner(part[0])
            assert not part.endswith(ZWJ)

    def test_tokens_are_not_cut(self) -> None:
        link = "<https://example.com/some/long/path|a label with several words>"
        text = ("filler " * 30) + link + (" tail" * 30)
        parts = split_message_text(text, max_bytes=250)
        _assert_fits(parts, 250)
        assert any(link in part for part in parts)


class TestCodeFences:
    """A split inside a code block closes and reopens the fence."""

    def test_split_inside_code_block(self) -> None:
        code = "\n".join(f"print({i})" for i in range(200))
        text = f"Here is the log:\n```\n{code}\n```\nDone."
        parts = split_message_text(text, max_bytes=400)
        _assert_fits(parts, 400)
        assert len(parts) > 2
        for part in parts:
            assert part.count("```") % 2 == 0
        assert parts[0].startswith("Here is the log:")
        assert parts[1].startswith("```\n")
        assert parts[-1].endswith("Done.")
        # All code lines survive, in order.
        joined = "\n".join(parts)
        positions = [joined.index(f"print({i})\n") for i in range(199)]
        assert positions == sorted(positions)

    def test_text_after_closed_block_has_no_fence(self) -> None:
        text = "```\nshort code\n```\n\n" + "\n\n".join("p" * 200 for _ in range(10))
        parts = split_message_text(text, max_bytes=500)
        _assert_fits(parts, 500)
        assert parts[0].startswith("```\nshort code\n```")
        assert all("```" not in part for part in parts[1:])

    def test_reserved_room_for_fences(self) -> None:
        text = "```\n" + "z" * 5000 + "\n```"
        parts = split_message_text(text, max_bytes=1000)
        _assert_fits(parts, 1000)
        assert all(part.count("```") == 2 for part in parts)
