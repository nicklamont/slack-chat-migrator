"""Unit tests for Slack emoji name to Unicode conversion."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path
from types import ModuleType
from unittest.mock import MagicMock

import emoji as emoji_lib
import pytest

from slack_chat_migrator.utils.emoji import (
    SKIN_TONE_MODIFIERS,
    apply_skin_tone,
    chat_custom_emoji_name,
    emoji_from_codepoints,
    is_emoji_modifier_or_joiner,
    slack_emoji_to_unicode,
    split_skin_tone,
)
from slack_chat_migrator.utils.slack_emoji_data import SLACK_EMOJI

THUMBS_UP = "\U0001f44d"
MAN_SHRUGGING = "\U0001f937\u200d\u2642\ufe0f"


class TestStandardNames:
    """Names resolved through the emoji package and Slack's own table."""

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("thumbsup", THUMBS_UP),
            ("+1", THUMBS_UP),
            ("-1", "\U0001f44e"),
            ("tada", "\U0001f389"),
            ("white_check_mark", "\u2705"),
            ("heart", "\u2764\ufe0f"),
            # Slack-only names the emoji package doesn't know
            ("man-shrugging", MAN_SHRUGGING),
            ("face_palm", "\U0001f926"),
            ("simple_smile", "\U0001f642"),
            ("keycap_star", "*\ufe0f\u20e3"),
            # Two-letter flags are derived from the ISO code
            ("flag-us", "\U0001f1fa\U0001f1f8"),
            ("flag-de", "\U0001f1e9\U0001f1ea"),
            # Subdivision flags come from the table
            (
                "flag-scotland",
                "\U0001f3f4\U000e0067\U000e0062\U000e0073\U000e0063\U000e0074\U000e007f",
            ),
        ],
    )
    def test_resolves(self, name: str, expected: str) -> None:
        assert slack_emoji_to_unicode(name) == expected

    def test_slack_name_overrides_conflicting_package_alias(self) -> None:
        """Slack's :email: is the envelope, not the e-mail symbol."""
        assert slack_emoji_to_unicode("email") == "\u2709\ufe0f"

    def test_colons_are_optional(self) -> None:
        assert slack_emoji_to_unicode(":tada:") == "\U0001f389"

    @pytest.mark.parametrize(
        "name", ["partyparrot", "squirrel", "flag-zz", "", "::", "not an emoji"]
    )
    def test_custom_or_unknown_returns_none(self, name: str) -> None:
        assert slack_emoji_to_unicode(name) is None

    def test_non_string_returns_none(self) -> None:
        assert slack_emoji_to_unicode(None) is None  # type: ignore[arg-type]

    def test_every_result_is_a_valid_emoji(self) -> None:
        for name in ["thumbsup", "man-shrugging", "flag-us", "simple_smile"]:
            assert emoji_lib.is_emoji(slack_emoji_to_unicode(name))


class TestSkinTones:
    """``name::skin-tone-N`` handling."""

    @pytest.mark.parametrize("tone", [2, 3, 4, 5, 6])
    def test_thumbsup_tones(self, tone: int) -> None:
        result = slack_emoji_to_unicode(f"thumbsup::skin-tone-{tone}")
        assert result == THUMBS_UP + SKIN_TONE_MODIFIERS[tone]
        assert emoji_lib.is_emoji(result)

    def test_tone_inside_zwj_sequence(self) -> None:
        """The modifier goes after the first code point of a ZWJ sequence."""
        result = slack_emoji_to_unicode("man-shrugging::skin-tone-3")
        assert result == "\U0001f937\U0001f3fc\u200d\u2642\ufe0f"
        assert emoji_lib.is_emoji(result)

    def test_tone_replaces_variation_selector(self) -> None:
        result = slack_emoji_to_unicode("point_up::skin-tone-4")
        assert result == "\u261d\U0001f3fd"

    def test_tone_dropped_when_emoji_has_no_skin_tones(self) -> None:
        assert slack_emoji_to_unicode("heart::skin-tone-2") == "\u2764\ufe0f"

    def test_two_person_tone_falls_back_to_base(self) -> None:
        result = slack_emoji_to_unicode("people_holding_hands::skin-tone-2-3")
        assert result == "\U0001f9d1\u200d\U0001f91d\u200d\U0001f9d1"

    def test_custom_emoji_with_tone_returns_none(self) -> None:
        assert slack_emoji_to_unicode("partyparrot::skin-tone-2") is None

    def test_split_skin_tone(self) -> None:
        assert split_skin_tone("wave::skin-tone-2") == ("wave", 2)
        assert split_skin_tone("wave") == ("wave", None)
        assert split_skin_tone("couple::skin-tone-2-5") == ("couple", 2)

    def test_apply_skin_tone_keeps_already_toned(self) -> None:
        toned = THUMBS_UP + SKIN_TONE_MODIFIERS[2]
        assert apply_skin_tone(toned, 5) == toned

    def test_apply_skin_tone_ignores_invalid_tone(self) -> None:
        assert apply_skin_tone(THUMBS_UP, 9) == THUMBS_UP
        assert apply_skin_tone("", 2) == ""


class TestCustomEmojiMap:
    """``config.custom_emoji_map`` is checked first."""

    def test_unicode_value(self) -> None:
        mapping = {"partyparrot": "\U0001f99c"}
        assert slack_emoji_to_unicode("partyparrot", mapping) == "\U0001f99c"

    def test_shortcode_value(self) -> None:
        mapping = {"partyparrot": ":tada:"}
        assert slack_emoji_to_unicode("partyparrot", mapping) == "\U0001f389"

    def test_bare_name_value(self) -> None:
        mapping = {"partyparrot": "bird"}
        assert slack_emoji_to_unicode("partyparrot", mapping) == "\U0001f426"

    def test_value_naming_a_chat_custom_emoji_returns_none(self) -> None:
        mapping = {"partyparrot": ":party-blob:"}
        assert slack_emoji_to_unicode("partyparrot", mapping) is None

    def test_overrides_standard_name(self) -> None:
        mapping = {"tada": ":confetti_ball:"}
        assert slack_emoji_to_unicode("tada", mapping) == "\U0001f38a"

    def test_tone_applies_to_mapped_base(self) -> None:
        mapping = {"approve": ":thumbsup:"}
        result = slack_emoji_to_unicode("approve::skin-tone-2", mapping)
        assert result == THUMBS_UP + SKIN_TONE_MODIFIERS[2]

    def test_full_name_with_tone_can_be_mapped(self) -> None:
        mapping = {"thumbsup::skin-tone-2": "\U0001f44c"}
        assert slack_emoji_to_unicode("thumbsup::skin-tone-2", mapping) == (
            "\U0001f44c"
        )

    def test_invalid_value_is_ignored_with_one_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        mapping = {"x": "NOT AN EMOJI!"}
        with caplog.at_level(logging.WARNING):
            assert slack_emoji_to_unicode("x", mapping) is None
            assert slack_emoji_to_unicode("x", mapping) is None
        warnings = [r for r in caplog.records if "NOT AN EMOJI!" in r.getMessage()]
        assert len(warnings) <= 1

    def test_non_string_values_and_maps_are_ignored(self) -> None:
        assert slack_emoji_to_unicode("tada", {"tada": 42}) == "\U0001f389"
        assert slack_emoji_to_unicode("tada", MagicMock()) == "\U0001f389"

    def test_chat_custom_emoji_name(self) -> None:
        mapping = {"partyparrot": ":party-blob:"}
        assert chat_custom_emoji_name("partyparrot", mapping) == "party-blob"
        assert chat_custom_emoji_name(":fast-parrot::skin-tone-2:") == "fast-parrot"
        assert chat_custom_emoji_name("x", {"x": "\U0001f99c"}) == "x"


class TestCodepoints:
    """Decoding the ``unicode`` field of rich-text emoji elements."""

    def test_single(self) -> None:
        assert emoji_from_codepoints("1f44d") == THUMBS_UP

    def test_sequence_with_tone(self) -> None:
        assert emoji_from_codepoints("1f44d-1f3fd") == THUMBS_UP + "\U0001f3fd"

    def test_adds_missing_variation_selector(self) -> None:
        assert emoji_from_codepoints("263a") == "\u263a\ufe0f"

    @pytest.mark.parametrize("value", [None, "", "zz", "41", 12, "110000"])
    def test_invalid(self, value: object) -> None:
        assert emoji_from_codepoints(value) is None


class TestModifierDetection:
    """Characters that must stay attached to the previous one."""

    @pytest.mark.parametrize(
        "char", ["\u200d", "\ufe0f", "\U0001f3fb", "\U000e0067", "\u0301", "\u20e3"]
    )
    def test_modifiers(self, char: str) -> None:
        assert is_emoji_modifier_or_joiner(char)

    @pytest.mark.parametrize("char", ["a", " ", THUMBS_UP, "\u00e9"])
    def test_regular_characters(self, char: str) -> None:
        assert not is_emoji_modifier_or_joiner(char)


class TestGeneratedTable:
    """The shipped Slack-only table and the script that generates it."""

    def test_values_are_valid_emoji(self) -> None:
        assert SLACK_EMOJI
        for name, value in SLACK_EMOJI.items():
            assert emoji_lib.is_emoji(value), name

    def test_no_two_letter_flags_listed(self) -> None:
        """Two-letter flags are derived at runtime, keeping the table small."""
        assert not [n for n in SLACK_EMOJI if n.startswith("flag-") and len(n) == 7]

    @staticmethod
    def _load_script() -> ModuleType:
        path = (
            Path(__file__).resolve().parents[2]
            / "scripts"
            / "generate_slack_emoji_data.py"
        )
        spec = importlib.util.spec_from_file_location("gen_slack_emoji", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_script_keeps_only_names_the_package_lacks(self) -> None:
        script = self._load_script()
        entries = [
            {"unified": "1F44D", "short_names": ["+1", "thumbsup"]},
            {"unified": "1F937-200D-2642-FE0F", "short_names": ["man-shrugging"]},
            {"unified": "1F1FA-1F1F8", "short_names": ["flag-us", "us"]},
            {"unified": "2709-FE0F", "short_names": ["email", "envelope"]},
        ]
        table = script.build_table(entries)
        assert table["man-shrugging"] == MAN_SHRUGGING
        assert table["email"] == "\u2709\ufe0f"  # conflicting package alias
        assert "thumbsup" not in table
        assert "flag-us" not in table
        assert table["simple_smile"] == "\U0001f642"

    def test_script_renders_importable_module(self) -> None:
        script = self._load_script()
        source = script.render_module({"man-shrugging": MAN_SHRUGGING})
        namespace: dict[str, object] = {}
        exec(compile(source, "slack_emoji_data.py", "exec"), namespace)  # noqa: S102
        assert namespace["SLACK_EMOJI"] == {"man-shrugging": MAN_SHRUGGING}
        assert "MIT License" in source
