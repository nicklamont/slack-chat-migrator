"""Slack emoji name to Unicode conversion.

Slack refers to emoji by short name: ``:tada:`` in message text, ``tada`` in
reactions and rich-text ``emoji`` elements.  Skin tones are a suffix
(``thumbsup::skin-tone-2``), some names are Slack-specific (``man-shrugging``,
``flag-us``) and workspace custom emoji (``partyparrot``) have no Unicode
equivalent at all.

:func:`slack_emoji_to_unicode` resolves a name in this order:

1. ``custom_emoji_map`` from the config: a Unicode value is used as is, a
   ``:name:`` value is resolved as a standard emoji.
2. Slack's own names that the ``emoji`` package lacks
   (:mod:`slack_chat_migrator.utils.slack_emoji_data`), and two-letter
   ``flag-xx`` country flags.
3. The ``emoji`` package's alias table.

It returns ``None`` for custom or unknown emoji, so callers never send
invalid Unicode to Google Chat.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Mapping
from functools import lru_cache

import emoji as emoji_lib

from slack_chat_migrator.utils.logging import log_with_context
from slack_chat_migrator.utils.slack_emoji_data import SLACK_EMOJI

#: Slack ``skin-tone-N`` (2..6) to the Fitzpatrick modifier U+1F3FB..U+1F3FF.
SKIN_TONE_MODIFIERS: dict[int, str] = {
    2: "\U0001f3fb",
    3: "\U0001f3fc",
    4: "\U0001f3fd",
    5: "\U0001f3fe",
    6: "\U0001f3ff",
}

_VARIATION_SELECTOR_16 = "\ufe0f"

# ``name::skin-tone-2``; Slack writes two-person tones as ``skin-tone-2-3``.
_SKIN_TONE_SUFFIX_RE = re.compile(r"::skin-tone-([2-6])(?:-[2-6])?$")

# A bare emoji name such as ``tada`` or ``man-shrugging``.
_BARE_NAME_RE = re.compile(r"[a-z0-9_+\-']+(?:::skin-tone-[2-6](?:-[2-6])?)?")

# custom_emoji_map values already reported as unusable, to warn only once.
_warned_custom_values: set[str] = set()


def split_skin_tone(name: str) -> tuple[str, int | None]:
    """Split a Slack emoji name into its base name and skin tone.

    Args:
        name: A Slack emoji name, e.g. ``"thumbsup::skin-tone-2"``.

    Returns:
        ``(base_name, tone)`` where *tone* is 2..6, or ``None`` when the name
        has no skin-tone suffix.  For a two-person tone (``skin-tone-2-3``)
        only the first tone is returned.
    """
    match = _SKIN_TONE_SUFFIX_RE.search(name)
    if not match:
        return name, None
    return name[: match.start()], int(match.group(1))


def apply_skin_tone(emoji_str: str, tone: int) -> str:
    """Apply a Slack skin tone to an emoji, if the emoji supports one.

    The Fitzpatrick modifier goes right after the first code point (replacing
    a variation selector there), which is where Unicode places it for single
    emoji and for ZWJ sequences such as "man shrugging".  The toned sequence
    is kept only if it is a valid emoji; otherwise the tone is dropped.

    Args:
        emoji_str: The Unicode emoji without a skin tone.
        tone: The Slack skin tone, 2..6.

    Returns:
        The toned emoji, or *emoji_str* unchanged when the tone can't apply.
    """
    modifier = SKIN_TONE_MODIFIERS.get(tone)
    if not modifier or not emoji_str:
        return emoji_str
    if any(m in emoji_str for m in SKIN_TONE_MODIFIERS.values()):
        return emoji_str
    rest = emoji_str[1:]
    if rest.startswith(_VARIATION_SELECTOR_16):
        rest = rest[1:]
    candidate = emoji_str[0] + modifier + rest
    return candidate if emoji_lib.is_emoji(candidate) else emoji_str


def emoji_from_codepoints(codepoints: object) -> str | None:
    """Decode a Slack ``unicode`` field (``"1f44d"``, ``"1f44d-1f3fd"``).

    Args:
        codepoints: Hyphen-separated hexadecimal code points.

    Returns:
        The emoji string, or ``None`` if the value isn't a valid emoji.
    """
    if not isinstance(codepoints, str) or not codepoints.strip():
        return None
    try:
        chars = "".join(chr(int(part, 16)) for part in codepoints.strip().split("-"))
    except (ValueError, OverflowError):
        return None
    return _fully_qualified(chars)


def _fully_qualified(chars: str) -> str | None:
    """Return the fully-qualified form of an emoji, or ``None`` if invalid.

    Slack sometimes omits the variation selector (``263a`` instead of
    ``263a-fe0f``), which makes the emoji render as plain text.
    """
    qualified = chars + _VARIATION_SELECTOR_16
    data = emoji_lib.EMOJI_DATA
    if (
        chars in data
        and data[chars].get("status") != emoji_lib.STATUS["fully_qualified"]
    ):
        if qualified in data:
            return qualified
    if emoji_lib.is_emoji(chars):
        return chars
    if emoji_lib.is_emoji(qualified):
        return qualified
    return None


def _regional_flag(code: str) -> str | None:
    """Return the flag emoji for a two-letter ISO 3166 code, if it exists."""
    if len(code) != 2 or not code.isascii() or not code.isalpha():
        return None
    flag = "".join(chr(0x1F1E6 + ord(c) - ord("a")) for c in code.lower())
    return flag if emoji_lib.is_emoji(flag) else None


@lru_cache(maxsize=4096)
def _resolve_standard(name: str) -> str | None:
    """Resolve a standard (non-custom) emoji name without a skin tone.

    Args:
        name: A Slack or ``emoji``-package short name, e.g. ``"tada"``.

    Returns:
        The Unicode emoji, or ``None`` when the name is unknown.
    """
    if not name:
        return None
    hit = SLACK_EMOJI.get(name)
    if hit:
        return hit
    if name.startswith("flag-"):
        flag = _regional_flag(name[5:])
        if flag:
            return flag
    shortcode = f":{name}:"
    out = emoji_lib.emojize(shortcode, language="alias")
    if out != shortcode and emoji_lib.is_emoji(out):
        return str(out)
    return None


def _resolve_name(name: str) -> str | None:
    """Resolve a standard emoji name that may carry a skin-tone suffix."""
    base, tone = split_skin_tone(name)
    resolved = _resolve_standard(base)
    if resolved and tone:
        return apply_skin_tone(resolved, tone)
    return resolved


def _strip_colons(name: str) -> str:
    """Turn ``":tada:"`` into ``"tada"``; leave other strings unchanged."""
    name = name.strip()
    if len(name) > 2 and name.startswith(":") and name.endswith(":"):
        return name[1:-1]
    return name


def _custom_value(
    name: str, custom_map: Mapping[str, object] | None
) -> tuple[str, bool] | None:
    """Look *name* up in ``custom_emoji_map``.

    Args:
        name: The Slack emoji name without colons.
        custom_map: ``config.custom_emoji_map``.

    Returns:
        ``(value, matched_base)``, where *matched_base* is True when only the
        name without its skin tone was mapped, or ``None`` when unmapped.
    """
    if not custom_map or not isinstance(custom_map, Mapping):
        return None
    base, _ = split_skin_tone(name)
    for key in (name, base):
        value = custom_map.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip(), key != name
    return None


def _resolve_custom_value(value: str) -> str | None:
    """Resolve a ``custom_emoji_map`` value to Unicode.

    Args:
        value: A Unicode emoji, or an emoji name with or without colons
            (``":parrot:"``, ``"parrot"``).

    Returns:
        The Unicode emoji, or ``None`` when the value names a Chat custom
        emoji (or nothing usable).
    """
    if emoji_lib.purely_emoji(value):
        return value
    name = _strip_colons(value)
    if _BARE_NAME_RE.fullmatch(name):
        return _resolve_name(name)
    if value not in _warned_custom_values:
        _warned_custom_values.add(value)
        log_with_context(
            logging.WARNING,
            f"Ignoring custom_emoji_map value {value!r}: use a Unicode emoji"
            " or an emoji name such as ':tada:'",
        )
    return None


def slack_emoji_to_unicode(
    name: str, custom_map: Mapping[str, object] | None = None
) -> str | None:
    """Convert a Slack emoji name to the Unicode emoji Google Chat accepts.

    Args:
        name: The Slack emoji name, with or without colons, optionally with a
            skin tone (``"thumbsup::skin-tone-2"``).
        custom_map: ``config.custom_emoji_map``: Slack name to a Unicode
            emoji, or to another emoji name written as ``:name:``.  It is
            checked first, so it can also override standard names.

    Returns:
        The Unicode emoji, or ``None`` for custom or unknown emoji (callers
        should skip a reaction, or keep ``:name:`` as text).
    """
    if not isinstance(name, str):
        return None
    name = _strip_colons(name)
    if not name:
        return None

    custom = _custom_value(name, custom_map)
    if custom is not None:
        value, matched_base = custom
        resolved = _resolve_custom_value(value)
        _, tone = split_skin_tone(name)
        if resolved and tone and matched_base:
            return apply_skin_tone(resolved, tone)
        return resolved

    return _resolve_name(name)


def chat_custom_emoji_name(
    name: str, custom_map: Mapping[str, object] | None = None
) -> str:
    """Return the Chat custom emoji name to use for an unresolved Slack emoji.

    When ``custom_emoji_map`` maps the Slack name to ``:other-name:`` and that
    isn't a standard emoji, it names a Chat custom emoji.  Otherwise the Slack
    name itself (without skin tone) is used.

    Args:
        name: The Slack emoji name, with or without colons.
        custom_map: ``config.custom_emoji_map``.

    Returns:
        The custom emoji name, without colons.
    """
    name = _strip_colons(name)
    custom = _custom_value(name, custom_map)
    if custom is not None:
        mapped = _strip_colons(custom[0])
        if _BARE_NAME_RE.fullmatch(mapped):
            return split_skin_tone(mapped)[0]
    return split_skin_tone(name)[0]


def is_emoji_modifier_or_joiner(char: str) -> bool:
    """Return True if *char* only modifies or joins the preceding character.

    Used to avoid splitting text between an emoji and its skin tone, variation
    selector or ZWJ partner.

    Args:
        char: A single character.

    Returns:
        True for ZWJ, variation selectors, skin-tone modifiers, tag
        characters and combining marks.
    """
    code = ord(char)
    return (
        code == 0x200D
        or 0xFE00 <= code <= 0xFE0F
        or 0x1F3FB <= code <= 0x1F3FF
        or 0xE0020 <= code <= 0xE007F
        or code == 0x20E3
        or unicodedata.combining(char) != 0
    )
