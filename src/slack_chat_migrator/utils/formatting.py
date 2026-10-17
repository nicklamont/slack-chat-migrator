"""
Message formatting utilities for converting Slack messages to Google Chat format.

Conversion happens in two steps:

1. :func:`parse_slack_blocks` renders a Slack message (Block Kit ``blocks``,
   attachments or the ``text`` field) as Slack *mrkdwn*.  Literal text from
   rich-text elements is entity-escaped (``&amp; &lt; &gt;``), exactly like
   Slack's own ``text`` field, so the result is well-formed mrkdwn.
2. :func:`convert_formatting` converts that mrkdwn into Google Chat markup
   (``MARKUP_SYNTAX_CHAT``): it parses ``<...>`` tokens on the escaped text,
   decodes entities only in the literal text between them, and replaces
   emoji shortcodes only in prose, never in code, URLs or tokens.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slack_chat_migrator.core.state import MigrationState
    from slack_chat_migrator.utils.user_validation import UnmappedUserTracker

from slack_chat_migrator.constants import MESSAGE_MAX_BYTES
from slack_chat_migrator.utils.emoji import (
    apply_skin_tone,
    chat_custom_emoji_name,
    emoji_from_codepoints,
    is_emoji_modifier_or_joiner,
    slack_emoji_to_unicode,
    split_skin_tone,
)

# This assumes a standard logging setup. If you don't have one,
# you can replace `from slack_chat_migrator.utils.logging import logger`
# with `import logging; logger = logging.getLogger(__name__)`
from slack_chat_migrator.utils.logging import log_with_context


@dataclass
class _RenderContext:
    """State for rendering one Slack message's blocks as mrkdwn.

    Attributes:
        labels: Labels harvested from the message's mrkdwn ``text`` for
            tokens that rich text carries only as IDs, keyed like the token
            (``"#C0123"`` -> ``"general"``, ``"!subteam^S0123"`` -> ``"@eng"``).
        custom_emoji_map: ``config.custom_emoji_map``.
        unknown_types: Block and element types that couldn't be rendered;
            any entry makes :func:`parse_slack_blocks` fall back to ``text``.
    """

    labels: dict[str, str] = field(default_factory=dict)
    custom_emoji_map: Mapping[str, object] | None = None
    unknown_types: list[str] = field(default_factory=list)


# Labelled channel and user group tokens in a message's mrkdwn ``text``.
_LABELLED_TOKEN_RE = re.compile(r"<(#[A-Z0-9]+|!subteam\^[A-Z0-9]+)\|([^<>|]+)>")

# Rich-text elements that link to a Slack object and carry ``text``/``url``.
_LINK_LIKE_ELEMENTS = frozenset(
    {
        "attachment_mention",
        "canvas",
        "citation",
        "file",
        "list_record",
        "message_mention",
        "work_object_mention",
        "workflow_mention",
    }
)

# Blocks that carry no message text (buttons, images, inputs, files).
_NON_TEXT_BLOCKS = frozenset({"actions", "call", "file", "image", "input"})

_STYLE_KEYS = ("bold", "italic", "strike", "code")

# A rich-text ``style`` object, e.g. ``{"bold": True, "code": True}``.
_Style = Mapping[str, object]


def _harvest_labels(text: str) -> dict[str, str]:
    """Collect channel and user group labels from mrkdwn text.

    Rich-text ``channel`` and ``usergroup`` elements carry only IDs, while
    the message's ``text`` usually spells them out (``<#C0123|general>``,
    ``<!subteam^S0123|@eng>``).

    Args:
        text: The message's mrkdwn ``text`` field.

    Returns:
        Token key (``"#C0123"``, ``"!subteam^S0123"``) -> escaped label.
    """
    return {m.group(1): m.group(2) for m in _LABELLED_TOKEN_RE.finditer(text)}


def _apply_styles(text: str, style: Mapping[str, object] | None) -> str:
    """Wrap mrkdwn *text* in style markers.

    ``code`` wins: the text is wrapped in backticks and other styles are
    skipped, since Chat doesn't format inside code.  ``underline`` and
    ``highlight`` have no Chat equivalent and leave the text plain.

    Args:
        text: The mrkdwn text to style.
        style: A rich-text style object, e.g. ``{"bold": True}``.

    Returns:
        The styled text.
    """
    if not style:
        return text
    if style.get("code"):
        return f"`{text}`"
    # Apply styles in the correct order: bold -> italic -> strikethrough
    result = text
    if style.get("bold"):
        result = f"*{result}*"
    if style.get("italic"):
        result = f"_{result}_"
    if style.get("strike"):
        result = f"~{result}~"
    return result


def _style_text(text: str, style: Mapping[str, object] | None) -> str:
    """Apply styles to each line of *text*, keeping surrounding whitespace.

    Style markers must hug the text (``*bold*``, not ``* bold *``) and can't
    span lines, so they wrap the non-blank part of every line.
    """
    if not style or not any(style.get(key) for key in _STYLE_KEYS):
        return text
    lines = []
    for line in text.split("\n"):
        core = line.strip()
        if not core:
            lines.append(line)
            continue
        leading = line[: len(line) - len(line.lstrip())]
        trailing = line[len(line.rstrip()) :]
        lines.append(f"{leading}{_apply_styles(core, style)}{trailing}")
    return "\n".join(lines)


def _render_emoji_element(element: dict, rctx: _RenderContext | None = None) -> str:
    """Render a rich-text ``emoji`` element as Unicode.

    ``custom_emoji_map`` entries win; otherwise the element's ``unicode``
    field (``"1f44d"``, ``"1f44d-1f3fd"``) is used when present, else the
    name is resolved.  Custom or unknown emoji are kept as ``:name:``.

    Args:
        element: A rich-text element of type ``emoji``.
        rctx: The render context (for ``custom_emoji_map``).

    Returns:
        The emoji, ``:name:`` for custom emoji, or ``""`` without a name.
    """
    name = str(element.get("name") or "")
    if not name:
        return emoji_from_codepoints(element.get("unicode")) or ""
    base, tone = split_skin_tone(name)
    custom_map = rctx.custom_emoji_map if rctx else None
    if custom_map and (name in custom_map or base in custom_map):
        resolved = slack_emoji_to_unicode(name, custom_map)
        return resolved or f":{chat_custom_emoji_name(name, custom_map)}:"
    unicode_emoji = emoji_from_codepoints(element.get("unicode"))
    if unicode_emoji:
        return apply_skin_tone(unicode_emoji, tone) if tone else unicode_emoji
    return slack_emoji_to_unicode(name) or f":{name}:"


def _render_text_element(element: dict, style: _Style, rctx: _RenderContext) -> str:
    """Render a ``text`` element; its text is literal, so it is escaped."""
    return _style_text(_escape_mrkdwn(str(element.get("text") or "")), style)


def _render_link_element(element: dict, style: _Style, rctx: _RenderContext) -> str:
    """Render a ``link`` element as ``<url|text>``."""
    url = _escape_mrkdwn(str(element.get("url") or ""))
    text = _escape_mrkdwn(str(element.get("text") or "")) or url
    return _apply_styles(f"<{url}|{text}>", style) if url else text


def _render_user_element(element: dict, style: _Style, rctx: _RenderContext) -> str:
    """Render a ``user`` (or ``canvas_user_mention``) element as ``<@U...>``."""
    user_id = str(element.get("user_id") or "")
    return _apply_styles(f"<@{user_id}>", style) if user_id else ""


def _render_usergroup_element(
    element: dict, style: _Style, rctx: _RenderContext
) -> str:
    """Render a ``usergroup`` element as ``<!subteam^S...|@handle>``."""
    group_id = str(element.get("usergroup_id") or "")
    if not group_id:
        return ""
    label = rctx.labels.get(f"!subteam^{group_id}")
    token = f"<!subteam^{group_id}|{label}>" if label else f"<!subteam^{group_id}>"
    return _apply_styles(token, style)


def _render_channel_element(element: dict, style: _Style, rctx: _RenderContext) -> str:
    """Render a ``channel`` element as ``<#C...|name>``."""
    channel_id = str(element.get("channel_id") or "")
    if not channel_id:
        return ""
    label = rctx.labels.get(f"#{channel_id}")
    token = f"<#{channel_id}|{label}>" if label else f"<#{channel_id}>"
    return _apply_styles(token, style)


def _render_broadcast_element(
    element: dict, style: _Style, rctx: _RenderContext
) -> str:
    """Render a ``broadcast`` element (``@here``...) as ``<!here>``."""
    return _apply_styles(f"<!{element.get('range') or 'channel'}>", style)


def _render_date_element(element: dict, style: _Style, rctx: _RenderContext) -> str:
    """Render a ``date`` element as its fallback text (or a UTC date)."""
    fallback = element.get("fallback")
    if fallback:
        return _style_text(_escape_mrkdwn(str(fallback)), style)
    return _style_text(format_epoch_utc(element.get("timestamp")) or "", style)


def _render_value_element(element: dict, style: _Style, rctx: _RenderContext) -> str:
    """Render a ``team``, ``color`` or ``tag`` element as its plain value."""
    keys = _VALUE_ELEMENT_KEYS[str(element.get("type"))]
    value = next((element[k] for k in keys if element.get(k)), "")
    return _style_text(_escape_mrkdwn(str(value)), style)


def _render_link_like_element(
    element: dict, style: _Style, rctx: _RenderContext
) -> str | None:
    """Render an element that links to a Slack object (canvas, file...).

    Returns:
        ``<url|text>``, ``<url>`` or the text, or ``None`` when the element
        has neither ``url`` nor ``text``.
    """
    url = _escape_mrkdwn(str(element.get("url") or ""))
    text = _escape_mrkdwn(str(element.get("text") or element.get("label") or ""))
    if url:
        return _apply_styles(f"<{url}|{text}>" if text else f"<{url}>", style)
    if text:
        return _style_text(text, style)
    return None


# Element type -> the key(s) holding its plain value, first non-empty wins.
_VALUE_ELEMENT_KEYS = {
    "team": ("name", "team_id"),
    "color": ("value",),
    "tag": ("text",),
}

# Element renderers; ``None`` from a renderer means "nothing to render".
_ELEMENT_RENDERERS: dict[str, Callable[[dict, _Style, _RenderContext], str | None]] = {
    "text": _render_text_element,
    "link": _render_link_element,
    "emoji": lambda element, style, rctx: _render_emoji_element(element, rctx),
    "user": _render_user_element,
    "canvas_user_mention": _render_user_element,
    "usergroup": _render_usergroup_element,
    "channel": _render_channel_element,
    "broadcast": _render_broadcast_element,
    "date": _render_date_element,
    **dict.fromkeys(_VALUE_ELEMENT_KEYS, _render_value_element),
    **dict.fromkeys(_LINK_LIKE_ELEMENTS, _render_link_like_element),
}


def _render_element(element: dict, rctx: _RenderContext, in_code: bool) -> str:
    """Render one rich-text element (text, link, mention...) as mrkdwn.

    Unknown element types render their ``text``/``fallback``, if any, and
    are recorded in ``rctx.unknown_types``.

    Args:
        element: The rich-text element.
        rctx: The render context.
        in_code: True inside a preformatted block, where styles are dropped.

    Returns:
        The mrkdwn for the element.
    """
    el_type = str(element.get("type"))
    style = element.get("style")
    if in_code or not isinstance(style, Mapping):
        style = {}
    renderer = _ELEMENT_RENDERERS.get(el_type)
    rendered = renderer(element, style, rctx) if renderer else None
    if rendered is not None:
        return rendered

    rctx.unknown_types.append(f"element:{el_type}")
    salvage = element.get("text") or element.get("fallback") or ""
    return _escape_mrkdwn(salvage) if isinstance(salvage, str) else ""


def _parse_rich_text_elements(
    elements: list[dict],
    rctx: _RenderContext | None = None,
    in_code: bool = False,
) -> str:
    """Render a list of rich-text elements from Slack's Block Kit as mrkdwn.

    Handles text (bold, italic, strike and code styles), links, emoji, user,
    channel and user group mentions, broadcasts (``@here``), dates, teams,
    colors, tags and the elements that link to Slack objects (canvases,
    files, messages...).  Unknown types render their ``text``/``fallback``
    and are recorded in ``rctx.unknown_types``.

    Args:
        elements: The rich-text elements.
        rctx: The render context; a fresh one is used when omitted.
        in_code: True inside a preformatted block (styles are dropped).

    Returns:
        The mrkdwn for the elements.
    """
    rctx = rctx or _RenderContext()
    return "".join(
        _render_element(el, rctx, in_code) for el in elements if isinstance(el, dict)
    )


# A Slack message permalink (``https://acme.slack.com/archives/C0123/p1700...``).
_SLACK_MESSAGE_URL_RE = re.compile(
    r"^https://[^/\s]*\bslack\.com/archives/[A-Z0-9]+/p\d+", re.IGNORECASE
)


def _is_message_share(attachment: dict) -> bool:
    """Return True if an attachment shares or unfurls a Slack message."""
    if attachment.get("is_share") or attachment.get("is_msg_unfurl"):
        return True
    from_url = attachment.get("from_url")
    return isinstance(from_url, str) and bool(_SLACK_MESSAGE_URL_RE.match(from_url))


def _attachment_str(attachment: dict, key: str) -> str:
    """Return a string attachment field, stripped, or ``""``."""
    value = attachment.get(key)
    return value.strip() if isinstance(value, str) else ""


def _render_shared_message(
    attachment: dict, custom_emoji_map: Mapping[str, object] | None
) -> str:
    """Render a forwarded/shared Slack message attachment.

    The header names the author and the original time, in UTC.

    Args:
        attachment: An ``is_share``/``is_msg_unfurl`` attachment.
        custom_emoji_map: ``config.custom_emoji_map``, for nested blocks.

    Returns:
        ``*Forwarded message from X (originally sent ...):*`` plus the
        shared text, or ``""`` when the attachment has no text.
    """
    author_info = ""
    if attachment.get("author_name"):
        author_info = f" from {attachment['author_name']}"
    elif attachment.get("author_subname"):
        author_info = f" from {attachment['author_subname']}"

    timestamp_info = ""
    if attachment.get("ts"):
        readable_time = format_epoch_utc(attachment["ts"], "%B %d, %Y at %I:%M %p UTC")
        if readable_time:
            timestamp_info = f" (originally sent {readable_time})"
        else:
            timestamp_info = f" (originally sent at {attachment['ts']})"

    # Prefer rich message_blocks over plain text
    forwarded_text = ""
    for msg_block in attachment.get("message_blocks") or []:
        nested = msg_block.get("message") if isinstance(msg_block, dict) else None
        if isinstance(nested, dict) and "blocks" in nested:
            forwarded_text = parse_slack_blocks(nested, custom_emoji_map)
            break

    if not forwarded_text:
        forwarded_text = attachment.get("text") or attachment.get("fallback") or ""

    if not forwarded_text.strip():
        return ""

    forwarded_text = _convert_bullets_to_gchat(forwarded_text.strip())
    header = f"*Forwarded message{author_info}{timestamp_info}:*"
    return f"{header}\n{forwarded_text}"


def _render_legacy_attachment(
    attachment: dict,
    custom_emoji_map: Mapping[str, object] | None,
    message_text: str = "",
) -> str:
    """Render a legacy (secondary) attachment, as bots and integrations post.

    Renders ``pretext``, the ``title`` (linked to ``title_link``), the
    ``text`` (or the attachment's own ``blocks``), the ``fields`` as
    ``*title*: value`` lines and the ``footer``.  ``fallback`` is used only
    when there is nothing else and the message text doesn't already say it,
    and never for link unfurls (``from_url``), whose URL is already in the
    message.

    Args:
        attachment: A Slack attachment that doesn't share a message.
        custom_emoji_map: ``config.custom_emoji_map``, for nested blocks.
        message_text: The message's own ``text``, to avoid repeating it.

    Returns:
        The attachment as mrkdwn lines, or ``""``.
    """
    parts: list[str] = []
    pretext = _attachment_str(attachment, "pretext")
    if pretext:
        parts.append(pretext)

    title = _attachment_str(attachment, "title")
    title_link = _attachment_str(attachment, "title_link")
    if title and title_link:
        parts.append(f"<{title_link}|{title}>")
    elif title:
        parts.append(f"*{title}*")

    body = _attachment_str(attachment, "text")
    if not body and attachment.get("blocks"):
        body = parse_slack_blocks(
            {"blocks": attachment["blocks"], "text": ""}, custom_emoji_map
        ).strip()
    if body:
        parts.append(_convert_bullets_to_gchat(body))

    for field_data in attachment.get("fields") or []:
        if not isinstance(field_data, dict):
            continue
        field_title = _attachment_str(field_data, "title")
        field_value = _attachment_str(field_data, "value")
        if field_title and field_value:
            parts.append(f"*{field_title}*: {field_value}")
        elif field_title or field_value:
            parts.append(f"*{field_title}*" if field_title else field_value)

    footer = _attachment_str(attachment, "footer")
    if footer:
        parts.append(footer)

    if not parts and not attachment.get("from_url"):
        fallback = _attachment_str(attachment, "fallback")
        if fallback and fallback not in message_text:
            parts.append(fallback)

    return "\n".join(parts)


def _render_attachments(
    message: dict, custom_emoji_map: Mapping[str, object] | None = None
) -> list[str]:
    """Render a message's attachments as text, in order.

    Shared/forwarded Slack messages get a "Forwarded message" header; other
    attachments (alerts, CI and ticket bots, link previews) are rendered
    from their pretext, title, text, fields and footer.

    Args:
        message: A Slack message dict with an optional 'attachments' field.
        custom_emoji_map: ``config.custom_emoji_map``, for nested blocks.

    Returns:
        One mrkdwn string per attachment that has text.
    """
    rendered: list[str] = []
    for attachment in message.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        if _is_message_share(attachment):
            text = _render_shared_message(attachment, custom_emoji_map)
        else:
            text = _render_legacy_attachment(
                attachment, custom_emoji_map, str(message.get("text") or "")
            )
        if text.strip():
            rendered.append(text)
    return rendered


# A bullet line in Slack's plain text (bullet, white bullet or small square).
_SLACK_BULLET_LINE_RE = re.compile(
    r"^([ \t]*)[\N{BULLET}\N{WHITE BULLET}\N{BLACK SMALL SQUARE}][ \t]*(.*)$"
)

# Chat markup nests list items by four spaces per level.
_LIST_INDENT = "    "


def _convert_bullets_to_gchat(text: str) -> str:
    """Convert Slack bullet lines to Google Chat bullets.

    Slack's plain text writes list items as lines starting with a bullet
    character, nested ones indented.  Chat reads ``* item``, nested by four
    spaces per level.

    Args:
        text: Text that may contain bullet-point lines.

    Returns:
        Text with bullets converted to Google Chat format.
    """
    lines: list[str] = []
    for line in text.split("\n"):
        match = _SLACK_BULLET_LINE_RE.match(line)
        if not match:
            lines.append(line)
            continue
        level = len(match.group(1).expandtabs(4)) // 4
        lines.append(f"{_LIST_INDENT * level}* {match.group(2).strip()}")
    return "\n".join(lines)


def _combine_with_attachments(main_text: str, attachment_texts: list[str]) -> str:
    """Combine main message text with rendered attachments.

    Args:
        main_text: The primary message text.
        attachment_texts: Rendered attachments (forwarded messages, alerts...).

    Returns:
        Combined text, using only the attachments when main text is empty.
    """
    if not main_text.strip() and attachment_texts:
        return "\n\n".join(attachment_texts)
    if main_text.strip() and attachment_texts:
        return main_text + "\n\n" + "\n\n".join(attachment_texts)
    return main_text


def _rich_text_list_prefix(style: str, indent: int, number: int) -> str:
    """Return the line prefix for a list item at nesting level *indent*.

    Chat markup nests ``*`` bullets by four spaces per level.  It has no
    numbered lists, so ordered items are numbered explicitly.
    """
    marker = f"{number}." if style == "ordered" else "*"
    return f"{_LIST_INDENT * indent}{marker} "


def _parse_rich_text_list(
    element: dict,
    list_items: list[str],
    rctx: _RenderContext | None = None,
    counters: dict[int, int] | None = None,
) -> None:
    """Render a ``rich_text_list`` element and append its lines.

    Slack writes a nested list as consecutive ``rich_text_list`` elements
    with increasing ``indent``.  Ordered lists start at ``offset + 1``; when
    ``offset`` is missing they continue the numbering of the previous list
    at the same level, so numbering survives an interleaved sub-list.

    Args:
        element: A Slack rich_text_list element dict.
        list_items: Accumulator list to append formatted lines to.
        rctx: The render context.
        counters: Next number per nesting level, shared by consecutive lists.
    """
    rctx = rctx or _RenderContext()
    counters = {} if counters is None else counters
    list_style = str(element.get("style") or "bullet")
    try:
        indent = max(0, int(element.get("indent") or 0))
    except (TypeError, ValueError):
        indent = 0

    # A shallower item ends the deeper lists below it.
    for level in [level for level in counters if level > indent]:
        del counters[level]

    number = counters.get(indent, 1)
    if "offset" in element:
        try:
            number = int(element["offset"]) + 1
        except (TypeError, ValueError):
            pass

    for item in element.get("elements", []):
        if not isinstance(item, dict):
            continue
        if item.get("type") != "rich_text_section":
            rctx.unknown_types.append(f"list_item:{item.get('type')}")
        item_text = _parse_rich_text_elements(item.get("elements", []), rctx)
        prefix = _rich_text_list_prefix(list_style, indent, number)
        list_items.append(prefix + item_text.strip("\n"))
        number += 1

    if list_style == "ordered":
        counters[indent] = number
    else:
        counters.pop(indent, None)


def _render_rich_text_quote(element: dict, rctx: _RenderContext) -> str:
    """Render a ``rich_text_quote`` element as a Chat ``>`` block quote."""
    quote_content = _parse_rich_text_elements(element.get("elements", []), rctx)
    lines = quote_content.strip("\n").split("\n")
    return "\n".join(f"> {line}" if line.strip() else ">" for line in lines)


def _parse_rich_text_block(block: dict, rctx: _RenderContext | None = None) -> str:
    """Render a ``rich_text`` block as mrkdwn.

    Handles rich_text_section, rich_text_list, rich_text_quote and
    rich_text_preformatted elements; other element types are recorded in
    ``rctx.unknown_types``.

    Args:
        block: A Slack rich_text block dict with an 'elements' field.
        rctx: The render context; a fresh one is used when omitted.

    Returns:
        Formatted text string from the rich text block.
    """
    rctx = rctx or _RenderContext()
    rich_text_parts: list[str] = []
    list_items: list[str] = []
    counters: dict[int, int] = {}

    def flush_list() -> None:
        if list_items:
            rich_text_parts.append("\n".join(list_items))
            list_items.clear()
        counters.clear()

    for element in block.get("elements", []):
        if not isinstance(element, dict):
            continue
        element_type = element.get("type")
        if element_type == "rich_text_list":
            _parse_rich_text_list(element, list_items, rctx, counters)
            continue
        flush_list()

        if element_type == "rich_text_section":
            content = _parse_rich_text_elements(element.get("elements", []), rctx)
            cleaned = content.rstrip("\n")
            if cleaned.strip():
                rich_text_parts.append(cleaned)
        elif element_type == "rich_text_quote":
            rich_text_parts.append(_render_rich_text_quote(element, rctx))
        elif element_type == "rich_text_preformatted":
            # Chat markup has no language fences, so ``language`` is dropped.
            code_text = _parse_rich_text_elements(
                element.get("elements", []), rctx, in_code=True
            ).strip("\n")
            if code_text.strip():
                rich_text_parts.append(f"```\n{code_text}\n```")
        else:
            rctx.unknown_types.append(f"rich_text:{element_type}")
            nested = element.get("elements")
            if isinstance(nested, list):
                rich_text_parts.append(_parse_rich_text_elements(nested, rctx))

    flush_list()
    return "\n\n".join(part for part in rich_text_parts if part.strip())


def _text_object(obj: object) -> str:
    """Return the text of a Block Kit text object (mrkdwn or plain_text)."""
    if isinstance(obj, dict):
        text = obj.get("text")
        if isinstance(text, str):
            return text
    return ""


# Standard Markdown (Slack ``markdown`` blocks) to mrkdwn.
_MD_CODE_RE = re.compile(r"```[^\n`]*\n?[\s\S]*?```|`[^`\n]+`")
_MD_FENCE_LANGUAGE_RE = re.compile(r"\A```[^\n`]*\n")
_MD_IMAGE_OR_LINK_RE = re.compile(r"!?\[([^\]\n]*)\]\(([^()\s]+)\)")
_MD_BOLD_ITALIC_RE = re.compile(r"\*\*\*(?!\s)(.+?)(?<!\s)\*\*\*")
_MD_ITALIC_STAR_RE = re.compile(r"(?<![*\w])\*(?![\s*])([^*\n]+?)(?<![\s*])\*(?![*\w])")
_MD_BOLD_RE = re.compile(r"(\*\*|__)(?!\s)(.+?)(?<!\s)\1")
_MD_STRIKE_RE = re.compile(r"~~(?!\s)(.+?)(?<!\s)~~")
_MD_HEADING_RE = re.compile(r"^#{1,6}[ \t]+(.+?)[ \t]*#*[ \t]*$", re.MULTILINE)


def _markdown_prose_to_mrkdwn(text: str) -> str:
    """Convert standard Markdown prose (no code) to escaped mrkdwn."""
    text = _escape_mrkdwn(text)
    text = _MD_IMAGE_OR_LINK_RE.sub(
        lambda m: f"<{m.group(2)}|{m.group(1)}>" if m.group(1) else f"<{m.group(2)}>",
        text,
    )
    text = _MD_BOLD_ITALIC_RE.sub(r"*_\1_*", text)
    # Single-star italics first, so the bold conversions below aren't re-read.
    text = _MD_ITALIC_STAR_RE.sub(r"_\1_", text)
    text = _MD_BOLD_RE.sub(r"*\2*", text)
    text = _MD_STRIKE_RE.sub(r"~\1~", text)
    return _MD_HEADING_RE.sub(r"*\1*", text)


def _markdown_to_mrkdwn(markdown: str) -> str:
    """Convert a Slack ``markdown`` block (standard Markdown) to mrkdwn.

    Converts links, images, headings, bold, italic and strikethrough; keeps
    code as is (dropping fence languages) and lists, quotes and tables as
    written, since Chat reads ``*``/``-`` bullets and ``>`` quotes too.

    Args:
        markdown: The block's Markdown text.

    Returns:
        Escaped mrkdwn.
    """
    out: list[str] = []
    last = 0
    for code in _MD_CODE_RE.finditer(markdown):
        out.append(_markdown_prose_to_mrkdwn(markdown[last : code.start()]))
        out.append(_MD_FENCE_LANGUAGE_RE.sub("```\n", _escape_mrkdwn(code.group(0))))
        last = code.end()
    out.append(_markdown_prose_to_mrkdwn(markdown[last:]))
    return "".join(out)


def _render_table(block: dict, rctx: _RenderContext) -> str:
    """Render a ``table`` block as pipe-separated rows."""
    lines = []
    for row in block.get("rows") or []:
        if not isinstance(row, list):
            continue
        cells = []
        for cell in row:
            if not isinstance(cell, dict):
                cells.append("")
                continue
            cell_type = cell.get("type")
            if cell_type == "rich_text":
                text = _parse_rich_text_block(cell, rctx)
            elif cell_type in ("raw_text", "raw_number"):
                value = cell.get("text", cell.get("value", ""))
                text = _escape_mrkdwn(str(value))
            else:
                rctx.unknown_types.append(f"table_cell:{cell_type}")
                text = _escape_mrkdwn(str(cell.get("text") or ""))
            cells.append(" ".join(text.split()))
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def _render_block(block: dict, rctx: _RenderContext) -> str:
    """Render one Block Kit block as mrkdwn.

    Args:
        block: A Slack block dict.
        rctx: The render context; unknown block types are recorded in it.

    Returns:
        The block's text, or ``""`` for blocks without text.
    """
    block_type = block.get("type")

    if block_type == "rich_text":
        return _parse_rich_text_block(block, rctx)
    if block_type == "section":
        texts = [_text_object(block.get("text"))]
        texts.extend(_text_object(f) for f in block.get("fields") or [])
        return "\n\n".join(t for t in texts if t.strip())
    if block_type == "header":
        header = _text_object(block.get("text")).strip()
        return f"*{header}*" if header else ""
    if block_type == "context":
        return " ".join(
            el.get("text", "")
            for el in block.get("elements", [])
            if isinstance(el, dict) and el.get("type") in ("mrkdwn", "plain_text")
        )
    if block_type == "divider":
        return "---"
    if block_type == "markdown":
        return _markdown_to_mrkdwn(str(block.get("text") or ""))
    if block_type == "table":
        return _render_table(block, rctx)
    if block_type == "video":
        title = _escape_mrkdwn(_text_object(block.get("title")))
        url = _escape_mrkdwn(
            str(block.get("title_url") or block.get("video_url") or "")
        )
        if url:
            return f"<{url}|{title}>" if title else f"<{url}>"
        return title
    if block_type in _NON_TEXT_BLOCKS:
        return ""

    rctx.unknown_types.append(f"block:{block_type}")
    return _text_object(block.get("text"))


def parse_slack_blocks(
    message: dict, custom_emoji_map: Mapping[str, object] | None = None
) -> str:
    """Render a Slack message as mrkdwn, from its blocks or its ``text``.

    Renders section, rich_text, header, context, divider, markdown and table
    blocks (and video titles), then the attachments: forwarded/shared
    messages and legacy attachments (pretext, title, text, fields, footer).
    Falls back to the mrkdwn ``text`` field when there are no blocks, when
    the blocks render nothing, or when any block or element type is unknown
    (so no words are silently dropped).

    Args:
        message: A Slack message dict with 'blocks' and/or 'text' fields.
        custom_emoji_map: ``config.custom_emoji_map``, for emoji elements.

    Returns:
        Escaped mrkdwn for :func:`convert_formatting`.
    """
    attachment_texts = _render_attachments(message, custom_emoji_map)
    text_field = str(message.get("text") or "")

    blocks = message.get("blocks")
    if not blocks or not isinstance(blocks, list):
        return _combine_with_attachments(text_field, attachment_texts)

    rctx = _RenderContext(
        labels=_harvest_labels(text_field), custom_emoji_map=custom_emoji_map
    )
    rendered = [_render_block(b, rctx) for b in blocks if isinstance(b, dict)]
    result = "\n\n".join(t.strip("\n") for t in rendered if t and t.strip())

    if rctx.unknown_types and text_field.strip():
        log_with_context(
            logging.DEBUG,
            "Unsupported Slack block/element types"
            f" {sorted(set(rctx.unknown_types))}; using the message text",
            message_ts=message.get("ts"),
        )
        result = text_field
    elif not result:
        result = text_field

    return _combine_with_attachments(result, attachment_texts)


# --- mrkdwn to Google Chat conversion -------------------------------------

# Slack's broadcast commands (``<!here>`` etc.), all mapped to Chat's @all.
_BROADCAST_COMMANDS = frozenset({"here", "channel", "everyone", "group"})

# A URI scheme at the start of a token: ``https:``, ``mailto:``, ``tel:``...
_URL_SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")

# Slack user / channel / user group IDs (``U0123``, ``W0123``, ``C0123``...).
_SLACK_ID_RE = re.compile(r"[A-Z][A-Z0-9]+")

# A literal "<" followed by a letter could open Chat markup (``<users/...>``,
# ``<https://...|...>``, ``<customEmojis/...>``).  A zero-width space keeps
# it text.
_CHAT_MARKUP_START_RE = re.compile(r"<(?=[A-Za-z])")
_ZERO_WIDTH_SPACE = "\N{ZERO WIDTH SPACE}"

# Characters that start a code span or a token in mrkdwn.
_MRKDWN_SPECIAL_RE = re.compile(r"[`<]")

# ``:name:`` or ``:name::skin-tone-2:`` (Slack emoji names are lowercase).
_SHORTCODE_RE = re.compile(r":([a-z0-9_+\-]+(?:::skin-tone-[2-6](?:-[2-6])?)?):")

# Bare URLs in prose; shortcodes inside them are part of the URL.
_BARE_URL_RE = re.compile(r"(?:\b[a-z][a-z0-9+.\-]*://|\bwww\.)[^\s<>]+", re.I)


@dataclass
class _ConversionContext:
    """Lookups and tracking shared by one :func:`convert_formatting` call."""

    user_map: Mapping[str, str]
    channel_id_to_name: Mapping[str, str]
    custom_emoji_map: Mapping[str, object] | None
    state: MigrationState | None
    unmapped_user_tracker: UnmappedUserTracker | None
    original_text: str


def _escape_mrkdwn(text: str) -> str:
    """Escape literal text for mrkdwn, as Slack does in a message's ``text``.

    Args:
        text: Literal text, e.g. from a rich-text ``text`` element.

    Returns:
        The text with ``&``, ``<`` and ``>`` replaced by HTML entities.
    """
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _decode_entities(text: str) -> str:
    """Decode the three HTML entities Slack uses in mrkdwn.

    Args:
        text: Escaped mrkdwn text without ``<...>`` tokens.

    Returns:
        The literal text.
    """
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def _replace_shortcodes(text: str, custom_map: Mapping[str, object] | None) -> str:
    """Replace ``:name:`` shortcodes that resolve to real emoji.

    A shortcode must stand on its own: not glued to a letter, digit, ``_`` or
    ``:`` on the left (unless it directly follows another shortcode, as in
    ``:smile::tada:``), nor to a letter, digit or ``_`` on the right.  So
    ``session:id:42``, ``key:ok:value``, ``fe80::a:b:1`` and ``12:30:00``
    are left alone.  Unresolved names (custom emoji) stay ``:name:``, renamed
    to their Chat custom emoji name when ``custom_emoji_map`` maps one.

    Args:
        text: Decoded prose (no code, URLs or tokens).
        custom_map: ``config.custom_emoji_map``.

    Returns:
        The text with emoji shortcodes replaced.
    """
    out: list[str] = []
    copied = 0
    previous_end = -1
    search_from = 0
    while True:
        match = _SHORTCODE_RE.search(text, search_from)
        if not match:
            break
        start, end = match.span()
        before = text[start - 1] if start else ""
        after = text[end] if end < len(text) else ""
        glued_left = before.isalnum() or before in ("_", ":")
        if (glued_left and start != previous_end) or after.isalnum() or after == "_":
            search_from = start + 1
            continue
        name = match.group(1)
        replacement = slack_emoji_to_unicode(name, custom_map)
        if replacement is None and custom_map:
            replacement = f":{chat_custom_emoji_name(name, custom_map)}:"
        if replacement is not None:
            out.append(text[copied:start])
            out.append(replacement)
            copied = end
        previous_end = search_from = end
    out.append(text[copied:])
    return "".join(out)


def _emojize_prose(text: str, custom_map: Mapping[str, object] | None) -> str:
    """Replace emoji shortcodes in prose, leaving bare URLs untouched."""
    if ":" not in text:
        return text
    out: list[str] = []
    last = 0
    for url in _BARE_URL_RE.finditer(text):
        out.append(_replace_shortcodes(text[last : url.start()], custom_map))
        out.append(url.group(0))
        last = url.end()
    out.append(_replace_shortcodes(text[last:], custom_map))
    return "".join(out)


def _render_prose(text: str, ctx: _ConversionContext) -> str:
    """Render escaped literal mrkdwn prose as Chat text.

    Entities are decoded and emoji shortcodes replaced.  A decoded ``<`` that
    could open Chat markup is defused, so text a user typed as
    ``&lt;https://a|b&gt;`` or ``&lt;users/x&gt;`` stays text instead of
    becoming a link or a mention.

    Args:
        text: Escaped literal text (no ``<...>`` tokens or code).
        ctx: Conversion context (for the custom emoji map).

    Returns:
        Text that is safe to embed in a Chat message.
    """
    decoded = _emojize_prose(_decode_entities(text), ctx.custom_emoji_map)
    return _CHAT_MARKUP_START_RE.sub("<" + _ZERO_WIDTH_SPACE, decoded)


def _render_text(text: str, ctx: _ConversionContext, in_code: bool) -> str:
    """Render literal text: decoded only inside code, full prose otherwise."""
    return _decode_entities(text) if in_code else _render_prose(text, ctx)


def _find_token_end(text: str, start: int) -> int:
    """Return the index of the ``>`` closing the ``<`` at *start*, or -1.

    Slack escapes every literal ``<`` and ``>``, so a raw ``<`` always opens a
    token; a ``<`` that isn't followed by a well-formed token (no ``>``, a
    nested ``<``, a newline or leading whitespace) is treated as text.
    """
    end = text.find(">", start + 1)
    if end == -1:
        return -1
    inner = text[start + 1 : end]
    if not inner or inner[0].isspace() or "<" in inner or "\n" in inner:
        return -1
    return end


def _find_code_end(text: str, start: int) -> tuple[int, str]:
    """Find the end of the code span or block opened at *start*.

    Args:
        text: The mrkdwn text.
        start: Index of a backtick.

    Returns:
        ``(end, fence)``: *fence* is three backticks for a block or one for
        inline code; *end* is the index of the closing fence, or -1 when the
        backticks don't open code.  Inline code must close on the same line
        and not be empty.
    """
    if text.startswith("```", start):
        return text.find("```", start + 3), "```"
    end = text.find("`", start + 1)
    newline = text.find("\n", start + 1)
    if end == -1 or end == start + 1 or (newline != -1 and newline < end):
        return -1, "`"
    return end, "`"


def _track_unmapped_mention(slack_user_id: str, ctx: _ConversionContext) -> None:
    """Log (and record, when a tracker is available) an unmapped mention."""
    state = ctx.state
    if ctx.unmapped_user_tracker and state is not None:
        current_channel = state.context.current_channel or "unknown"
        current_ts = state.context.current_message_ts or "unknown"
        ctx.unmapped_user_tracker.track_unmapped_mention(
            slack_user_id, current_channel, current_ts, ctx.original_text
        )
        log_with_context(
            logging.ERROR,
            f"Could not map Slack user ID: {slack_user_id} in message mention (channel: {current_channel})",
            user_id=slack_user_id,
            channel=current_channel,
            message_ts=current_ts,
        )
    else:
        log_with_context(
            logging.WARNING, f"Could not map Slack user ID: {slack_user_id}"
        )


def _render_user_token(
    user_id: str, label: str, ctx: _ConversionContext, in_code: bool
) -> str:
    """Render ``<@U123>`` / ``<@U123|alice>`` as a Chat mention.

    Unmapped users become ``@alice`` (or ``@U123`` without a label) and are
    tracked as unmapped mentions.  Inside code the mention is plain text.
    """
    if not _SLACK_ID_RE.fullmatch(user_id):
        return _render_text(label or f"@{user_id}", ctx, in_code)
    gchat_user_id = ctx.user_map.get(user_id)
    if in_code:
        return "@" + _decode_entities(label.lstrip("@") or gchat_user_id or user_id)
    if gchat_user_id:
        return f"<users/{gchat_user_id}>"
    _track_unmapped_mention(user_id, ctx)
    return "@" + _render_prose(label.lstrip("@") or user_id, ctx)


def _render_channel_token(
    channel_id: str, label: str, ctx: _ConversionContext, in_code: bool
) -> str:
    """Render ``<#C123|name>``, ``<#C123|>`` and ``<#C123>`` as ``#name``."""
    name = label or ctx.channel_id_to_name.get(channel_id) or channel_id
    return "#" + _render_text(name.lstrip("#"), ctx, in_code)


def format_epoch_utc(epoch: object, fmt: str = "%Y-%m-%d %H:%M UTC") -> str | None:
    """Format a Unix timestamp in UTC.

    Args:
        epoch: Seconds since the epoch (number or numeric string, e.g. a
            Slack ``ts``).
        fmt: ``strftime`` format; it should name the zone.

    Returns:
        The formatted time, or ``None`` if *epoch* isn't a valid timestamp.
    """
    try:
        moment = datetime.fromtimestamp(float(str(epoch)), tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None
    return moment.strftime(fmt)


def _render_special_token(
    command: str, label: str, inner: str, ctx: _ConversionContext, in_code: bool
) -> str:
    """Render ``<!here>``, ``<!subteam^S1|@eng>``, ``<!date^...|fallback>``..."""
    name, _, args = command.partition("^")
    if name in _BROADCAST_COMMANDS:
        return f"@{name}" if in_code else "<users/all>"
    if name == "subteam":
        handle = label or args.split("^", 1)[0]
        if not handle:
            return "@subteam"
        return _render_text(
            handle if handle.startswith("@") else f"@{handle}", ctx, in_code
        )
    if name == "date":
        if label:
            return _render_text(label, ctx, in_code)
        return format_epoch_utc(args.split("^", 1)[0]) or ""
    return _render_text(label or inner, ctx, in_code)


def _render_link_token(
    url: str, label: str, ctx: _ConversionContext, in_code: bool
) -> str:
    """Render ``<https://...|label>``, ``<mailto:...|...>``, ``<tel:...>``.

    Returns the bare URL when there is no label or it equals the URL, else
    Chat's ``<url|label>``.  Inside code, the label (or URL) as plain text.
    """
    url = _decode_entities(url)
    if in_code:
        return _decode_entities(label) or url
    if not label or _decode_entities(label) == url:
        return url
    # A ">" would end Chat's link early, and "<" could nest markup.
    safe_label = (
        _render_prose(label, ctx)
        .replace(">", "\N{SINGLE RIGHT-POINTING ANGLE QUOTATION MARK}")
        .replace("<", "\N{SINGLE LEFT-POINTING ANGLE QUOTATION MARK}")
    )
    return f"<{url}|{safe_label}>"


def _render_token(inner: str, ctx: _ConversionContext, in_code: bool = False) -> str:
    """Render the inside of one Slack ``<...>`` token as Chat text.

    Args:
        inner: The token without its angle brackets, still escaped.
        ctx: Lookups and tracking for this conversion.
        in_code: True inside a code span or block, where Chat shows markup
            literally, so tokens become plain text.

    Returns:
        The Chat markup or text for the token.
    """
    target, _, label = inner.partition("|")
    if target.startswith("@"):
        return _render_user_token(target[1:], label, ctx, in_code)
    if target.startswith("#"):
        return _render_channel_token(target[1:], label, ctx, in_code)
    if target.startswith("!"):
        return _render_special_token(target[1:], label, inner, ctx, in_code)
    if _URL_SCHEME_RE.match(target):
        return _render_link_token(target, label, ctx, in_code)
    # Unknown token: keep what a reader would have seen.
    return _render_text(label or inner, ctx, in_code)


def _convert_segment(text: str, ctx: _ConversionContext, in_code: bool) -> str:
    """Convert mrkdwn without code spans: tokens plus literal text."""
    out: list[str] = []
    literal_start = 0
    pos = 0
    while True:
        pos = text.find("<", pos)
        if pos == -1:
            break
        end = _find_token_end(text, pos)
        if end == -1:
            pos += 1
            continue
        out.append(_render_text(text[literal_start:pos], ctx, in_code))
        out.append(_render_token(text[pos + 1 : end], ctx, in_code))
        pos = literal_start = end + 1
    out.append(_render_text(text[literal_start:], ctx, in_code))
    return "".join(out)


def _convert_mrkdwn(text: str, ctx: _ConversionContext) -> str:
    """Convert escaped Slack mrkdwn into Chat markup.

    Code spans and blocks are found first; inside them only entities are
    decoded (and tokens become plain text), so emoji shortcodes and markup
    there stay exactly as typed.
    """
    out: list[str] = []
    prose_start = 0
    pos = 0
    while True:
        match = _MRKDWN_SPECIAL_RE.search(text, pos)
        if not match:
            break
        pos = match.start()
        if text[pos] == "<":
            # Skip whole tokens so a backtick inside a link label can't open code.
            end = _find_token_end(text, pos)
            pos = end + 1 if end != -1 else pos + 1
            continue
        end, fence = _find_code_end(text, pos)
        if end == -1:
            pos += len(fence)
            continue
        out.append(_convert_segment(text[prose_start:pos], ctx, in_code=False))
        code = text[pos + len(fence) : end]
        out.append(fence + _convert_segment(code, ctx, in_code=True) + fence)
        pos = prose_start = end + len(fence)
    out.append(_convert_segment(text[prose_start:], ctx, in_code=False))
    return "".join(out)


def convert_formatting(
    text: str,
    user_map: Mapping[str, str],
    state: MigrationState | None = None,
    unmapped_user_tracker: UnmappedUserTracker | None = None,
    channel_id_to_name: Mapping[str, str] | None = None,
    custom_emoji_map: Mapping[str, object] | None = None,
) -> str:
    """Convert Slack mrkdwn to Google Chat markup (``MARKUP_SYNTAX_CHAT``).

    Slack ``<...>`` tokens are parsed on the still-escaped text, and HTML
    entities (``&lt; &gt; &amp;``) are decoded only in the literal text in
    between, so text a user typed as ``<...>`` never becomes a live link or
    mention.  Tokens are converted as follows:

    - ``<@U123>``, ``<@U123|name>``, ``<@W123>`` -> ``<users/{email}>``, or
      ``@name`` / ``@U123`` when the user is unmapped (tracked)
    - ``<#C123|name>``, ``<#C123|>``, ``<#C123>`` -> ``#name`` (via
      *channel_id_to_name*), or ``#C123`` when unknown
    - ``<!here>``, ``<!channel>``, ``<!everyone>`` -> ``<users/all>``
    - ``<!subteam^S123|@eng>`` -> ``@eng``; ``<!date^...|fallback>`` ->
      the fallback
    - ``<https://...|label>``, ``<mailto:...|...>``, ``<tel:...|...>`` ->
      ``<url|label>``, or the bare URL when there is no label or it equals
      the URL
    - anything else -> its label, or its inner text

    Emoji shortcodes (``:tada:``) are replaced only in prose: never inside
    code spans, code blocks, URLs or tokens, and only when the name is a
    real emoji.  Inside code, tokens become plain text.

    Args:
        text: The Slack mrkdwn text to convert (escaped, as in ``text``).
        user_map: Slack user ID -> Google Chat user ID or email.
        state: Optional MigrationState for context
            (``context.current_channel``, ``context.current_message_ts``).
        unmapped_user_tracker: Optional tracker for unmapped user mentions.
        channel_id_to_name: Slack channel ID -> channel name, used for channel
            references without a label.
        custom_emoji_map: ``config.custom_emoji_map`` (Slack emoji name ->
            Unicode or ``:name:``).

    Returns:
        The text formatted for Google Chat.
    """
    if not text:
        return ""

    ctx = _ConversionContext(
        user_map=user_map,
        channel_id_to_name=channel_id_to_name or {},
        custom_emoji_map=custom_emoji_map,
        state=state,
        unmapped_user_tracker=unmapped_user_tracker,
        original_text=text,
    )
    return _convert_mrkdwn(text, ctx)


# --- Message size ---------------------------------------------------------

_FENCE = "```"
_FENCE_REOPEN = "```\n"
_FENCE_CLOSE = "\n```"
# Smallest budget that leaves room for fence markers and some text.
_MIN_SPLIT_BYTES = 64
_WHITESPACE_RE = re.compile(r"[ \t]+")


def _utf8_len(text: str) -> int:
    """Return the size of *text* in UTF-8 bytes."""
    return len(text.encode("utf-8"))


def _chars_fitting(text: str, budget: int) -> int:
    """Return how many leading characters of *text* fit in *budget* bytes."""
    head = text[:budget].encode("utf-8")[:budget]
    # A multi-byte character cut in half is dropped, never split.
    return len(head.decode("utf-8", errors="ignore"))


def _safe_hard_cut(text: str, cut: int) -> int:
    """Move a hard cut back so it doesn't split an emoji or a code fence."""
    while cut > 1 and (
        is_emoji_modifier_or_joiner(text[cut])
        or text[cut - 1] == "\N{ZERO WIDTH JOINER}"
        or (text[cut - 1] == "`" and text[cut] == "`")
    ):
        cut -= 1
    return cut


def _choose_split(text: str, cut: int) -> tuple[int, int]:
    """Pick where to split *text*, at or before character index *cut*.

    Prefers a paragraph break, then a line break, then whitespace in the
    second half of the window, and otherwise any line break, before falling
    back to a hard cut.  The separator itself is dropped.

    Returns:
        ``(end, next_start)``: the part is ``text[:end]`` and the rest
        starts at ``text[next_start:]``.
    """
    window = text[:cut]
    half = cut // 2
    paragraph = window.rfind("\n\n")
    if paragraph >= half and paragraph > 0:
        return paragraph, paragraph + 2
    line = window.rfind("\n")
    if line >= half and line > 0:
        return line, line + 1
    spaces = [m for m in _WHITESPACE_RE.finditer(window) if m.start() > 0]
    if spaces and spaces[-1].start() >= half:
        return spaces[-1].start(), spaces[-1].end()
    if line > 0:
        return line, line + 1
    hard = _safe_hard_cut(text, cut)
    return hard, hard


def _avoid_splitting_token(text: str, end: int, next_start: int) -> tuple[int, int]:
    """Move a split back to before a ``<...>`` token it would cut in two."""
    opening = text.rfind("<", 0, end)
    if opening > 0 and opening > text.rfind(">", 0, end):
        closing = text.find(">", end)
        if closing != -1 and "\n" not in text[opening:closing]:
            return opening, opening
    return end, next_start


def split_message_text(text: str, max_bytes: int = MESSAGE_MAX_BYTES) -> list[str]:
    """Split message text into parts that fit Chat's message size limit.

    Google Chat rejects a message over 32,000 bytes.  Parts are measured in
    UTF-8 bytes and split at a paragraph break where possible, else a line
    break, else whitespace; a hard cut never splits a character, an emoji
    sequence or a ``<...>`` link/mention token.  When a split falls inside a
    code block, the part closes the fence and the next part reopens it.

    Args:
        text: The Chat message text.
        max_bytes: Budget per part.  Pass less than ``MESSAGE_MAX_BYTES`` to
            leave room for what the caller adds (attribution prefix, edit
            marker, Drive links, the rest of the request body).

    Returns:
        ``[text]`` when it fits, otherwise the parts in order, each at most
        *max_bytes* bytes.

    Raises:
        ValueError: If *max_bytes* is too small to split sensibly.
    """
    if max_bytes < _MIN_SPLIT_BYTES:
        raise ValueError(
            f"max_bytes must be at least {_MIN_SPLIT_BYTES}, got {max_bytes}"
        )
    if _utf8_len(text) <= max_bytes:
        return [text]

    parts: list[str] = []
    rest = text
    in_code = False
    while rest:
        prefix = _FENCE_REOPEN if in_code else ""
        if _utf8_len(prefix) + _utf8_len(rest) <= max_bytes:
            parts.append(prefix + rest)
            break
        budget = max_bytes - _utf8_len(prefix) - _utf8_len(_FENCE_CLOSE)
        end, next_start = _choose_split(rest, _chars_fitting(rest, budget))
        end, next_start = _avoid_splitting_token(rest, end, next_start)
        if end <= 0:
            # Nothing but a token fits; cut it rather than loop forever.
            end = next_start = _safe_hard_cut(rest, _chars_fitting(rest, budget))
        piece = rest[:end]
        if in_code ^ (piece.count(_FENCE) % 2 == 1):
            parts.append(prefix + piece + _FENCE_CLOSE)
            in_code = True
        else:
            parts.append(prefix + piece)
            in_code = False
        rest = rest[next_start:]
    return parts
