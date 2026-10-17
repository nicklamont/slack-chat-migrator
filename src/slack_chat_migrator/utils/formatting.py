"""
Message formatting utilities for converting Slack messages to Google Chat format.

Conversion happens in two steps:

1. :func:`parse_slack_blocks` renders a Slack message (Block Kit ``blocks``,
   attachments or the ``text`` field) as Slack *mrkdwn*.  Literal text from
   rich-text elements is entity-escaped (``&amp; &lt; &gt;``), exactly like
   Slack's own ``text`` field, so the result is well-formed mrkdwn.
2. :func:`convert_formatting` converts that mrkdwn into Google Chat markup
   (``MARKUP_SYNTAX_CHAT``): it parses ``<…>`` tokens on the escaped text and
   decodes entities only in the literal text between them.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from slack_chat_migrator.core.state import MigrationState
    from slack_chat_migrator.utils.user_validation import UnmappedUserTracker

import emoji

# This assumes a standard logging setup. If you don't have one,
# you can replace `from slack_chat_migrator.utils.logging import logger`
# with `import logging; logger = logging.getLogger(__name__)`
from slack_chat_migrator.utils.logging import log_with_context


def _parse_rich_text_elements(elements: list[dict]) -> str:
    """
    Helper function to parse a list of rich text elements from Slack's block kit format.

    This function processes different types of rich text elements (text, links, emojis, user mentions)
    and applies appropriate styling (bold, italic, strikethrough) based on the element's style
    attributes.

    Args:
        elements: A list of dictionaries representing rich text elements from Slack's block kit

    Returns:
        A string with the processed rich text content including all formatting
    """

    def _apply_styles(text: str, style: dict) -> str:
        """
        Applies markdown styling to a string based on a style object.

        Takes a style dictionary containing boolean flags for different styling options
        (bold, italic, strikethrough) and applies the appropriate markdown formatting
        to the text.

        Args:
            text: The text content to style
            style: A dictionary with style flags (e.g., {'bold': True, 'italic': True})

        Returns:
            The text with markdown styling applied
        """
        if not style:
            return text

        # Apply styles in the correct order: bold -> italic -> strikethrough
        # This ensures proper nesting of markdown markers
        result = text

        if style.get("bold"):
            result = f"*{result}*"
        if style.get("italic"):
            result = f"_{result}_"
        if style.get("strike"):
            result = f"~{result}~"

        return result

    output_parts: list[str] = []
    for el in elements:
        el_type = el.get("type")
        style = el.get("style", {})

        if el_type == "text":
            # Rich-text text is literal; escape it so it stays text in mrkdwn.
            text_content = _escape_mrkdwn(el.get("text", ""))

            # Handle the simple cases first
            if not text_content:
                output_parts.append(text_content)
            elif not style:
                # No styling to apply - preserve text exactly
                output_parts.append(text_content)
            elif not text_content.strip():
                # Text is all whitespace - preserve exactly (no styling possible)
                output_parts.append(text_content)
            else:
                # We have both content and styling - apply styles while preserving whitespace
                # Find the span of actual content (first to last non-whitespace character)
                first_char = next(
                    i for i, c in enumerate(text_content) if not c.isspace()
                )
                last_char = next(
                    i for i, c in enumerate(reversed(text_content)) if not c.isspace()
                )
                last_char = len(text_content) - 1 - last_char

                leading_whitespace = text_content[:first_char]
                content = text_content[first_char : last_char + 1]
                trailing_whitespace = text_content[last_char + 1 :]

                styled_content = _apply_styles(content, style)
                output_parts.append(
                    f"{leading_whitespace}{styled_content}{trailing_whitespace}"
                )

        elif el_type == "link":
            url = _escape_mrkdwn(el.get("url", ""))
            text = _escape_mrkdwn(el.get("text", "")) or url
            # Create the base link markdown
            link_markdown = f"<{url}|{text}>"
            # Apply styles to the entire link
            output_parts.append(_apply_styles(link_markdown, style))

        elif el_type == "emoji":
            output_parts.append(f":{el.get('name', '')}:")

        elif el_type == "user":
            user_mention = f"<@{el.get('user_id', '')}>"
            # Apply styles to the user mention if any are specified
            # Note that Google Chat does not support bold or italic for user mentions
            output_parts.append(_apply_styles(user_mention, style))

    return "".join(output_parts)


def _extract_forwarded_messages(message: dict) -> list[str]:
    """Extract forwarded/shared message content from Slack attachments.

    Processes attachments that represent forwarded messages, extracting
    text content with author and timestamp metadata. Converts bullet
    formatting to Google Chat compatible format.

    Args:
        message: A Slack message dict with an optional 'attachments' field.

    Returns:
        List of formatted forwarded message strings.
    """
    forwarded_texts: list[str] = []
    for attachment in message.get("attachments", []):
        if not (attachment.get("is_share") or attachment.get("is_msg_unfurl")):
            continue

        author_info = ""
        if attachment.get("author_name"):
            author_info = f" from {attachment['author_name']}"
        elif attachment.get("author_subname"):
            author_info = f" from {attachment['author_subname']}"

        timestamp_info = ""
        if attachment.get("ts"):
            try:
                timestamp = float(attachment["ts"])
                readable_time = datetime.fromtimestamp(timestamp).strftime(
                    "%B %d, %Y at %I:%M %p"
                )
                timestamp_info = f" (originally sent {readable_time})"
            except (ValueError, OSError):
                timestamp_info = f" (originally sent at {attachment['ts']})"

        # Prefer rich message_blocks over plain text
        forwarded_text = ""
        if "message_blocks" in attachment:
            for msg_block in attachment.get("message_blocks", []):
                if "message" in msg_block and "blocks" in msg_block["message"]:
                    forwarded_text = parse_slack_blocks(msg_block["message"])
                    break

        if not forwarded_text:
            forwarded_text = attachment.get("text") or attachment.get("fallback") or ""

        if not forwarded_text.strip():
            continue

        forwarded_text = _convert_bullets_to_gchat(forwarded_text.strip())
        header = f"*Forwarded message{author_info}{timestamp_info}:*"
        forwarded_texts.append(f"{header}\n{forwarded_text}")

    return forwarded_texts


def _convert_bullets_to_gchat(text: str) -> str:
    """Convert Slack bullet formatting to Google Chat compatible bullets.

    Args:
        text: Text that may contain bullet-point lines.

    Returns:
        Text with bullets converted to Google Chat format.
    """
    lines = text.split("\n")
    improved_lines: list[str] = []

    for line in lines:
        stripped = line.strip()
        if re.match(r"^\s+•", line):
            indent_level = len(line) - len(line.lstrip())
            content = stripped[1:].strip() if stripped.startswith("•") else stripped
            if indent_level <= 4:
                improved_lines.append(f"* {content}")
            elif indent_level <= 8:
                indent_spaces = " " * (4 + 5)
                improved_lines.append(f"{indent_spaces}◦ {content}")
            else:
                level = 2 if indent_level <= 12 else 3
                indent_spaces = " " * (4 + (5 * level))
                improved_lines.append(f"{indent_spaces}▪ {content}")
        elif stripped.startswith("•"):
            content = stripped[1:].strip() if len(stripped) > 1 else ""
            improved_lines.append(f"* {content}")
        else:
            improved_lines.append(line)

    return "\n".join(improved_lines)


def _combine_with_forwarded(main_text: str, forwarded_texts: list[str]) -> str:
    """Combine main message text with forwarded message content.

    Args:
        main_text: The primary message text.
        forwarded_texts: List of formatted forwarded message strings.

    Returns:
        Combined text, preferring forwarded content when main text is empty.
    """
    if not main_text.strip() and forwarded_texts:
        return "\n\n".join(forwarded_texts)
    if main_text.strip() and forwarded_texts:
        return main_text + "\n\n" + "\n\n".join(forwarded_texts)
    return main_text


def _parse_rich_text_block(block: dict) -> str:
    """Parse a single rich_text block into formatted text.

    Handles rich_text_section, rich_text_list, rich_text_quote,
    and rich_text_preformatted element types.

    Args:
        block: A Slack rich_text block dict with an 'elements' field.

    Returns:
        Formatted text string from the rich text block.
    """
    rich_text_parts: list[str] = []
    list_items: list[str] = []

    for element in block.get("elements", []):
        element_type = element.get("type")

        if element_type == "rich_text_section":
            if list_items:
                rich_text_parts.append("\n".join(list_items))
                list_items = []
            content = _parse_rich_text_elements(element.get("elements", []))
            cleaned = content.rstrip("\n")
            if cleaned.strip():
                rich_text_parts.append(cleaned)

        elif element_type == "rich_text_list":
            _parse_rich_text_list(element, list_items)

        elif element_type == "rich_text_quote":
            if list_items:
                rich_text_parts.append("\n".join(list_items))
                list_items = []
            quote_content = _parse_rich_text_elements(element.get("elements", []))
            paragraphs = quote_content.strip().split("\n\n")
            italicized = [f"_{p.strip()}_" for p in paragraphs if p.strip()]
            rich_text_parts.append("\n\n".join(italicized))

        elif element_type == "rich_text_preformatted":
            if list_items:
                rich_text_parts.append("\n".join(list_items))
                list_items = []
            code_text = _parse_rich_text_elements(element.get("elements", []))
            rich_text_parts.append(f"```\n{code_text}\n```")

    if list_items:
        rich_text_parts.append("\n".join(list_items))

    return "\n\n".join(part for part in rich_text_parts if part.strip())


def _parse_rich_text_list(element: dict, list_items: list[str]) -> None:
    """Parse a rich_text_list element and append items to list_items.

    Args:
        element: A Slack rich_text_list element dict.
        list_items: Accumulator list to append formatted items to.
    """
    list_style = element.get("style", "bullet")
    indent_level = element.get("indent", 0)
    prefix = "*"  # Defensive default; overwritten for bullet lists below

    if list_style == "bullet":
        if indent_level == 0:
            prefix = "*"
        elif indent_level == 1:
            prefix = "◦"
        else:
            prefix = "▪"

    for i, item in enumerate(element.get("elements", [])):
        item_text = _parse_rich_text_elements(item.get("elements", []))
        if list_style == "bullet":
            if indent_level == 0:
                list_items.append(f"* {item_text}")
            else:
                indent_spaces = " " * (4 + (5 * indent_level))
                list_items.append(f"{indent_spaces}{prefix} {item_text}")
        else:
            if indent_level == 0:
                list_items.append(f"{i + 1}. {item_text}")
            else:
                indent_spaces = " " * (4 + (5 * indent_level))
                list_items.append(f"{indent_spaces}{i + 1}. {item_text}")


def _parse_single_block(block: dict, texts: list[str]) -> None:
    """Parse a single Slack block and append its text content.

    Args:
        block: A single Slack block dict.
        texts: Accumulator list to append extracted text to.
    """
    block_type = block.get("type")

    if block_type == "section":
        if text_obj := block.get("text"):
            texts.append(text_obj.get("text", ""))
        for field in block.get("fields", []):
            if field and isinstance(field, dict):
                texts.append(field.get("text", ""))

    elif block_type == "rich_text":
        rich_text = _parse_rich_text_block(block)
        if rich_text:
            texts.append(rich_text)

    elif block_type == "header":
        if text_obj := block.get("text"):
            texts.append(f"*{text_obj.get('text', '')}*")

    elif block_type == "context":
        context_texts = [
            el.get("text", "")
            for el in block.get("elements", [])
            if el.get("type") in ("mrkdwn", "plain_text")
        ]
        if context_texts:
            texts.append(" ".join(context_texts))

    elif block_type == "divider":
        texts.append("---")


def parse_slack_blocks(message: dict) -> str:
    """Parse Slack block kit format from a message to extract rich text content.

    Handles section, rich_text, header, context, and divider block types.
    Also checks for forwarded/shared message content in the attachments array.

    Args:
        message: A Slack message dict with 'blocks' and/or 'text' fields.

    Returns:
        Formatted text content from the message blocks, or the raw text field
        if no blocks are present or no content could be extracted.
    """
    forwarded_texts = _extract_forwarded_messages(message)

    if "blocks" not in message or not message["blocks"]:
        return _combine_with_forwarded(message.get("text", ""), forwarded_texts)

    texts: list[str] = []
    for block in message.get("blocks", []):
        _parse_single_block(block, texts)

    result = "\n\n".join(text.strip() for text in texts if text and text.strip())

    if not result:
        return _combine_with_forwarded(str(message.get("text", "")), forwarded_texts)

    if forwarded_texts:
        result = result + "\n\n" + "\n\n".join(forwarded_texts)

    return result


# --- mrkdwn → Google Chat conversion -------------------------------------

# Slack's broadcast commands (``<!here>`` etc.), all mapped to Chat's @all.
_BROADCAST_COMMANDS = frozenset({"here", "channel", "everyone", "group"})

# A URI scheme at the start of a token: ``https:``, ``mailto:``, ``tel:``...
_URL_SCHEME_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.\-]*:")

# Slack user / channel / user group IDs (``U0123``, ``W0123``, ``C0123``...).
_SLACK_ID_RE = re.compile(r"[A-Z][A-Z0-9]+")

# A literal "<" followed by a letter could open Chat markup (``<users/…>``,
# ``<https://…|…>``, ``<customEmojis/…>``).  A zero-width space keeps it text.
_CHAT_MARKUP_START_RE = re.compile(r"<(?=[A-Za-z])")
_ZERO_WIDTH_SPACE = "\N{ZERO WIDTH SPACE}"


@dataclass
class _ConversionContext:
    """Lookups and tracking shared by one :func:`convert_formatting` call."""

    user_map: Mapping[str, str]
    channel_id_to_name: Mapping[str, str]
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
        text: Escaped mrkdwn text without ``<…>`` tokens.

    Returns:
        The literal text.
    """
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def _render_literal(text: str) -> str:
    """Render escaped literal mrkdwn text as Chat text.

    Entities are decoded, and a decoded ``<`` that could open Chat markup is
    defused, so text a user typed as ``&lt;https://a|b&gt;`` or
    ``&lt;users/x&gt;`` stays text instead of becoming a link or a mention.

    Args:
        text: Escaped literal text (no ``<…>`` tokens).

    Returns:
        Text that is safe to embed in a Chat message.
    """
    decoded = _decode_entities(text)
    decoded = _CHAT_MARKUP_START_RE.sub("<" + _ZERO_WIDTH_SPACE, decoded)
    return str(emoji.emojize(decoded, language="alias"))


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


def _render_user_token(user_id: str, label: str, ctx: _ConversionContext) -> str:
    """Render ``<@U123>`` / ``<@U123|alice>`` as a Chat mention.

    Unmapped users become ``@alice`` (or ``@U123`` without a label) and are
    tracked as unmapped mentions.
    """
    if not _SLACK_ID_RE.fullmatch(user_id):
        return _render_literal(label or f"@{user_id}")
    gchat_user_id = ctx.user_map.get(user_id)
    if gchat_user_id:
        return f"<users/{gchat_user_id}>"
    _track_unmapped_mention(user_id, ctx)
    display = label.lstrip("@") or user_id
    return "@" + _render_literal(display)


def _render_channel_token(channel_id: str, label: str, ctx: _ConversionContext) -> str:
    """Render ``<#C123|name>``, ``<#C123|>`` and ``<#C123>`` as ``#name``."""
    name = label or ctx.channel_id_to_name.get(channel_id) or channel_id
    return "#" + _render_literal(name.lstrip("#"))


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


def _render_special_token(command: str, label: str, inner: str) -> str:
    """Render ``<!here>``, ``<!subteam^S1|@eng>``, ``<!date^…|fallback>``…"""
    name, _, args = command.partition("^")
    if name in _BROADCAST_COMMANDS:
        return "<users/all>"
    if name == "subteam":
        handle = label or args.split("^", 1)[0]
        if not handle:
            return "@subteam"
        return _render_literal(handle if handle.startswith("@") else f"@{handle}")
    if name == "date":
        if label:
            return _render_literal(label)
        return format_epoch_utc(args.split("^", 1)[0]) or ""
    return _render_literal(label or inner)


def _render_link_token(url: str, label: str) -> str:
    """Render ``<https://…|label>``, ``<mailto:…|…>``, ``<tel:…>`` for Chat.

    Returns the bare URL when there is no label or it equals the URL, else
    Chat's ``<url|label>``.
    """
    url = _decode_entities(url)
    if not label or _decode_entities(label) == url:
        return url
    # A ">" would end Chat's link early, and "<" could nest markup.
    safe_label = (
        _render_literal(label)
        .replace(">", "\N{SINGLE RIGHT-POINTING ANGLE QUOTATION MARK}")
        .replace("<", "\N{SINGLE LEFT-POINTING ANGLE QUOTATION MARK}")
    )
    return f"<{url}|{safe_label}>"


def _render_token(inner: str, ctx: _ConversionContext) -> str:
    """Render the inside of one Slack ``<…>`` token as Chat text.

    Args:
        inner: The token without its angle brackets, still escaped.
        ctx: Lookups and tracking for this conversion.

    Returns:
        The Chat markup or text for the token.
    """
    target, _, label = inner.partition("|")
    if target.startswith("@"):
        return _render_user_token(target[1:], label, ctx)
    if target.startswith("#"):
        return _render_channel_token(target[1:], label, ctx)
    if target.startswith("!"):
        return _render_special_token(target[1:], label, inner)
    if _URL_SCHEME_RE.match(target):
        return _render_link_token(target, label)
    # Unknown token: keep what a reader would have seen.
    return _render_literal(label or inner)


def _convert_mrkdwn(text: str, ctx: _ConversionContext) -> str:
    """Convert escaped Slack mrkdwn into Chat markup, token by token."""
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
        out.append(_render_literal(text[literal_start:pos]))
        out.append(_render_token(text[pos + 1 : end], ctx))
        pos = literal_start = end + 1
    out.append(_render_literal(text[literal_start:]))
    return "".join(out)


def convert_formatting(
    text: str,
    user_map: Mapping[str, str],
    state: MigrationState | None = None,
    unmapped_user_tracker: UnmappedUserTracker | None = None,
    channel_id_to_name: Mapping[str, str] | None = None,
) -> str:
    """Convert Slack mrkdwn to Google Chat markup (``MARKUP_SYNTAX_CHAT``).

    Slack ``<…>`` tokens are parsed on the still-escaped text, and HTML
    entities (``&lt; &gt; &amp;``) are decoded only in the literal text in
    between, so text a user typed as ``<…>`` never becomes a live link or
    mention.  Tokens are converted as follows:

    - ``<@U123>``, ``<@U123|name>``, ``<@W123>`` → ``<users/{email}>``, or
      ``@name`` / ``@U123`` when the user is unmapped (tracked)
    - ``<#C123|name>``, ``<#C123|>``, ``<#C123>`` → ``#name`` (via
      *channel_id_to_name*), or ``#C123`` when unknown
    - ``<!here>``, ``<!channel>``, ``<!everyone>`` → ``<users/all>``
    - ``<!subteam^S123|@eng>`` → ``@eng``; ``<!date^…|fallback>`` → fallback
    - ``<https://…|label>``, ``<mailto:…|…>``, ``<tel:…|…>`` → ``<url|label>``,
      or the bare URL when there is no label or it equals the URL
    - anything else → its label, or its inner text

    Args:
        text: The Slack mrkdwn text to convert (escaped, as in ``text``).
        user_map: Slack user ID → Google Chat user ID or email.
        state: Optional MigrationState for context
            (``context.current_channel``, ``context.current_message_ts``).
        unmapped_user_tracker: Optional tracker for unmapped user mentions.
        channel_id_to_name: Slack channel ID → channel name, used for channel
            references without a label.

    Returns:
        The text formatted for Google Chat.
    """
    if not text:
        return ""

    ctx = _ConversionContext(
        user_map=user_map,
        channel_id_to_name=channel_id_to_name or {},
        state=state,
        unmapped_user_tracker=unmapped_user_tracker,
        original_text=text,
    )
    return _convert_mrkdwn(text, ctx)
