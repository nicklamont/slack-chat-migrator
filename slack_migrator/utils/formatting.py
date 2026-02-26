"""
Message formatting utilities for converting Slack messages to Google Chat format.

This module provides functions to parse Slack's block kit structure and
convert Slack's markdown syntax to the format expected by Google Chat.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import emoji

# This assumes a standard logging setup. If you don't have one,
# you can replace `from slack_migrator.utils.logging import logger`
# with `import logging; logger = logging.getLogger(__name__)`
from slack_migrator.utils.logging import log_with_context


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

    output_parts = []
    for el in elements:
        el_type = el.get("type")
        style = el.get("style", {})

        if el_type == "text":
            text_content = el.get("text", "")

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
            url = el.get("url", "")
            text = el.get("text", url)
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


def convert_formatting(
    text: str,
    user_map: dict[str, str],
    state: Any = None,
    unmapped_user_tracker: Any = None,
) -> str:
    """
    Convert Slack-specific markdown to Google Chat compatible format.

    Args:
        text: The Slack message text to convert
        user_map: A dictionary mapping Slack user IDs to Google Chat user IDs/emails
        state: Optional MigrationState for context (current_channel, current_message_ts)
        unmapped_user_tracker: Optional tracker for unmapped user mentions

    Returns:
        The formatted text with Slack mentions converted to Google Chat format
    """
    if not text:
        return ""

    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")

    def replace_user_mention(match: re.Match) -> str:
        """Replace a Slack ``<@UID>`` mention with Google Chat format.

        Args:
            match: Regex match containing the Slack user ID.

        Returns:
            The Google Chat mention, or an ``@``-prefixed Slack UID fallback.
        """
        slack_user_id = match.group(1)
        gchat_user_id = user_map.get(slack_user_id)

        if gchat_user_id:
            return f"<users/{gchat_user_id}>"

        # Enhanced logging and tracking for unmapped user mentions
        if unmapped_user_tracker:
            current_channel = getattr(state, "current_channel", "unknown")
            current_ts = getattr(state, "current_message_ts", "unknown")

            # Track this unmapped mention
            unmapped_user_tracker.track_unmapped_mention(
                slack_user_id, current_channel, current_ts, text
            )

            log_with_context(
                logging.ERROR,
                f"Could not map Slack user ID: {slack_user_id} in message mention (channel: {current_channel})",
                user_id=slack_user_id,
                channel=current_channel,
                message_ts=current_ts,
            )
        else:
            # Fallback to original logging if no tracker
            log_with_context(
                logging.WARNING, f"Could not map Slack user ID: {slack_user_id}"
            )

        return f"@{slack_user_id}"

    text = re.sub(r"<@([A-Z0-9]+)>", replace_user_mention, text)
    text = re.sub(r"<#C[A-Z0-9]+\|([^>]+)>", r"#\1", text)

    def replace_link(match: re.Match) -> str:
        """
        Replace Slack-formatted links with appropriate formatting for Google Chat.

        In Slack, links are formatted as <url|text>. If the URL and display text
        are identical, this function returns just the URL. Otherwise, it maintains
        the link format expected by Google Chat.

        Args:
            match: A regex match object containing the URL and link text

        Returns:
            Properly formatted link for Google Chat
        """
        url, link_text = match.group(1), match.group(2)
        return url if url == link_text else f"<{url}|{link_text}>"

    text = re.sub(r"<(https?://[^|]+)\|([^>]+)>", replace_link, text)
    text = re.sub(r"<(https?://[^|>]+)>", r"\1", text)
    text = re.sub(r"<!([^|>]+)(?:\|([^>]+))?>", r"@\1", text)
    text = emoji.emojize(text, language="alias")

    return text
