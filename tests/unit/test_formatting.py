"""Unit tests for the formatting module."""

from unittest.mock import MagicMock

import pytest

from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.utils.formatting import (
    _parse_rich_text_elements,
    convert_formatting,
    parse_slack_blocks,
)

# --- _parse_rich_text_elements tests ---


class TestParseRichTextElements:
    """Tests for _parse_rich_text_elements()."""

    def test_plain_text(self):
        elements = [{"type": "text", "text": "hello world"}]
        assert _parse_rich_text_elements(elements) == "hello world"

    def test_bold_text(self):
        elements = [{"type": "text", "text": "bold", "style": {"bold": True}}]
        assert _parse_rich_text_elements(elements) == "*bold*"

    def test_italic_text(self):
        elements = [{"type": "text", "text": "italic", "style": {"italic": True}}]
        assert _parse_rich_text_elements(elements) == "_italic_"

    def test_strikethrough_text(self):
        elements = [{"type": "text", "text": "strike", "style": {"strike": True}}]
        assert _parse_rich_text_elements(elements) == "~strike~"

    def test_bold_italic_text(self):
        elements = [
            {"type": "text", "text": "both", "style": {"bold": True, "italic": True}}
        ]
        assert _parse_rich_text_elements(elements) == "_*both*_"

    def test_preserves_leading_trailing_whitespace_with_styles(self):
        elements = [{"type": "text", "text": "  styled  ", "style": {"bold": True}}]
        assert _parse_rich_text_elements(elements) == "  *styled*  "

    def test_whitespace_only_text_no_styling(self):
        elements = [{"type": "text", "text": "   ", "style": {"bold": True}}]
        assert _parse_rich_text_elements(elements) == "   "

    def test_empty_text(self):
        elements = [{"type": "text", "text": ""}]
        assert _parse_rich_text_elements(elements) == ""

    def test_link_element(self):
        elements = [{"type": "link", "url": "https://example.com", "text": "Example"}]
        assert _parse_rich_text_elements(elements) == "<https://example.com|Example>"

    def test_link_without_text_uses_url(self):
        elements = [{"type": "link", "url": "https://example.com"}]
        assert (
            _parse_rich_text_elements(elements)
            == "<https://example.com|https://example.com>"
        )

    def test_emoji_element(self):
        elements = [{"type": "emoji", "name": "thumbsup"}]
        assert _parse_rich_text_elements(elements) == "\U0001f44d"

    def test_user_mention(self):
        elements = [{"type": "user", "user_id": "U12345"}]
        assert _parse_rich_text_elements(elements) == "<@U12345>"

    def test_multiple_elements(self):
        elements = [
            {"type": "text", "text": "Hello "},
            {"type": "user", "user_id": "U12345"},
            {"type": "text", "text": ", check "},
            {"type": "link", "url": "https://example.com", "text": "this"},
        ]
        result = _parse_rich_text_elements(elements)
        assert result == "Hello <@U12345>, check <https://example.com|this>"

    def test_empty_elements_list(self):
        assert _parse_rich_text_elements([]) == ""


# --- parse_slack_blocks tests ---


class TestParseSlackBlocks:
    """Tests for parse_slack_blocks()."""

    def test_no_blocks_returns_text_field(self):
        message = {"text": "simple message"}
        assert parse_slack_blocks(message) == "simple message"

    def test_empty_blocks_returns_text_field(self):
        message = {"blocks": [], "text": "fallback"}
        assert parse_slack_blocks(message) == "fallback"

    def test_section_block(self):
        message = {
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "section text"}}
            ],
            "text": "",
        }
        assert parse_slack_blocks(message) == "section text"

    def test_header_block(self):
        message = {
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": "Title"}}
            ],
            "text": "",
        }
        assert parse_slack_blocks(message) == "*Title*"

    def test_divider_block(self):
        message = {"blocks": [{"type": "divider"}], "text": ""}
        assert parse_slack_blocks(message) == "---"

    def test_context_block(self):
        message = {
            "blocks": [
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "context info"}],
                }
            ],
            "text": "",
        }
        assert parse_slack_blocks(message) == "context info"

    def test_rich_text_section(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [{"type": "text", "text": "rich text"}],
                        }
                    ],
                }
            ],
            "text": "",
        }
        assert parse_slack_blocks(message) == "rich text"

    def test_rich_text_quote(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_quote",
                            "elements": [{"type": "text", "text": "quoted text"}],
                        }
                    ],
                }
            ],
            "text": "",
        }
        assert parse_slack_blocks(message) == "_quoted text_"

    def test_rich_text_preformatted(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_preformatted",
                            "elements": [{"type": "text", "text": "code here"}],
                        }
                    ],
                }
            ],
            "text": "",
        }
        assert parse_slack_blocks(message) == "```\ncode here\n```"

    def test_rich_text_bullet_list(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "item one"}],
                                },
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "item two"}],
                                },
                            ],
                        }
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "* item one" in result
        assert "* item two" in result

    def test_rich_text_numbered_list(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "ordered",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "first item"}
                                    ],
                                },
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "second item"}
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "1. first item" in result
        assert "2. second item" in result

    def test_forwarded_message(self):
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded content",
                    "author_name": "Alice",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "Forwarded message" in result
        assert "Alice" in result
        assert "forwarded content" in result

    def test_no_text_no_blocks(self):
        message = {}
        assert parse_slack_blocks(message) == ""


# --- convert_formatting tests ---


class TestConvertFormatting:
    """Tests for convert_formatting()."""

    def test_empty_text(self):
        assert convert_formatting("", {}) == ""

    def test_html_entity_unescaping(self):
        result = convert_formatting("a &lt; b &amp; c &gt; d", {})
        assert result == "a < b & c > d"

    def test_user_mention_with_mapping(self):
        user_map = {"U12345": "user@example.com"}
        result = convert_formatting("Hey <@U12345>!", user_map)
        assert result == "Hey <users/user@example.com>!"

    def test_user_mention_without_mapping(self):
        result = convert_formatting("Hey <@UUNKNOWN>!", {})
        assert result == "Hey @UUNKNOWN!"

    def test_channel_reference(self):
        result = convert_formatting("Check <#C12345|general>", {})
        assert result == "Check #general"

    def test_link_with_same_text_and_url(self):
        url = "https://example.com"
        result = convert_formatting(f"<{url}|{url}>", {})
        assert result == url

    def test_link_with_different_text(self):
        result = convert_formatting("<https://example.com|click here>", {})
        assert result == "<https://example.com|click here>"

    def test_bare_link(self):
        result = convert_formatting("<https://example.com>", {})
        assert result == "https://example.com"

    def test_special_mention(self):
        result = convert_formatting("<!here> <!channel|channel>", {})
        assert result == "<users/all> <users/all>"

    def test_emoji_conversion(self):
        result = convert_formatting("great :thumbsup:", {})
        assert "👍" in result

    def test_plain_text_passthrough(self):
        text = "Hello, world!"
        assert convert_formatting(text, {}) == text

    def test_multiple_user_mentions(self):
        user_map = {"U111": "alice@co.com", "U222": "bob@co.com"}
        result = convert_formatting("<@U111> and <@U222>", user_map)
        assert "<users/alice@co.com>" in result
        assert "<users/bob@co.com>" in result

    def test_unmapped_user_with_tracker(self):
        """Covers unmapped user with unmapped_user_tracker."""
        state = MigrationState()
        state.context.current_channel = "general"
        state.context.current_message_ts = "1234567890.000100"
        tracker = MagicMock()

        result = convert_formatting(
            "Hey <@UUNKNOWN>!", {}, state=state, unmapped_user_tracker=tracker
        )

        assert result == "Hey @UUNKNOWN!"
        tracker.track_unmapped_mention.assert_called_once_with(
            "UUNKNOWN", "general", "1234567890.000100", "Hey <@UUNKNOWN>!"
        )


# --- Additional parse_slack_blocks tests for missing coverage ---


class TestParseSlackBlocksForwardedMessages:
    """Tests for forwarded/shared message handling in parse_slack_blocks()."""

    def test_forwarded_message_with_author_subname(self):
        """Covers lines 168-169: author_subname fallback when no author_name."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded content",
                    "author_subname": "bob@example.com",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "from bob@example.com" in result
        assert "forwarded content" in result

    def test_forwarded_message_with_valid_timestamp(self):
        """Covers lines 172-179: timestamp conversion to readable format."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded content",
                    "author_name": "Alice",
                    "ts": "1700000000",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "originally sent" in result
        # The formatted time should be present (exact format depends on locale)
        assert "2023" in result or "November" in result

    def test_forwarded_message_with_invalid_timestamp(self):
        """Covers lines 180-182: ValueError/OSError fallback for bad timestamp."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded content",
                    "ts": "not-a-number",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "originally sent at not-a-number" in result

    def test_forwarded_message_with_message_blocks(self):
        """Covers lines 185-190: recursive parsing of message_blocks."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "message_blocks": [
                        {
                            "message": {
                                "blocks": [
                                    {
                                        "type": "section",
                                        "text": {
                                            "type": "mrkdwn",
                                            "text": "rich forwarded text",
                                        },
                                    }
                                ],
                                "text": "",
                            }
                        }
                    ],
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "rich forwarded text" in result
        assert "Forwarded message" in result

    def test_forwarded_message_fallback_text(self):
        """Covers lines 196-197: fallback field when text is empty."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "fallback": "fallback content here",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "fallback content here" in result

    def test_forwarded_message_is_msg_unfurl(self):
        """Test is_msg_unfurl triggers forwarded message handling."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_msg_unfurl": True,
                    "text": "unfurled content",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "Forwarded message" in result
        assert "unfurled content" in result

    def test_forwarded_message_top_level_bullet(self):
        """Covers lines 227-230: top-level bullet conversion."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "• first item\n• second item",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "* first item" in result
        assert "* second item" in result

    def test_forwarded_message_indented_bullets_level1(self):
        """Covers lines 207-221: indented bullets at level 1 (indent <= 4)."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "  • level one bullet",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "* level one bullet" in result

    def test_forwarded_message_indented_bullets_level2(self):
        """Covers lines 218-221: indented bullets at level 2 (indent 5-8)."""
        # Indented bullet must be on a non-first line so strip() doesn't remove
        # its leading whitespace before line splitting.
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "Header line\n      \u2022 level two bullet",
                }
            ],
        }
        result = parse_slack_blocks(message)
        # Level 2 should use hollow bullet (indent 6 is > 4 and <= 8)
        assert "\u25e6" in result  # ◦ character
        assert "level two bullet" in result

    def test_forwarded_message_indented_bullets_level3(self):
        """Covers lines 222-226: deeply indented bullets (indent > 8)."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "Header line\n          \u2022 deep bullet",
                }
            ],
        }
        result = parse_slack_blocks(message)
        # Level 3+ should use small bullet (indent 10 is > 8)
        assert "\u25aa" in result  # ▪ character
        assert "deep bullet" in result

    def test_forwarded_message_very_deep_bullets(self):
        """Covers line 224: indent_level > 12 triggers level 3."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "Header line\n              \u2022 very deep bullet",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "\u25aa" in result  # ▪ character
        assert "very deep bullet" in result

    def test_forwarded_message_single_bullet_char(self):
        """Covers edge case: bullet character with no trailing text."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "\u2022",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "* " in result

    def test_no_blocks_main_text_with_forwarded(self):
        """Covers line 249: main text + forwarded content, no blocks."""
        message = {
            "text": "Main message text",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded content",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "Main message text" in result
        assert "forwarded content" in result
        assert "Forwarded message" in result


class TestParseSlackBlocksSectionFields:
    """Tests for section block fields handling."""

    def test_section_block_with_fields(self):
        """Covers lines 264-265: section block with fields array."""
        message = {
            "blocks": [
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": "Field 1"},
                        {"type": "mrkdwn", "text": "Field 2"},
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "Field 1" in result
        assert "Field 2" in result

    def test_section_block_with_text_and_fields(self):
        """Section block with both text and fields."""
        message = {
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "Section heading"},
                    "fields": [
                        {"type": "mrkdwn", "text": "Field A"},
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "Section heading" in result
        assert "Field A" in result

    def test_section_block_with_none_field(self):
        """Fields list may contain None or non-dict entries."""
        message = {
            "blocks": [
                {
                    "type": "section",
                    "fields": [
                        None,
                        {"type": "mrkdwn", "text": "Valid field"},
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "Valid field" in result


class TestParseSlackBlocksIndentedLists:
    """Tests for indented list handling in rich_text blocks."""

    def test_bullet_list_indent_level_1(self):
        """Covers lines 303-304, 319-320: indent level 1 bullet uses hollow bullet."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "top item"}],
                                },
                            ],
                        },
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 1,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "sub item"}],
                                },
                            ],
                        },
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        # indent level 1: uses ◦ hollow bullet
        assert "\u25e6 sub item" in result
        assert "* top item" in result

    def test_bullet_list_indent_level_2(self):
        """Covers lines 305-306: indent level 2+ bullet uses small bullet."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "top item"}],
                                },
                            ],
                        },
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 2,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "deep item"}],
                                },
                            ],
                        },
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        # indent level 2: uses ▪ small bullet
        assert "\u25aa deep item" in result

    def test_numbered_list_indent_level_1(self):
        """Covers lines 329-330: indented numbered list."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "ordered",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "top numbered"}
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "rich_text_list",
                            "style": "ordered",
                            "indent": 1,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "nested first"}
                                    ],
                                },
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "nested second"}
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "1. top numbered" in result
        # indent level 1: 4 + 5*1 = 9 spaces
        assert "         1. nested first" in result
        assert "         2. nested second" in result


class TestParseSlackBlocksListFlush:
    """Tests for list item flushing before other block types."""

    def test_list_items_flushed_before_section(self):
        """Covers lines 278-279: accumulated list items flushed before rich_text_section."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [{"type": "text", "text": "list item"}],
                                },
                            ],
                        },
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {"type": "text", "text": "section after list"}
                            ],
                        },
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "* list item" in result
        assert "section after list" in result
        # List item should come before section text
        assert result.index("* list item") < result.index("section after list")

    def test_list_items_flushed_before_quote(self):
        """Covers lines 337-338: accumulated list items flushed before rich_text_quote."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "list before quote"}
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "rich_text_quote",
                            "elements": [{"type": "text", "text": "quoted after list"}],
                        },
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "* list before quote" in result
        assert "_quoted after list_" in result

    def test_list_items_flushed_before_preformatted(self):
        """Covers lines 353-354: accumulated list items flushed before rich_text_preformatted."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_list",
                            "style": "bullet",
                            "indent": 0,
                            "elements": [
                                {
                                    "type": "rich_text_section",
                                    "elements": [
                                        {"type": "text", "text": "list before code"}
                                    ],
                                },
                            ],
                        },
                        {
                            "type": "rich_text_preformatted",
                            "elements": [{"type": "text", "text": "code after list"}],
                        },
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "* list before code" in result
        assert "```\ncode after list\n```" in result


class TestParseSlackBlocksFallbackAndForwarded:
    """Tests for blocks-present-but-empty fallback paths."""

    def test_blocks_with_empty_content_falls_back_to_text(self):
        """Covers lines 390-399: blocks present but yield no content, fall back to text."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [{"type": "text", "text": "   "}],
                        }
                    ],
                }
            ],
            "text": "fallback text",
        }
        result = parse_slack_blocks(message)
        assert result == "fallback text"

    def test_blocks_empty_content_with_forwarded_no_main_text(self):
        """Covers lines 392-393: blocks yield nothing, no main text, but forwarded exists."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [{"type": "text", "text": "   "}],
                        }
                    ],
                }
            ],
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded only",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "Forwarded message" in result
        assert "forwarded only" in result

    def test_blocks_empty_content_with_forwarded_and_main_text(self):
        """Covers lines 395-396: blocks yield nothing, but main text + forwarded exist."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [{"type": "text", "text": "   "}],
                        }
                    ],
                }
            ],
            "text": "main text here",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded text",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "main text here" in result
        assert "Forwarded message" in result
        assert "forwarded text" in result

    def test_blocks_with_content_and_forwarded(self):
        """Covers line 403: blocks produce content, and forwarded texts are appended."""
        message = {
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "block content"},
                }
            ],
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "text": "forwarded part",
                }
            ],
        }
        result = parse_slack_blocks(message)
        assert "block content" in result
        assert "Forwarded message" in result
        assert "forwarded part" in result
        # Block content should come before forwarded message
        assert result.index("block content") < result.index("Forwarded message")


class TestParseRichTextElementsEdgeCases:
    """Additional edge case tests for _parse_rich_text_elements."""

    def test_link_with_bold_style(self):
        """Link elements with styling applied."""
        elements = [
            {
                "type": "link",
                "url": "https://example.com",
                "text": "Example",
                "style": {"bold": True},
            }
        ]
        result = _parse_rich_text_elements(elements)
        assert result == "*<https://example.com|Example>*"

    def test_user_mention_with_bold_style(self):
        """User mention elements with styling applied."""
        elements = [{"type": "user", "user_id": "U12345", "style": {"bold": True}}]
        result = _parse_rich_text_elements(elements)
        assert result == "*<@U12345>*"

    def test_unknown_element_type_ignored(self):
        """Unknown element types should be silently skipped."""
        elements = [
            {"type": "text", "text": "before"},
            {"type": "unknown_type", "data": "irrelevant"},
            {"type": "text", "text": "after"},
        ]
        result = _parse_rich_text_elements(elements)
        assert result == "beforeafter"

    def test_text_with_all_three_styles(self):
        """Bold + italic + strikethrough combined."""
        elements = [
            {
                "type": "text",
                "text": "all styles",
                "style": {"bold": True, "italic": True, "strike": True},
            }
        ]
        result = _parse_rich_text_elements(elements)
        assert result == "~_*all styles*_~"

    def test_emoji_with_missing_name(self):
        """Emoji element with no name field renders nothing."""
        elements = [{"type": "emoji"}]
        result = _parse_rich_text_elements(elements)
        assert result == ""

    def test_text_element_with_empty_style_dict(self):
        """Empty style dict should not apply any formatting."""
        elements = [{"type": "text", "text": "plain", "style": {}}]
        result = _parse_rich_text_elements(elements)
        assert result == "plain"


class TestParseSlackBlocksContextEdgeCases:
    """Edge cases for context block handling."""

    def test_context_block_multiple_elements(self):
        """Multiple context elements joined with spaces."""
        message = {
            "blocks": [
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": "posted by"},
                        {"type": "plain_text", "text": "Alice"},
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert result == "posted by Alice"

    def test_context_block_filters_non_text_elements(self):
        """Context elements that are not mrkdwn or plain_text are skipped."""
        message = {
            "blocks": [
                {
                    "type": "context",
                    "elements": [
                        {"type": "image", "image_url": "https://example.com/img.png"},
                        {"type": "mrkdwn", "text": "caption text"},
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert result == "caption text"


class TestParseSlackBlocksMultipleBlocks:
    """Tests combining multiple block types in a single message."""

    def test_multiple_block_types_combined(self):
        """Multiple blocks are joined with double newlines."""
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "My Header"},
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "body text"},
                },
                {"type": "divider"},
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": "footer"}],
                },
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "*My Header*" in result
        assert "body text" in result
        assert "---" in result
        assert "footer" in result

    def test_rich_text_quote_with_multiple_paragraphs(self):
        """Quote with multiple paragraphs should each be italicized."""
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_quote",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "paragraph one\n\nparagraph two",
                                }
                            ],
                        }
                    ],
                }
            ],
            "text": "",
        }
        result = parse_slack_blocks(message)
        assert "_paragraph one_" in result
        assert "_paragraph two_" in result

    def test_forwarded_message_empty_text_stripped(self):
        """Forwarded attachment with whitespace-only text should not produce output."""
        message = {
            "text": "main text",
            "attachments": [
                {
                    "is_share": True,
                    "text": "   ",
                }
            ],
        }
        result = parse_slack_blocks(message)
        # No forwarded content since it's whitespace-only
        assert "Forwarded message" not in result
        assert result == "main text"

    def test_forwarded_message_message_blocks_no_blocks_key(self):
        """message_blocks entry without proper message/blocks structure falls back."""
        message = {
            "text": "",
            "attachments": [
                {
                    "is_share": True,
                    "message_blocks": [{"message": {"text": "no blocks key"}}],
                    "text": "fallback text",
                }
            ],
        }
        result = parse_slack_blocks(message)
        # message_blocks didn't produce content, falls back to text
        assert "fallback text" in result


# --- mrkdwn <...> tokens ---

TOKEN_USER_MAP = {"U123": "alice@example.com", "W456": "bob@example.com"}
TOKEN_CHANNELS = {"C0123ABC": "general", "G0456DEF": "secret-plans"}
ZWSP = "\N{ZERO WIDTH SPACE}"


class TestMrkdwnTokens:
    """Every Slack <...> token form converts to the expected Chat text."""

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            # Users
            ("<@U123> hi", "<users/alice@example.com> hi"),
            ("<@U123|alice> hi", "<users/alice@example.com> hi"),
            ("<@W456> (enterprise)", "<users/bob@example.com> (enterprise)"),
            ("<@U999|carol> hi", "@carol hi"),
            ("<@U999>", "@U999"),
            # Channels
            ("<#C0123ABC|general>", "#general"),
            ("<#C0123ABC|>", "#general"),
            ("<#C0123ABC>", "#general"),
            ("<#G0456DEF>", "#secret-plans"),
            ("<#G0456DEF|renamed>", "#renamed"),
            ("<#C9999999|>", "#C9999999"),
            # Broadcasts
            ("<!here> deploy", "<users/all> deploy"),
            ("<!channel>", "<users/all>"),
            ("<!everyone>", "<users/all>"),
            ("<!here|@here>", "<users/all>"),
            # User groups
            ("<!subteam^S0123|@eng-team> please look", "@eng-team please look"),
            ("<!subteam^S0123|eng-team>", "@eng-team"),
            ("<!subteam^S0123>", "@S0123"),
            # Dates
            (
                "<!date^1392734382^{date_num} {time_secs}"
                "|February 18th, 2014 at 6:39 AM PST>",
                "February 18th, 2014 at 6:39 AM PST",
            ),
            ("<!date^1392734382^{date}^https://example.com|Feb 18>", "Feb 18"),
            ("<!date^1392734382^{date_num}>", "2014-02-18 14:39 UTC"),
            # Links
            (
                "<mailto:bob@example.com|bob@example.com>",
                "<mailto:bob@example.com|bob@example.com>",
            ),
            ("<mailto:bob@example.com>", "mailto:bob@example.com"),
            ("<tel:+15551234|+1 555 1234>", "<tel:+15551234|+1 555 1234>"),
            ("<https://example.com|docs>", "<https://example.com|docs>"),
            ("<https://example.com|https://example.com>", "https://example.com"),
            ("<https://example.com>", "https://example.com"),
            (
                "<https://example.com/?a=1&amp;b=2|Q&amp;A>",
                "<https://example.com/?a=1&b=2|Q&A>",
            ),
            # Unknown tokens keep their label, or their inner text
            ("<weird-token>", "weird-token"),
            ("<weird|label>", "label"),
        ],
    )
    def test_token(self, text, expected):
        result = convert_formatting(
            text, TOKEN_USER_MAP, channel_id_to_name=TOKEN_CHANNELS
        )
        assert result == expected

    def test_unmapped_labelled_mention_is_tracked(self):
        state = MigrationState()
        state.context.current_channel = "general"
        state.context.current_message_ts = "1700000000.000100"
        tracker = MagicMock()
        result = convert_formatting(
            "<@U999|carol> hi", {}, state=state, unmapped_user_tracker=tracker
        )
        assert result == "@carol hi"
        tracker.track_unmapped_mention.assert_called_once_with(
            "U999", "general", "1700000000.000100", "<@U999|carol> hi"
        )

    def test_link_label_cannot_break_out_of_chat_link(self):
        result = convert_formatting("<https://example.com|a &gt; b>", {})
        assert result.startswith("<https://example.com|a ")
        assert result.count(">") == 1

    def test_raw_angle_bracket_without_token_is_text(self):
        assert convert_formatting("a < b and c > d", {}) == "a < b and c > d"


class TestEscapedTextStaysLiteral:
    """Text a user typed as <...> never becomes a live link or mention."""

    def test_escaped_link_is_not_a_link(self):
        result = convert_formatting(
            "&lt;https://evil.example|https://bank.example&gt;", TOKEN_USER_MAP
        )
        assert "<https://evil.example|" not in result
        assert result.replace(ZWSP, "") == (
            "<https://evil.example|https://bank.example>"
        )

    def test_escaped_slack_mention_is_not_a_mention(self):
        result = convert_formatting("&lt;@U123&gt;", TOKEN_USER_MAP)
        assert "users/" not in result
        assert result == "<@U123>"

    def test_escaped_chat_markup_is_defused(self):
        result = convert_formatting("&lt;users/all&gt; ping", {})
        assert "<users/all>" not in result
        assert result.replace(ZWSP, "") == "<users/all> ping"

    def test_entities_are_decoded_once(self):
        assert convert_formatting("&amp;lt;b&amp;gt; &amp;amp;", {}) == (
            "&lt;b&gt; &amp;"
        )

    def test_comparison_operators_are_untouched(self):
        assert convert_formatting("x &lt;= 3 &amp;&amp; y &gt; 2 &lt;3", {}) == (
            "x <= 3 && y > 2 <3"
        )

    def test_rich_text_literal_tokens_stay_text(self):
        """Rich-text text is literal: typed <@U123> or <url|label> stays text."""
        message = {
            "text": "&lt;@U123&gt; &lt;https://evil|https://bank&gt;",
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": "<@U123> <https://evil|https://bank> & co",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
        result = convert_formatting(parse_slack_blocks(message), TOKEN_USER_MAP)
        assert "users/" not in result
        assert "<https://evil|" not in result
        assert result.replace(ZWSP, "") == "<@U123> <https://evil|https://bank> & co"


# --- emoji shortcodes ---

TADA = "\U0001f389"


class TestEmojiOnlyInProse:
    """Shortcodes are replaced only in prose, and only for real emoji."""

    @pytest.mark.parametrize(
        "text",
        [
            "redis key session:id:42",
            "key:ok:value",
            "fe80::a:b:1",
            "2001:db8::ab:cd:1",
            "at 12:30:00 UTC",
            "x:b:y",
            "`arr[1:x:2]`",
            "```\nkey:ok:value\n```",
            "```key:ok:value```",
            "https://example.com/a?q=b:ok:c",
            "see https://host/x/:ok:/y",
            "std::vector::size",
        ],
    )
    def test_technical_text_unchanged(self, text):
        assert convert_formatting(text, {}) == text

    def test_shortcode_inside_link_token_unchanged(self):
        text = "<https://grafana.example/d?from=now:ok:x|dash>"
        assert convert_formatting(text, {}) == text

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            (":tada:", TADA),
            ("we shipped :tada:", f"we shipped {TADA}"),
            ("(:tada:)", f"({TADA})"),
            (":tada:!", f"{TADA}!"),
            (":smile::tada:", f"\U0001f604{TADA}"),
            (":partyparrot::tada:", f":partyparrot:{TADA}"),
            (":thumbsup::skin-tone-2:", "\U0001f44d\U0001f3fb"),
            (":man-shrugging:", "\U0001f937\N{ZWJ}\N{MALE SIGN}\N{VS16}"),
            ("`code` then :tada:", f"`code` then {TADA}"),
            ("it`s :tada:", f"it`s {TADA}"),
        ],
    )
    def test_prose_shortcodes_replaced(self, text, expected):
        assert convert_formatting(text, {}) == expected

    def test_custom_emoji_stays_literal(self):
        assert convert_formatting("so :partyparrot: today", {}) == (
            "so :partyparrot: today"
        )

    def test_custom_emoji_map_applies_in_text(self):
        mapping = {"partyparrot": "\U0001f99c", "shipit": ":ship-it:"}
        result = convert_formatting(
            ":partyparrot: :shipit: :tada:", {}, custom_emoji_map=mapping
        )
        assert result == f"\U0001f99c :ship-it: {TADA}"

    def test_emoji_in_link_label_replaced(self):
        assert convert_formatting("<https://x.example|:tada: party>", {}) == (
            f"<https://x.example|{TADA} party>"
        )

    def test_preformatted_block_unchanged(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_preformatted",
                            "elements": [
                                {"type": "text", "text": "key:ok:value fe80::a:b:1"}
                            ],
                        }
                    ],
                }
            ],
            "text": "",
        }
        result = convert_formatting(parse_slack_blocks(message), {})
        assert result == "```\nkey:ok:value fe80::a:b:1\n```"


class TestCodeSpans:
    """Inside code, entities are decoded and tokens become plain text."""

    def test_entities_decoded_without_defusing(self):
        assert convert_formatting("`if a &lt;b &amp;&amp; c&gt;d`", {}) == (
            "`if a <b && c>d`"
        )

    def test_link_tokens_become_plain_text(self):
        text = "```curl <https://api.example.com/v1?a=1&amp;b=2>```"
        assert convert_formatting(text, {}) == (
            "```curl https://api.example.com/v1?a=1&b=2```"
        )

    def test_labelled_link_shows_label(self):
        assert convert_formatting("`<http://x.com|x.com>`", {}) == "`x.com`"

    def test_mention_in_code_is_plain(self):
        result = convert_formatting("`<@U123>` and <@U123>", TOKEN_USER_MAP)
        assert result == "`@alice@example.com` and <users/alice@example.com>"

    def test_broadcast_in_code_is_plain(self):
        assert convert_formatting("`<!here>`", {}) == "`@here`"

    def test_backtick_in_link_label_does_not_open_code(self):
        result = convert_formatting("<https://x.example|a`b> :tada: `c`", {})
        assert result == f"<https://x.example|a`b> {TADA} `c`"

    def test_unterminated_fence_is_text(self):
        assert convert_formatting("``` :tada:", {}) == f"``` {TADA}"


class TestRichTextEmojiElements:
    """Rich-text emoji elements are converted to Unicode directly."""

    def test_unicode_field_preferred(self):
        elements = [{"type": "emoji", "name": "thumbsup", "unicode": "1f44d-1f3fd"}]
        assert _parse_rich_text_elements(elements) == "\U0001f44d\U0001f3fd"

    def test_skin_tone_name_without_unicode(self):
        elements = [{"type": "emoji", "name": "wave::skin-tone-3"}]
        assert _parse_rich_text_elements(elements) == "\U0001f44b\U0001f3fc"

    def test_skin_tone_name_with_untoned_unicode(self):
        elements = [{"type": "emoji", "name": "wave::skin-tone-3", "unicode": "1f44b"}]
        assert _parse_rich_text_elements(elements) == "\U0001f44b\U0001f3fc"

    def test_custom_emoji_kept_as_shortcode(self):
        elements = [{"type": "emoji", "name": "partyparrot"}]
        assert _parse_rich_text_elements(elements) == ":partyparrot:"

    def test_emoji_glued_to_word_still_converted(self):
        message = {
            "blocks": [
                {
                    "type": "rich_text",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {"type": "text", "text": "nice"},
                                {"type": "emoji", "name": "tada", "unicode": "1f389"},
                            ],
                        }
                    ],
                }
            ],
            "text": "nice:tada:",
        }
        assert convert_formatting(parse_slack_blocks(message), {}) == f"nice{TADA}"
