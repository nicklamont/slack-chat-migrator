"""Unit tests for the formatting module."""

from slack_migrator.utils.formatting import (
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
        assert _parse_rich_text_elements(elements) == ":thumbsup:"

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
        assert result == "@here @channel"

    def test_emoji_conversion(self):
        result = convert_formatting("great :thumbsup:", {})
        assert ":thumbsup:" not in result or "👍" in result

    def test_plain_text_passthrough(self):
        text = "Hello, world!"
        assert convert_formatting(text, {}) == text

    def test_multiple_user_mentions(self):
        user_map = {"U111": "alice@co.com", "U222": "bob@co.com"}
        result = convert_formatting("<@U111> and <@U222>", user_map)
        assert "<users/alice@co.com>" in result
        assert "<users/bob@co.com>" in result
