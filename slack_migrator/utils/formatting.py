"""
Message formatting utilities for converting Slack messages to Google Chat format.

This module provides functions to parse Slack's block kit structure and
convert Slack's markdown syntax to the format expected by Google Chat.
"""

import re
from typing import Dict, List

import emoji
# This assumes a standard logging setup. If you don't have one,
# you can replace `from slack_migrator.utils.logging import logger`
# with `import logging; logger = logging.getLogger(__name__)`
from slack_migrator.utils.logging import logger


def _parse_rich_text_elements(elements: List[Dict]) -> str:
    """
    Helper function to parse a list of rich text elements.
    """
    output_parts = []
    for text_el in elements:
        el_type = text_el.get('type')
        if el_type == 'text':
            text_content = text_el.get('text', '')
            if style := text_el.get('style'):
                if style.get('bold'):
                    text_content = f"*{text_content}*"
                if style.get('italic'):
                    text_content = f"_{text_content}_"
                if style.get('strike'):
                    text_content = f"~{text_content}~"
                if style.get('code'):
                    text_content = f"`{text_content}`"
            output_parts.append(text_content)
        elif el_type == 'link':
            url = text_el.get('url', '')
            text = text_el.get('text', url)
            output_parts.append(f"<{url}|{text}>")
        elif el_type == 'emoji':
            output_parts.append(f":{text_el.get('name', '')}:")
        elif el_type == 'user':
            output_parts.append(f"<@{text_el.get('user_id', '')}>")
    return ''.join(output_parts)


def parse_slack_blocks(message: Dict) -> str:
    """
    Parse Slack block kit format from a message to extract rich text content.
    """
    if 'blocks' not in message or not message['blocks']:
        return message.get('text', '')

    texts = []
    blocks_data = message.get('blocks', [])

    for block in blocks_data:
        block_type = block.get('type')

        if block_type == 'section':
            if text_obj := block.get('text'):
                texts.append(text_obj.get('text', ''))
            for field in block.get('fields', []):
                if field and isinstance(field, dict):
                    texts.append(field.get('text', ''))

        elif block_type == 'rich_text':
            for element in block.get('elements', []):
                element_type = element.get('type')

                if element_type == 'rich_text_section':
                    texts.append(_parse_rich_text_elements(element.get('elements', [])))

                elif element_type == 'rich_text_list':
                    list_items = []
                    list_style = element.get('style', 'bullet')
                    for i, item in enumerate(element.get('elements', [])):
                        item_text = _parse_rich_text_elements(item.get('elements', []))
                        prefix = "•" if list_style == 'bullet' else f"{i + 1}."
                        list_items.append(f"{prefix} {item_text}")
                    texts.append('\n'.join(list_items))

                elif element_type == 'rich_text_quote':
                    quote_content = _parse_rich_text_elements(element.get('elements', []))
                    # FIX 1: Split by paragraph, wrap each in italics, and rejoin.
                    paragraphs = quote_content.strip().split('\n\n')
                    italicized_paragraphs = [f"_{p.strip()}_" for p in paragraphs if p.strip()]
                    texts.append('\n\n'.join(italicized_paragraphs))

                elif element_type == 'rich_text_preformatted':
                    code_text = _parse_rich_text_elements(element.get('elements', []))
                    texts.append(f"```\n{code_text}\n```")

        elif block_type == 'header':
            if text_obj := block.get('text'):
                texts.append(f"*{text_obj.get('text', '')}*")

        elif block_type == 'context':
            context_texts = [
                element.get('text', '')
                for element in block.get('elements', [])
                if element.get('type') in ('mrkdwn', 'plain_text')
            ]
            if context_texts:
                texts.append(' '.join(context_texts))

        elif block_type == 'divider':
            texts.append('---')
    
    stripped_texts = [s.strip() for s in texts]
    return '\n\n'.join(filter(None, stripped_texts)) or message.get('text', '')


def convert_formatting(text: str, user_map: Dict[str, str]) -> str:
    """
    Convert Slack-specific markdown to Google Chat compatible format.
    """
    if not text:
        return ""

    text = text.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')

    def replace_user_mention(match: re.Match) -> str:
        slack_user_id = match.group(1)
        gchat_user_id = user_map.get(slack_user_id)
        if gchat_user_id:
            return f"<users/{gchat_user_id}>"
        logger.warning(f"Could not map Slack user ID: {slack_user_id}")
        return f"@{slack_user_id}"

    text = re.sub(r'<@([A-Z0-9]+)>', replace_user_mention, text)
    text = re.sub(r'<#C[A-Z0-9]+\|([^>]+)>', r'#\1', text)

    def replace_link(match: re.Match) -> str:
        url, link_text = match.group(1), match.group(2)
        return url if url == link_text else f'<{url}|{link_text}>'

    text = re.sub(r'<(https?://[^|]+)\|([^>]+)>', replace_link, text)
    text = re.sub(r'<(https?://[^|>]+)>', r'\1', text)
    text = re.sub(r'<!([^|>]+)(?:\|([^>]+))?>', r'@\1', text)
    text = emoji.emojize(text, language='alias')

    return text