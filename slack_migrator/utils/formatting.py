"""
Message formatting utilities for converting Slack messages to Google Chat format
"""

import re
from typing import Dict, Any

import emoji

from slack_migrator.utils.logging import logger

# Check if slackblocks is available
try:
    import slackblocks
    SLACKBLOCKS_AVAILABLE = True
except ImportError:
    SLACKBLOCKS_AVAILABLE = False
    logger.warning("Warning: slackblocks library not found. Rich text parsing will be limited.")


def parse_slack_blocks(message: Dict) -> str:
    """Parse Slack block kit format from message to extract rich text content.
    
    This extracts and formats text from Slack block kit structure including:
    - Rich text sections
    - Code blocks
    - Lists
    - Quotes
    
    Uses slackblocks library when available for better parsing of complex blocks.
    """
    if 'blocks' not in message:
        return message.get('text', '') or ''
        
    # If slackblocks library is available, use it for better parsing
    if SLACKBLOCKS_AVAILABLE:
        try:
            # Try to extract blocks data
            blocks_data = message.get('blocks', [])
            
            # Manual conversion of blocks to text
            texts = []
            
            for block in blocks_data:
                block_type = block.get('type')
                
                # Extract text from section blocks
                if block_type == 'section':
                    text_obj = block.get('text', {})
                    if text_obj:
                        texts.append(text_obj.get('text', ''))
                    # Extract text from fields (if present)
                    for field in block.get('fields', []):
                        if field and isinstance(field, dict):
                            texts.append(field.get('text', ''))
                
                # Extract text from rich_text blocks
                elif block_type == 'rich_text':
                    for element in block.get('elements', []):
                        element_type = element.get('type')
                        
                        # Handle rich_text_section
                        if element_type == 'rich_text_section':
                            section_text = []
                            for text_el in element.get('elements', []):
                                # Extract text with formatting
                                if text_el.get('type') == 'text':
                                    text_content = text_el.get('text', '')
                                    style = text_el.get('style', {})
                                    
                                    if style.get('bold'):
                                        text_content = f"*{text_content}*"
                                    if style.get('italic'):
                                        text_content = f"_{text_content}_"
                                    if style.get('strike'):
                                        text_content = f"~{text_content}~"
                                    if style.get('code'):
                                        text_content = f"`{text_content}`"
                                        
                                    section_text.append(text_content)
                                # Extract links
                                elif text_el.get('type') == 'link':
                                    url = text_el.get('url', '')
                                    text = text_el.get('text', url)
                                    section_text.append(f"<{url}|{text}>")
                                # Extract emoji
                                elif text_el.get('type') == 'emoji':
                                    emoji_name = text_el.get('name', '')
                                    section_text.append(f":{emoji_name}:")
                                # Extract user mentions
                                elif text_el.get('type') == 'user':
                                    user_id = text_el.get('user_id', '')
                                    section_text.append(f"<@{user_id}>")
                            texts.append(''.join(section_text))
                        
                        # Handle rich_text_list
                        elif element_type == 'rich_text_list':
                            list_items = []
                            list_style = element.get('style', 'bullet')
                            for item in element.get('elements', []):
                                if item.get('type') == 'rich_text_section':
                                    item_text = []
                                    for text_el in item.get('elements', []):
                                        if text_el.get('type') == 'text':
                                            item_text.append(text_el.get('text', ''))
                                    
                                    if list_style == 'bullet':
                                        list_items.append(f"• {' '.join(item_text)}")
                                    else:
                                        list_items.append(f"{len(list_items)+1}. {' '.join(item_text)}")
                            
                            texts.append('\n'.join(list_items))
                        
                        # Handle rich_text_quote
                        elif element_type == 'rich_text_quote':
                            quote_text = []
                            for text_el in element.get('elements', []):
                                if text_el.get('type') == 'text':
                                    quote_text.append(text_el.get('text', ''))
                            
                            formatted_quote = '\n'.join([f"> {line}" for line in ' '.join(quote_text).split('\n')])
                            texts.append(formatted_quote)
                        
                        # Handle rich_text_preformatted
                        elif element_type == 'rich_text_preformatted':
                            code_text = []
                            for text_el in element.get('elements', []):
                                if text_el.get('type') == 'text':
                                    code_text.append(text_el.get('text', ''))
                            
                            texts.append(f"```\n{' '.join(code_text)}\n```")
                
                # Extract text from header blocks
                elif block_type == 'header':
                    text_obj = block.get('text', {})
                    if text_obj:
                        texts.append(f"*{text_obj.get('text', '')}*")
                
                # Extract text from context blocks
                elif block_type == 'context':
                    context_texts = []
                    for element in block.get('elements', []):
                        if element.get('type') in ('mrkdwn', 'plain_text'):
                            context_texts.append(element.get('text', ''))
                    
                    if context_texts:
                        texts.append(' '.join(context_texts))
                
                # Extract text from divider (add a separator)
                elif block_type == 'divider':
                    texts.append('---')
            
            return '\n\n'.join(filter(None, texts)) or message.get('text', '') or ''
            
        except Exception as e:
            logger.warning(f"Error parsing blocks with slackblocks: {e}")
            # Fall back to basic text extraction
            return message.get('text', '') or ''
    else:
        # Fallback to basic text extraction if slackblocks not available
        result = []
        
        for block in message.get('blocks', []):
            block_type = block.get('type')
            
            if block_type == 'section':
                text_obj = block.get('text', {})
                if isinstance(text_obj, dict) and text_obj:
                    result.append(text_obj.get('text', ''))
                
                # Extract text from fields if present
                for field in block.get('fields', []):
                    if field and isinstance(field, dict):
                        result.append(field.get('text', ''))
                        
            elif block_type == 'rich_text':
                # Simple extraction of text from rich_text blocks
                for element in block.get('elements', []):
                    if element.get('type') == 'rich_text_section':
                        for text_el in element.get('elements', []):
                            if text_el.get('type') == 'text':
                                result.append(text_el.get('text', ''))
            
            elif block_type in ('header', 'context'):
                text_obj = block.get('text', {})
                if isinstance(text_obj, dict) and text_obj:
                    result.append(text_obj.get('text', ''))
                elif 'elements' in block:
                    for element in block.get('elements', []):
                        if isinstance(element, dict) and element.get('type') in ('mrkdwn', 'plain_text'):
                            result.append(element.get('text', ''))
        
        # Fall back to the message text if we couldn't extract anything from blocks
        if not result:
            return message.get('text', '') or ''
            
        return '\n\n'.join(filter(None, result))


def convert_formatting(text: str, user_map: Dict[str, str]) -> str:
    """Convert Slack markdown formatting to Google Chat markdown formatting."""
    if not text:
        return text
        
    # Handle user mentions first to avoid conflicts with other syntax
    def replace_user_mention(match):
        user_id_or_name = match.group(1)
        if user_id_or_name in user_map:
            return f"<users/{user_map[user_id_or_name]}>"
        else:
            # Fallback for user mentions that might not be in the map
            return f"@{user_id_or_name}"

    text = re.sub(r'<@([A-Z0-9]+)>', replace_user_mention, text)

    # Handle channel mentions: <#C024BE91L|general> -> #general
    text = re.sub(r'<#C[A-Z0-9]+\|([^>]+)>', r'#\1', text)

    # Handle special links with pipe syntax <http://www.example.com|Example> -> <http://www.example.com|Example>
    # Check if the link text is the same as the URL
    def replace_link(match):
        url = match.group(1)
        link_text = match.group(2)
        if url == link_text:
            return url
        return f'<{url}|{link_text}>'
        
    text = re.sub(r'<(https?://[^|]+)\|([^>]+)>', replace_link, text)
    
    # Handle plain links <http://www.example.com> -> http://www.example.com
    text = re.sub(r'<(https?://[^>]+)>', r'\1', text)

    # Handle HTML entities
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    
    # Convert bold, italic, strikethrough
    text = re.sub(r'\*(.*?)\*', r'*\1*', text)
    text = re.sub(r'_(.*?)_', r'_\1_', text)
    text = re.sub(r'~(.*?)~', r'~\1~', text)

    # Convert code blocks (multi-line) and inline code
    text = re.sub(r'```([\s\S]+?)```', r'```\1```', text)
    text = re.sub(r'`([^`]+)`', r'`\1`', text)
    
    # Handle special commands like <!channel> or <!here>
    text = re.sub(r'<!([^|>]+)(?:\|([^>]+))?>', r'@\1', text)

    return text


def sanitize_text(text: str) -> str:
    """Sanitize text to avoid Google Chat import issues."""
    if not text:
        return "No content"
        
    # Remove emoji patterns
    text = re.sub(r':[a-zA-Z0-9_\-+]+:', '', text)
    
    # Remove user mentions to avoid issues
    text = re.sub(r'<users/[^>]+>', '', text)
    
    # Replace problematic Unicode characters
    text = text.replace('\u2019', "'")  # Right single quotation mark
    text = text.replace('\u2018', "'")  # Left single quotation mark
    text = text.replace('\u201C', '"')  # Left double quotation mark
    text = text.replace('\u201D', '"')  # Right double quotation mark
    text = text.replace('\u2014', '-')  # Em dash
    text = text.replace('\u2013', '-')  # En dash
    
    # Strip all emojis and special Unicode characters
    text = emoji.replace_emoji(text, replace='')
    
    # Replace URLs with simplified versions
    text = re.sub(r'<(https?://[^|]+)\|([^>]+)>', r'[\2](\1)', text)
    text = re.sub(r'<(https?://[^>]+)>', r'\1', text)
    
    # Handle Slack mentions
    text = re.sub(r'<@([A-Z0-9]+)>', r'@user', text)
    text = re.sub(r'<!([^>]+)>', r'@\1', text)
    text = re.sub(r'<#([A-Z0-9]+)(?:\|([^>]+))>', r'#\2', text)
    
    # Remove all non-basic ASCII characters to be safe
    text = re.sub(r'[^\x00-\x7F]+', '', text)
    
    # Replace multiple spaces with a single space
    text = re.sub(r'\s+', ' ', text)
    
    # Remove special characters that might cause issues
    text = re.sub(r'[^\w\s.,;:!?@#$%&*()[\]{}|/<>\'"-+=]', '', text)
    
    # Truncate to 4000 characters (Google Chat limit)
    if len(text) > 4000:
        text = text[:3997] + '...'
    
    # Return a default message if we've stripped everything
    if not text.strip():
        return "Message content"
        
    return text.strip() 