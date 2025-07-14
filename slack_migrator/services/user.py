"""
User mapping functionality for Slack to Google Chat migration
"""

import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

from slack_migrator.utils.logging import logger, log_with_context


def generate_user_map(export_root: Path, config: Dict) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """Generate user mapping from users.json file.
    
    Args:
        export_root: Path to the Slack export directory
        config: Configuration dictionary
        
    Returns:
        Tuple of (user_map, users_without_email) where:
        - user_map is a dictionary mapping Slack user IDs to email addresses
        - users_without_email is a list of dictionaries with info about users without emails
    """
    user_map = {}
    users_without_email = []
    users_file = export_root / 'users.json'
    
    if not users_file.exists():
        logger.error("users.json not found in export directory")
        sys.exit(1)
    
    try:
        with users_file.open() as f:
            users = json.load(f)
    except json.JSONDecodeError:
        logger.error("Failed to parse users.json")
        sys.exit(1)
    
    # Get email domain override from config
    email_domain_override = config.get('email_domain_override', '')
    
    # Get user mapping overrides from config
    user_mapping_overrides = config.get('user_mapping_overrides') or {}
    
    for user in users:
        if not user.get('deleted', False):  # Skip deleted users
            user_id = user.get('id')
            if not user_id:
                continue
            
            # Check if there's an override for this user
            if user_id in user_mapping_overrides:
                user_map[user_id] = user_mapping_overrides[user_id]
                continue
            
            # Get email from profile
            email = user.get('profile', {}).get('email')
            username = user.get('name', '').lower() or f"user_{user_id.lower()}"
            
            # If no email is found, track it but don't create a fake one
            if not email:
                user_info = {
                    'id': user_id,
                    'name': username,
                    'real_name': user.get('profile', {}).get('real_name', ''),
                    'is_bot': user.get('is_bot', False),
                    'is_app_user': user.get('is_app_user', False)
                }
                users_without_email.append(user_info)
                logger.warning(f"No email found for user {user_id} ({username}). Add to user_mapping_overrides in config.yaml.")
                continue
            
            # No need to check external_email_mapping as we've consolidated all mappings into user_mapping_overrides
            # Apply domain override if specified
            elif email_domain_override:
                username = email.split('@')[0]
                email = f"{username}@{email_domain_override}"
            
            user_map[user_id] = email
    
    if users_without_email:
        logger.warning(f"Found {len(users_without_email)} users without email addresses:")
        for user in users_without_email:
            user_type = "Bot" if user['is_bot'] or user['is_app_user'] else "User"
            logger.warning(f"  - {user_type}: {user['name']} (ID: {user['id']})")
        
        logger.warning("\nTo map these users, add entries to user_mapping_overrides in config.yaml:")
        for user in users_without_email:
            logger.warning(f'  "{user["id"]}": ""  # {user["name"]}')
    
    if not user_map:
        logger.error("No valid users found in users.json")
        sys.exit(1)
        
    logger.info(f"Generated user mapping for {len(user_map)} users")
    return user_map, users_without_email


def export_user_map_to_csv(user_map: Dict[str, str], output_path: Path) -> bool:
    """Export user mapping to a CSV file for review or editing."""
    import csv
    
    try:
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Slack User ID', 'Google Workspace Email'])
            
            for slack_id, email in sorted(user_map.items()):
                writer.writerow([slack_id, email])
                
        logger.info(f"User mapping exported to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to export user mapping: {e}")
        return False


def import_user_map_from_csv(input_path: Path) -> Dict[str, str]:
    """Import user mapping from a CSV file."""
    import csv
    
    user_map = {}
    try:
        with open(input_path, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header
            
            for row in reader:
                if len(row) >= 2:
                    slack_id, email = row[0], row[1]
                    user_map[slack_id] = email
                    
        logger.info(f"Imported user mapping for {len(user_map)} users from {input_path}")
        return user_map
    except Exception as e:
        logger.error(f"Failed to import user mapping: {e}")
        return {}

# The export_users_without_email function has been removed as we now include this information
# directly in the migration report and logs instead of creating a separate file 