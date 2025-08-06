"""
User mapping functionality for Slack to Google Chat migration
"""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import logging

from slack_migrator.utils.logging import log_with_context

# Create logger instance
logger = logging.getLogger("slack_migrator")


def generate_user_map(
    export_root: Path, config: Dict
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
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
    users_file = export_root / "users.json"

    if not users_file.exists():
        log_with_context(logging.ERROR, "users.json not found in export directory")
        sys.exit(1)

    try:
        with users_file.open() as f:
            users = json.load(f)
    except json.JSONDecodeError:
        log_with_context(logging.ERROR, "Failed to parse users.json")
        sys.exit(1)

    # Get email domain override from config
    email_domain_override = config.get("email_domain_override", "")

    # Get user mapping overrides from config
    user_mapping_overrides = config.get("user_mapping_overrides") or {}

    # Get bot ignoring setting from config
    ignore_bots = config.get("ignore_bots", False)
    ignored_bots_count = 0

    for user in users:
        # Process all users, including deleted ones, as they may still be referenced in messages
        user_id = user.get("id")
        if not user_id:
            continue

        # Skip bots if ignore_bots is enabled
        if ignore_bots and user.get("is_bot", False):
            ignored_bots_count += 1
            log_with_context(
                logging.INFO,
                f"Ignoring bot user {user_id} ({user.get('real_name', user.get('name', 'Unknown'))}) - ignore_bots enabled",
            )
            continue

        # Check if there's an override for this user
        if user_id in user_mapping_overrides:
            user_map[user_id] = user_mapping_overrides[user_id]
            continue

        # Get email from profile
        email = user.get("profile", {}).get("email")
        username = user.get("name", "").lower() or f"user_{user_id.lower()}"

        # If no email is found, track it but don't create a fake one
        if not email:
            user_info = {
                "id": user_id,
                "name": username,
                "real_name": user.get("profile", {}).get("real_name", ""),
                "is_bot": user.get("is_bot", False),
                "is_app_user": user.get("is_app_user", False),
                "deleted": user.get("deleted", False),
            }
            users_without_email.append(user_info)
            log_with_context(
                logging.WARNING,
                f"No email found for user {user_id} ({username}). Add to user_mapping_overrides in config.yaml.",
            )
            continue

        # Apply domain override if specified
        elif email_domain_override:
            username = email.split("@")[0]
            email = f"{username}@{email_domain_override}"

        user_map[user_id] = email

    if users_without_email:
        log_with_context(
            logging.WARNING,
            f"Found {len(users_without_email)} users without email addresses:",
        )
        for user in users_without_email:
            user_type = "Bot" if user["is_bot"] or user["is_app_user"] else "User"
            deleted_status = " (DELETED)" if user.get("deleted", False) else ""
            log_with_context(
                logging.WARNING,
                f"  - {user_type}: {user['name']} (ID: {user['id']}){deleted_status}",
            )

        log_with_context(
            logging.WARNING,
            "\nTo map these users, add entries to user_mapping_overrides in config.yaml:",
        )
        for user in users_without_email:
            deleted_comment = (
                " # DELETED USER - still referenced in messages"
                if user.get("deleted", False)
                else f" # {user['name']}"
            )
            log_with_context(logging.WARNING, f'  "{user["id"]}": ""{deleted_comment}')

    # Add any user_mapping_overrides that weren't already processed
    # This handles cases where users are mentioned in messages but not in users.json
    for override_user_id, override_email in user_mapping_overrides.items():
        if override_user_id not in user_map:
            user_map[override_user_id] = override_email
            log_with_context(
                logging.INFO,
                f"Added user mapping override for {override_user_id} -> {override_email} (not in users.json)",
            )

    if not user_map:
        log_with_context(logging.ERROR, "No valid users found in users.json")
        sys.exit(1)

    log_with_context(logging.INFO, f"Generated user mapping for {len(user_map)} users")
    if ignored_bots_count > 0:
        log_with_context(
            logging.INFO,
            f"Ignored {ignored_bots_count} bot users (ignore_bots enabled)",
        )

    return user_map, users_without_email
