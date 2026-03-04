"""
User mapping functionality for Slack to Google Chat migration
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.exceptions import ExportError, UserMappingError
from slack_chat_migrator.utils.logging import log_with_context

# Create logger instance
logger = logging.getLogger("slack_chat_migrator")


def _load_users_json(users_file: Path) -> list[dict[str, Any]]:
    """Load and parse users.json, raising ExportError on failure."""
    if not users_file.exists():
        raise ExportError("users.json not found in export directory")
    try:
        with users_file.open() as f:
            result: list[dict[str, Any]] = json.load(f)
            return result
    except json.JSONDecodeError as e:
        raise ExportError("Failed to parse users.json") from e
    except OSError as e:
        raise ExportError(f"Failed to read users.json: {e}") from e


def _process_single_user(
    user: dict[str, Any],
    user_map: dict[str, str],
    users_without_email: list[dict[str, Any]],
    ignore_bots: bool,
    overrides: dict[str, str],
    domain_override: str | None,
) -> bool:
    """Process one user entry, updating user_map or users_without_email.

    Returns True if the user was an ignored bot.
    """
    user_id = user.get("id")
    if not user_id:
        return False

    if ignore_bots and user.get("is_bot", False):
        log_with_context(
            logging.INFO,
            f"Ignoring bot user {user_id} ({user.get('real_name', user.get('name', 'Unknown'))}) - ignore_bots enabled",
        )
        return True

    if user_id in overrides:
        user_map[user_id] = overrides[user_id]
        return False

    email = user.get("profile", {}).get("email")
    username = user.get("name", "").lower() or f"user_{user_id.lower()}"

    if not email:
        users_without_email.append(
            {
                "id": user_id,
                "name": username,
                "real_name": user.get("profile", {}).get("real_name", ""),
                "is_bot": user.get("is_bot", False),
                "is_app_user": user.get("is_app_user", False),
                "deleted": user.get("deleted", False),
            }
        )
        log_with_context(
            logging.WARNING,
            f"No email found for user {user_id} ({username}). Add to user_mapping_overrides in config.yaml.",
        )
        return False

    if domain_override:
        username = email.split("@")[0]
        email = f"{username}@{domain_override}"

    user_map[user_id] = email
    return False


def _log_unmapped_users(users_without_email: list[dict[str, Any]]) -> None:
    """Log a summary of users without email addresses with config hints."""
    if not users_without_email:
        return

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


def generate_user_map(
    export_root: Path, config: MigrationConfig
) -> tuple[dict[str, str], list[dict[str, Any]], frozenset[str]]:
    """Generate user mapping from users.json file.

    Args:
        export_root: Path to the Slack export directory
        config: Configuration dictionary

    Returns:
        Tuple of (user_map, users_without_email, bot_user_ids) where:
        - user_map is a dictionary mapping Slack user IDs to email addresses
        - users_without_email is a list of dictionaries with info about users without emails
        - bot_user_ids is a frozenset of Slack user IDs that were ignored as bots
    """
    user_map: dict[str, str] = {}
    users_without_email: list[dict[str, Any]] = []
    bot_user_ids: set[str] = set()
    users = _load_users_json(export_root / "users.json")

    ignored_bots_count = 0
    for user in users:
        was_bot = _process_single_user(
            user,
            user_map,
            users_without_email,
            config.ignore_bots,
            config.user_mapping_overrides,
            config.email_domain_override,
        )
        if was_bot:
            ignored_bots_count += 1
            user_id = user.get("id")
            if user_id:
                bot_user_ids.add(user_id)

    _log_unmapped_users(users_without_email)

    # Add overrides for users not in users.json (e.g. mentioned in messages)
    for override_user_id, override_email in config.user_mapping_overrides.items():
        if override_user_id not in user_map:
            user_map[override_user_id] = override_email
            log_with_context(
                logging.INFO,
                f"Added user mapping override for {override_user_id} -> {override_email} (not in users.json)",
            )

    if not user_map:
        total = len(users)
        no_id = sum(1 for u in users if not u.get("id"))
        no_email = len(users_without_email)

        lines = [
            f"No valid users found in users.json ({total} entries parsed).",
        ]
        if no_id > 0:
            lines.append(f"  - {no_id}/{total} entries have no 'id' field.")
            # Show a sample entry (one that lacks 'id') to help diagnose schema issues
            # no_id > 0 guarantees at least one entry without an 'id' field
            sample_user = next(u for u in users if not u.get("id"))
            sample_keys = list(sample_user.keys())[:8]
            lines.append(f"  - Sample entry keys: {sample_keys}")
            lines.append(
                "  - Expected format: each entry needs 'id' and 'profile.email'."
            )
        if no_email > 0:
            lines.append(
                f"  - {no_email} user{' has' if no_email == 1 else 's have'} no email in 'profile.email'."
            )
        if ignored_bots_count > 0:
            lines.append(
                f"  - {ignored_bots_count} bot users were ignored (ignore_bots enabled)."
            )
        lines.append(
            "  Tip: map users manually via user_mapping_overrides in config.yaml."
        )

        detail = "\n".join(lines)
        raise UserMappingError(detail)

    log_with_context(logging.INFO, f"Generated user mapping for {len(user_map)} users")
    if ignored_bots_count > 0:
        log_with_context(
            logging.INFO,
            f"Ignored {ignored_bots_count} bot users (ignore_bots enabled)",
        )

    return user_map, users_without_email, frozenset(bot_user_ids)
