"""
Simple unmapped user tracking integrated into existing user mapping logic.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, Set

from slack_migrator.utils.logging import log_with_context


class UnmappedUserTracker:
    """Simple tracker for unmapped users detected during migration."""

    def __init__(self):
        self.unmapped_users: Set[str] = set()  # Just track the user IDs
        self.user_contexts: Dict[str, Set[str]] = defaultdict(
            set
        )  # Track where they were encountered

    def add_unmapped_user(self, user_id: str, context: str = ""):
        """Add an unmapped user to the tracker.

        Args:
            user_id: The unmapped Slack user ID
            context: Optional context about where this user was encountered
        """
        self.unmapped_users.add(user_id)
        if context:
            self.user_contexts[user_id].add(context)

    def track_unmapped_mention(
        self, user_id: str, channel: str = "", message_ts: str = "", text: str = ""
    ):
        """Track an unmapped user mention with detailed context.

        Args:
            user_id: The unmapped Slack user ID
            channel: The channel where the mention was found
            message_ts: The timestamp of the message containing the mention
            text: The message text containing the mention
        """
        # Create a descriptive context from the available information
        context_parts = ["mention"]
        if channel and channel != "unknown":
            context_parts.append(f"channel:{channel}")
        if message_ts and message_ts != "unknown":
            context_parts.append(f"ts:{message_ts}")

        context = ", ".join(context_parts)
        self.add_unmapped_user(user_id, context)

    def track_unmapped_channel_member(self, user_id: str, channel: str):
        """Track an unmapped user found in channel membership.

        Args:
            user_id: The unmapped Slack user ID
            channel: The channel where this user is a member
        """
        context = f"channel_member:#{channel}"
        self.add_unmapped_user(user_id, context)

    def has_unmapped_users(self) -> bool:
        """Check if any unmapped users were found."""
        return len(self.unmapped_users) > 0

    def get_unmapped_count(self) -> int:
        """Get the count of unmapped users."""
        return len(self.unmapped_users)

    def get_unmapped_users_list(self) -> list:
        """Get a sorted list of unmapped user IDs."""
        return sorted(self.unmapped_users)


def log_unmapped_user_summary_for_dry_run(migrator) -> None:
    """Log a simple summary of unmapped users during dry run.

    Args:
        migrator: The SlackToChatMigrator instance
    """
    if (
        not hasattr(migrator, "unmapped_user_tracker")
        or not migrator.unmapped_user_tracker.has_unmapped_users()
    ):
        log_with_context(logging.INFO, "✅ No unmapped users detected during dry run")
        return

    tracker = migrator.unmapped_user_tracker
    unmapped_users = tracker.get_unmapped_users_list()

    # Analyze unmapped users to provide better guidance
    user_analysis = analyze_unmapped_users(migrator, unmapped_users)

    log_with_context(
        logging.ERROR, f"🚨 DRY RUN DETECTED {len(unmapped_users)} UNMAPPED USERS 🚨"
    )

    log_with_context(logging.ERROR, "")
    log_with_context(logging.ERROR, "IMMEDIATE ACTION REQUIRED:")
    log_with_context(logging.ERROR, "Add these unmapped users to your config.yaml:")
    log_with_context(logging.ERROR, "")
    log_with_context(logging.ERROR, "user_mapping_overrides:")

    for user_id in unmapped_users:
        # Show contexts where this user was encountered if available
        contexts = tracker.user_contexts.get(user_id, set())
        context_info = (
            f" # Found in: {', '.join(contexts)}" if contexts else " # Unmapped user"
        )

        # Add user type information from analysis
        user_info = user_analysis.get(user_id, {})
        user_type = user_info.get("type", "unknown")
        if user_type != "unknown":
            context_info += f" - {user_type}"

        log_with_context(
            logging.ERROR, f'  "{user_id}": "user@yourdomain.com"{context_info}'
        )

    log_with_context(logging.ERROR, "")
    log_with_context(logging.ERROR, "📋 RECOMMENDED ACTIONS:")
    log_with_context(logging.ERROR, "")

    # Provide specific recommendations based on analysis
    if user_analysis:
        analysis_summary = categorize_user_analysis(user_analysis)

        # Provide specific guidance for each category
        if analysis_summary.get("Bots and workflow automations", 0) > 0:
            log_with_context(logging.ERROR, "🤖 For BOTS and WORKFLOW AUTOMATIONS:")
            log_with_context(
                logging.ERROR,
                "   OPTION 1 (Easiest): Enable 'ignore_bots: true' in config.yaml",
            )
            log_with_context(
                logging.ERROR,
                "   OPTION 2: Map to archive account 'bot-archive@yourdomain.com'",
            )
            log_with_context(
                logging.ERROR,
                "   NOTE: Bot integrations won't work in Google Chat anyway",
            )
            log_with_context(logging.ERROR, "")

        if analysis_summary.get("Deleted users", 0) > 0:
            log_with_context(logging.ERROR, "🗑️  For DELETED USERS:")
            log_with_context(
                logging.ERROR, "   RECOMMENDED: Map to 'deleted-user@yourdomain.com'"
            )
            log_with_context(logging.ERROR, "")

        if analysis_summary.get("Users without email addresses", 0) > 0:
            log_with_context(logging.ERROR, "📧 For USERS WITHOUT EMAIL:")
            log_with_context(
                logging.ERROR,
                "   RECOMMENDED: Find their real email or use placeholder",
            )
            log_with_context(logging.ERROR, "")

    log_with_context(logging.ERROR, "⚙️  ADD TO CONFIG.YAML:")
    log_with_context(logging.ERROR, "")

    # Check if we have bots and suggest the ignore_bots option
    has_bots = any(
        user_analysis.get(uid, {}).get("type") in ["bot", "workflow_bot"]
        for uid in unmapped_users
    )
    if has_bots:
        log_with_context(logging.ERROR, "1. EASIEST SOLUTION - Ignore all bots:")
        log_with_context(logging.ERROR, "   ignore_bots: true")
        log_with_context(logging.ERROR, "")
        log_with_context(logging.ERROR, "2. OR manually map specific users:")
    else:
        log_with_context(logging.ERROR, "Add user mappings:")

    log_with_context(logging.ERROR, "user_mapping_overrides:")

    for user_id in unmapped_users:
        user_info = user_analysis.get(user_id, {})
        user_type = user_info.get("type", "unknown")
        user_name = user_info.get("name", "Unknown")

        if user_type in ["bot", "workflow_bot"]:
            log_with_context(
                logging.ERROR,
                f'  "{user_id}": "bot-archive@yourdomain.com"  # {user_name} (bot)',
            )
        elif user_type == "deleted_user":
            log_with_context(
                logging.ERROR,
                f'  "{user_id}": "deleted-user@yourdomain.com"  # {user_name} (deleted)',
            )
        else:
            log_with_context(
                logging.ERROR,
                f'  "{user_id}": "user@yourdomain.com"  # {user_name} ({user_type})',
            )

    log_with_context(logging.ERROR, "")
    log_with_context(logging.ERROR, "💡 WHY IGNORE BOTS:")
    log_with_context(
        logging.ERROR,
        "   • Most bot content is automated notifications or integrations",
    )
    log_with_context(
        logging.ERROR, "   • Bot functionality won't work in Google Chat anyway"
    )
    log_with_context(logging.ERROR, "   • Focus on human conversations that matter")
    log_with_context(
        logging.ERROR, "   • Simplifies migration by removing mapping requirements"
    )


def analyze_unmapped_users(
    migrator, unmapped_user_ids: list
) -> Dict[str, Dict[str, Any]]:
    """Analyze unmapped users to determine their types and provide better guidance.

    Args:
        migrator: The SlackToChatMigrator instance
        unmapped_user_ids: List of unmapped user IDs to analyze

    Returns:
        Dict mapping user_id to analysis info (type, name, details, etc.)
    """
    import json
    from pathlib import Path

    analysis = {}

    try:
        # Load users.json to get detailed user information
        users_file = Path(migrator.export_root) / "users.json"
        if not users_file.exists():
            log_with_context(
                logging.WARNING, "users.json not found, cannot analyze unmapped users"
            )
            return analysis

        with open(users_file, "r") as f:
            users_data = json.load(f)

        # Create lookup map for user data
        user_lookup = {user["id"]: user for user in users_data}

        for user_id in unmapped_user_ids:
            user_data = user_lookup.get(user_id, {})

            if not user_data:
                analysis[user_id] = {"type": "missing_from_export", "name": "Unknown"}
                continue

            # Determine user type based on available data
            user_type = "regular_user"
            details = []

            if user_data.get("is_bot", False):
                if user_data.get("is_workflow_bot", False):
                    user_type = "workflow_bot"
                    details.append("Slack workflow automation")
                else:
                    user_type = "bot"
                    details.append("Bot/app integration")
            elif user_data.get("deleted", False):
                user_type = "deleted_user"
                details.append("Deleted from Slack")
            elif user_data.get("is_restricted", False):
                user_type = "restricted_user"
                details.append("Restricted/guest user")
            elif not user_data.get("profile", {}).get("email"):
                user_type = "no_email"
                details.append("No email address")

            real_name = user_data.get("real_name", user_data.get("name", "Unknown"))

            analysis[user_id] = {
                "type": user_type,
                "name": real_name,
                "details": details,
                "data": user_data,
            }

    except Exception as e:
        log_with_context(logging.WARNING, f"Error analyzing unmapped users: {e}")

    return analysis


def categorize_user_analysis(
    user_analysis: Dict[str, Dict[str, Any]],
) -> Dict[str, int]:
    """Categorize analyzed users for summary reporting.

    Args:
        user_analysis: Analysis results from analyze_unmapped_users

    Returns:
        Dict mapping category names to counts
    """
    categories = {
        "Bots and workflow automations": 0,
        "Deleted users": 0,
        "Users without email addresses": 0,
        "Restricted/guest users": 0,
        "Missing from export": 0,
        "Other": 0,
    }

    for user_info in user_analysis.values():
        user_type = user_info.get("type", "unknown")

        if user_type in ["bot", "workflow_bot"]:
            categories["Bots and workflow automations"] += 1
        elif user_type == "deleted_user":
            categories["Deleted users"] += 1
        elif user_type == "no_email":
            categories["Users without email addresses"] += 1
        elif user_type == "restricted_user":
            categories["Restricted/guest users"] += 1
        elif user_type == "missing_from_export":
            categories["Missing from export"] += 1
        else:
            categories["Other"] += 1

    return categories


def initialize_unmapped_user_tracking(migrator):
    """Initialize simple unmapped user tracking for the migrator.

    Args:
        migrator: The SlackToChatMigrator instance

    Returns:
        UnmappedUserTracker: The initialized tracker instance
    """
    if not hasattr(migrator, "unmapped_user_tracker"):
        migrator.unmapped_user_tracker = UnmappedUserTracker()

    return migrator.unmapped_user_tracker


def scan_channel_members_for_unmapped_users(migrator) -> None:
    """Scan channels.json for users listed as members but not in user_map.

    This is crucial because Google Chat will try to add all channel members
    to the migrated spaces, so we need mappings for all of them.

    Args:
        migrator: The SlackToChatMigrator instance
    """
    import json
    from pathlib import Path

    if not hasattr(migrator, "unmapped_user_tracker"):
        initialize_unmapped_user_tracking(migrator)

    tracker = migrator.unmapped_user_tracker

    try:
        channels_file = Path(migrator.export_root) / "channels.json"
        if not channels_file.exists():
            log_with_context(
                logging.WARNING,
                "channels.json not found, skipping channel member validation",
            )
            return

        with open(channels_file, "r") as f:
            channels_data = json.load(f)

        channels_to_check = []

        # Determine which channels to check based on include/exclude settings
        if migrator.config.get("include_channels"):
            # Only check included channels
            include_set = set(migrator.config["include_channels"])
            channels_to_check = [
                ch for ch in channels_data if ch.get("name") in include_set
            ]
        else:
            # Check all channels except excluded ones
            exclude_set = set(migrator.config.get("exclude_channels", []))
            channels_to_check = [
                ch for ch in channels_data if ch.get("name") not in exclude_set
            ]

        unmapped_members_found = 0
        total_members_checked = 0

        # Load user data once if ignore_bots is enabled
        user_lookup = {}
        ignore_bots = migrator.config.get("ignore_bots", False)
        if ignore_bots:
            try:
                users_file = Path(migrator.export_root) / "users.json"
                if users_file.exists():
                    with open(users_file, "r") as f:
                        users_data = json.load(f)
                    user_lookup = {user["id"]: user for user in users_data}
            except Exception as e:
                log_with_context(
                    logging.WARNING, f"Error loading users.json for bot checking: {e}"
                )
                user_lookup = {}

        for channel in channels_to_check:
            channel_name = channel.get("name", "unknown")
            members = channel.get("members", [])

            for member_id in members:
                total_members_checked += 1

                # Check if this member has a mapping
                if member_id not in migrator.user_map:
                    # If ignore_bots is enabled, check if this is a bot before tracking as unmapped
                    if ignore_bots and user_lookup:
                        user_data = user_lookup.get(member_id, {})
                        if user_data.get("is_bot", False):
                            # Skip tracking this bot as unmapped
                            log_with_context(
                                logging.DEBUG,
                                f"Skipping bot channel member {member_id} ({user_data.get('real_name', 'Unknown')}) in #{channel_name} - ignore_bots enabled",
                            )
                            continue

                    tracker.track_unmapped_channel_member(member_id, channel_name)
                    unmapped_members_found += 1

        if unmapped_members_found > 0:
            log_with_context(
                logging.WARNING,
                f"Found {unmapped_members_found} channel members without user mappings "
                f"(checked {total_members_checked} total members across {len(channels_to_check)} channels)",
            )
        else:
            log_with_context(
                logging.INFO,
                f"✅ All {total_members_checked} channel members have user mappings "
                f"(checked {len(channels_to_check)} channels)",
            )

    except Exception as e:
        log_with_context(
            logging.ERROR, f"Error scanning channel members for unmapped users: {e}"
        )
