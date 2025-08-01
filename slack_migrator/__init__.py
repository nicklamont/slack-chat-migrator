#!/usr/bin/env python3
"""
Slack to Google Chat migration tool
"""

__version__ = "0.2.0"

# Import CLI utilities
from slack_migrator.cli.report import generate_report
from slack_migrator.core.config import load_config

# Import the main classes and functions for easier access
from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.services.message import process_reactions_batch, send_message

# Import key service functions
from slack_migrator.services.space import (
    add_users_to_space,
    create_space,
)
from slack_migrator.services.user import generate_user_map

# Import unified permission validation
from slack_migrator.utils.permissions import validate_permissions
