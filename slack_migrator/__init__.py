#!/usr/bin/env python3
"""
Slack to Google Chat migration tool
"""

__version__ = "0.1.0"

# Import the main classes and functions for easier access
from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.core.config import load_config

# Import key service functions
from slack_migrator.services.space import create_space, test_space_creation, add_users_to_space
from slack_migrator.services.message import send_message, process_reactions_batch
from slack_migrator.services.user import generate_user_map

# Import CLI utilities
from slack_migrator.cli.report import generate_report
from slack_migrator.cli.permission import check_permissions 