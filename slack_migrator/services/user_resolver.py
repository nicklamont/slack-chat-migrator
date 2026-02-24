"""
User resolution logic for the Slack to Google Chat migration tool.

Handles mapping Slack users to Google Workspace identities, including
impersonation delegation, external user detection, and unmapped user handling.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from slack_migrator.core.migrator import SlackToChatMigrator

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError

from slack_migrator.utils.api import get_gcp_service
from slack_migrator.utils.logging import log_with_context


class UserResolver:
    """Resolves Slack users to Google Workspace identities."""

    def __init__(self, migrator: SlackToChatMigrator) -> None:
        """Initialize with a reference to the parent migrator.

        Args:
            migrator: The SlackToChatMigrator instance.
        """
        self.migrator = migrator
        self._users_data: dict[str, dict[str, Any]] | None = None

    def get_delegate(self, email: str) -> Any:
        """Get a Google Chat API service with user impersonation."""
        if not email:
            return self.migrator.chat

        if email not in self.migrator.state.valid_users:
            try:
                test_service = get_gcp_service(
                    str(self.migrator.creds_path),
                    email,
                    "chat",
                    "v1",
                    getattr(self.migrator.state, "current_channel", None),
                    retry_config=self.migrator.config,
                )
                test_service.spaces().list(pageSize=1).execute()
                self.migrator.state.valid_users[email] = True
                self.migrator.state.chat_delegates[email] = test_service
            except (HttpError, RefreshError, TransportError) as e:
                error_code = e.resp.status if isinstance(e, HttpError) else "N/A"
                log_with_context(
                    logging.WARNING,
                    f"Impersonation failed for {email}, falling back to admin user. Error: {e}",
                    user=email,
                    error_code=error_code,
                )
                self.migrator.state.valid_users[email] = False
                return self.migrator.chat

        return self.migrator.state.chat_delegates.get(email, self.migrator.chat)

    def get_internal_email(
        self, user_id: str, user_email: str | None = None
    ) -> str | None:
        """Get internal email for a user, optionally ignoring bots and tracking unmapped users.

        Args:
            user_id: The Slack user ID
            user_email: Optional email if already known

        Returns:
            The internal email to use for this user, or None if the user should be
            ignored (e.g. bot user with ignore_bots enabled, or no email mapping exists)
        """
        if self.migrator.config.ignore_bots:
            user_data = self.get_user_data(user_id)
            if user_data and user_data.get("is_bot", False):
                log_with_context(
                    logging.DEBUG,
                    f"Ignoring bot user {user_id} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                    user_id=user_id,
                    channel=getattr(self.migrator.state, "current_channel", "unknown"),
                )
                return None

        if user_email is None:
            user_email = self.migrator.user_map.get(user_id)
            if not user_email:
                current_channel = getattr(
                    self.migrator.state, "current_channel", "unknown"
                )
                self.migrator.unmapped_user_tracker.add_unmapped_user(
                    user_id, current_channel
                )

                log_with_context(
                    logging.DEBUG,
                    f"No email mapping found for user {user_id}",
                    user_id=user_id,
                    channel=getattr(self.migrator.state, "current_channel", "unknown"),
                )
                return None

        return user_email

    def get_user_data(self, user_id: str) -> dict[str, Any] | None:
        """Get user data from the users.json export file.

        Args:
            user_id: The Slack user ID

        Returns:
            User data dictionary or None if not found
        """
        if self._users_data is None:
            users_file = Path(self.migrator.export_root) / "users.json"
            if users_file.exists():
                try:
                    with open(users_file) as f:
                        users_list = json.load(f)
                    self._users_data = {user["id"]: user for user in users_list}
                except (OSError, json.JSONDecodeError) as e:
                    log_with_context(logging.WARNING, f"Error loading users.json: {e}")
                    self._users_data = {}
            else:
                self._users_data = {}

        return self._users_data.get(user_id)

    def handle_unmapped_user_message(
        self, user_id: str, original_text: str
    ) -> tuple[str, str]:
        """Handle messages from unmapped users by using workspace admin with attribution.

        Args:
            user_id: The unmapped Slack user ID
            original_text: The original message text

        Returns:
            Tuple of (sender_email, modified_message_text)
        """
        current_channel = getattr(self.migrator.state, "current_channel", "unknown")
        self.migrator.unmapped_user_tracker.add_unmapped_user(
            user_id, f"message_sender:{current_channel}"
        )

        user_info = self.get_user_data(user_id)

        override_email = self.migrator.config.user_mapping_overrides.get(user_id)
        if override_email:
            attribution = f"*[From: {override_email}]*"
        elif user_info:
            real_name = user_info.get("profile", {}).get("real_name", "")
            email = user_info.get("profile", {}).get("email", "")

            if real_name and email:
                attribution = f"*[From: {real_name} ({email})]*"
            elif email:
                attribution = f"*[From: {email}]*"
            elif real_name:
                attribution = f"*[From: {real_name}]*"
            else:
                attribution = f"*[From: {user_id}]*"
        else:
            attribution = f"*[From: {user_id}]*"

        modified_text = f"{attribution}\n{original_text}"
        admin_email = self.migrator.workspace_admin

        log_with_context(
            logging.WARNING,
            f"Sending message from unmapped user {user_id} via workspace admin {admin_email}",
            user_id=user_id,
            channel=getattr(self.migrator.state, "current_channel", "unknown"),
            attribution=attribution,
        )

        return admin_email, modified_text

    def handle_unmapped_user_reaction(
        self, user_id: str, reaction: str, message_ts: str
    ) -> bool:
        """Handle reactions from unmapped users by logging and skipping.

        Args:
            user_id: The unmapped Slack user ID
            reaction: The reaction emoji
            message_ts: The timestamp of the message being reacted to

        Returns:
            False to indicate the reaction should be skipped
        """
        current_channel = getattr(self.migrator.state, "current_channel", "unknown")
        self.migrator.unmapped_user_tracker.add_unmapped_user(
            user_id, f"reaction:{current_channel}"
        )

        log_with_context(
            logging.WARNING,
            f"Skipping reaction '{reaction}' from unmapped user {user_id} on message {message_ts}",
            user_id=user_id,
            reaction=reaction,
            message_ts=message_ts,
            channel=current_channel,
        )

        self.migrator.state.skipped_reactions.append(
            {
                "user_id": user_id,
                "reaction": reaction,
                "message_ts": message_ts,
                "channel": getattr(self.migrator.state, "current_channel", "unknown"),
            }
        )

        return False

    def is_external_user(self, email: str | None) -> bool:
        """Check if a user is external based on their email domain.

        Args:
            email: The user's email address

        Returns:
            True if the user is external, False otherwise
        """
        if (
            not email
            or not isinstance(email, str)
            or not self.migrator.workspace_domain
        ):
            return False

        domain = email.split("@")[-1]
        return domain.lower() != self.migrator.workspace_domain.lower()
