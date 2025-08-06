"""
Main migrator class for the Slack to Google Chat migration tool
"""

import json
import logging
import signal
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.cli.report import (
    generate_report,
    print_dry_run_summary,
)
from slack_migrator.core.config import should_process_channel
from slack_migrator.services.file import FileHandler
from slack_migrator.services.message import (
    send_message,
    track_message_stats,
)

# Import functionality from service modules
from slack_migrator.services.space import (
    add_regular_members,
    add_users_to_space,
    create_space,
)
from slack_migrator.services.user import generate_user_map
from slack_migrator.utils.api import get_gcp_service
from slack_migrator.utils.logging import log_with_context, logger


class SlackToChatMigrator:
    """Main class for migrating Slack exports to Google Chat."""

    def __init__(
        self,
        creds_path: str,
        export_path: str,
        workspace_admin: str,
        config_path: str,
        dry_run: bool = False,
        verbose: bool = False,
        update_mode: bool = False,
        debug_api: bool = False,
    ):
        """Initialize the migrator with the required parameters."""
        self.creds_path = creds_path
        self.export_root = Path(export_path)
        self.workspace_admin = workspace_admin
        self.config_path = Path(config_path)
        self.dry_run = dry_run
        self.verbose = verbose
        self.debug_api = debug_api
        self.update_mode = update_mode
        self.import_mode = (
            not update_mode
        )  # Set import_mode to True when not in update mode

        if self.update_mode:
            log_with_context(
                logging.INFO, f"Running in update mode - will update existing spaces"
            )

        # Initialize caches and state tracking
        self.space_cache = {}  # channel -> space_name
        self.created_spaces = {}  # channel -> space_name
        self.user_map = {}  # slack_user_id -> google_email
        self.drive_files_cache = {}  # file_id -> drive_file
        self.progress_file = self.export_root / ".migration_progress.json"
        self.thread_map = {}  # slack_thread_ts -> google_chat_thread_name
        self.external_users = set()  # Set of external user emails
        self.users_without_email = []  # List of users without email mappings
        self.failed_messages = []  # List of failed message details
        self.channel_handlers = {}  # Store channel-specific log handlers
        self.channel_to_space = {}  # channel -> space_name for file attachments
        self.current_space = None  # Current space being processed

        # Extract workspace domain from admin email for external user detection
        self.workspace_domain = (
            self.workspace_admin.split("@")[1] if "@" in self.workspace_admin else None
        )

        # Initialize API clients
        self._validate_export_format()

        # Load config using the shared load_config function
        from slack_migrator.core.config import load_config

        self.config = load_config(self.config_path)

        # Generate user mapping from users.json
        self.user_map, self.users_without_email = generate_user_map(
            self.export_root, self.config
        )

        # Initialize simple unmapped user tracking
        from slack_migrator.utils.user_validation import (
            initialize_unmapped_user_tracking,
            scan_channel_members_for_unmapped_users,
        )

        self.unmapped_user_tracker = initialize_unmapped_user_tracking(self)

        # Scan channel members to ensure all channel members have user mappings
        # This is crucial because Google Chat needs to add all channel members to spaces
        scan_channel_members_for_unmapped_users(self)

        # API services will be initialized later after permission checks
        self.chat = None
        self.drive = None
        self._api_services_initialized = False

        self.chat_delegates: Dict[str, Any] = {}
        self.valid_users: Dict[str, bool] = {}
        self.channel_to_space: Dict[str, str] = {}

        # Initialize channel ID mapping
        self.channel_id_to_space_id: Dict[str, str] = {}

        # Load channel metadata from channels.json
        self.channels_meta, self.channel_id_to_name = self._load_channels_meta()

        # Create reverse mapping for convenience
        self.channel_name_to_id = {
            name: id for id, name in self.channel_id_to_name.items()
        }

    def _initialize_api_services(self):
        """Initialize Google API services after permission validation."""
        if self._api_services_initialized:
            return

        log_with_context(
            logging.INFO, "Initializing Google Chat and Drive API services..."
        )

        # Convert Path to str for API clients
        creds_path_str = str(self.creds_path)
        self.chat = get_gcp_service(
            creds_path_str, self.workspace_admin, "chat", "v1", retry_config=self.config
        )
        self.drive = get_gcp_service(
            creds_path_str,
            self.workspace_admin,
            "drive",
            "v3",
            retry_config=self.config,
        )

        self._api_services_initialized = True
        log_with_context(
            logging.INFO, "Google Chat and Drive API services initialized successfully"
        )

        # Initialize dependent services
        self._initialize_dependent_services()

    def _initialize_dependent_services(self):
        """Initialize services that depend on API clients."""
        # Initialize file handler
        self.file_handler = FileHandler(
            self.drive, self.chat, folder_id=None, migrator=self, dry_run=self.dry_run
        )
        # FileHandler now handles its own drive folder initialization automatically

        # Initialize message attachment processor
        from slack_migrator.services.message_attachments import (
            MessageAttachmentProcessor,
        )

        self.attachment_processor = MessageAttachmentProcessor(
            self.file_handler, dry_run=self.dry_run
        )

        # Initialize caches and state tracking
        self.created_spaces: Dict[str, str] = {}  # channel -> space_name
        self.current_channel: Optional[str] = (
            None  # Track current channel being processed
        )

        # Track spaces with external users
        self.spaces_with_external_users: Dict[str, bool] = {}

        # Track message statistics per channel
        self.channel_stats: Dict[str, Dict[str, int]] = {}

        # Track current message timestamp for enhanced error context
        self.current_message_ts: Optional[str] = None

        # Permission validation is now handled by the CLI layer to avoid duplicates
        # The CLI will call validate_permissions() unless --skip_permission_check is used

        if self.verbose:
            log_with_context(
                logging.DEBUG, "Migrator initialized with verbose logging enabled"
            )

        # Load existing space mappings for update mode or file attachments
        self._load_existing_space_mappings()

    def _validate_export_format(self):
        """Validate that the export directory has the expected structure."""
        if not (self.export_root / "channels.json").exists():
            log_with_context(
                logging.WARNING, "channels.json not found in export directory"
            )

        if not (self.export_root / "users.json").exists():
            log_with_context(
                logging.WARNING, "users.json not found in export directory"
            )
            raise ValueError(
                f"users.json not found in {self.export_root}. This file is required for user mapping."
            )

        # Check that at least one channel directory exists
        channel_dirs = [d for d in self.export_root.iterdir() if d.is_dir()]
        if not channel_dirs:
            raise ValueError(f"No channel directories found in {self.export_root}")

        # Check that each channel directory has at least one JSON file
        for ch_dir in channel_dirs:
            if not list(ch_dir.glob("*.json")):
                log_with_context(
                    logging.WARNING,
                    f"No JSON files found in channel directory {ch_dir.name}",
                )

    def _load_channels_meta(self):
        """
        Load channel metadata from channels.json file.

        Returns:
            tuple: (name_to_data, id_to_name) where:
                - name_to_data: Dict mapping channel names to their metadata
                - id_to_name: Dict mapping channel IDs to channel names
        """
        f = self.export_root / "channels.json"
        name_to_data = {}
        id_to_name = {}

        if f.exists():
            with open(f) as f_in:
                channels = json.load(f_in)
                name_to_data = {ch["name"]: ch for ch in channels}
                id_to_name = {ch["id"]: ch["name"] for ch in channels}

        return name_to_data, id_to_name

    def _get_delegate(self, email: str):
        """Get a Google Chat API service with user impersonation."""
        if not email:
            return self.chat

        if email not in self.valid_users:
            try:
                # Verify user exists by making a simple API call
                test_service = get_gcp_service(
                    str(self.creds_path),
                    email,
                    "chat",
                    "v1",
                    getattr(self, "current_channel", None),
                    retry_config=self.config,
                )
                test_service.spaces().list(pageSize=1).execute()
                self.valid_users[email] = True
                self.chat_delegates[email] = test_service
            except (HttpError, RefreshError) as e:
                # If we get an error on impersonation, fall back to admin
                error_code = e.resp.status if isinstance(e, HttpError) else "N/A"
                log_with_context(
                    logging.WARNING,
                    f"Impersonation failed for {email}, falling back to admin user. Error: {e}",
                    user=email,
                    error_code=error_code,
                )
                self.valid_users[email] = False
                return self.chat

        return self.chat_delegates.get(email, self.chat)

    def _discover_channel_resources(self, channel: str):
        """
        Find the last message timestamp in a space to determine where to resume migration.

        This approach simply finds the timestamp of the last message in the space and
        only imports messages after that time.

        Args:
            channel: The Slack channel name
        """
        # Check if we have a space for this channel
        space_name = self.channel_to_space.get(channel)
        if not space_name:
            log_with_context(
                logging.WARNING,
                f"No space found for channel {channel}, cannot determine last message timestamp",
                channel=channel,
            )
            return

        # Import discovery functions
        from slack_migrator.services.discovery import get_last_message_timestamp

        # Initialize the last_processed_timestamps dict if it doesn't exist
        if not hasattr(self, "last_processed_timestamps"):
            self.last_processed_timestamps = {}

        # Get the timestamp of the last message in the space
        last_timestamp = get_last_message_timestamp(self, channel, space_name)

        if last_timestamp > 0:
            log_with_context(
                logging.INFO,
                f"Found last message timestamp for channel {channel}: {last_timestamp}",
                channel=channel,
            )

            # Store the last timestamp for this channel
            self.last_processed_timestamps[channel] = last_timestamp

            # Initialize an empty thread_map so we don't try to load it again
            if not hasattr(self, "thread_map") or self.thread_map is None:
                self.thread_map = {}
        else:
            # If no messages were found, log it but don't set a last timestamp
            # This will cause all messages to be imported
            log_with_context(
                logging.INFO,
                f"No existing messages found in space for channel {channel}, will import all messages",
                channel=channel,
            )

    def _should_abort_import(
        self, channel: str, processed_count: int, failed_count: int
    ) -> bool:
        """Determine if we should abort the import after errors in a channel.

        This can be configured in the config file with abort_on_error: true|false
        """
        if self.dry_run:
            return False

        # Only consider aborting if we had failures
        if failed_count > 0:
            log_with_context(
                logging.WARNING,
                f"Channel '{channel}' had {failed_count} message import errors.",
                channel=channel,
            )

            # Check config for abort_on_error setting
            should_abort = self.config.get("abort_on_error", False)

            if should_abort:
                log_with_context(
                    logging.WARNING,
                    f"Aborting import due to errors (abort_on_error is enabled in config)",
                    channel=channel,
                )
                return True
            else:
                log_with_context(
                    logging.WARNING,
                    f"Continuing with migration despite errors (abort_on_error is disabled in config)",
                    channel=channel,
                )

        return False

    def _delete_space_if_errors(self, space_name, channel):
        """Delete a space if it had errors and cleanup is enabled."""
        if not self.config.get("cleanup_on_error", False):
            log_with_context(
                logging.INFO,
                f"Not deleting space {space_name} despite errors (cleanup_on_error is disabled in config)",
                space_name=space_name,
            )
            return

        try:
            log_with_context(
                logging.WARNING,
                f"Deleting space {space_name} due to errors",
                space_name=space_name,
            )
            self.chat.spaces().delete(name=space_name).execute()
            log_with_context(
                logging.INFO,
                f"Successfully deleted space {space_name}",
                space_name=space_name,
            )

            # Remove from created_spaces
            if channel in self.created_spaces:
                del self.created_spaces[channel]

            # Decrement space count
            self.migration_summary["spaces_created"] -= 1
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to delete space {space_name}: {e}",
                space_name=space_name,
            )

        log_with_context(logging.INFO, "Cleanup completed")

    def _get_internal_email(
        self, user_id: str, user_email: Optional[str] = None
    ) -> Optional[str]:
        """Get internal email for a user, handling external users and tracking unmapped users.

        Args:
            user_id: The Slack user ID
            user_email: Optional email if already known

        Returns:
            The internal email to use for this user, or None if user should be ignored
        """
        # Check if this user is a bot and bots are being ignored FIRST
        if self.config.get("ignore_bots", False):
            # Check if this is a bot user
            user_data = self._get_user_data(user_id)
            if user_data and user_data.get("is_bot", False):
                log_with_context(
                    logging.DEBUG,
                    f"Ignoring bot user {user_id} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                    user_id=user_id,
                    channel=getattr(self, "current_channel", "unknown"),
                )
                return None  # Explicitly ignore this bot - don't track as unmapped

        # Get the email from our user mapping if not provided
        if user_email is None:
            user_email = self.user_map.get(user_id)
            if not user_email:
                # Track this unmapped user automatically (only non-ignored users reach here)
                if hasattr(self, "unmapped_user_tracker"):
                    current_channel = getattr(self, "current_channel", "unknown")
                    self.unmapped_user_tracker.add_unmapped_user(
                        user_id, current_channel
                    )

                log_with_context(
                    logging.DEBUG,  # Reduced to DEBUG since we now handle this gracefully
                    f"No email mapping found for user {user_id}",
                    user_id=user_id,
                    channel=getattr(self, "current_channel", "unknown"),
                )
                return None  # Return None instead of fallback email

        # Always return the mapped email - don't create fake external emails
        # If the user has a mapping override, that takes precedence
        # If they're external but have a real email mapping, use that
        return user_email

    def _get_user_data(self, user_id: str) -> Optional[Dict]:
        """Get user data from the users.json export file.

        Args:
            user_id: The Slack user ID

        Returns:
            User data dictionary or None if not found
        """
        if not hasattr(self, "_users_data"):
            # Load and cache users data
            import json
            from pathlib import Path

            users_file = Path(self.export_root) / "users.json"
            if users_file.exists():
                try:
                    with open(users_file, "r") as f:
                        users_list = json.load(f)
                    self._users_data = {user["id"]: user for user in users_list}
                except Exception as e:
                    log_with_context(logging.WARNING, f"Error loading users.json: {e}")
                    self._users_data = {}
            else:
                self._users_data = {}

        return self._users_data.get(user_id)

    def _handle_unmapped_user_message(
        self, user_id: str, original_text: str
    ) -> tuple[str, str]:
        """Handle messages from unmapped users by using workspace admin with attribution.

        Args:
            user_id: The unmapped Slack user ID
            original_text: The original message text

        Returns:
            Tuple of (sender_email, modified_message_text)
        """
        # Track this unmapped user
        if hasattr(self, "unmapped_user_tracker"):
            current_channel = getattr(self, "current_channel", "unknown")
            self.unmapped_user_tracker.add_unmapped_user(
                user_id, f"message_sender:{current_channel}"
            )

        # Try to get any additional info about this user from users.json
        user_info = None
        try:
            import json
            from pathlib import Path

            users_file = Path(self.export_root) / "users.json"
            if users_file.exists():
                with open(users_file, "r") as f:
                    users_data = json.load(f)
                    user_info = next(
                        (u for u in users_data if u.get("id") == user_id), None
                    )
        except Exception:
            pass  # Continue without user info if we can't load it

        # Create attribution prefix
        # First check if we have a mapping override (this takes precedence)
        override_email = self.config.get("user_mapping_overrides", {}).get(user_id)
        if override_email:
            attribution = f"*[From: {override_email}]*"
        elif user_info:
            real_name = user_info.get("profile", {}).get("real_name", "")
            username = user_info.get("name", user_id)
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
            # User not in users.json (external or deactivated) and no override
            attribution = f"*[From: {user_id}]*"

        # Combine attribution with original message
        modified_text = f"{attribution}\n{original_text}"

        # Use workspace admin as sender
        admin_email = self.workspace_admin

        log_with_context(
            logging.WARNING,
            f"Sending message from unmapped user {user_id} via workspace admin {admin_email}",
            user_id=user_id,
            channel=getattr(self, "current_channel", "unknown"),
            attribution=attribution,
        )

        return admin_email, modified_text

    def _handle_unmapped_user_reaction(
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
        # Track this unmapped user
        if hasattr(self, "unmapped_user_tracker"):
            current_channel = getattr(self, "current_channel", "unknown")
            self.unmapped_user_tracker.add_unmapped_user(
                user_id, f"reaction:{current_channel}"
            )

        log_with_context(
            logging.WARNING,
            f"Skipping reaction '{reaction}' from unmapped user {user_id} on message {message_ts}",
            user_id=user_id,
            reaction=reaction,
            message_ts=message_ts,
            channel=getattr(self, "current_channel", "unknown"),
        )

        # TODO: Add to migration report for surfacing during reporting
        if not hasattr(self, "skipped_reactions"):
            self.skipped_reactions = []

        self.skipped_reactions.append(
            {
                "user_id": user_id,
                "reaction": reaction,
                "message_ts": message_ts,
                "channel": getattr(self, "current_channel", "unknown"),
            }
        )

        return False  # Skip this reaction

    def _get_space_name(self, channel: str) -> str:
        """Get a consistent display name for a Google Chat space based on channel name."""
        return f"Slack #{channel}"

    def _get_all_channel_names(self) -> List[str]:
        """Get a list of all channel names from the export directory."""
        return [d.name for d in self.export_root.iterdir() if d.is_dir()]

    def _is_external_user(self, email: Optional[str]) -> bool:
        """Check if a user is external based on their email domain.

        Args:
            email: The user's email address

        Returns:
            True if the user is external, False otherwise
        """
        # Fix for syntax error: ensure email is a string before calling .split()
        if not email or not isinstance(email, str) or not self.workspace_domain:
            return False

        # Extract domain from email
        try:
            domain = email.split("@")[-1]
            # Compare with workspace domain
            return domain.lower() != self.workspace_domain.lower()
        except Exception:
            return False

    def migrate(self):
        """Main migration function that orchestrates the entire process."""
        migration_start_time = time.time()
        log_with_context(logging.INFO, "Starting migration process")

        # Import report generation function for use in both success and failure paths
        from slack_migrator.cli.report import generate_report

        # Set up signal handler to ensure we log migration status on interrupt
        def signal_handler(signum, frame):
            """Handle SIGINT (Ctrl+C) by logging migration status and exiting gracefully."""
            migration_duration = time.time() - migration_start_time
            log_with_context(logging.WARNING, "")
            log_with_context(logging.WARNING, "🚨 MIGRATION INTERRUPTED BY SIGNAL")
            self._log_migration_failure(
                KeyboardInterrupt("Migration interrupted by signal"), migration_duration
            )
            # Exit with standard interrupted code
            exit(130)

        # Install the signal handler
        old_signal_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            # Ensure API services are initialized (if not done during permission checks)
            self._initialize_api_services()

            # Initialize the thread map if not already done
            if not hasattr(self, "thread_map"):
                self.thread_map = {}

            # Output directory should already be set up by CLI, but provide a sensible default
            if not hasattr(self, "output_dir") or not self.output_dir:
                # Create default output directory with timestamp
                import datetime

                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                self.output_dir = f"migration_logs/run_{timestamp}"
                log_with_context(
                    logging.INFO, f"Using default output directory: {self.output_dir}"
                )
                # Create the directory
                import os

                os.makedirs(self.output_dir, exist_ok=True)

            # Initialize dictionary to store channel-specific log handlers
            self.channel_handlers = {}

            # Initialize migration summary and error tracking
            self.migration_summary = {
                "channels_processed": [],
                "spaces_created": 0,
                "messages_created": 0,
                "reactions_created": 0,
                "files_created": 0,
            }
            self.migration_errors = []
            self.channels_with_errors = []

            # Report unmapped user issues before starting migration (if any detected during initialization)
            if (
                hasattr(self, "unmapped_user_tracker")
                and self.unmapped_user_tracker.has_unmapped_users()
            ):
                unmapped_users = self.unmapped_user_tracker.get_unmapped_users_list()
                log_with_context(
                    logging.WARNING,
                    f"Found {len(unmapped_users)} unmapped users during setup: {', '.join(unmapped_users)}",
                )
                log_with_context(
                    logging.WARNING,
                    "These will be tracked during migration. Consider adding them to user_mapping_overrides in config.yaml.",
                )

            # In update mode, discover existing spaces via API
            if self.update_mode:
                from slack_migrator.services.message import load_space_mappings

                discovered_spaces = load_space_mappings(self)
                if discovered_spaces:
                    log_with_context(
                        logging.INFO,
                        f"[UPDATE MODE] Discovered {len(discovered_spaces)} existing spaces via API",
                    )
                    self.created_spaces = discovered_spaces
                else:
                    log_with_context(
                        logging.WARNING,
                        f"[UPDATE MODE] No existing spaces found via API. Will create new spaces.",
                    )

            # Get all channel directories
            all_channel_dirs = [d for d in self.export_root.iterdir() if d.is_dir()]
            log_with_context(
                logging.INFO,
                f"Found {len(all_channel_dirs)} channel directories in export",
            )

            # Add ability to abort after first channel error
            self.channel_error_count = 0
            self.first_channel_processed = False

            # Process each channel
            for ch in all_channel_dirs:
                # Track the current channel being processed
                self.current_channel = ch.name

                mode_prefix = "[DRY RUN] "
                if self.update_mode:
                    mode_prefix = (
                        "[UPDATE MODE] "
                        if not self.dry_run
                        else "[DRY RUN] [UPDATE MODE] "
                    )

                log_with_context(
                    logging.INFO,
                    f"{mode_prefix if self.dry_run or self.update_mode else ''}Processing channel: {ch.name}",
                    channel=ch.name,
                )
                self.migration_summary["channels_processed"].append(ch.name)

                # Check if channel should be processed
                if not should_process_channel(ch.name, self.config):
                    log_with_context(
                        logging.WARNING,
                        f"Skipping channel {ch.name} based on configuration",
                        channel=ch.name,
                    )
                    continue

                # Check if this channel has unresolved space conflicts
                if (
                    hasattr(self, "channel_conflicts")
                    and ch.name in self.channel_conflicts
                ):
                    log_with_context(
                        logging.ERROR,
                        f"Skipping channel {ch.name} due to unresolved duplicate space conflict",
                        channel=ch.name,
                    )
                    # Mark this channel as having a conflict in the migration report
                    if not hasattr(self, "migration_issues"):
                        self.migration_issues = {}
                    self.migration_issues[ch.name] = (
                        "Skipped due to duplicate space conflict - requires disambiguation in config.yaml"
                    )
                    continue

                # Setup channel-specific logging for channels that will be processed
                from slack_migrator.utils.logging import (
                    is_debug_api_enabled,
                    setup_channel_logger,
                )

                channel_handler = setup_channel_logger(
                    self.output_dir, ch.name, self.verbose, is_debug_api_enabled()
                )
                self.channel_handlers[ch.name] = channel_handler

                # Initialize error tracking variables
                channel_had_errors = False
                space_name = None

                # Check if we're in update mode and already have a space for this channel
                if self.update_mode and ch.name in self.created_spaces:
                    space = self.created_spaces[ch.name]
                    space_id = (
                        space.split("/")[-1] if space.startswith("spaces/") else space
                    )
                    log_with_context(
                        logging.INFO,
                        f"[UPDATE MODE] Using existing space {space_id} for channel {ch.name}",
                        channel=ch.name,
                    )
                    self.space_cache[ch.name] = space
                else:
                    # Create new space (either in import mode, or update mode with no existing space)
                    action_desc = (
                        "Creating new import mode space"
                        if not self.update_mode
                        else "Creating new space (none found in update mode)"
                    )
                    log_with_context(
                        logging.INFO,
                        f"{'[DRY RUN] ' if self.dry_run else ''}Step 1/6: {action_desc} for {ch.name}",
                        channel=ch.name,
                    )
                    space = self.space_cache.get(ch.name) or create_space(self, ch.name)
                    self.space_cache[ch.name] = space

                # Skip processing if we couldn't create a space due to permissions
                if space and space.startswith("ERROR_NO_PERMISSION_"):
                    log_with_context(
                        logging.WARNING,
                        f"Skipping channel {ch.name} due to space creation permission error",
                        channel=ch.name,
                    )
                    continue

                # Set current space for file attachments
                self.current_space = space
                self.channel_to_space[ch.name] = space

                # Store in created_spaces for future reference
                self.created_spaces[ch.name] = space

                # Log that we're setting the current space
                log_with_context(
                    logging.DEBUG,  # Changed to DEBUG for less verbose output
                    f"Setting current space to {space} for channel {ch.name} and storing in channel_to_space mapping",
                    channel=ch.name,
                )

                # In update mode, skip adding users and sending intro message
                if not self.update_mode:
                    # Step 2: Add historical memberships
                    log_with_context(
                        logging.INFO,
                        f"{'[DRY RUN] ' if self.dry_run else ''}Step 2/6: Adding historical memberships for {ch.name}",
                        channel=ch.name,
                    )
                    add_users_to_space(self, space, ch.name)
                else:
                    log_with_context(
                        logging.INFO,
                        f"[UPDATE MODE] Skipping user addition for existing space",
                        channel=ch.name,
                    )

                # Track if we had errors processing this channel
                space_name = space

                # Process messages for this channel
                mode_prefix = "[DRY RUN]"
                if self.update_mode:
                    mode_prefix = (
                        "[UPDATE MODE]"
                        if not self.dry_run
                        else "[DRY RUN] [UPDATE MODE]"
                    )

                log_with_context(
                    logging.INFO,
                    f"{mode_prefix if self.dry_run or self.update_mode else ''} Step 3/6: Processing messages for {ch.name}",
                    channel=ch.name,
                )

                # Get all messages for this channel
                ch_dir = self.export_root / ch.name
                msgs = []
                for jf in sorted(ch_dir.glob("*.json")):
                    try:
                        with open(jf) as f:
                            msgs.extend(json.load(f))
                    except Exception as e:
                        log_with_context(
                            logging.WARNING,
                            f"Failed to load messages from {jf}: {e}",
                            channel=ch.name,
                        )

                # Sort messages by timestamp to maintain chronological order
                msgs = sorted(msgs, key=lambda m: float(m.get("ts", "0")))

                # Deduplicate messages by timestamp to prevent processing thread replies twice
                # Thread replies appear both in parent's "replies" array and as standalone message objects
                seen_timestamps = set()
                deduped_msgs = []
                duplicate_count = 0

                for msg in msgs:
                    ts = msg.get("ts")
                    if ts and ts not in seen_timestamps:
                        seen_timestamps.add(ts)
                        deduped_msgs.append(msg)
                    elif ts:
                        duplicate_count += 1
                        log_with_context(
                            logging.DEBUG,
                            f"Skipping duplicate message with timestamp {ts}",
                            channel=ch.name,
                            ts=ts,
                        )

                if duplicate_count > 0:
                    log_with_context(
                        logging.INFO,
                        f"Deduplicated {duplicate_count} messages in channel {ch.name} (likely thread reply duplicates)",
                        channel=ch.name,
                    )

                msgs = deduped_msgs

                # Count messages in dry run mode
                if self.dry_run:
                    # Count only actual messages, not other events
                    message_count = sum(1 for m in msgs if m.get("type") == "message")
                    log_with_context(
                        logging.INFO,
                        f"{mode_prefix} Found {message_count} messages in channel {ch.name}",
                        channel=ch.name,
                    )
                    # Add to the total message count
                    self.migration_summary["messages_created"] += message_count

                # Load previously processed messages and thread mappings
                processed_ts = []

                # Discover existing resources (find the last message timestamp) from Google Chat
                if not self.dry_run or self.update_mode:
                    self._discover_channel_resources(ch.name)

                processed_count = 0
                failed_count = 0

                # Get failure threshold configuration
                max_failure_percentage = self.config.get("max_failure_percentage", 10)

                # Track failures for this channel
                channel_failures = []

                # Create more informative progress bar description
                mode_prefix = ""
                if self.dry_run:
                    mode_prefix = "[DRY RUN] "
                elif self.update_mode:
                    mode_prefix = "[UPDATE] "

                progress_desc = f"{mode_prefix}Adding messages to {ch.name}"
                pbar = tqdm(msgs, desc=progress_desc)
                for m in pbar:
                    if m.get("type") != "message":
                        continue

                    ts = m["ts"]

                    # Skip already processed messages (only in non-dry run mode)
                    if ts in processed_ts and not self.dry_run:
                        processed_count += 1
                        continue

                    # Track statistics for this message
                    track_message_stats(self, m)

                    if self.dry_run:
                        continue

                    # Send message using the new method
                    result = send_message(self, space, m)

                    if result:
                        if result != "SKIPPED":
                            # Message was sent successfully
                            processed_ts.append(ts)
                            processed_count += 1
                    else:
                        failed_count += 1
                        channel_failures.append(ts)

                        # Check if we've exceeded our failure threshold
                        if processed_count > 0:  # Avoid division by zero
                            failure_percentage = (
                                failed_count / (processed_count + failed_count)
                            ) * 100
                            if failure_percentage > max_failure_percentage:
                                log_with_context(
                                    logging.WARNING,
                                    f"Failure rate {failure_percentage:.1f}% exceeds threshold {max_failure_percentage}% for channel {ch.name}",
                                    channel=ch.name,
                                )
                                # Flag the channel as having a high error rate, but don't break the loop
                                channel_had_errors = True
                                # Track channels with high failure rates
                                if not hasattr(self, "high_failure_rate_channels"):
                                    self.high_failure_rate_channels = {}
                                self.high_failure_rate_channels[ch.name] = (
                                    failure_percentage
                                )
                                # Don't break the loop - continue processing messages
                                # break  # This line is commented out to continue processing

                    # Add a small delay between messages to avoid rate limits
                    time.sleep(0.05)

                # Record failures for reporting
                if channel_failures:
                    if not hasattr(self, "failed_messages_by_channel"):
                        self.failed_messages_by_channel = {}
                    self.failed_messages_by_channel[ch.name] = channel_failures
                    channel_had_errors = True

                log_with_context(
                    logging.INFO,
                    f"Channel {ch.name} message import: processed {processed_count}, failed {failed_count}",
                    channel=ch.name,
                )

                # Step 4: Complete import mode (only if not in update mode)
                if not self.update_mode:
                    log_with_context(
                        logging.INFO,
                        f"{'[DRY RUN] ' if self.dry_run else ''}Step 4/6: Completing import mode for {ch.name}",
                        channel=ch.name,
                    )

                    # Get the completion strategy from config
                    completion_strategy = self.config.get(
                        "import_completion_strategy", "skip_on_error"
                    )

                    # Only complete import if there were no errors or we're using force_complete strategy
                    if (
                        not channel_had_errors
                        or completion_strategy == "force_complete"
                    ) and not self.dry_run:
                        try:
                            log_with_context(
                                logging.DEBUG,
                                f"Attempting to complete import mode for space {space}",
                                channel=ch.name,
                            )

                            result = (
                                self.chat.spaces().completeImport(name=space).execute()
                            )

                            log_with_context(
                                logging.INFO,
                                f"Successfully completed import mode for space: {space}",
                                channel=ch.name,
                            )

                            # Step 5: Add regular members back to the space
                            log_with_context(
                                logging.INFO,
                                f"{'[DRY RUN] ' if self.dry_run else ''}Step 5/6: Adding current members to space for {ch.name}",
                                channel=ch.name,
                            )

                            try:
                                from slack_migrator.services.space import (
                                    add_regular_members,
                                )

                                add_regular_members(self, space, ch.name)
                                log_with_context(
                                    logging.DEBUG,
                                    f"Successfully added current members to space {space} for channel {ch.name}",
                                    channel=ch.name,
                                )
                            except Exception as e:
                                log_with_context(
                                    logging.ERROR,
                                    f"Error adding current members to space {space}: {e}",
                                    channel=ch.name,
                                )
                                import traceback

                                log_with_context(
                                    logging.DEBUG,
                                    f"Exception traceback: {traceback.format_exc()}",
                                    channel=ch.name,
                                )

                        except Exception as e:
                            log_with_context(
                                logging.ERROR,
                                f"Failed to complete import for space {space}: {e}",
                                channel=ch.name,
                            )
                            channel_had_errors = True

                            # Track spaces that failed to complete import
                            if not hasattr(self, "incomplete_import_spaces"):
                                self.incomplete_import_spaces = []
                            self.incomplete_import_spaces.append((space, ch.name))
                    elif channel_had_errors and not self.dry_run:
                        log_with_context(
                            logging.WARNING,
                            f"Skipping import completion for space {space} due to errors (strategy: {completion_strategy})",
                            channel=ch.name,
                        )

                        # Track spaces that weren't completed due to errors
                        if not hasattr(self, "incomplete_import_spaces"):
                            self.incomplete_import_spaces = []
                        self.incomplete_import_spaces.append((space, ch.name))
                else:
                    log_with_context(
                        logging.INFO,
                        f"[UPDATE MODE] Skipping import completion for existing space",
                        channel=ch.name,
                    )

                # Log completion for this channel
                log_with_context(
                    logging.DEBUG,
                    f"Channel log file completed for channel: {ch.name}",
                    channel=ch.name,
                )

                # Check if we should abort after first channel error
                if self._should_abort_import(ch.name, processed_count, failed_count):
                    log_with_context(
                        logging.WARNING,
                        f"Aborting import after first channel due to errors",
                        channel=ch.name,
                    )
                    break

                # Delete space if there were errors and we're not in dry run mode
                if channel_had_errors and not self.dry_run and not self.update_mode:
                    self._delete_space_if_errors(space_name, ch.name)

            # Log any space mapping conflicts that should be added to config
            from slack_migrator.services.message import log_space_mapping_conflicts

            log_space_mapping_conflicts(self)

            # Generate final unmapped user report
            if (
                hasattr(self, "unmapped_user_tracker")
                and self.unmapped_user_tracker.has_unmapped_users()
            ):
                unmapped_users = self.unmapped_user_tracker.get_unmapped_users_list()
                log_with_context(
                    logging.ERROR,
                    f"MIGRATION COMPLETED WITH {len(unmapped_users)} UNMAPPED USERS:",
                )
                log_with_context(
                    logging.ERROR, f"  Users found: {', '.join(unmapped_users)}"
                )
                log_with_context(
                    logging.ERROR,
                    "  These users likely represent deleted Slack users or bots without email mappings.",
                )
                log_with_context(
                    logging.ERROR,
                    "  Add them to user_mapping_overrides in your config.yaml to resolve.",
                )

            # If this was a dry run, provide specific unmapped user guidance
            if self.dry_run and hasattr(self, "unmapped_user_tracker"):
                from slack_migrator.utils.user_validation import (
                    log_unmapped_user_summary_for_dry_run,
                )

                log_unmapped_user_summary_for_dry_run(self)

            # Generate report
            report_file = generate_report(self)

            # Print summary
            if self.dry_run:
                print_dry_run_summary(self, report_file)

            # Calculate migration duration
            migration_duration = time.time() - migration_start_time

            # Log final success status
            self._log_migration_success(migration_duration)

            # Clean up channel handlers in success case (finally block will also run)
            self._cleanup_channel_handlers()

            return True

        except BaseException as e:
            # Calculate migration duration
            migration_duration = time.time() - migration_start_time

            # Log final failure status
            self._log_migration_failure(e, migration_duration)

            # Generate report even on failure to show progress made
            try:
                report_file = generate_report(self)

                # Log the report location for user reference
                if isinstance(e, KeyboardInterrupt):
                    log_with_context(
                        logging.INFO,
                        f"📋 Partial migration report available at: {report_file}",
                    )
                    log_with_context(
                        logging.INFO,
                        "📋 This report shows progress made before interruption.",
                    )
                else:
                    log_with_context(
                        logging.INFO,
                        f"📋 Migration report (with partial results) available at: {report_file}",
                    )
            except Exception as report_error:
                # Don't let report generation failure mask the original failure
                log_with_context(
                    logging.WARNING,
                    f"Failed to generate migration report after failure: {report_error}",
                )

            # Re-raise the exception to maintain existing error handling behavior
            raise
        finally:
            # Restore the original signal handler
            signal.signal(signal.SIGINT, old_signal_handler)
            # Always ensure proper cleanup of channel log handlers
            self._cleanup_channel_handlers()

    def _cleanup_channel_handlers(self):
        """Clean up and close all channel-specific log handlers."""
        if not hasattr(self, "channel_handlers") or not self.channel_handlers:
            return

        logger = logging.getLogger("slack_migrator")

        for channel_name, handler in list(self.channel_handlers.items()):
            try:
                # Flush any pending log entries
                handler.flush()
                # Close the file handler
                handler.close()
                # Remove the handler from the logger
                logger.removeHandler(handler)
                log_with_context(
                    logging.DEBUG, f"Cleaned up log handler for channel: {channel_name}"
                )
            except Exception as e:
                # Don't let handler cleanup failure prevent the main cleanup
                # Use print to avoid potential logging issues during cleanup
                print(
                    f"Warning: Failed to clean up log handler for channel {channel_name}: {e}"
                )

        # Clear the handlers dictionary
        self.channel_handlers.clear()

    def cleanup(self):
        """Clean up resources and complete import mode on spaces."""
        # Clear current_channel so cleanup operations don't get tagged with channel context
        self.current_channel = None

        if self.dry_run:
            log_with_context(
                logging.INFO, "[DRY RUN] Would perform post-migration cleanup"
            )
            return

        log_with_context(logging.INFO, "Performing post-migration cleanup")

        # Check for spaces that might still be in import mode
        try:
            # List all spaces created by this app
            log_with_context(
                logging.DEBUG, "Listing all spaces to check for import mode..."
            )
            try:
                spaces = self.chat.spaces().list().execute().get("spaces", [])
            except HttpError as http_e:
                log_with_context(
                    logging.ERROR,
                    f"HTTP error listing spaces during cleanup: {http_e} (Status: {http_e.resp.status})",
                    error_code=http_e.resp.status,
                )
                if http_e.resp.status >= 500:
                    log_with_context(
                        logging.WARNING,
                        f"Server error listing spaces - this might be a temporary issue, skipping cleanup",
                    )
                return
            except Exception as list_e:
                log_with_context(
                    logging.ERROR,
                    f"Failed to list spaces during cleanup: {list_e}",
                )
                return

            import_mode_spaces = []

            for space in spaces:
                space_name = space.get("name", "")
                if not space_name:
                    continue

                # Check if space is in import mode
                try:
                    space_info = self.chat.spaces().get(name=space_name).execute()
                    # Use the correct field name: importMode (boolean) instead of importState
                    if space_info.get("importMode") == True:
                        import_mode_spaces.append((space_name, space_info))
                except HttpError as http_e:
                    log_with_context(
                        logging.WARNING,
                        f"HTTP error checking space status during cleanup: {http_e} (Status: {http_e.resp.status})",
                        space_name=space_name,
                        error_code=http_e.resp.status,
                    )
                    if http_e.resp.status >= 500:
                        log_with_context(
                            logging.WARNING,
                            f"Server error checking space - this might be a temporary issue",
                            space_name=space_name,
                        )
                except Exception as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to get space info during cleanup: {e}",
                        space_name=space_name,
                    )

            # Attempt to complete import mode for these spaces
            if import_mode_spaces:
                log_with_context(
                    logging.INFO,
                    f"Found {len(import_mode_spaces)} spaces still in import mode. Attempting to complete import.",
                )

                # Log the current channel_to_space mapping
                log_with_context(
                    logging.INFO,
                    f"Current channel_to_space mapping: {self.channel_to_space}",
                )

                # Log the created_spaces mapping
                log_with_context(
                    logging.INFO,
                    f"Current created_spaces mapping: {self.created_spaces}",
                )

                pbar = tqdm(
                    import_mode_spaces, desc="Completing import mode for spaces"
                )
                for space_name, space_info in pbar:
                    log_with_context(
                        logging.WARNING,
                        f"Found space in import mode during cleanup: {space_name}",
                    )

                    try:
                        # Check if external users are allowed in this space
                        external_users_allowed = space_info.get(
                            "externalUserAllowed", False
                        )

                        # Also check if this space has external users based on our tracking
                        if not external_users_allowed and hasattr(
                            self, "spaces_with_external_users"
                        ):
                            external_users_allowed = (
                                self.spaces_with_external_users.get(space_name, False)
                            )

                            # If we detect external users but the flag isn't set, log this
                            if external_users_allowed:
                                log_with_context(
                                    logging.INFO,
                                    f"Space {space_name} has external users but flag not set, will enable after import",
                                    space_name=space_name,
                                )

                        log_with_context(
                            logging.DEBUG,
                            f"Attempting to complete import mode for space: {space_name}",
                        )

                        try:
                            self.chat.spaces().completeImport(name=space_name).execute()
                            log_with_context(
                                logging.DEBUG,
                                f"Successfully completed import mode for space: {space_name}",
                                channel=channel_name if channel_name else None,
                            )
                        except HttpError as http_e:
                            log_with_context(
                                logging.ERROR,
                                f"HTTP error completing import for space {space_name}: {http_e} (Status: {http_e.resp.status})",
                                space_name=space_name,
                                error_code=http_e.resp.status,
                            )
                            if http_e.resp.status >= 500:
                                log_with_context(
                                    logging.WARNING,
                                    f"Server error completing import - this might be a temporary issue",
                                    space_name=space_name,
                                )
                            continue
                        except Exception as e:
                            log_with_context(
                                logging.ERROR,
                                f"Failed to complete import: {e}",
                                channel=channel_name if channel_name else None,
                            )
                            continue

                        # Ensure external user setting is preserved after import completion
                        if external_users_allowed:
                            try:
                                # Update space to ensure externalUserAllowed is set
                                update_body = {"externalUserAllowed": True}
                                update_mask = "externalUserAllowed"
                                self.chat.spaces().patch(
                                    name=space_name,
                                    updateMask=update_mask,
                                    body=update_body,
                                ).execute()
                                log_with_context(
                                    logging.INFO,
                                    f"Preserved external user access for space: {space_name}",
                                )
                            except HttpError as http_e:
                                log_with_context(
                                    logging.WARNING,
                                    f"HTTP error preserving external user access for space {space_name}: {http_e} (Status: {http_e.resp.status})",
                                    space_name=space_name,
                                    error_code=http_e.resp.status,
                                )
                                if http_e.resp.status >= 500:
                                    log_with_context(
                                        logging.WARNING,
                                        f"Server error updating space - this might be a temporary issue",
                                        space_name=space_name,
                                    )
                            except Exception as e:
                                log_with_context(
                                    logging.WARNING,
                                    f"Failed to preserve external user access: {e}",
                                    space_name=space_name,
                                )

                        # First try to find the channel using our channel_to_space mapping
                        channel_name = None
                        for ch, sp in self.channel_to_space.items():
                            if sp == space_name:
                                channel_name = ch
                                log_with_context(
                                    logging.INFO,
                                    f"Found channel {channel_name} for space {space_name} using channel_to_space mapping",
                                )
                                break

                        # If not found in channel_to_space, try the space display name
                        if not channel_name:
                            display_name = space_info.get("displayName", "")
                            log_with_context(
                                logging.DEBUG,
                                f"Attempting to extract channel name from display name: {display_name}",
                            )

                            # Try to extract channel name based on our naming convention
                            for ch in self._get_all_channel_names():
                                ch_name = self._get_space_name(ch)
                                if ch_name in display_name:
                                    channel_name = ch
                                    log_with_context(
                                        logging.INFO,
                                        f"Found channel {channel_name} for space {space_name} using display name",
                                    )
                                    break

                        if channel_name:
                            # Step 5: Add regular members back to the space
                            log_with_context(
                                logging.INFO,
                                f"Step 5/6: Adding regular members to space for channel: {channel_name}",
                            )
                            try:
                                add_regular_members(self, space_name, channel_name)
                                log_with_context(
                                    logging.DEBUG,
                                    f"Successfully added regular members to space {space_name} for channel: {channel_name}",
                                )
                            except Exception as e:
                                log_with_context(
                                    logging.ERROR,
                                    f"Error adding regular members to space {space_name}: {e}",
                                    channel=channel_name,
                                )
                                import traceback

                                log_with_context(
                                    logging.DEBUG,
                                    f"Exception traceback: {traceback.format_exc()}",
                                    channel=channel_name,
                                )
                        else:
                            log_with_context(
                                logging.WARNING,
                                f"Could not determine channel name for space {space_name}, skipping adding members",
                                space_name=space_name,
                            )

                    except HttpError as http_e:
                        log_with_context(
                            logging.ERROR,
                            f"HTTP error during cleanup for space {space_name}: {http_e} (Status: {http_e.resp.status})",
                            space_name=space_name,
                            error_code=http_e.resp.status,
                        )
                        if http_e.resp.status >= 500:
                            log_with_context(
                                logging.WARNING,
                                f"Server error during cleanup - this might be a temporary issue",
                                space_name=space_name,
                            )
                    except Exception as e:
                        log_with_context(
                            logging.ERROR,
                            f"Failed to complete import mode for space {space_name} during cleanup: {e}",
                            space_name=space_name,
                        )
            else:
                log_with_context(
                    logging.INFO, "No spaces found in import mode during cleanup."
                )

        except HttpError as http_e:
            log_with_context(
                logging.ERROR,
                f"HTTP error during post-migration cleanup: {http_e} (Status: {http_e.resp.status})",
                error_code=http_e.resp.status,
            )
            if http_e.resp.status >= 500:
                log_with_context(
                    logging.WARNING,
                    f"Server error during cleanup - Google's servers may be experiencing issues",
                )
            elif http_e.resp.status == 403:
                log_with_context(
                    logging.WARNING,
                    f"Permission error during cleanup - service account may lack required permissions",
                )
            elif http_e.resp.status == 429:
                log_with_context(
                    logging.WARNING,
                    f"Rate limit exceeded during cleanup - too many API requests",
                )
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Unexpected error during cleanup: {e}",
            )
            import traceback

            log_with_context(
                logging.DEBUG,
                f"Cleanup exception traceback: {traceback.format_exc()}",
            )

        log_with_context(logging.INFO, "Cleanup completed")

    def _load_existing_space_mappings(self):
        """
        Load existing space mappings from Google Chat API.

        This method only discovers spaces when in update mode. In regular import mode,
        we want to create new spaces, not reuse existing ones.
        """
        # Only discover existing spaces in update mode
        if not self.update_mode:
            log_with_context(
                logging.INFO,
                "Import mode: Will create new spaces (not discovering existing spaces)",
            )
            return

        try:
            # Import the discovery module
            from slack_migrator.services.discovery import discover_existing_spaces

            # Discover existing spaces from Google Chat API
            log_with_context(
                logging.INFO, "[UPDATE MODE] Discovering existing Google Chat spaces"
            )

            # Query Google Chat API to find spaces that match our naming pattern
            # This will also detect duplicate spaces with the same channel name
            discovered_spaces, duplicate_spaces = discover_existing_spaces(self)

            # Initialize conflict tracking
            if not hasattr(self, "channel_conflicts"):
                self.channel_conflicts = set()

            # Check if we have any spaces with duplicate names that need disambiguation
            if duplicate_spaces:
                # Check config for space_mapping to disambiguate
                space_mapping = self.config.get("space_mapping") or {}

                log_with_context(
                    logging.WARNING,
                    f"Found {len(duplicate_spaces)} channels with duplicate spaces",
                )

                # Initialize arrays to track conflicts
                unresolved_conflicts = []
                resolved_conflicts = []

                for channel_name, spaces in duplicate_spaces.items():
                    # Check if this channel has a mapping in the config
                    if space_mapping and channel_name in space_mapping:
                        # Get the space ID from the config
                        configured_space_id = space_mapping[channel_name]

                        # Find the space with matching ID
                        matching_space = None
                        for space_info in spaces:
                            if space_info["space_id"] == configured_space_id:
                                matching_space = space_info
                                break

                        if matching_space:
                            # Replace the automatically selected space with the configured one
                            log_with_context(
                                logging.INFO,
                                f"Using configured space mapping for channel '{channel_name}': {configured_space_id}",
                            )
                            discovered_spaces[channel_name] = matching_space[
                                "space_name"
                            ]
                            resolved_conflicts.append(channel_name)
                        else:
                            # The configured space ID doesn't match any of the duplicates
                            unresolved_conflicts.append(channel_name)
                            self.channel_conflicts.add(channel_name)
                            log_with_context(
                                logging.ERROR,
                                f"Configured space ID for channel '{channel_name}' ({configured_space_id}) "
                                f"doesn't match any discovered spaces",
                            )
                    else:
                        # No mapping in config - this is an unresolved conflict
                        unresolved_conflicts.append(channel_name)
                        self.channel_conflicts.add(channel_name)
                        log_with_context(
                            logging.ERROR,
                            f"Channel '{channel_name}' has {len(spaces)} duplicate spaces and no mapping in config",
                        )
                        # Print information about each space to help the user decide
                        log_with_context(
                            logging.ERROR,
                            "Please add a space_mapping entry to config.yaml to disambiguate:",
                        )
                        log_with_context(logging.ERROR, "space_mapping:")
                        for space_info in spaces:
                            log_with_context(
                                logging.ERROR,
                                f"  # {space_info['display_name']} (Members: {space_info['member_count']}, Created: {space_info['create_time']})",
                            )
                            log_with_context(
                                logging.ERROR,
                                f'  "{channel_name}": "{space_info["space_id"]}"',
                            )

                # Mark unresolved conflicts but don't abort the entire migration
                if unresolved_conflicts:
                    if not hasattr(self, "migration_issues"):
                        self.migration_issues = {}

                    for channel in unresolved_conflicts:
                        self.migration_issues[channel] = (
                            "Duplicate spaces found - requires disambiguation in config.yaml"
                        )

                    log_with_context(
                        logging.ERROR,
                        f"Found unresolved duplicate space conflicts for channels: {', '.join(unresolved_conflicts)}. "
                        "These channels will be marked as failed. Add space_mapping entries to config.yaml to resolve.",
                    )

                if resolved_conflicts:
                    log_with_context(
                        logging.INFO,
                        f"Successfully resolved space conflicts for channels: {', '.join(resolved_conflicts)}",
                    )

            if discovered_spaces:
                log_with_context(
                    logging.INFO,
                    f"Found {len(discovered_spaces)} existing spaces in Google Chat",
                )

                # Log detailed information about what will happen with each discovered space
                for channel, space_name in discovered_spaces.items():
                    space_id = (
                        space_name.split("/")[-1]
                        if space_name.startswith("spaces/")
                        else space_name
                    )

                    mode_info = "[UPDATE MODE] " if self.update_mode else ""
                    log_with_context(
                        logging.INFO,
                        f"{mode_info}Will use existing space {space_id} for channel '{channel}'",
                        channel=channel,
                    )

                # Initialize channel_id_to_space_id mapping if it doesn't exist
                if not hasattr(self, "channel_id_to_space_id"):
                    self.channel_id_to_space_id = {}

                # Update the channel_to_space mapping
                for channel, space_name in discovered_spaces.items():
                    # Store the space name for backward compatibility
                    self.channel_to_space[channel] = space_name

                    # Extract space ID from space_name (format: spaces/{space_id})
                    space_id = (
                        space_name.split("/")[-1]
                        if space_name.startswith("spaces/")
                        else space_name
                    )

                    # Look up the channel ID if available
                    channel_id = self.channel_name_to_id.get(channel, "")
                    if channel_id:
                        # Store using channel ID -> space ID mapping for more robust identification
                        self.channel_id_to_space_id[channel_id] = space_id

                        log_with_context(
                            logging.DEBUG,
                            f"Mapped channel ID {channel_id} to space ID {space_id}",
                        )

                    # Also update created_spaces for consistency
                    if self.update_mode:
                        self.created_spaces[channel] = space_name

                log_with_context(
                    logging.INFO,
                    f"Space discovery complete: {len(self.channel_to_space)} channels have existing spaces, others will create new spaces",
                )
            else:
                log_with_context(
                    logging.INFO, "No existing spaces found in Google Chat"
                )

        except Exception as e:
            log_with_context(
                logging.ERROR, f"Failed to load existing space mappings: {e}"
            )
            if not self.dry_run:
                # In dry run, continue even with errors
                raise

    def _log_migration_success(self, duration: float) -> None:
        """Log final migration success status with comprehensive summary.

        Args:
            duration: Migration duration in seconds
        """
        duration_minutes = duration / 60

        # Count various statistics
        channels_processed = len(self.migration_summary.get("channels_processed", []))
        spaces_created = self.migration_summary.get("spaces_created", 0)
        messages_created = self.migration_summary.get("messages_created", 0)
        reactions_created = self.migration_summary.get("reactions_created", 0)
        files_created = self.migration_summary.get("files_created", 0)

        # Count channels with errors
        channels_with_errors = len(getattr(self, "channels_with_errors", []))

        # Count unmapped users
        unmapped_user_count = 0
        if (
            hasattr(self, "unmapped_user_tracker")
            and self.unmapped_user_tracker.has_unmapped_users()
        ):
            unmapped_user_count = self.unmapped_user_tracker.get_unmapped_count()

        # Count incomplete imports
        incomplete_imports = len(getattr(self, "incomplete_import_spaces", []))

        # Log comprehensive final status
        if self.dry_run:
            log_with_context(
                logging.INFO,
                "=" * 80,
            )
            log_with_context(
                logging.INFO,
                "🔍 DRY RUN VALIDATION COMPLETED SUCCESSFULLY",
            )
        else:
            log_with_context(
                logging.INFO,
                "=" * 80,
            )
            # Check if any actual migration work was done
            no_work_done = spaces_created == 0 and messages_created == 0
            interrupted_early = channels_processed == 0

            if no_work_done:
                if interrupted_early:
                    log_with_context(
                        logging.WARNING,
                        "⚠️  MIGRATION WAS INTERRUPTED DURING INITIALIZATION - NO CHANNELS PROCESSED",
                    )
                else:
                    log_with_context(
                        logging.WARNING,
                        "⚠️  MIGRATION WAS INTERRUPTED BEFORE ANY SPACES WERE IMPORTED",
                    )
            else:
                log_with_context(
                    logging.INFO,
                    "🎉 SLACK-TO-GOOGLE-CHAT MIGRATION COMPLETED SUCCESSFULLY!",
                )

        log_with_context(
            logging.INFO,
            "=" * 80,
        )

        # Migration statistics
        log_with_context(
            logging.INFO,
            f"📊 MIGRATION STATISTICS:",
        )
        log_with_context(
            logging.INFO,
            f"   • Duration: {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
        )
        log_with_context(
            logging.INFO,
            f"   • Channels processed: {channels_processed}",
        )
        if not self.dry_run:
            log_with_context(
                logging.INFO,
                f"   • Spaces created/updated: {spaces_created}",
            )
            log_with_context(
                logging.INFO,
                f"   • Messages migrated: {messages_created}",
            )
            log_with_context(
                logging.INFO,
                f"   • Reactions migrated: {reactions_created}",
            )
            log_with_context(
                logging.INFO,
                f"   • Files migrated: {files_created}",
            )

        # Issues and warnings
        issues_found = False
        if unmapped_user_count > 0:
            issues_found = True
            log_with_context(
                logging.WARNING,
                f"   • Unmapped users: {unmapped_user_count}",
            )

        if channels_with_errors > 0:
            issues_found = True
            log_with_context(
                logging.WARNING,
                f"   • Channels with errors: {channels_with_errors}",
            )

        if incomplete_imports > 0:
            issues_found = True
            log_with_context(
                logging.WARNING,
                f"   • Incomplete imports: {incomplete_imports}",
            )

        if not issues_found:
            log_with_context(
                logging.INFO,
                f"   • Issues detected: None! 🎉",
            )

        log_with_context(
            logging.INFO,
            "=" * 80,
        )

        if self.dry_run:
            log_with_context(
                logging.INFO,
                "✅ Validation complete! Review the logs and run without --dry_run to migrate.",
            )
        else:
            # Check if any actual migration work was done
            no_work_done = spaces_created == 0 and messages_created == 0
            interrupted_early = channels_processed == 0

            if no_work_done:
                if interrupted_early:
                    log_with_context(
                        logging.WARNING,
                        "⚠️  Migration was interrupted during setup before any channels were processed.",
                    )
                    log_with_context(
                        logging.INFO,
                        "💡 The migration may have been interrupted during channel filtering or initialization.",
                    )
                else:
                    log_with_context(
                        logging.WARNING,
                        "⚠️  Migration was interrupted before any spaces were successfully imported.",
                    )
                log_with_context(
                    logging.INFO,
                    "💡 To complete the migration, run the command again.",
                )
                log_with_context(
                    logging.INFO,
                    "📋 Check the migration report and logs for any issues that need to be addressed.",
                )
            elif issues_found:
                log_with_context(
                    logging.WARNING,
                    "✅ Migration completed with some issues. Check the detailed logs and report.",
                )
            else:
                log_with_context(
                    logging.INFO,
                    "✅ Migration completed successfully with no issues detected!",
                )

        log_with_context(
            logging.INFO,
            "=" * 80,
        )

    def _log_migration_failure(self, exception: BaseException, duration: float) -> None:
        """Log final migration failure status with error details.

        Args:
            exception: The exception that caused the failure
            duration: Migration duration in seconds before failure
        """
        import traceback

        duration_minutes = duration / 60

        # Count what we accomplished before failure
        channels_processed = len(self.migration_summary.get("channels_processed", []))
        spaces_created = self.migration_summary.get("spaces_created", 0)
        messages_created = self.migration_summary.get("messages_created", 0)

        log_with_context(
            logging.ERROR,
            "=" * 80,
        )

        # Handle KeyboardInterrupt differently
        if isinstance(exception, KeyboardInterrupt):
            if self.dry_run:
                log_with_context(
                    logging.WARNING,
                    "⏹️  DRY RUN VALIDATION INTERRUPTED BY USER",
                )
            else:
                log_with_context(
                    logging.WARNING,
                    "⏹️  SLACK-TO-GOOGLE-CHAT MIGRATION INTERRUPTED BY USER",
                )
        else:
            if self.dry_run:
                log_with_context(
                    logging.ERROR,
                    "❌ DRY RUN VALIDATION FAILED",
                )
            else:
                log_with_context(
                    logging.ERROR,
                    "❌ SLACK-TO-GOOGLE-CHAT MIGRATION FAILED",
                )

        log_with_context(
            logging.ERROR,
            "=" * 80,
        )

        # Error details
        if isinstance(exception, KeyboardInterrupt):
            log_with_context(
                logging.WARNING,
                f"⏹️  INTERRUPTION DETAILS:",
            )
            log_with_context(
                logging.WARNING,
                f"   • Type: User interruption (Ctrl+C)",
            )
            log_with_context(
                logging.WARNING,
                f"   • Duration before interruption: {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
            )
        else:
            log_with_context(
                logging.ERROR,
                f"💥 ERROR DETAILS:",
            )
            log_with_context(
                logging.ERROR,
                f"   • Exception: {type(exception).__name__}",
            )
            log_with_context(
                logging.ERROR,
                f"   • Message: {str(exception)}",
            )
            log_with_context(
                logging.ERROR,
                f"   • Duration before failure: {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
            )

        # Progress before failure/interruption
        progress_level = (
            logging.WARNING
            if isinstance(exception, KeyboardInterrupt)
            else logging.ERROR
        )
        progress_label = (
            "PROGRESS BEFORE INTERRUPTION"
            if isinstance(exception, KeyboardInterrupt)
            else "PROGRESS BEFORE FAILURE"
        )

        log_with_context(
            progress_level,
            f"📊 {progress_label}:",
        )
        log_with_context(
            progress_level,
            f"   • Channels processed: {channels_processed}",
        )
        if not self.dry_run:
            log_with_context(
                progress_level,
                f"   • Spaces created: {spaces_created}",
            )
            log_with_context(
                progress_level,
                f"   • Messages migrated: {messages_created}",
            )

        # Log the full traceback for debugging (skip for KeyboardInterrupt as it's not useful)
        if not isinstance(exception, KeyboardInterrupt):
            log_with_context(
                logging.ERROR,
                f"🔍 FULL TRACEBACK:",
            )
            log_with_context(
                logging.ERROR,
                traceback.format_exc(),
            )

        log_with_context(
            (
                logging.ERROR
                if not isinstance(exception, KeyboardInterrupt)
                else logging.WARNING
            ),
            "=" * 80,
        )

        if isinstance(exception, KeyboardInterrupt):
            if self.dry_run:
                log_with_context(
                    logging.WARNING,
                    "⏹️  Validation interrupted. You can restart the validation anytime.",
                )
            else:
                log_with_context(
                    logging.WARNING,
                    "⏹️  Migration interrupted. Use --update_mode to resume from where you left off.",
                )
        else:
            if self.dry_run:
                log_with_context(
                    logging.ERROR,
                    "❌ Fix the validation issues above and try again.",
                )
            else:
                log_with_context(
                    logging.ERROR,
                    "❌ Migration failed. Check the error details and try --update_mode to resume.",
                )

        log_with_context(
            (
                logging.ERROR
                if not isinstance(exception, KeyboardInterrupt)
                else logging.WARNING
            ),
            "=" * 80,
        )
