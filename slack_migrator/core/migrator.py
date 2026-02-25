"""
Main migrator class for the Slack to Google Chat migration tool
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import signal
import time
from pathlib import Path
from typing import Any

from slack_migrator.constants import SPACE_NAME_PREFIX
from slack_migrator.core.channel_processor import ChannelProcessor
from slack_migrator.core.cleanup import cleanup_channel_handlers
from slack_migrator.core.config import load_config
from slack_migrator.core.migration_logging import (
    log_migration_failure,
    log_migration_success,
)
from slack_migrator.core.state import MigrationState
from slack_migrator.services.discovery import (
    load_existing_space_mappings,
    load_space_mappings,
    log_space_mapping_conflicts,
)
from slack_migrator.services.file import FileHandler
from slack_migrator.services.message_attachments import MessageAttachmentProcessor
from slack_migrator.services.user import generate_user_map
from slack_migrator.services.user_resolver import UserResolver
from slack_migrator.utils.api import get_gcp_service
from slack_migrator.utils.logging import log_with_context
from slack_migrator.utils.user_validation import (
    initialize_unmapped_user_tracking,
    log_unmapped_user_summary_for_dry_run,
    scan_channel_members_for_unmapped_users,
)


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
        self.workspace_admin = workspace_admin.strip()
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
                logging.INFO, "Running in update mode - will update existing spaces"
            )

        # All mutable tracking state lives in MigrationState
        self.state = MigrationState()

        # Immutable data loaded once during init
        self.user_map: dict[str, str] = {}  # slack_user_id -> google_email
        self.users_without_email: list[
            dict[str, Any]
        ] = []  # List of users without email mappings
        self.progress_file = self.export_root / ".migration_progress.json"

        # Validate workspace admin email format
        if (
            "@" not in self.workspace_admin
            or self.workspace_admin.count("@") != 1
            or not self.workspace_admin.split("@")[0]
            or not self.workspace_admin.split("@")[1]
        ):
            raise ValueError(
                f"Invalid workspace_admin email: '{self.workspace_admin}'. Must be a valid email address."
            )

        # Extract workspace domain from admin email for external user detection
        self.workspace_domain = self.workspace_admin.split("@")[1]

        # Initialize API clients
        self._validate_export_format()

        # Load config using the shared load_config function
        self.config = load_config(self.config_path)

        # Generate user mapping from users.json
        self.user_map, self.users_without_email = generate_user_map(
            self.export_root, self.config
        )

        # Initialize simple unmapped user tracking
        self.unmapped_user_tracker = initialize_unmapped_user_tracking(self)

        # Scan channel members to ensure all channel members have user mappings
        # This is crucial because Google Chat needs to add all channel members to spaces
        scan_channel_members_for_unmapped_users(self)

        # API services will be initialized later after permission checks
        self.chat: Any | None = None
        self.drive: Any | None = None
        self._api_services_initialized = False

        # User resolver is needed before API services (e.g. for is_external_user)
        self.user_resolver = UserResolver.from_migrator(self)

        # Load channel metadata from channels.json
        self.channels_meta, self.channel_id_to_name = self._load_channels_meta()

        # Create reverse mapping for convenience
        self.channel_name_to_id = {
            name: id for id, name in self.channel_id_to_name.items()
        }

    def _initialize_api_services(self) -> None:
        """Initialize Google API services after permission validation."""
        if self._api_services_initialized:
            return

        log_with_context(
            logging.INFO, "Initializing Google Chat and Drive API services..."
        )

        # Convert Path to str for API clients
        creds_path_str = str(self.creds_path)
        self.chat = get_gcp_service(
            creds_path_str,
            self.workspace_admin,
            "chat",
            "v1",
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )
        self.drive = get_gcp_service(
            creds_path_str,
            self.workspace_admin,
            "drive",
            "v3",
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
        )

        # Update the user resolver's chat reference — it was None at construction
        # time because API services are initialized after the resolver.
        self.user_resolver.chat = self.chat

        self._api_services_initialized = True
        log_with_context(
            logging.INFO, "Google Chat and Drive API services initialized successfully"
        )

        # Initialize dependent services
        self._initialize_dependent_services()

    def _initialize_dependent_services(self) -> None:
        """Initialize services that depend on API clients."""
        # Initialize file handler
        self.file_handler = FileHandler(
            self.drive, self.chat, folder_id=None, migrator=self, dry_run=self.dry_run
        )
        # FileHandler now handles its own drive folder initialization automatically

        # Initialize message attachment processor
        self.attachment_processor = MessageAttachmentProcessor(
            self.file_handler, dry_run=self.dry_run
        )

        # Reset mutable state for this run
        self.state.created_spaces.clear()
        self.state.current_channel = None

        # Permission validation is now handled by the CLI layer to avoid duplicates
        # The CLI will call validate_permissions() unless --skip_permission_check is used

        if self.verbose:
            log_with_context(
                logging.DEBUG, "Migrator initialized with verbose logging enabled"
            )

        # Load existing space mappings for update mode or file attachments
        load_existing_space_mappings(self)

    def _validate_export_format(self) -> None:
        """Validate that the export directory has the expected structure."""
        # Check that the export root is a valid directory before inspecting contents
        if not self.export_root.is_dir():
            raise ValueError(
                f"Export path is not a valid directory: {self.export_root}"
            )

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

    def _load_channels_meta(self) -> tuple[dict[str, Any], dict[str, str]]:
        """
        Load channel metadata from channels.json file.

        Returns:
            tuple: (name_to_data, id_to_name) where:
                - name_to_data: Dict mapping channel names to their metadata
                - id_to_name: Dict mapping channel IDs to channel names
        """
        channels_file = self.export_root / "channels.json"
        name_to_data = {}
        id_to_name = {}

        if channels_file.exists():
            with open(channels_file) as f_in:
                channels = json.load(f_in)
                name_to_data = {ch["name"]: ch for ch in channels}
                id_to_name = {ch["id"]: ch["name"] for ch in channels}

        return name_to_data, id_to_name

    def _get_space_name(self, channel: str) -> str:
        """Get a consistent display name for a Google Chat space based on channel name."""
        return f"{SPACE_NAME_PREFIX}{channel}"

    def _get_all_channel_names(self) -> list[str]:
        """Get a list of all channel names from the export directory."""
        return [d.name for d in self.export_root.iterdir() if d.is_dir()]

    def migrate(self) -> bool:
        """Main migration function that orchestrates the entire process.

        Returns:
            True on successful completion.
        """
        migration_start_time = time.time()
        log_with_context(logging.INFO, "Starting migration process")

        # Set up signal handler to ensure we log migration status on interrupt
        def signal_handler(signum: int, frame: Any) -> None:
            """Handle SIGINT (Ctrl+C) gracefully.

            Args:
                signum: Signal number received.
                frame: Current stack frame (unused).
            """
            migration_duration = time.time() - migration_start_time
            log_with_context(logging.WARNING, "")
            log_with_context(logging.WARNING, "MIGRATION INTERRUPTED BY SIGNAL")
            log_migration_failure(
                self,
                KeyboardInterrupt("Migration interrupted by signal"),
                migration_duration,
            )
            # Exit with standard interrupted code
            exit(130)

        # Install the signal handler
        old_signal_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            # Ensure API services are initialized (if not done during permission checks)
            self._initialize_api_services()

            # Output directory should already be set up by CLI, but provide a sensible default
            if not self.state.output_dir:
                # Create default output directory with timestamp
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                self.state.output_dir = f"migration_logs/run_{timestamp}"
                log_with_context(
                    logging.INFO,
                    f"Using default output directory: {self.state.output_dir}",
                )
                # Create the directory
                os.makedirs(self.state.output_dir, exist_ok=True)

            # Reset per-run state
            self.state.reset_for_run()

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
                discovered_spaces = load_space_mappings(self)
                if discovered_spaces:
                    log_with_context(
                        logging.INFO,
                        f"[UPDATE MODE] Discovered {len(discovered_spaces)} existing spaces via API",
                    )
                    self.state.created_spaces = discovered_spaces
                else:
                    log_with_context(
                        logging.WARNING,
                        "[UPDATE MODE] No existing spaces found via API. Will create new spaces.",
                    )

            # Get all channel directories
            all_channel_dirs = [d for d in self.export_root.iterdir() if d.is_dir()]
            log_with_context(
                logging.INFO,
                f"Found {len(all_channel_dirs)} channel directories in export",
            )

            # Process each channel
            self.channel_processor = ChannelProcessor(self)
            for ch in all_channel_dirs:
                should_abort = self.channel_processor.process_channel(ch)
                if should_abort:
                    break

            # Log any space mapping conflicts that should be added to config
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
                log_unmapped_user_summary_for_dry_run(self)

            # Calculate migration duration
            migration_duration = time.time() - migration_start_time

            # Log final success status
            log_migration_success(self, migration_duration)

            # Clean up channel handlers in success case (finally block will also run)
            cleanup_channel_handlers(self)

            return True

        except BaseException as e:
            # Calculate migration duration
            migration_duration = time.time() - migration_start_time

            # Log final failure status
            log_migration_failure(self, e, migration_duration)

            # Re-raise the exception to maintain existing error handling behavior
            raise
        finally:
            # Restore the original signal handler
            signal.signal(signal.SIGINT, old_signal_handler)
            # Always ensure proper cleanup of channel log handlers
            cleanup_channel_handlers(self)
