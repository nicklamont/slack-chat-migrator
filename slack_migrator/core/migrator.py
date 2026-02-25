"""
Main migrator class for the Slack to Google Chat migration tool
"""

import json
import logging
import signal
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from slack_migrator.core.channel_processor import ChannelProcessor


from slack_migrator.cli.report import (
    generate_report,
    print_dry_run_summary,
)
from slack_migrator.core.cleanup import cleanup_channel_handlers
from slack_migrator.core.migration_logging import (
    log_migration_failure,
    log_migration_success,
)
from slack_migrator.core.state import MigrationState
from slack_migrator.services.file import FileHandler
from slack_migrator.services.user import generate_user_map
from slack_migrator.services.user_resolver import UserResolver
from slack_migrator.utils.api import get_gcp_service
from slack_migrator.utils.logging import log_with_context


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
        self.chat: Optional[Any] = None
        self.drive: Optional[Any] = None
        self._api_services_initialized = False

        # User resolver is needed before API services (e.g. for _is_external_user)
        self.user_resolver = UserResolver.from_migrator(self)

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
        from slack_migrator.services.discovery import load_existing_space_mappings

        load_existing_space_mappings(self)

    def _validate_export_format(self):
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
        return self.user_resolver.get_delegate(email)

    def _discover_channel_resources(self, channel: str):
        """Find the last message timestamp in a space to determine where to resume."""
        self._get_channel_processor()._discover_channel_resources(channel)

    def _should_abort_import(
        self, channel: str, processed_count: int, failed_count: int
    ) -> bool:
        """Determine if we should abort the import after errors in a channel."""
        return self._get_channel_processor()._should_abort_import(
            channel, processed_count, failed_count
        )

    def _delete_space_if_errors(self, space_name, channel):
        """Delete a space if it had errors and cleanup is enabled."""
        self._get_channel_processor()._delete_space_if_errors(space_name, channel)

    def _get_channel_processor(self) -> "ChannelProcessor":
        """Get or create the ChannelProcessor instance."""
        if not hasattr(self, "channel_processor"):
            from slack_migrator.core.channel_processor import ChannelProcessor

            self.channel_processor = ChannelProcessor(self)
        return self.channel_processor

    def _get_internal_email(
        self, user_id: str, user_email: Optional[str] = None
    ) -> Optional[str]:
        """Get internal email for a user, handling external users and tracking unmapped users."""
        return self.user_resolver.get_internal_email(user_id, user_email)

    def _get_user_data(self, user_id: str) -> Optional[dict]:
        """Get user data from the users.json export file."""
        return self.user_resolver.get_user_data(user_id)

    def _handle_unmapped_user_message(
        self, user_id: str, original_text: str
    ) -> tuple[str, str]:
        """Handle messages from unmapped users by using workspace admin with attribution."""
        return self.user_resolver.handle_unmapped_user_message(user_id, original_text)

    def _handle_unmapped_user_reaction(
        self, user_id: str, reaction: str, message_ts: str
    ) -> bool:
        """Handle reactions from unmapped users by logging and skipping."""
        return self.user_resolver.handle_unmapped_user_reaction(
            user_id, reaction, message_ts
        )

    def _get_space_name(self, channel: str) -> str:
        """Get a consistent display name for a Google Chat space based on channel name."""
        return f"Slack #{channel}"

    def _get_all_channel_names(self) -> list[str]:
        """Get a list of all channel names from the export directory."""
        return [d.name for d in self.export_root.iterdir() if d.is_dir()]

    def _is_external_user(self, email: Optional[str]) -> bool:
        """Check if a user is external based on their email domain."""
        return self.user_resolver.is_external_user(email)

    def migrate(self):
        """Main migration function that orchestrates the entire process.

        Returns:
            True on successful completion.
        """
        migration_start_time = time.time()
        log_with_context(logging.INFO, "Starting migration process")

        # Import report generation function for use in both success and failure paths

        # Set up signal handler to ensure we log migration status on interrupt
        def signal_handler(signum, frame):
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
                import datetime

                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                self.state.output_dir = f"migration_logs/run_{timestamp}"
                log_with_context(
                    logging.INFO,
                    f"Using default output directory: {self.state.output_dir}",
                )
                # Create the directory
                import os

                os.makedirs(self.state.output_dir, exist_ok=True)

            # Reset per-run state
            self.state.channel_handlers = {}
            self.state.thread_map = {}
            self.state.migration_summary = {
                "channels_processed": [],
                "spaces_created": 0,
                "messages_created": 0,
                "reactions_created": 0,
                "files_created": 0,
            }
            self.state.migration_errors = []
            self.state.channels_with_errors = []

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
                from slack_migrator.services.discovery import load_space_mappings

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

            # Add ability to abort after first channel error
            self.state.channel_error_count = 0
            self.state.first_channel_processed = False

            # Process each channel
            from slack_migrator.core.channel_processor import ChannelProcessor

            self.channel_processor = ChannelProcessor(self)
            for ch in all_channel_dirs:
                should_abort = self.channel_processor.process_channel(ch)
                if should_abort:
                    break

            # Log any space mapping conflicts that should be added to config
            from slack_migrator.services.discovery import log_space_mapping_conflicts

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
            log_migration_success(self, migration_duration)

            # Clean up channel handlers in success case (finally block will also run)
            cleanup_channel_handlers(self)

            return True

        except BaseException as e:
            # Calculate migration duration
            migration_duration = time.time() - migration_start_time

            # Log final failure status
            log_migration_failure(self, e, migration_duration)

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
            cleanup_channel_handlers(self)
