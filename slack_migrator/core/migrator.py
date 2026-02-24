"""
Main migrator class for the Slack to Google Chat migration tool
"""

import json
import logging
import signal
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from slack_migrator.core.channel_processor import ChannelProcessor

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.cli.report import (
    generate_report,
    print_dry_run_summary,
)
from slack_migrator.core.state import MigrationState
from slack_migrator.services.file import FileHandler
from slack_migrator.services.membership_manager import add_regular_members
from slack_migrator.services.user import generate_user_map
from slack_migrator.services.user_resolver import UserResolver
from slack_migrator.utils.api import get_gcp_service
from slack_migrator.utils.logging import log_with_context


def _list_all_spaces(chat_service) -> list[dict[str, Any]]:
    """Paginate through ``spaces().list()`` and return every space."""
    spaces = []
    page_token = None
    while True:
        try:
            response = (
                chat_service.spaces().list(pageSize=100, pageToken=page_token).execute()
            )
            spaces.extend(response.get("spaces", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        except HttpError as http_e:
            log_with_context(
                logging.ERROR,
                f"HTTP error listing spaces during cleanup: {http_e} "
                f"(Status: {http_e.resp.status})",
            )
            break
        except (RefreshError, TransportError) as e:
            log_with_context(logging.ERROR, f"Failed to list spaces: {e}")
            break
    return spaces


def cleanup_import_mode_spaces(chat_service) -> None:
    """
    Complete import mode on any spaces still stuck in import mode.

    This is a standalone version of the cleanup logic that only requires a
    Chat API service client — no export data or user mappings needed.
    It finds all spaces in import mode and calls completeImport() on each.

    Member-adding is skipped because that requires export data. Users can
    run ``slack-migrator migrate --update_mode`` afterwards to add members.

    Args:
        chat_service: An authenticated Google Chat API service resource.
    """
    log_with_context(logging.INFO, "Running standalone cleanup...")

    spaces = _list_all_spaces(chat_service)
    if not spaces:
        log_with_context(logging.INFO, "No spaces found.")
        return

    import_mode_spaces = []
    for space in spaces:
        space_name = space.get("name", "")
        if not space_name:
            continue
        try:
            space_info = chat_service.spaces().get(name=space_name).execute()
            if space_info.get("importMode"):
                import_mode_spaces.append((space_name, space_info))
        except (HttpError, RefreshError, TransportError) as e:
            log_with_context(
                logging.WARNING,
                f"Failed to check space {space_name}: {e}",
            )

    if not import_mode_spaces:
        log_with_context(logging.INFO, "No spaces found in import mode during cleanup.")
        return

    log_with_context(
        logging.INFO,
        f"Found {len(import_mode_spaces)} space(s) still in import mode.",
    )

    for space_name, space_info in import_mode_spaces:
        try:
            chat_service.spaces().completeImport(name=space_name).execute()
            log_with_context(
                logging.INFO,
                f"Completed import mode for space: {space_name}",
            )

            # Preserve external user access if it was set
            if space_info.get("externalUserAllowed"):
                try:
                    chat_service.spaces().patch(
                        name=space_name,
                        updateMask="externalUserAllowed",
                        body={"externalUserAllowed": True},
                    ).execute()
                    log_with_context(
                        logging.INFO,
                        f"Preserved external user access for: {space_name}",
                    )
                except (HttpError, RefreshError, TransportError) as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to preserve external user access for "
                        f"{space_name}: {e}",
                    )
        except HttpError as http_e:
            log_with_context(
                logging.ERROR,
                f"HTTP error completing import for {space_name}: {http_e} "
                f"(Status: {http_e.resp.status})",
            )
        except (RefreshError, TransportError) as e:
            log_with_context(
                logging.ERROR,
                f"Failed to complete import for {space_name}: {e}",
            )

    log_with_context(logging.INFO, "Standalone cleanup completed.")


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
        self._load_existing_space_mappings()

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
        """Main migration function that orchestrates the entire process."""
        migration_start_time = time.time()
        log_with_context(logging.INFO, "Starting migration process")

        # Import report generation function for use in both success and failure paths

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
        if not self.state.channel_handlers:
            return

        logger = logging.getLogger("slack_migrator")

        for channel_name, handler in list(self.state.channel_handlers.items()):
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
        self.state.channel_handlers.clear()

    def cleanup(self):  # noqa: C901
        """Clean up resources and complete import mode on spaces."""
        # Clear current_channel so cleanup operations don't get tagged with channel context
        self.state.current_channel = None

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
                assert self.chat is not None
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
                        "Server error listing spaces - this might be a temporary issue, skipping cleanup",
                    )
                return
            except (RefreshError, TransportError) as list_e:
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
                    assert self.chat is not None
                    space_info = self.chat.spaces().get(name=space_name).execute()
                    # Use the correct field name: importMode (boolean) instead of importState
                    if space_info.get("importMode"):
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
                            "Server error checking space - this might be a temporary issue",
                            space_name=space_name,
                        )
                except (RefreshError, TransportError) as e:
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
                    f"Current channel_to_space mapping: {self.state.channel_to_space}",
                )

                # Log the created_spaces mapping
                log_with_context(
                    logging.INFO,
                    f"Current created_spaces mapping: {self.state.created_spaces}",
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
                        if not external_users_allowed:
                            external_users_allowed = (
                                self.state.spaces_with_external_users.get(
                                    space_name, False
                                )
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
                            assert self.chat is not None
                            self.chat.spaces().completeImport(name=space_name).execute()
                            log_with_context(
                                logging.DEBUG,
                                f"Successfully completed import mode for space: {space_name}",
                                space_name=space_name,
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
                                    "Server error completing import - this might be a temporary issue",
                                    space_name=space_name,
                                )
                            continue
                        except (RefreshError, TransportError) as e:
                            log_with_context(
                                logging.ERROR,
                                f"Failed to complete import: {e}",
                                space_name=space_name,
                            )
                            continue

                        # Ensure external user setting is preserved after import completion
                        if external_users_allowed:
                            try:
                                # Update space to ensure externalUserAllowed is set
                                update_body = {"externalUserAllowed": True}
                                update_mask = "externalUserAllowed"
                                assert self.chat is not None
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
                                        "Server error updating space - this might be a temporary issue",
                                        space_name=space_name,
                                    )
                            except (RefreshError, TransportError) as e:
                                log_with_context(
                                    logging.WARNING,
                                    f"Failed to preserve external user access: {e}",
                                    space_name=space_name,
                                )

                        # First try to find the channel using our channel_to_space mapping
                        channel_name = None
                        for ch, sp in self.state.channel_to_space.items():
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
                                "Server error during cleanup - this might be a temporary issue",
                                space_name=space_name,
                            )
                    except (RefreshError, TransportError) as e:
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
                    "Server error during cleanup - Google's servers may be experiencing issues",
                )
            elif http_e.resp.status == 403:
                log_with_context(
                    logging.WARNING,
                    "Permission error during cleanup - service account may lack required permissions",
                )
            elif http_e.resp.status == 429:
                log_with_context(
                    logging.WARNING,
                    "Rate limit exceeded during cleanup - too many API requests",
                )
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Unexpected error during cleanup: {e}",
            )
            log_with_context(
                logging.DEBUG,
                f"Cleanup exception traceback: {traceback.format_exc()}",
            )

        log_with_context(logging.INFO, "Cleanup completed")

    def _load_existing_space_mappings(self):  # noqa: C901
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

            # Check if we have any spaces with duplicate names that need disambiguation
            if duplicate_spaces:
                # Check config for space_mapping to disambiguate
                space_mapping = self.config.space_mapping

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
                            self.state.channel_conflicts.add(channel_name)
                            log_with_context(
                                logging.ERROR,
                                f"Configured space ID for channel '{channel_name}' ({configured_space_id}) "
                                f"doesn't match any discovered spaces",
                            )
                    else:
                        # No mapping in config - this is an unresolved conflict
                        unresolved_conflicts.append(channel_name)
                        self.state.channel_conflicts.add(channel_name)
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
                    for channel in unresolved_conflicts:
                        self.state.migration_issues[channel] = (
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
                # channel_id_to_space_id is initialized by MigrationState

                # Update the channel_to_space mapping
                for channel, space_name in discovered_spaces.items():
                    # Store the space name for backward compatibility
                    self.state.channel_to_space[channel] = space_name

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
                        self.state.channel_id_to_space_id[channel_id] = space_id

                        log_with_context(
                            logging.DEBUG,
                            f"Mapped channel ID {channel_id} to space ID {space_id}",
                        )

                    # Also update created_spaces for consistency
                    if self.update_mode:
                        self.state.created_spaces[channel] = space_name

                log_with_context(
                    logging.INFO,
                    f"Space discovery complete: {len(self.state.channel_to_space)} channels have existing spaces, others will create new spaces",
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
        channels_processed = len(
            self.state.migration_summary.get("channels_processed", [])
        )
        spaces_created = self.state.migration_summary.get("spaces_created", 0)
        messages_created = self.state.migration_summary.get("messages_created", 0)
        reactions_created = self.state.migration_summary.get("reactions_created", 0)
        files_created = self.state.migration_summary.get("files_created", 0)

        # Count channels with errors
        channels_with_errors = len(self.state.channels_with_errors)

        # Count unmapped users
        unmapped_user_count = 0
        if (
            hasattr(self, "unmapped_user_tracker")
            and self.unmapped_user_tracker.has_unmapped_users()
        ):
            unmapped_user_count = self.unmapped_user_tracker.get_unmapped_count()

        # Count incomplete imports
        incomplete_imports = len(self.state.incomplete_import_spaces)

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
            "📊 MIGRATION STATISTICS:",
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
                "   • Issues detected: None! 🎉",
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

        duration_minutes = duration / 60

        # Count what we accomplished before failure
        channels_processed = len(
            self.state.migration_summary.get("channels_processed", [])
        )
        spaces_created = self.state.migration_summary.get("spaces_created", 0)
        messages_created = self.state.migration_summary.get("messages_created", 0)

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
                "⏹️  INTERRUPTION DETAILS:",
            )
            log_with_context(
                logging.WARNING,
                "   • Type: User interruption (Ctrl+C)",
            )
            log_with_context(
                logging.WARNING,
                f"   • Duration before interruption: {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
            )
        else:
            log_with_context(
                logging.ERROR,
                "💥 ERROR DETAILS:",
            )
            log_with_context(
                logging.ERROR,
                f"   • Exception: {type(exception).__name__}",
            )
            log_with_context(
                logging.ERROR,
                f"   • Message: {exception!s}",
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
                "🔍 FULL TRACEBACK:",
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
