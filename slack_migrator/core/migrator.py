"""
Main migrator class for the Slack to Google Chat migration tool
"""

import json
import logging
from pathlib import Path
import time
import traceback
from typing import Dict, List, Optional, Any

from google.auth.exceptions import RefreshError
from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.utils.logging import logger, log_with_context
from slack_migrator.utils.api import get_gcp_service, set_global_retry_config

from slack_migrator.services.user import generate_user_map
from slack_migrator.services.file import FileHandler
from slack_migrator.core.config import should_process_channel

# Import functionality from service modules
from slack_migrator.services.space import (
    create_space, 
    send_intro, 
    add_users_to_space, 
    add_regular_members,
    test_space_creation
)
from slack_migrator.services.message import (
    send_message,
    track_message_stats,
)
from slack_migrator.cli.report import (
    generate_report,
    print_dry_run_summary,
    create_output_directory,
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
        self.workspace_admin = workspace_admin
        self.config_path = Path(config_path)
        self.dry_run = dry_run
        self.verbose = verbose
        self.debug_api = debug_api
        self.update_mode = update_mode
        self.import_mode = not update_mode  # Set import_mode to True when not in update mode
        
        # Set up logger with verbosity and debug_api flag
        from slack_migrator.utils.logging import setup_logger
        global logger
        logger = setup_logger(verbose, debug_api)
        
        if self.update_mode:
            logger.info(f"Running in update mode - will update existing spaces")
        
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
        self.workspace_domain = self.workspace_admin.split('@')[1] if '@' in self.workspace_admin else None
        
        # Initialize API clients
        self._validate_export_format()

        # Load config using the shared load_config function
        from slack_migrator.core.config import load_config
        self.config = load_config(self.config_path)
        
        # Set global retry config for all API calls
        set_global_retry_config(self.config)

        # Generate user mapping from users.json
        self.user_map, self.users_without_email = generate_user_map(self.export_root, self.config)

        # Convert Path to str for API clients
        creds_path_str = str(self.creds_path)
        self.chat = get_gcp_service(creds_path_str, self.workspace_admin, "chat", "v1")
        self.drive = get_gcp_service(
            creds_path_str, self.workspace_admin, "drive", "v3"
        )

        self.chat_delegates: Dict[str, Any] = {}
        self.valid_users: Dict[str, bool] = {}
        self.channel_to_space: Dict[str, str] = {}

        self.channels_meta = self._load_channels_meta()

        # Initialize file handler
        self.file_handler = FileHandler(
            self.drive, self.chat, folder_id=None, migrator=self, dry_run=self.dry_run
        )
        # FileHandler now handles its own drive folder initialization automatically
        
        # Initialize message attachment processor
        from slack_migrator.services.message_attachments import MessageAttachmentProcessor
        self.attachment_processor = MessageAttachmentProcessor(self.file_handler, dry_run=self.dry_run)

        # Initialize caches and state tracking
        self.created_spaces: Dict[str, str] = {}  # channel -> space_name
        self.current_channel: Optional[str] = (
            None  # Track current channel being processed
        )
        
        # Track spaces with external users
        self.spaces_with_external_users: Dict[str, bool] = {}
        
        # Track message statistics per channel
        self.channel_stats: Dict[str, Dict[str, int]] = {}

        # Test space creation to verify permissions
        if not dry_run:
            test_space_creation(self)
            
        if verbose:
            logger.debug("Migrator initialized with verbose logging enabled")
            
        # Load existing space mappings for update mode or file attachments
        self._load_existing_space_mappings()

    def _validate_export_format(self):
        """Validate that the export directory has the expected structure."""
        if not (self.export_root / "channels.json").exists():
            logger.warning("channels.json not found in export directory")

        if not (self.export_root / "users.json").exists():
            logger.warning("users.json not found in export directory")
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
                logger.warning(
                    f"No JSON files found in channel directory {ch_dir.name}"
                )

    def _load_channels_meta(self) -> Dict[str, Dict]:
        """Load channel metadata from channels.json file."""
        f = self.export_root / "channels.json"
        if f.exists():
            with open(f) as f_in:
                return {ch["name"]: ch for ch in json.load(f_in)}
        return {}

    def _get_delegate(self, email: str):
        """Get a Google Chat API service with user impersonation."""
        if not email:
            return self.chat

        if email not in self.valid_users:
            try:
                # Verify user exists by making a simple API call
                test_service = get_gcp_service(
                    str(self.creds_path), email, "chat", "v1"
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

    def _save_progress(self, channel: str, processed_ts: List[str]):
        """Save migration progress to resume later if needed."""
        pass

    def _load_progress(self, channel: str) -> List[str]:
        """Load previously processed message timestamps."""
        return []

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
                channel=channel
            )
            
            # Check config for abort_on_error setting
            should_abort = self.config.get("abort_on_error", False)
            
            if should_abort:
                log_with_context(
                    logging.WARNING,
                    f"Aborting import due to errors (abort_on_error is enabled in config)",
                    channel=channel
                )
                return True
            else:
                log_with_context(
                    logging.WARNING,
                    f"Continuing with migration despite errors (abort_on_error is disabled in config)",
                    channel=channel
                )
        
        return False

    def _delete_space_if_errors(self, space_name, channel):
        """Delete a space if it had errors and cleanup is enabled."""
        if not self.config.get("cleanup_on_error", False):
            log_with_context(
                logging.INFO,
                f"Not deleting space {space_name} despite errors (cleanup_on_error is disabled)",
                space_name=space_name
            )
            return

        try:
            log_with_context(
                logging.WARNING,
                f"Deleting space {space_name} due to errors",
                space_name=space_name
            )
            self.chat.spaces().delete(name=space_name).execute()
            log_with_context(
                logging.INFO,
                f"Successfully deleted space {space_name}",
                space_name=space_name
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
                space_name=space_name
            )

        log_with_context(logging.INFO, "Cleanup completed")

    def _get_internal_email(self, user_id: str, user_email: Optional[str] = None) -> str:
        """Get internal email for a user, handling external users.
        
        Args:
            user_id: The Slack user ID
            user_email: Optional email if already known
            
        Returns:
            The internal email to use for this user
        """
        # Get the email from our user mapping if not provided
        if user_email is None:
            user_email = self.user_map.get(user_id)
            if not user_email:
                log_with_context(
                    logging.WARNING,
                    f"No email mapping found for user {user_id}",
                    user_id=user_id
                )
                return f"unknown-user-{user_id}@example.com"
        
        # Check if this is an external user
        if self._is_external_user(user_email):
            # For external users, we need to use a special format
            # This ensures messages are properly attributed to external users
            external_user_domain = self.config.get("external_user_domain", "external.example.com")
            return f"external-{user_id}@{external_user_domain}"
        
        return user_email

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

    def export_users_without_email(self, output_path: Optional[str] = None):
        """Log users without email addresses for reference."""
        if not hasattr(self, 'users_without_email') or not self.users_without_email:
            log_with_context(logging.INFO, "No users without email addresses detected")
            return
            
        # Just log the information instead of creating a separate file
        log_with_context(logging.INFO, f"Found {len(self.users_without_email)} users without email addresses:")
        log_with_context(logging.INFO, "To map these users, add entries to user_mapping_overrides in config.yaml:")
        
        for user in self.users_without_email:
            user_id = user.get('id', '')
            name = user.get('name', '')
            user_type = "Bot" if user.get('is_bot', False) or user.get('is_app_user', False) else "User"
            log_with_context(logging.INFO, f'  "{user_id}": ""  # {user_type}: {name}')
        
        log_with_context(logging.INFO, "This information is also available in the migration report")

    def migrate(self):
        """Main migration function that orchestrates the entire process."""
        log_with_context(logging.INFO, "Starting migration process")
        
        # Initialize the thread map if not already done
        if not hasattr(self, "thread_map"):
            self.thread_map = {}
        
        # Create output directory structure
        self.output_dir = create_output_directory(self)
        
        # Set up main log file in the output directory
        from slack_migrator.utils.logging import setup_main_log_file
        self.main_log_handler = setup_main_log_file(self.output_dir, debug_api=self.debug_api)
        
        # Initialize dictionary to store channel-specific log handlers
        self.channel_handlers = {}
        
        # Initialize migration summary
        self.migration_summary = {
            "channels_processed": [],
            "spaces_created": 0,
            "messages_created": 0,
            "reactions_created": 0,
            "files_created": 0,
        }

        # In update mode, load existing space mappings
        if self.update_mode:
            from slack_migrator.services.message import load_space_mappings
            existing_spaces = load_space_mappings(self)
            if existing_spaces:
                log_with_context(
                    logging.INFO,
                    f"[UPDATE MODE] Loaded {len(existing_spaces)} existing space mappings"
                )
                self.created_spaces = existing_spaces
            else:
                log_with_context(
                    logging.WARNING,
                    f"[UPDATE MODE] No existing space mappings found. Will create new spaces."
                )

        # Get all channel directories
        all_channel_dirs = [d for d in self.export_root.iterdir() if d.is_dir()]
        log_with_context(
            logging.INFO,
            f"Found {len(all_channel_dirs)} channel directories in export"
        )

        # Add ability to abort after first channel error
        self.channel_error_count = 0
        self.first_channel_processed = False

        # Process each channel
        for ch in all_channel_dirs:
            # Track the current channel being processed
            self.current_channel = ch.name
            
            mode_prefix = "[DRY RUN]"
            if self.update_mode:
                mode_prefix = "[UPDATE MODE]" if not self.dry_run else "[DRY RUN] [UPDATE MODE]"
                
            log_with_context(
                logging.INFO,
                f"{mode_prefix if self.dry_run or self.update_mode else ''} Processing channel: {ch.name}",
                channel=ch.name
            )
            self.migration_summary["channels_processed"].append(ch.name)
            
            # Check if channel should be processed
            if not should_process_channel(ch.name, self.config):
                log_with_context(
                    logging.INFO,
                    f"Skipping channel {ch.name} based on configuration",
                    channel=ch.name
                )
                continue

            # Setup channel-specific logging for channels that will be processed
            from slack_migrator.utils.logging import setup_channel_logger, is_debug_api_enabled
            channel_handler = setup_channel_logger(self.output_dir, ch.name, self.verbose, is_debug_api_enabled())
            self.channel_handlers[ch.name] = channel_handler

            # Initialize error tracking variables
            channel_had_errors = False
            space_name = None
            
            # Check if we're in update mode and already have a space for this channel
            if self.update_mode and ch.name in self.created_spaces:
                space = self.created_spaces[ch.name]
                log_with_context(
                    logging.INFO,
                    f"[UPDATE MODE] Using existing space {space} for channel {ch.name}",
                    channel=ch.name
                )
                self.space_cache[ch.name] = space
            else:
                # Step 1: Create space in import mode
                log_with_context(
                    logging.INFO,
                    f"{'[DRY RUN] ' if self.dry_run else ''}Step 1/5: Creating import mode space for {ch.name}",
                    channel=ch.name
                )
                space = self.space_cache.get(ch.name) or create_space(self, ch.name)
                self.space_cache[ch.name] = space

            # Skip processing if we couldn't create a space due to permissions
            if space and space.startswith("ERROR_NO_PERMISSION_"):
                log_with_context(
                    logging.WARNING,
                    f"Skipping channel {ch.name} due to space creation permission error",
                    channel=ch.name
                )
                continue

            # Set current space for file attachments
            self.current_space = space
            self.channel_to_space[ch.name] = space
            
            # Store in created_spaces for future reference
            self.created_spaces[ch.name] = space
            
            # Log that we're setting the current space
            log_with_context(
                logging.INFO,  # Changed from DEBUG to INFO for better visibility
                f"Setting current space to {space} for channel {ch.name} and storing in channel_to_space mapping",
                channel=ch.name
            )

            # In update mode, skip adding users and sending intro message
            if not self.update_mode:
                # Step 2: Add historical memberships
                log_with_context(
                    logging.INFO,
                    f"{'[DRY RUN] ' if self.dry_run else ''}Step 2/5: Adding historical memberships for {ch.name}",
                    channel=ch.name
                )
                add_users_to_space(self, space, ch.name)

                # Step 3: Send intro message with channel metadata
                log_with_context(
                    logging.INFO,
                    f"{'[DRY RUN] ' if self.dry_run else ''}Step 3/5: Sending channel metadata as intro message for {ch.name}",
                    channel=ch.name
                )
                send_intro(self, space, ch.name)
            else:
                log_with_context(
                    logging.INFO,
                    f"[UPDATE MODE] Skipping user addition and intro message for existing space",
                    channel=ch.name
                )

            # Track if we had errors processing this channel
            space_name = space

            # Process messages for this channel
            mode_prefix = "[DRY RUN]"
            if self.update_mode:
                mode_prefix = "[UPDATE MODE]" if not self.dry_run else "[DRY RUN] [UPDATE MODE]"
                
            log_with_context(
                logging.INFO,
                f"{mode_prefix if self.dry_run or self.update_mode else ''} Processing messages for {ch.name}",
                channel=ch.name
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
                        channel=ch.name
                    )
            
            # Sort messages by timestamp to maintain chronological order
            msgs = sorted(msgs, key=lambda m: float(m.get("ts", "0")))

            # Count messages in dry run mode
            if self.dry_run:
                # Count only actual messages, not other events
                message_count = sum(1 for m in msgs if m.get("type") == "message")
                log_with_context(
                    logging.INFO,
                    f"{mode_prefix} Found {message_count} messages in channel {ch.name}",
                    channel=ch.name
                )
                # Add to the total message count
                self.migration_summary["messages_created"] += message_count

            # Load previously processed messages and thread mappings
            processed_ts = [] if self.dry_run else self._load_progress(ch.name)
            
            # In update mode, always load thread mappings
            if not self.dry_run or self.update_mode:
                from slack_migrator.services.message import load_thread_mappings
                load_thread_mappings(self, ch.name)
                
            processed_count = 0
            failed_count = 0
            
            # Get failure threshold configuration
            max_failure_percentage = self.config.get("max_failure_percentage", 10)
            
            # Track failures for this channel
            channel_failures = []

            pbar = tqdm(msgs, desc=f"{ch.name} - Messages")
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
                        # Save progress after each successful message
                        processed_ts.append(ts)
                        self._save_progress(ch.name, processed_ts)
                        processed_count += 1
                else:
                    failed_count += 1
                    channel_failures.append(ts)
                    
                    # Check if we've exceeded our failure threshold
                    if processed_count > 0:  # Avoid division by zero
                        failure_percentage = (failed_count / (processed_count + failed_count)) * 100
                        if failure_percentage > max_failure_percentage:
                            log_with_context(
                                logging.WARNING,
                                f"Failure rate {failure_percentage:.1f}% exceeds threshold {max_failure_percentage}% for channel {ch.name}",
                                channel=ch.name
                            )
                            # Flag the channel as having a high error rate, but don't break the loop
                            channel_had_errors = True
                            # Track channels with high failure rates
                            if not hasattr(self, "high_failure_rate_channels"):
                                self.high_failure_rate_channels = {}
                            self.high_failure_rate_channels[ch.name] = failure_percentage
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
                channel=ch.name
            )
            
            # Save thread mappings for this channel
            from slack_migrator.services.message import save_thread_mappings
            save_thread_mappings(self, ch.name)

            # Step 5: Complete import mode (only if not in update mode)
            if not self.update_mode:
                log_with_context(
                    logging.INFO,
                    f"{'[DRY RUN] ' if self.dry_run else ''}Step 5/5: Completing import mode for {ch.name}",
                    channel=ch.name
                )
                
                # Get the completion strategy from config
                completion_strategy = self.config.get("import_completion_strategy", "skip_on_error")
                
                # Only complete import if there were no errors or we're using force_complete strategy
                if (not channel_had_errors or completion_strategy == "force_complete") and not self.dry_run:
                    try:
                        log_with_context(
                            logging.INFO,
                            f"Attempting to complete import mode for space {space}",
                            channel=ch.name
                        )
                        
                        # Add retry logic for completeImport
                        max_retries = 3
                        retry_delay = 2  # seconds
                        success = False
                        
                        for retry_count in range(max_retries):
                            try:
                                result = self.chat.spaces().completeImport(
                                    name=space
                                ).execute()
                                
                                log_with_context(
                                    logging.INFO,
                                    f"Successfully completed import for space {space}",
                                    channel=ch.name
                                )
                                
                                # Add regular members back to the space
                                log_with_context(
                                    logging.INFO,
                                    f"Adding regular members to space after completing import: {space}",
                                    channel=ch.name
                                )
                                
                                try:
                                    from slack_migrator.services.space import add_regular_members
                                    add_regular_members(self, space, ch.name)
                                    log_with_context(
                                        logging.INFO,
                                        f"Successfully added regular members to space {space} for channel {ch.name}",
                                        channel=ch.name
                                    )
                                except Exception as e:
                                    log_with_context(
                                        logging.ERROR,
                                        f"Error adding regular members to space {space}: {e}",
                                        channel=ch.name
                                    )
                                    import traceback
                                    log_with_context(
                                        logging.DEBUG,
                                        f"Exception traceback: {traceback.format_exc()}",
                                        channel=ch.name
                                    )
                                
                                success = True
                                break
                            except HttpError as e:
                                log_with_context(
                                    logging.WARNING,
                                    f"Retry {retry_count+1}/{max_retries}: Failed to complete import: {e}",
                                    channel=ch.name
                                )
                                if retry_count < max_retries - 1:
                                    time.sleep(retry_delay)
                        
                        if not success:
                            log_with_context(
                                logging.ERROR,
                                f"Failed to complete import after {max_retries} retries",
                                channel=ch.name
                            )
                            channel_had_errors = True
                            
                            # Track spaces that failed to complete import
                            if not hasattr(self, "incomplete_import_spaces"):
                                self.incomplete_import_spaces = []
                            self.incomplete_import_spaces.append((space, ch.name))
                            
                    except Exception as e:
                        log_with_context(
                            logging.ERROR,
                            f"Failed to complete import for space {space}: {e}",
                            channel=ch.name
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
                        channel=ch.name
                    )
                    
                    # Track spaces that weren't completed due to errors
                    if not hasattr(self, "incomplete_import_spaces"):
                        self.incomplete_import_spaces = []
                    self.incomplete_import_spaces.append((space, ch.name))
            else:
                log_with_context(
                    logging.INFO,
                    f"[UPDATE MODE] Skipping import completion for existing space",
                    channel=ch.name
                )

            # Log completion for this channel
            log_with_context(
                logging.INFO,
                f"Channel log file completed for: {ch.name}",
                channel=ch.name
            )
            
            # Check if we should abort after first channel error
            if self._should_abort_import(ch.name, processed_count, failed_count):
                log_with_context(
                    logging.WARNING,
                    f"Aborting import after first channel due to errors",
                    channel=ch.name
                )
                break

            # Delete space if there were errors and we're not in dry run mode
            if channel_had_errors and not self.dry_run and not self.update_mode:
                self._delete_space_if_errors(space_name, ch.name)

        # Save space mappings for future update mode runs
        from slack_migrator.services.message import save_space_mappings
        save_space_mappings(self)
        
        # Generate report
        report_file = generate_report(self)
        
        # Print summary
        if self.dry_run:
            print_dry_run_summary(self, report_file)
            
        return True

    def cleanup(self):
        """Clean up resources and complete import mode on spaces."""
        if self.dry_run:
            log_with_context(logging.INFO, "[DRY RUN] Would perform post-migration cleanup")
            return

        log_with_context(logging.INFO, "Performing post-migration cleanup")

        # Check for spaces that might still be in import mode
        try:
            # List all spaces created by this app
            spaces = self.chat.spaces().list().execute().get("spaces", [])
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
                except Exception as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to get space info during cleanup: {e}",
                        space_name=space_name
                    )

            # Attempt to complete import mode for these spaces
            if import_mode_spaces:
                log_with_context(
                    logging.INFO,
                    f"Found {len(import_mode_spaces)} spaces still in import mode. Attempting to complete import."
                )
                
                # Log the current channel_to_space mapping
                log_with_context(
                    logging.INFO,
                    f"Current channel_to_space mapping: {self.channel_to_space}"
                )
                
                # Log the created_spaces mapping
                log_with_context(
                    logging.INFO,
                    f"Current created_spaces mapping: {self.created_spaces}"
                )
                
                pbar = tqdm(import_mode_spaces, desc="Completing import mode for spaces")
                for space_name, space_info in pbar:
                    log_with_context(
                        logging.WARNING,
                        f"Found space in import mode during cleanup: {space_name}"
                    )

                    try:
                        # Check if external users are allowed in this space
                        external_users_allowed = space_info.get(
                            "externalUserAllowed", False
                        )
                        
                        # Also check if this space has external users based on our tracking
                        if not external_users_allowed and hasattr(self, "spaces_with_external_users"):
                            external_users_allowed = self.spaces_with_external_users.get(space_name, False)
                            
                            # If we detect external users but the flag isn't set, log this
                            if external_users_allowed:
                                log_with_context(
                                    logging.INFO,
                                    f"Space {space_name} has external users but flag not set, will enable after import",
                                    space_name=space_name
                                )

                        log_with_context(
                            logging.INFO,
                            f"Attempting to complete import mode for space: {space_name}"
                        )
                        
                        # Add retry logic for completeImport
                        max_retries = 3
                        retry_delay = 2  # seconds
                        success = False
                        
                        for retry_count in range(max_retries):
                            try:
                                self.chat.spaces().completeImport(name=space_name).execute()
                                log_with_context(
                                    logging.INFO,
                                    f"Successfully completed import mode for space: {space_name}"
                                )
                                success = True
                                break
                            except Exception as e:
                                log_with_context(
                                    logging.WARNING,
                                    f"Retry {retry_count+1}/{max_retries}: Failed to complete import: {e}",
                                    space_name=space_name
                                )
                                if retry_count < max_retries - 1:
                                    time.sleep(retry_delay)
                        
                        if not success:
                            log_with_context(
                                logging.ERROR,
                                f"Failed to complete import after {max_retries} retries",
                                space_name=space_name
                            )
                            continue

                        # Ensure external user setting is preserved after import completion
                        if external_users_allowed:
                            try:
                                # Update space to ensure externalUserAllowed is set
                                update_body = {
                                    "externalUserAllowed": True
                                }
                                update_mask = "externalUserAllowed"
                                self.chat.spaces().patch(
                                    name=space_name,
                                    updateMask=update_mask,
                                    body=update_body,
                                ).execute()
                                log_with_context(
                                    logging.INFO,
                                    f"Preserved external user access for space: {space_name}"
                                )
                            except Exception as e:
                                log_with_context(
                                    logging.WARNING,
                                    f"Failed to preserve external user access: {e}",
                                    space_name=space_name
                                )

                        # First try to find the channel using our channel_to_space mapping
                        channel_name = None
                        for ch, sp in self.channel_to_space.items():
                            if sp == space_name:
                                channel_name = ch
                                log_with_context(
                                    logging.INFO,
                                    f"Found channel {channel_name} for space {space_name} using channel_to_space mapping"
                                )
                                break
                                
                        # If not found in channel_to_space, try the space display name
                        if not channel_name:
                            display_name = space_info.get("displayName", "")
                            log_with_context(
                                logging.DEBUG,
                                f"Attempting to extract channel name from display name: {display_name}"
                            )
                            
                            # Try to extract channel name based on our naming convention
                            for ch in self._get_all_channel_names():
                                ch_name = self._get_space_name(ch)
                                if ch_name in display_name:
                                    channel_name = ch
                                    log_with_context(
                                        logging.INFO,
                                        f"Found channel {channel_name} for space {space_name} using display name"
                                    )
                                    break

                        if channel_name:
                            # Add regular members back to the space
                            log_with_context(
                                logging.INFO,
                                f"Adding regular members to space after completing import: {space_name}"
                            )
                            try:
                                add_regular_members(self, space_name, channel_name)
                                log_with_context(
                                    logging.INFO,
                                    f"Successfully added regular members to space {space_name} for channel {channel_name}"
                                )
                            except Exception as e:
                                log_with_context(
                                    logging.ERROR,
                                    f"Error adding regular members to space {space_name}: {e}",
                                    channel=channel_name
                                )
                                import traceback
                                log_with_context(
                                    logging.DEBUG,
                                    f"Exception traceback: {traceback.format_exc()}",
                                    channel=channel_name
                                )
                        else:
                            log_with_context(
                                logging.WARNING,
                                f"Could not determine channel name for space {space_name}, skipping adding members",
                                space_name=space_name
                            )

                    except Exception as e:
                        log_with_context(
                            logging.ERROR,
                            f"Failed to complete import mode for space {space_name} during cleanup: {e}",
                            space_name=space_name
                        )
            else:
                log_with_context(
                    logging.INFO,
                    "No spaces found in import mode during cleanup."
                )

        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error during cleanup: {e}",
            )
            
        # Clean up main log handler if it exists
        if hasattr(self, 'main_log_handler'):
            logger.removeHandler(self.main_log_handler)

        log_with_context(logging.INFO, "Cleanup completed")

    def _load_existing_space_mappings(self):
        """Load existing space mappings from saved files for update mode or file attachments."""
        try:
            # Check if there's a space mappings file
            from slack_migrator.services.message import load_space_mappings
            
            # Load existing space mappings
            space_mappings = load_space_mappings(self)
            
            if space_mappings:
                logger.info(f"Loaded {len(space_mappings)} existing space mappings")
                
                # Update the channel_to_space mapping
                for channel, space_name in space_mappings.items():
                    self.channel_to_space[channel] = space_name
                    
                    # Also update created_spaces for consistency
                    if self.update_mode:
                        self.created_spaces[channel] = space_name
                
                logger.debug(f"Updated channel_to_space mapping with {len(self.channel_to_space)} entries")
            else:
                logger.debug("No existing space mappings found")
        except Exception as e:
            logger.warning(f"Failed to load existing space mappings: {e}")
            # Continue without mappings
