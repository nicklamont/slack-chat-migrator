"""Channel-level processing logic extracted from the main migrator."""

from __future__ import annotations

import json
import logging
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from slack_migrator.core.migrator import SlackToChatMigrator

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.core.config import ImportCompletionStrategy, should_process_channel
from slack_migrator.services.discovery import get_last_message_timestamp
from slack_migrator.services.membership_manager import (
    add_regular_members,
    add_users_to_space,
)
from slack_migrator.services.message import (
    MessageResult,
    send_message,
    track_message_stats,
)
from slack_migrator.services.space_creator import create_space
from slack_migrator.utils.logging import (
    is_debug_api_enabled,
    log_with_context,
    setup_channel_logger,
)


class ChannelProcessor:
    """Handles per-channel processing during migration."""

    def __init__(self, migrator: SlackToChatMigrator) -> None:
        self.migrator = migrator

    def process_channel(self, ch_dir: Path) -> bool:
        """Process a single channel directory.

        Creates or reuses a space, imports messages, completes import mode,
        and adds members.

        Args:
            ch_dir: Path to the channel's export directory.

        Returns:
            True if the migration should abort (break the outer loop).
        """
        migrator = self.migrator
        channel = ch_dir.name

        migrator.state.current_channel = channel

        # Determine mode prefix for logging
        mode_prefix = "[DRY RUN] "
        if migrator.update_mode:
            mode_prefix = (
                "[UPDATE MODE] " if not migrator.dry_run else "[DRY RUN] [UPDATE MODE] "
            )

        log_with_context(
            logging.INFO,
            f"{mode_prefix if migrator.dry_run or migrator.update_mode else ''}Processing channel: {channel}",
            channel=channel,
        )
        migrator.state.migration_summary["channels_processed"].append(channel)

        # Check if channel should be processed
        if not should_process_channel(channel, migrator.config):
            log_with_context(
                logging.WARNING,
                f"Skipping channel {channel} based on configuration",
                channel=channel,
            )
            return False

        # Check for unresolved space conflicts
        if (
            hasattr(migrator.state, "channel_conflicts")
            and channel in migrator.state.channel_conflicts
        ):
            log_with_context(
                logging.ERROR,
                f"Skipping channel {channel} due to unresolved duplicate space conflict",
                channel=channel,
            )
            migrator.state.migration_issues[channel] = (
                "Skipped due to duplicate space conflict - requires disambiguation in config.yaml"
            )
            return False

        # Setup channel-specific logging
        self._setup_channel_logging(channel)

        # Initialize error tracking
        channel_had_errors = False

        # Create or reuse space
        space, is_newly_created = self._create_or_reuse_space(ch_dir)

        # Skip if permission error
        if space and space.startswith("ERROR_NO_PERMISSION_"):
            log_with_context(
                logging.WARNING,
                f"Skipping channel {channel} due to space creation permission error",
                channel=channel,
            )
            return False

        # Set current space
        migrator.state.current_space = space
        migrator.state.channel_to_space[channel] = space
        migrator.state.created_spaces[channel] = space

        log_with_context(
            logging.DEBUG,
            f"Setting current space to {space} for channel {channel} and storing in channel_to_space mapping",
            channel=channel,
        )

        # Add historical memberships for newly created spaces
        if is_newly_created:
            log_with_context(
                logging.INFO,
                f"{'[DRY RUN] ' if migrator.dry_run else ''}Step 2/6: Adding historical memberships for {channel}",
                channel=channel,
            )
            add_users_to_space(
                migrator.ctx,
                migrator.state,
                migrator.chat,
                migrator.user_resolver,
                space,
                channel,
            )
        else:
            log_with_context(
                logging.INFO,
                "[UPDATE MODE] Skipping historical memberships for existing space (already has history)",
                channel=channel,
            )

        # Process messages
        processed_count, failed_count, channel_had_errors = self._process_messages(
            ch_dir, space, channel_had_errors
        )

        # Complete import mode for newly created spaces
        if is_newly_created:
            channel_had_errors = self._complete_import_mode(
                space, channel, channel_had_errors
            )

        # Add members
        channel_had_errors = self._add_members(
            space, channel, is_newly_created, channel_had_errors
        )

        # Log completion
        log_with_context(
            logging.DEBUG,
            f"Channel log file completed for channel: {channel}",
            channel=channel,
        )

        # Check if we should abort
        if self._should_abort_import(channel, processed_count, failed_count):
            log_with_context(
                logging.WARNING,
                "Aborting import after first channel due to errors",
                channel=channel,
            )
            return True  # Signal to break the loop

        # Delete space if errors
        if channel_had_errors and not migrator.dry_run and not migrator.update_mode:
            self._delete_space_if_errors(space, channel)

        return False

    def _setup_channel_logging(self, channel: str) -> None:
        """Set up channel-specific log handler."""
        migrator = self.migrator
        if migrator.state.output_dir is None:
            raise RuntimeError("Output directory not set")
        channel_handler = setup_channel_logger(
            migrator.state.output_dir, channel, migrator.verbose, is_debug_api_enabled()
        )
        migrator.state.channel_handlers[channel] = channel_handler

    def _create_or_reuse_space(self, ch_dir: Path) -> tuple[str, bool]:
        """Create a new space or reuse an existing one.

        Returns (space_name, is_newly_created).
        """
        migrator = self.migrator
        channel = ch_dir.name

        if migrator.update_mode and channel in migrator.state.created_spaces:
            space = migrator.state.created_spaces[channel]
            space_id = space.split("/")[-1] if space.startswith("spaces/") else space
            log_with_context(
                logging.INFO,
                f"[UPDATE MODE] Using existing space {space_id} for channel {channel}",
                channel=channel,
            )
            migrator.state.space_cache[channel] = space
            return space, False
        else:
            action_desc = (
                "Creating new import mode space"
                if not migrator.update_mode
                else "Creating new space (none found in update mode)"
            )
            log_with_context(
                logging.INFO,
                f"{'[DRY RUN] ' if migrator.dry_run else ''}Step 1/6: {action_desc} for {channel}",
                channel=channel,
            )
            space = migrator.state.space_cache.get(channel) or create_space(
                migrator.ctx,
                migrator.state,
                migrator.chat,
                migrator.user_resolver,
                channel,
            )
            migrator.state.space_cache[channel] = space
            return space, True

    def _process_messages(  # noqa: C901
        self, ch_dir: Path, space: str, channel_had_errors: bool
    ) -> tuple[int, int, bool]:
        """Load, deduplicate, and send messages for a channel.

        Returns (processed_count, failed_count, channel_had_errors).
        """
        migrator = self.migrator
        channel = ch_dir.name

        # Determine mode prefix
        mode_prefix = "[DRY RUN]"
        if migrator.update_mode:
            mode_prefix = (
                "[UPDATE MODE]" if not migrator.dry_run else "[DRY RUN] [UPDATE MODE]"
            )

        log_with_context(
            logging.INFO,
            f"{mode_prefix if migrator.dry_run or migrator.update_mode else ''} Step 3/6: Processing messages for {channel}",
            channel=channel,
        )

        # Load messages from JSON files
        msg_dir = migrator.export_root / channel
        msgs: list[dict[str, Any]] = []
        for jf in sorted(msg_dir.glob("*.json")):
            try:
                with open(jf) as f:
                    msgs.extend(json.load(f))
            except (OSError, ValueError) as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to load messages from {jf}: {e}",
                    channel=channel,
                )

        # Sort by timestamp
        msgs = sorted(msgs, key=lambda m: float(m.get("ts", "0")))

        # Deduplicate
        seen_timestamps: set[str] = set()
        deduped_msgs: list[dict[str, Any]] = []
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
                    channel=channel,
                    ts=ts,
                )

        if duplicate_count > 0:
            log_with_context(
                logging.INFO,
                f"Deduplicated {duplicate_count} messages in channel {channel} (likely thread reply duplicates)",
                channel=channel,
            )

        msgs = deduped_msgs

        # Count messages in dry run mode
        if migrator.dry_run:
            message_count = sum(1 for m in msgs if m.get("type") == "message")
            log_with_context(
                logging.INFO,
                f"{mode_prefix} Found {message_count} messages in channel {channel}",
                channel=channel,
            )
            migrator.state.migration_summary["messages_created"] += message_count

        # Load previously processed messages and thread mappings
        processed_ts: list[str] = []

        # Discover existing resources (find the last message timestamp) from Google Chat
        if not migrator.dry_run or migrator.update_mode:
            self._discover_channel_resources(channel)

        processed_count = 0
        failed_count = 0

        # Get failure threshold configuration
        max_failure_percentage = migrator.config.max_failure_percentage

        # Track failures for this channel
        channel_failures: list[str] = []

        # Create progress bar
        progress_prefix = ""
        if migrator.dry_run:
            progress_prefix = "[DRY RUN] "
        elif migrator.update_mode:
            progress_prefix = "[UPDATE] "

        progress_desc = f"{progress_prefix}Adding messages to {channel}"
        pbar = tqdm(msgs, desc=progress_desc)
        for m in pbar:
            if m.get("type") != "message":
                continue

            ts = m["ts"]

            # Skip already processed messages (only in non-dry run mode)
            if ts in processed_ts and not migrator.dry_run:
                processed_count += 1
                continue

            # Track statistics for this message
            track_message_stats(
                migrator.ctx,
                migrator.state,
                migrator.user_resolver,
                migrator.attachment_processor,
                m,
            )

            if migrator.dry_run:
                continue

            # Send message using the new method
            result = send_message(
                migrator.ctx,
                migrator.state,
                migrator.chat,
                migrator.user_resolver,
                migrator.attachment_processor,
                space,
                m,
            )

            if result:
                if result != MessageResult.SKIPPED:
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
                            f"Failure rate {failure_percentage:.1f}% exceeds threshold {max_failure_percentage}% for channel {channel}",
                            channel=channel,
                        )
                        # Flag the channel as having a high error rate
                        channel_had_errors = True
                        migrator.state.high_failure_rate_channels[channel] = (
                            failure_percentage
                        )

            # Add a small delay between messages to avoid rate limits
            time.sleep(0.05)

        # Record failures for reporting
        if channel_failures:
            migrator.state.failed_messages_by_channel[channel] = channel_failures
            channel_had_errors = True

        log_with_context(
            logging.INFO,
            f"Channel {channel} message import: processed {processed_count}, failed {failed_count}",
            channel=channel,
        )

        return processed_count, failed_count, channel_had_errors

    def _complete_import_mode(
        self, space: str, channel: str, channel_had_errors: bool
    ) -> bool:
        """Complete import mode for a newly created space.

        Returns updated channel_had_errors.
        """
        migrator = self.migrator

        log_with_context(
            logging.INFO,
            f"{'[DRY RUN] ' if migrator.dry_run else ''}Step 4/6: Completing import mode for {channel}",
            channel=channel,
        )

        # Get the completion strategy from config
        completion_strategy = migrator.config.import_completion_strategy

        # Only complete import if there were no errors or we're using force_complete strategy
        if (
            not channel_had_errors
            or completion_strategy == ImportCompletionStrategy.FORCE_COMPLETE
        ) and not migrator.dry_run:
            try:
                log_with_context(
                    logging.DEBUG,
                    f"Attempting to complete import mode for space {space}",
                    channel=channel,
                )

                if migrator.chat is None:
                    raise RuntimeError("Chat API service not initialized")
                migrator.chat.spaces().completeImport(name=space).execute()

                log_with_context(
                    logging.INFO,
                    f"Successfully completed import mode for space: {space}",
                    channel=channel,
                )

            except (HttpError, RefreshError, TransportError) as e:
                log_with_context(
                    logging.ERROR,
                    f"Failed to complete import for space {space}: {e}",
                    channel=channel,
                )
                channel_had_errors = True
                migrator.state.incomplete_import_spaces.append((space, channel))
        elif channel_had_errors and not migrator.dry_run:
            log_with_context(
                logging.WARNING,
                f"Skipping import completion for space {space} due to errors (strategy: {completion_strategy})",
                channel=channel,
            )
            migrator.state.incomplete_import_spaces.append((space, channel))

        return channel_had_errors

    def _add_members(
        self,
        space: str,
        channel: str,
        is_newly_created: bool,
        channel_had_errors: bool,
    ) -> bool:
        """Add or update current members in a space.

        Returns updated channel_had_errors.
        """
        migrator = self.migrator

        step_desc = (
            "Adding current members to space"
            if is_newly_created
            else "Updating current members in existing space"
        )
        log_with_context(
            logging.INFO,
            f"{'[DRY RUN] ' if migrator.dry_run else ''}Step 5/6: {step_desc} for {channel}",
            channel=channel,
        )

        if not channel_had_errors or not is_newly_created:
            # For existing spaces, we always try to update members even if there were message errors
            # For new spaces, only add members if import completed successfully
            try:
                add_regular_members(
                    migrator.ctx,
                    migrator.state,
                    migrator.chat,
                    migrator.user_resolver,
                    getattr(migrator, "file_handler", None),
                    space,
                    channel,
                )
                log_with_context(
                    logging.DEBUG,
                    f"Successfully updated current members for space {space} and channel {channel}",
                    channel=channel,
                )
            except (HttpError, RefreshError, TransportError) as e:
                log_with_context(
                    logging.ERROR,
                    f"Error updating current members for space {space}: {e}",
                    channel=channel,
                )
                log_with_context(
                    logging.DEBUG,
                    f"Exception traceback: {traceback.format_exc()}",
                    channel=channel,
                )
                channel_had_errors = True
            except Exception as e:
                # Catch-all: add_regular_members is a complex function that can raise
                # unexpected errors from file I/O, data lookups, and multiple API calls
                log_with_context(
                    logging.ERROR,
                    f"Unexpected error updating current members for space {space}: {e}",
                    channel=channel,
                )
                log_with_context(
                    logging.DEBUG,
                    f"Exception traceback: {traceback.format_exc()}",
                    channel=channel,
                )
                channel_had_errors = True
        else:
            log_with_context(
                logging.WARNING,
                f"Skipping member addition for newly created space {space} due to import completion errors",
                channel=channel,
            )

        return channel_had_errors

    def _should_abort_import(
        self, channel: str, processed_count: int, failed_count: int
    ) -> bool:
        """Determine if the migration should abort after errors in a channel."""
        migrator = self.migrator

        if migrator.dry_run:
            return False

        # Only consider aborting if we had failures
        if failed_count > 0:
            log_with_context(
                logging.WARNING,
                f"Channel '{channel}' had {failed_count} message import errors.",
                channel=channel,
            )

            # Check config for abort_on_error setting
            should_abort = migrator.config.abort_on_error

            if should_abort:
                log_with_context(
                    logging.WARNING,
                    "Aborting import due to errors (abort_on_error is enabled in config)",
                    channel=channel,
                )
                return True
            else:
                log_with_context(
                    logging.WARNING,
                    "Continuing with migration despite errors (abort_on_error is disabled in config)",
                    channel=channel,
                )

        return False

    def _delete_space_if_errors(self, space_name: str, channel: str) -> None:
        """Delete a space if it had errors and cleanup is enabled."""
        migrator = self.migrator

        if not migrator.config.cleanup_on_error:
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
            if migrator.chat is None:
                raise RuntimeError("Chat API service not initialized")
            migrator.chat.spaces().delete(name=space_name).execute()
            log_with_context(
                logging.INFO,
                f"Successfully deleted space {space_name}",
                space_name=space_name,
            )

            # Remove from created_spaces
            if channel in migrator.state.created_spaces:
                del migrator.state.created_spaces[channel]

            # Decrement space count
            migrator.state.migration_summary["spaces_created"] -= 1
        except (HttpError, RefreshError, TransportError) as e:
            log_with_context(
                logging.ERROR,
                f"Failed to delete space {space_name}: {e}",
                space_name=space_name,
            )

        log_with_context(logging.INFO, "Cleanup completed")

    def _discover_channel_resources(self, channel: str) -> None:
        """Find the last message timestamp in a space to determine where to resume."""
        migrator = self.migrator

        # Check if we have a space for this channel
        space_name = migrator.state.channel_to_space.get(channel)
        if not space_name:
            log_with_context(
                logging.WARNING,
                f"No space found for channel {channel}, cannot determine last message timestamp",
                channel=channel,
            )
            return

        # Get the timestamp of the last message in the space
        last_timestamp = get_last_message_timestamp(migrator.chat, channel, space_name)

        if last_timestamp > 0:
            log_with_context(
                logging.INFO,
                f"Found last message timestamp for channel {channel}: {last_timestamp}",
                channel=channel,
            )

            # Store the last timestamp for this channel
            migrator.state.last_processed_timestamps[channel] = last_timestamp

            # Initialize an empty thread_map so we don't try to load it again
            if (
                not hasattr(migrator.state, "thread_map")
                or migrator.state.thread_map is None
            ):
                migrator.state.thread_map = {}
        else:
            # If no messages were found, log it but don't set a last timestamp
            # This will cause all messages to be imported
            log_with_context(
                logging.INFO,
                f"No existing messages found in space for channel {channel}, will import all messages",
                channel=channel,
            )
