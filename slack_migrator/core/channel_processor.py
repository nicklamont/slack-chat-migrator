"""Channel-level processing logic extracted from the main migrator."""

from __future__ import annotations

import json
import logging
import time
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.core.config import ImportCompletionStrategy, should_process_channel
from slack_migrator.services.discovery import get_last_message_timestamp
from slack_migrator.services.historical_membership import add_users_to_space
from slack_migrator.services.message_sender import (
    MessageResult,
    send_message,
    track_message_stats,
)
from slack_migrator.services.regular_membership import add_regular_members
from slack_migrator.services.space_creator import create_space
from slack_migrator.utils.logging import (
    is_debug_api_enabled,
    log_with_context,
    setup_channel_logger,
)


class ChannelProcessor:
    """Handles per-channel processing during migration."""

    def __init__(
        self,
        ctx: MigrationContext,
        state: MigrationState,
        chat: Any,
        user_resolver: Any,
        file_handler: Any | None,
        attachment_processor: Any,
    ) -> None:
        self.ctx = ctx
        self.state = state
        self.chat = chat
        self.user_resolver = user_resolver
        self.file_handler = file_handler
        self.attachment_processor = attachment_processor

    def process_channel(self, ch_dir: Path) -> bool:
        """Process a single channel directory.

        Creates or reuses a space, imports messages, completes import mode,
        and adds members.

        Args:
            ch_dir: Path to the channel's export directory.

        Returns:
            True if the migration should abort (break the outer loop).
        """
        channel = ch_dir.name

        self.state.context.current_channel = channel

        log_with_context(
            logging.INFO,
            f"{self.ctx.log_prefix}Processing channel: {channel}",
            channel=channel,
        )
        self.state.progress.migration_summary["channels_processed"].append(channel)

        # Check if channel should be processed
        if not should_process_channel(channel, self.ctx.config):
            log_with_context(
                logging.WARNING,
                f"Skipping channel {channel} based on configuration",
                channel=channel,
            )
            return False

        # Check for unresolved space conflicts
        if channel in self.state.errors.channel_conflicts:
            log_with_context(
                logging.ERROR,
                f"Skipping channel {channel} due to unresolved duplicate space conflict",
                channel=channel,
            )
            self.state.errors.migration_issues[channel] = (
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
        self.state.context.current_space = space
        self.state.spaces.channel_to_space[channel] = space
        self.state.spaces.created_spaces[channel] = space

        log_with_context(
            logging.DEBUG,
            f"Setting current space to {space} for channel {channel} and storing in channel_to_space mapping",
            channel=channel,
        )

        # Add historical memberships for newly created spaces
        if is_newly_created:
            log_with_context(
                logging.INFO,
                f"{self.ctx.log_prefix}Step 2/6: Adding historical memberships for {channel}",
                channel=channel,
            )
            add_users_to_space(
                self.ctx,
                self.state,
                self.chat,
                self.user_resolver,
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
        if channel_had_errors and not self.ctx.dry_run and not self.ctx.update_mode:
            self._delete_space_if_errors(space, channel)

        return False

    def _setup_channel_logging(self, channel: str) -> None:
        """Set up channel-specific log handler."""
        if self.state.context.output_dir is None:
            raise RuntimeError("Output directory not set")
        channel_handler = setup_channel_logger(
            self.state.context.output_dir,
            channel,
            self.ctx.verbose,
            is_debug_api_enabled(),
        )
        self.state.spaces.channel_handlers[channel] = channel_handler

    def _create_or_reuse_space(self, ch_dir: Path) -> tuple[str, bool]:
        """Create a new space or reuse an existing one.

        Returns (space_name, is_newly_created).
        """
        channel = ch_dir.name

        if self.ctx.update_mode and channel in self.state.spaces.created_spaces:
            space = self.state.spaces.created_spaces[channel]
            space_id = space.split("/")[-1] if space.startswith("spaces/") else space
            log_with_context(
                logging.INFO,
                f"[UPDATE MODE] Using existing space {space_id} for channel {channel}",
                channel=channel,
            )
            self.state.spaces.space_cache[channel] = space
            return space, False
        else:
            action_desc = (
                "Creating new import mode space"
                if not self.ctx.update_mode
                else "Creating new space (none found in update mode)"
            )
            log_with_context(
                logging.INFO,
                f"{self.ctx.log_prefix}Step 1/6: {action_desc} for {channel}",
                channel=channel,
            )
            space = self.state.spaces.space_cache.get(channel) or create_space(
                self.ctx,
                self.state,
                self.chat,
                self.user_resolver,
                channel,
            )
            self.state.spaces.space_cache[channel] = space
            return space, True

    def _process_messages(
        self, ch_dir: Path, space: str, channel_had_errors: bool
    ) -> tuple[int, int, bool]:
        """Load, deduplicate, and send messages for a channel.

        Returns (processed_count, failed_count, channel_had_errors).
        """
        channel = ch_dir.name

        log_with_context(
            logging.INFO,
            f"{self.ctx.log_prefix}Step 3/6: Processing messages for {channel}",
            channel=channel,
        )

        msgs = self._load_and_sort_messages(channel)
        msgs = self._deduplicate_messages(msgs, channel)

        if self.ctx.dry_run:
            message_count = sum(1 for m in msgs if m.get("type") == "message")
            log_with_context(
                logging.INFO,
                f"{self.ctx.log_prefix}Found {message_count} messages in channel {channel}",
                channel=channel,
            )
            self.state.progress.migration_summary["messages_created"] += message_count

        if not self.ctx.dry_run or self.ctx.update_mode:
            self._discover_channel_resources(channel)

        processed_count, failed_count, channel_had_errors = self._send_messages_loop(
            msgs, space, channel, channel_had_errors
        )

        log_with_context(
            logging.INFO,
            f"Channel {channel} message import: processed {processed_count}, failed {failed_count}",
            channel=channel,
        )

        return processed_count, failed_count, channel_had_errors

    def _load_and_sort_messages(self, channel: str) -> list[dict[str, Any]]:
        """Load all messages from JSON files and sort by timestamp."""
        msg_dir = self.ctx.export_root / channel
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

        return sorted(msgs, key=lambda m: float(m.get("ts", "0")))

    def _deduplicate_messages(
        self, msgs: list[dict[str, Any]], channel: str
    ) -> list[dict[str, Any]]:
        """Remove duplicate messages based on timestamp."""
        seen_timestamps: set[str] = set()
        deduped: list[dict[str, Any]] = []
        duplicate_count = 0

        for msg in msgs:
            ts = msg.get("ts")
            if ts and ts not in seen_timestamps:
                seen_timestamps.add(ts)
                deduped.append(msg)
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

        return deduped

    def _send_messages_loop(
        self,
        msgs: list[dict[str, Any]],
        space: str,
        channel: str,
        channel_had_errors: bool,
    ) -> tuple[int, int, bool]:
        """Iterate over messages, sending each and tracking results.

        Returns (processed_count, failed_count, channel_had_errors).
        """
        processed_ts: list[str] = []
        processed_count = 0
        failed_count = 0
        max_failure_percentage = self.ctx.config.max_failure_percentage
        channel_failures: list[str] = []

        progress_desc = f"{self.ctx.log_prefix}Adding messages to {channel}"
        pbar = tqdm(msgs, desc=progress_desc)
        for m in pbar:
            if m.get("type") != "message":
                continue

            ts = m["ts"]

            if ts in processed_ts and not self.ctx.dry_run:
                processed_count += 1
                continue

            track_message_stats(
                self.ctx,
                self.state,
                self.user_resolver,
                self.attachment_processor,
                m,
            )

            if self.ctx.dry_run:
                continue

            result = send_message(
                self.ctx,
                self.state,
                self.chat,
                self.user_resolver,
                self.attachment_processor,
                space,
                m,
            )

            if result:
                if result != MessageResult.SKIPPED:
                    processed_ts.append(ts)
                    processed_count += 1
            else:
                failed_count += 1
                channel_failures.append(ts)

                if processed_count > 0:
                    failure_percentage = (
                        failed_count / (processed_count + failed_count)
                    ) * 100
                    if failure_percentage > max_failure_percentage:
                        log_with_context(
                            logging.WARNING,
                            f"Failure rate {failure_percentage:.1f}% exceeds threshold {max_failure_percentage}% for channel {channel}",
                            channel=channel,
                        )
                        channel_had_errors = True
                        self.state.errors.high_failure_rate_channels[channel] = (
                            failure_percentage
                        )

            time.sleep(0.05)  # Throttle to avoid Chat API rate limits

        if channel_failures:
            self.state.messages.failed_messages_by_channel[channel] = channel_failures
            channel_had_errors = True

        return processed_count, failed_count, channel_had_errors

    def _complete_import_mode(
        self, space: str, channel: str, channel_had_errors: bool
    ) -> bool:
        """Complete import mode for a newly created space.

        Returns updated channel_had_errors.
        """
        log_with_context(
            logging.INFO,
            f"{self.ctx.log_prefix}Step 4/6: Completing import mode for {channel}",
            channel=channel,
        )

        # Get the completion strategy from config
        completion_strategy = self.ctx.config.import_completion_strategy

        # Only complete import if there were no errors or we're using force_complete strategy
        if (
            not channel_had_errors
            or completion_strategy == ImportCompletionStrategy.FORCE_COMPLETE
        ):
            try:
                log_with_context(
                    logging.DEBUG,
                    f"Attempting to complete import mode for space {space}",
                    channel=channel,
                )

                if self.chat is None:
                    raise RuntimeError("Chat API service not initialized")
                self.chat.spaces().completeImport(name=space).execute()

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
                self.state.errors.incomplete_import_spaces.append((space, channel))
        elif channel_had_errors:
            log_with_context(
                logging.WARNING,
                f"Skipping import completion for space {space} due to errors (strategy: {completion_strategy})",
                channel=channel,
            )
            self.state.errors.incomplete_import_spaces.append((space, channel))

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
        step_desc = (
            "Adding current members to space"
            if is_newly_created
            else "Updating current members in existing space"
        )
        log_with_context(
            logging.INFO,
            f"{self.ctx.log_prefix}Step 5/6: {step_desc} for {channel}",
            channel=channel,
        )

        if not channel_had_errors or not is_newly_created:
            # For existing spaces, we always try to update members even if there were message errors
            # For new spaces, only add members if import completed successfully
            try:
                add_regular_members(
                    self.ctx,
                    self.state,
                    self.chat,
                    self.user_resolver,
                    self.file_handler,
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
        if self.ctx.dry_run:
            return False

        # Only consider aborting if we had failures
        if failed_count > 0:
            log_with_context(
                logging.WARNING,
                f"Channel '{channel}' had {failed_count} message import errors.",
                channel=channel,
            )

            # Check config for abort_on_error setting
            should_abort = self.ctx.config.abort_on_error

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
        if not self.ctx.config.cleanup_on_error:
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
            if self.chat is None:
                raise RuntimeError("Chat API service not initialized")
            self.chat.spaces().delete(name=space_name).execute()
            log_with_context(
                logging.INFO,
                f"Successfully deleted space {space_name}",
                space_name=space_name,
            )

            # Remove from created_spaces
            if channel in self.state.spaces.created_spaces:
                del self.state.spaces.created_spaces[channel]

            # Decrement space count
            self.state.progress.migration_summary["spaces_created"] -= 1
        except (HttpError, RefreshError, TransportError) as e:
            log_with_context(
                logging.ERROR,
                f"Failed to delete space {space_name}: {e}",
                space_name=space_name,
            )

        log_with_context(logging.INFO, "Cleanup completed")

    def _discover_channel_resources(self, channel: str) -> None:
        """Find the last message timestamp in a space to determine where to resume."""
        # Check if we have a space for this channel
        space_name = self.state.spaces.channel_to_space.get(channel)
        if not space_name:
            log_with_context(
                logging.WARNING,
                f"No space found for channel {channel}, cannot determine last message timestamp",
                channel=channel,
            )
            return

        # Get the timestamp of the last message in the space
        last_timestamp = get_last_message_timestamp(self.chat, channel, space_name)

        if last_timestamp > 0:
            log_with_context(
                logging.INFO,
                f"Found last message timestamp for channel {channel}: {last_timestamp}",
                channel=channel,
            )

            # Store the last timestamp for this channel
            self.state.progress.last_processed_timestamps[channel] = last_timestamp

            # Initialize an empty thread_map so we don't try to load it again
            if self.state.messages.thread_map is None:
                self.state.messages.thread_map = {}
        else:
            # If no messages were found, log it but don't set a last timestamp
            # This will cause all messages to be imported
            log_with_context(
                logging.INFO,
                f"No existing messages found in space for channel {channel}, will import all messages",
                channel=channel,
            )
