"""
Batch reaction processing for Google Chat message import.

Handles grouping reactions by user, batch API requests, and fallback
to synchronous processing when impersonation is unavailable.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest

from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState


def process_reactions_batch(  # noqa: C901
    ctx: MigrationContext,
    state: MigrationState,
    chat: Any,
    user_resolver: Any,
    message_name: str,
    reactions: list[dict[str, Any]],
    message_id: str,
) -> None:
    """Process reactions for a message in import mode.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service (admin).
        user_resolver: UserResolver for email lookups and impersonation.
        message_name: Google Chat resource name of the parent message.
        reactions: List of Slack reaction dicts (each with ``name`` and ``users``).
        message_id: Short message identifier for logging.
    """

    def reaction_callback(
        request_id: str, response: dict[str, Any] | None, exception: HttpError | None
    ) -> None:
        """Handle the result of a single batched reaction API call.

        Args:
            request_id: Identifier assigned by the batch request.
            response: API response dict on success, or None on failure.
            exception: The HTTP error if the call failed, or None.
        """
        if exception:
            log_with_context(
                logging.WARNING,
                "Failed to add reaction in batch",
                error=str(exception),
                message_id=message_id,
                request_id=request_id,
                channel=state.current_channel,
            )
        else:
            log_with_context(
                logging.DEBUG,
                "Successfully added reaction in batch",
                message_id=message_id,
                request_id=request_id,
                channel=state.current_channel,
            )

    # Group reactions by user for batch processing
    requests_by_user: dict[str, list[str]] = defaultdict(list)
    reaction_count = 0

    log_with_context(
        logging.DEBUG,
        f"Processing {len(reactions)} reaction types for message {message_id}",
        message_id=message_id,
        channel=state.current_channel,
    )

    for react in reactions:
        try:
            # Convert Slack emoji name to Unicode emoji if possible
            import emoji

            emo = emoji.emojize(f":{react['name']}:", language="alias")
            emoji_name = react["name"]
            emoji_users = react.get("users", [])

            log_with_context(
                logging.DEBUG,
                f"Processing emoji :{emoji_name}: with {len(emoji_users)} users",
                message_id=message_id,
                emoji=emoji_name,
                channel=state.current_channel,
            )

            for uid in emoji_users:
                # Check if this reaction is from a bot and bots should be ignored
                if ctx.config.ignore_bots:
                    user_data = user_resolver.get_user_data(uid)
                    if user_data and user_data.get("is_bot", False):
                        log_with_context(
                            logging.DEBUG,
                            f"Skipping reaction :{emoji_name}: from bot user {uid} ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
                            message_id=message_id,
                            emoji=emoji_name,
                            user_id=uid,
                            channel=state.current_channel,
                        )
                        continue

                email = ctx.user_map.get(uid)
                if email:
                    # Get the internal email for this user (handles external users)
                    internal_email = user_resolver.get_internal_email(uid, email)
                    if internal_email:  # Only process if we get a valid internal email
                        requests_by_user[internal_email].append(emo)
                        reaction_count += 1  # Count every reaction we process
                    # If internal_email is None, this user was filtered out (e.g., ignored bot)
                else:
                    # Handle unmapped user reaction with new graceful approach
                    reaction_name = react.get("name", "unknown")
                    message_ts = state.current_message_ts or "unknown"
                    user_resolver.handle_unmapped_user_reaction(
                        uid, reaction_name, message_ts
                    )
                    continue
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to process reaction {react.get('name')}: {e!s}",
                message_id=message_id,
                error=str(e),
                channel=state.current_channel,
            )

    # Always increment the reaction count, regardless of dry run mode
    state.migration_summary["reactions_created"] += reaction_count

    # Keep dry-run check: DryRunChatService doesn't support new_batch_http_request()
    # which is used below for batched reaction processing
    if ctx.dry_run:
        log_with_context(
            logging.DEBUG,
            f"{ctx.log_prefix}Would add {reaction_count} reactions from {len(requests_by_user)} users to message {message_id}",
            message_id=message_id,
            channel=state.current_channel,
        )
        return

    log_with_context(
        logging.DEBUG,
        f"Adding {reaction_count} reactions from {len(requests_by_user)} users to message {message_id}",
        message_id=message_id,
        channel=state.current_channel,
    )

    user_batches: dict[str, BatchHttpRequest] = {}

    for email, emojis in requests_by_user.items():
        # Always skip external users' reactions to avoid false attribution to admin
        if user_resolver.is_external_user(email):
            log_with_context(
                logging.INFO,
                f"Skipping {len(emojis)} reactions from external user {email} to avoid admin attribution",
                message_id=message_id,
                user=email,
                channel=state.current_channel,
            )
            continue

        # Process reactions normally for mapped internal users
        # We already skipped unmapped users earlier in the code

        svc = user_resolver.get_delegate(email)
        # If impersonation failed, svc will be the admin service.
        # We process these synchronously as we can't batch across users.
        if svc == chat:
            log_with_context(
                logging.DEBUG,
                f"Using admin account for user {email} (impersonation not available)",
                message_id=message_id,
                user=email,
                channel=state.current_channel,
            )

            for emo in emojis:
                try:
                    reaction_body = {"emoji": {"unicode": emo}}
                    (
                        chat.spaces()
                        .messages()
                        .reactions()
                        .create(parent=message_name, body=reaction_body)
                        .execute()
                    )
                except HttpError as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction via admin fallback: {e}",
                        error_code=e.resp.status,
                        user=email,
                        message_id=message_id,
                        channel=state.current_channel,
                    )
            continue

        # For impersonated users, create batches
        log_with_context(
            logging.DEBUG,
            f"Creating batch request for user {email} with {len(emojis)} reactions",
            message_id=message_id,
            user=email,
            channel=state.current_channel,
        )

        if email not in user_batches:
            user_batches[email] = svc.new_batch_http_request(callback=reaction_callback)

        for emo in emojis:
            # Format reaction body according to the import documentation
            # https://developers.google.com/workspace/chat/import-data
            reaction_body = {"emoji": {"unicode": emo}}

            try:
                # Make sure we're using the correct API method format
                # The Google Chat API expects spaces().messages().reactions().create()
                request = (
                    svc.spaces()
                    .messages()
                    .reactions()
                    .create(parent=message_name, body=reaction_body)
                )
                user_batches[email].add(request)
            except AttributeError as e:
                # Handle the 'Resource' object has no attribute 'create' error
                log_with_context(
                    logging.WARNING,
                    f"Failed to create reaction request: {e}. Falling back to direct API call.",
                    message_id=message_id,
                    user=email,
                    emoji=emo,
                    channel=state.current_channel,
                )
                # Fall back to direct API call
                try:
                    (
                        svc.spaces()
                        .messages()
                        .reactions()
                        .create(parent=message_name, body=reaction_body)
                        .execute()
                    )
                except HttpError as inner_e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add reaction in fallback mode: {inner_e}",
                        message_id=message_id,
                        user=email,
                        emoji=emo,
                    )

    for email, batch in user_batches.items():
        try:
            log_with_context(
                logging.DEBUG,
                f"Executing batch request for user {email}",
                message_id=message_id,
                user=email,
                channel=state.current_channel,
            )
            batch.execute()
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Reaction batch execution failed for user {email}: {e}",
                message_id=message_id,
                user=email,
                channel=state.current_channel,
                error=str(e),
            )
