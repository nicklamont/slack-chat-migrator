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


def process_reactions_batch(
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
    requests_by_user, reaction_count = _group_and_filter_reactions(
        ctx, state, user_resolver, reactions, message_id
    )

    state.migration_summary["reactions_created"] += reaction_count

    if ctx.dry_run:
        log_with_context(
            logging.DEBUG,
            f"{ctx.log_prefix}Would add {reaction_count} reactions from"
            f" {len(requests_by_user)} users to message {message_id}",
            message_id=message_id,
            channel=state.current_channel,
        )
        return

    log_with_context(
        logging.DEBUG,
        f"Adding {reaction_count} reactions from"
        f" {len(requests_by_user)} users to message {message_id}",
        message_id=message_id,
        channel=state.current_channel,
    )

    user_batches = _build_user_batches(
        state, chat, user_resolver, requests_by_user, message_name, message_id
    )

    _execute_reaction_batches(state, user_batches, message_id)


def _group_and_filter_reactions(
    ctx: MigrationContext,
    state: MigrationState,
    user_resolver: Any,
    reactions: list[dict[str, Any]],
    message_id: str,
) -> tuple[dict[str, list[str]], int]:
    """Group reactions by user email and filter bots/unmapped users.

    Returns:
        Tuple of (requests_by_user, reaction_count) where requests_by_user
        maps internal email → list of emoji strings.
    """
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
                if _should_skip_bot_reaction(
                    ctx, user_resolver, uid, emoji_name, message_id, state
                ):
                    continue

                email = ctx.user_map.get(uid)
                if email:
                    internal_email = user_resolver.get_internal_email(uid, email)
                    if internal_email:
                        requests_by_user[internal_email].append(emo)
                        reaction_count += 1
                else:
                    reaction_name = react.get("name", "unknown")
                    message_ts = state.current_message_ts or "unknown"
                    user_resolver.handle_unmapped_user_reaction(
                        uid, reaction_name, message_ts
                    )
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to process reaction {react.get('name')}: {e!s}",
                message_id=message_id,
                error=str(e),
                channel=state.current_channel,
            )

    return requests_by_user, reaction_count


def _should_skip_bot_reaction(
    ctx: MigrationContext,
    user_resolver: Any,
    uid: str,
    emoji_name: str,
    message_id: str,
    state: MigrationState,
) -> bool:
    """Return True if this reaction is from a bot and bots are ignored."""
    if not ctx.config.ignore_bots:
        return False

    user_data = user_resolver.get_user_data(uid)
    if user_data and user_data.get("is_bot", False):
        log_with_context(
            logging.DEBUG,
            f"Skipping reaction :{emoji_name}: from bot user {uid}"
            f" ({user_data.get('real_name', 'Unknown')}) - ignore_bots enabled",
            message_id=message_id,
            emoji=emoji_name,
            user_id=uid,
            channel=state.current_channel,
        )
        return True
    return False


def _build_user_batches(
    state: MigrationState,
    chat: Any,
    user_resolver: Any,
    requests_by_user: dict[str, list[str]],
    message_name: str,
    message_id: str,
) -> dict[str, BatchHttpRequest]:
    """Build batched reaction requests, falling back to sync for admin.

    Returns:
        Dict mapping email → BatchHttpRequest for impersonated users.
        Admin reactions are processed synchronously and not returned.
    """
    user_batches: dict[str, BatchHttpRequest] = {}

    def reaction_callback(
        request_id: str,
        response: dict[str, Any] | None,
        exception: HttpError | None,
    ) -> None:
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

    for email, emojis in requests_by_user.items():
        if user_resolver.is_external_user(email):
            log_with_context(
                logging.INFO,
                f"Skipping {len(emojis)} reactions from external user"
                f" {email} to avoid admin attribution",
                message_id=message_id,
                user=email,
                channel=state.current_channel,
            )
            continue

        svc = user_resolver.get_delegate(email)

        if svc == chat:
            _process_admin_reactions(
                state, chat, message_name, message_id, email, emojis
            )
            continue

        log_with_context(
            logging.DEBUG,
            f"Creating batch request for user {email} with {len(emojis)} reactions",
            message_id=message_id,
            user=email,
            channel=state.current_channel,
        )

        if email not in user_batches:
            user_batches[email] = svc.new_batch_http_request(callback=reaction_callback)

        _add_reactions_to_batch(
            state, svc, user_batches[email], message_name, message_id, email, emojis
        )

    return user_batches


def _process_admin_reactions(
    state: MigrationState,
    chat: Any,
    message_name: str,
    message_id: str,
    email: str,
    emojis: list[str],
) -> None:
    """Process reactions synchronously via admin when impersonation is unavailable."""
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


def _add_reactions_to_batch(
    state: MigrationState,
    svc: Any,
    batch: BatchHttpRequest,
    message_name: str,
    message_id: str,
    email: str,
    emojis: list[str],
) -> None:
    """Add individual reaction requests to a user's batch."""
    for emo in emojis:
        reaction_body = {"emoji": {"unicode": emo}}

        try:
            request = (
                svc.spaces()
                .messages()
                .reactions()
                .create(parent=message_name, body=reaction_body)
            )
            batch.add(request)
        except AttributeError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to create reaction request: {e}."
                " Falling back to direct API call.",
                message_id=message_id,
                user=email,
                emoji=emo,
                channel=state.current_channel,
            )
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


def _execute_reaction_batches(
    state: MigrationState,
    user_batches: dict[str, BatchHttpRequest],
    message_id: str,
) -> None:
    """Execute all queued batch reaction requests."""
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
