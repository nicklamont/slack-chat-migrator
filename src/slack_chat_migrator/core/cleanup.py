"""
Post-migration cleanup: complete import mode and add members back to spaces.

Extracted from ``migrator.py`` to keep the orchestrator focused on control flow.
Each function receives only the specific dependencies it needs.

Completing import mode is irreversible, so :func:`run_cleanup` only touches
spaces this run created (``state.spaces.created_spaces``), never spaces left
in import mode on purpose (``state.errors.incomplete_import_spaces``, and
channels with errors under ``skip_on_error``), and only spaces that are
still in import mode.  The CLI calls it only after a live ``migrate()``
finished.
"""

from __future__ import annotations

import logging
import traceback
from pathlib import Path
from typing import TYPE_CHECKING

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    HTTP_FORBIDDEN,
    HTTP_RATE_LIMIT,
    HTTP_SERVER_ERROR_MIN,
    SPACE_NAME_PREFIX,
)
from slack_chat_migrator.core.config import ImportCompletionStrategy
from slack_chat_migrator.services.export_inspector import list_channel_dirs
from slack_chat_migrator.services.spaces.regular_membership import add_regular_members
from slack_chat_migrator.services.spaces.space_creator import ImportCompletionResult
from slack_chat_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_chat_migrator.core.context import MigrationContext
    from slack_chat_migrator.core.state import MigrationState
    from slack_chat_migrator.services.chat_adapter import ChatAdapter
    from slack_chat_migrator.services.files.file import FileHandler
    from slack_chat_migrator.services.user_resolver import UserResolver


def cleanup_channel_handlers(state: MigrationState) -> None:
    """Clean up and close all channel-specific log handlers.

    Args:
        state: The migration state holding channel handlers.
    """
    if not state.spaces.channel_handlers:
        return

    logger = logging.getLogger("slack_chat_migrator")

    for channel_name, handler in list(state.spaces.channel_handlers.items()):
        try:
            handler.flush()
            handler.close()
            logger.removeHandler(handler)
            log_with_context(
                logging.DEBUG, f"Cleaned up log handler for channel: {channel_name}"
            )
        except OSError as e:
            # Use print to avoid potential logging issues during cleanup
            print(
                f"Warning: Failed to clean up log handler"
                f" for channel {channel_name}: {e}"
            )

    state.spaces.channel_handlers.clear()


def _spaces_to_complete(
    ctx: MigrationContext, state: MigrationState
) -> list[tuple[str, str]]:
    """Return ``(channel, space)`` pairs this run may still complete.

    Only spaces created (or reused) by this run are candidates.  Spaces the
    run left in import mode on purpose are excluded: those recorded in
    ``incomplete_import_spaces`` (skipped or failed completions) and, under
    the ``skip_on_error`` strategy, those of channels that had errors.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.

    Returns:
        The candidate ``(channel, space_name)`` pairs.
    """
    left_open = {space for space, _channel in state.errors.incomplete_import_spaces}
    errored = set(state.errors.channels_with_errors)
    skip_errored = (
        ctx.config.import_completion_strategy == ImportCompletionStrategy.SKIP_ON_ERROR
    )
    candidates: list[tuple[str, str]] = []
    for channel, space in state.spaces.created_spaces.items():
        if not space or space in left_open:
            continue
        if skip_errored and channel in errored:
            continue
        candidates.append((channel, space))
    return candidates


def run_cleanup(
    ctx: MigrationContext,
    state: MigrationState,
    chat: ChatAdapter,
    user_resolver: UserResolver,
    file_handler: FileHandler | None,
) -> ImportCompletionResult:
    """Complete import mode on this run's spaces and add regular members back.

    This is the safety net that runs after a finished live migration.  For
    each space this run created that is still in import mode (and wasn't
    left in import mode on purpose), it completes the import, restores
    external-user access and adds the regular (non-historical) members.
    Spaces that aren't in ``created_spaces`` are never touched.

    Args:
        ctx: Immutable migration context (config, paths, flags).
        state: Mutable migration state.
        chat: Google Chat API service.
        user_resolver: User identity resolver.
        file_handler: File handler for drive operations, or None.

    Returns:
        The completion outcome (``failed`` lists completeImport failures).
    """
    state.context.current_channel = None
    result = ImportCompletionResult()

    if ctx.dry_run:
        log_with_context(logging.INFO, "[DRY RUN] Would perform post-migration cleanup")
        return result

    log_with_context(logging.INFO, "Performing post-migration cleanup")

    try:
        for channel, space_name in _spaces_to_complete(ctx, state):
            try:
                space_info = chat.get_space(space_name)
            except (HttpError, RefreshError, TransportError) as e:
                log_with_context(
                    logging.WARNING,
                    f"Could not check whether space {space_name} (#{channel}) is"
                    f" still in import mode, leaving it unchanged: {e}",
                    space_name=space_name,
                )
                result.errors.append(f"{space_name}: {e}")
                continue
            if not space_info.get("importMode"):
                continue
            result.candidates.append(space_info)
            log_with_context(
                logging.WARNING,
                f"Space {space_name} for channel {channel} is still in import mode;"
                " completing it now",
                space_name=space_name,
            )
            completed = _complete_single_space(
                ctx,
                state,
                chat,
                user_resolver,
                file_handler,
                space_name,
                space_info,
                channel_name=channel,
            )
            if completed:
                result.completed.append(space_name)
            else:
                result.failed[space_name] = "completeImport failed (see log)"

        if not result.candidates:
            log_with_context(
                logging.INFO, "No spaces found in import mode during cleanup."
            )

    except HttpError as http_e:
        log_with_context(
            logging.ERROR,
            f"HTTP error during post-migration cleanup: {http_e}"
            f" (Status: {http_e.resp.status})",
            error_code=http_e.resp.status,
        )
        result.errors.append(str(http_e))
        if http_e.resp.status >= HTTP_SERVER_ERROR_MIN:
            log_with_context(
                logging.WARNING,
                "Server error during cleanup"
                " - Google's servers may be experiencing issues",
            )
        elif http_e.resp.status == HTTP_FORBIDDEN:
            log_with_context(
                logging.WARNING,
                "Permission error during cleanup"
                " - service account may lack required permissions",
            )
        elif http_e.resp.status == HTTP_RATE_LIMIT:
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
        result.errors.append(str(e))

    log_with_context(logging.INFO, "Cleanup completed")
    return result


def _complete_single_space(
    ctx: MigrationContext,
    state: MigrationState,
    chat: ChatAdapter,
    user_resolver: UserResolver,
    file_handler: FileHandler | None,
    space_name: str,
    space_info: dict,
    channel_name: str | None = None,
) -> bool:
    """Complete import mode for a single space, preserve settings, and add members.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        chat: Google Chat API service.
        user_resolver: User identity resolver.
        file_handler: File handler for drive operations, or None.
        space_name: The Google Chat space resource name (e.g. ``spaces/AAAA``).
        space_info: The space metadata dict from the API.
        channel_name: The Slack channel the space belongs to, if known;
            otherwise it is resolved from the state and display name.

    Returns:
        True if ``completeImport`` succeeded, False if it failed.
    """
    external_users_allowed = space_info.get("externalUserAllowed", False)

    if not external_users_allowed:
        external_users_allowed = state.progress.spaces_with_external_users.get(
            space_name, False
        )
        if external_users_allowed:
            log_with_context(
                logging.INFO,
                f"Space {space_name} has external users but flag not set,"
                " will enable after import",
                space_name=space_name,
            )

    log_with_context(
        logging.DEBUG,
        f"Attempting to complete import mode for space: {space_name}",
    )

    # --- Complete import mode ---------------------------------------------
    try:
        chat.complete_import(space_name)
        log_with_context(
            logging.DEBUG,
            f"Successfully completed import mode for space: {space_name}",
            space_name=space_name,
        )
    except HttpError as http_e:
        log_with_context(
            logging.ERROR,
            f"HTTP error completing import for space {space_name}: {http_e}"
            f" (Status: {http_e.resp.status})",
            space_name=space_name,
            error_code=http_e.resp.status,
        )
        if http_e.resp.status >= HTTP_SERVER_ERROR_MIN:
            log_with_context(
                logging.WARNING,
                "Server error completing import - this might be a temporary issue",
                space_name=space_name,
            )
        return False
    except (RefreshError, TransportError) as e:
        log_with_context(
            logging.ERROR,
            f"Failed to complete import: {e}",
            space_name=space_name,
        )
        return False

    # --- Preserve external user access ------------------------------------
    if external_users_allowed:
        try:
            chat.patch_space(
                name=space_name,
                update_mask="externalUserAllowed",
                body={"externalUserAllowed": True},
            )
            log_with_context(
                logging.INFO,
                f"Preserved external user access for space: {space_name}",
            )
        except HttpError as http_e:
            log_with_context(
                logging.WARNING,
                f"HTTP error preserving external user access for space"
                f" {space_name}: {http_e} (Status: {http_e.resp.status})",
                space_name=space_name,
                error_code=http_e.resp.status,
            )
            if http_e.resp.status >= HTTP_SERVER_ERROR_MIN:
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

    # --- Resolve channel name and add members -----------------------------
    if channel_name is None:
        channel_name = _resolve_channel_name(
            state, ctx.export_root, space_name, space_info
        )

    if channel_name:
        log_with_context(
            logging.INFO,
            f"Step 5/6: Adding regular members to space for channel: {channel_name}",
        )
        try:
            add_regular_members(
                ctx,
                state,
                chat,
                user_resolver,
                file_handler,
                space_name,
                channel_name,
            )
            log_with_context(
                logging.DEBUG,
                f"Successfully added regular members to space"
                f" {space_name} for channel: {channel_name}",
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
            f"Could not determine channel name for space {space_name},"
            " skipping adding members",
            space_name=space_name,
        )
    return True


def _resolve_channel_name(
    state: MigrationState,
    export_root: Path,
    space_name: str,
    space_info: dict,
) -> str | None:
    """Try to find the Slack channel name that corresponds to a space.

    First checks the ``channel_to_space`` mapping, then falls back to
    matching the space display name exactly against the names this tool
    gives spaces (``"Slack #<channel>"`` or ``"Slack #<channel> (General)"``),
    so ``Slack #eng-leadership`` never resolves to ``eng``.

    Args:
        state: Migration state with channel_to_space mapping.
        export_root: Path to the Slack export directory.
        space_name: The Google Chat space resource name.
        space_info: The space metadata dict from the API.

    Returns:
        The channel name if found, or ``None``.
    """
    # Try channel_to_space mapping first
    for ch, sp in state.spaces.channel_to_space.items():
        if sp == space_name:
            log_with_context(
                logging.INFO,
                f"Found channel {ch} for space {space_name}"
                " using channel_to_space mapping",
            )
            return ch

    # Fall back to display name matching
    display_name = space_info.get("displayName", "")
    log_with_context(
        logging.DEBUG,
        f"Attempting to extract channel name from display name: {display_name}",
    )

    all_channel_names = [d.name for d in list_channel_dirs(export_root)]
    for ch in all_channel_names:
        ch_display = f"{SPACE_NAME_PREFIX}{ch}"
        if display_name in (ch_display, f"{ch_display} (General)"):
            log_with_context(
                logging.INFO,
                f"Found channel {ch} for space {space_name} using display name",
            )
            return ch

    return None
