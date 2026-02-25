"""
Post-migration cleanup: complete import mode and add members back to spaces.

Extracted from ``migrator.py`` to keep the orchestrator focused on control flow.
Each function takes a ``migrator`` instance as its first argument, following the
same pattern used by ``space_creator.py`` and ``reaction_processor.py``.
"""

from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING

from google.auth.exceptions import RefreshError, TransportError
from googleapiclient.errors import HttpError
from tqdm import tqdm

from slack_migrator.services.membership_manager import add_regular_members
from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.migrator import SlackToChatMigrator


def cleanup_channel_handlers(migrator: SlackToChatMigrator) -> None:
    """Clean up and close all channel-specific log handlers.

    Args:
        migrator: The migrator instance whose state holds channel handlers.
    """
    if not migrator.state.channel_handlers:
        return

    logger = logging.getLogger("slack_migrator")

    for channel_name, handler in list(migrator.state.channel_handlers.items()):
        try:
            handler.flush()
            handler.close()
            logger.removeHandler(handler)
            log_with_context(
                logging.DEBUG, f"Cleaned up log handler for channel: {channel_name}"
            )
        except Exception as e:
            # Use print to avoid potential logging issues during cleanup
            print(
                f"Warning: Failed to clean up log handler"
                f" for channel {channel_name}: {e}"
            )

    migrator.state.channel_handlers.clear()


def run_cleanup(migrator: SlackToChatMigrator) -> None:  # noqa: C901
    """Complete import mode on spaces and add regular members back.

    This is the instance-level cleanup that runs after a migration. It
    inspects every space visible to the service account, finds those still
    in import mode, completes the import, and then attempts to add regular
    (non-historical) members.

    Args:
        migrator: The migrator instance providing API services and state.
    """
    # Clear current_channel so cleanup operations don't get tagged with channel context
    migrator.state.current_channel = None

    if migrator.dry_run:
        log_with_context(logging.INFO, "[DRY RUN] Would perform post-migration cleanup")
        return

    log_with_context(logging.INFO, "Performing post-migration cleanup")

    try:
        log_with_context(
            logging.DEBUG, "Listing all spaces to check for import mode..."
        )
        try:
            if migrator.chat is None:
                raise RuntimeError("Chat API service not initialized")
            spaces = migrator.chat.spaces().list().execute().get("spaces", [])
        except HttpError as http_e:
            log_with_context(
                logging.ERROR,
                f"HTTP error listing spaces during cleanup: {http_e}"
                f" (Status: {http_e.resp.status})",
                error_code=http_e.resp.status,
            )
            if http_e.resp.status >= 500:
                log_with_context(
                    logging.WARNING,
                    "Server error listing spaces"
                    " - this might be a temporary issue, skipping cleanup",
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

            try:
                if migrator.chat is None:
                    raise RuntimeError("Chat API service not initialized")
                space_info = migrator.chat.spaces().get(name=space_name).execute()
                if space_info.get("importMode"):
                    import_mode_spaces.append((space_name, space_info))
            except HttpError as http_e:
                log_with_context(
                    logging.WARNING,
                    f"HTTP error checking space status during cleanup: {http_e}"
                    f" (Status: {http_e.resp.status})",
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

        if import_mode_spaces:
            _complete_import_mode_spaces(migrator, import_mode_spaces)
        else:
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
        if http_e.resp.status >= 500:
            log_with_context(
                logging.WARNING,
                "Server error during cleanup"
                " - Google's servers may be experiencing issues",
            )
        elif http_e.resp.status == 403:
            log_with_context(
                logging.WARNING,
                "Permission error during cleanup"
                " - service account may lack required permissions",
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


def _complete_import_mode_spaces(
    migrator: SlackToChatMigrator,
    import_mode_spaces: list[tuple[str, dict]],
) -> None:
    """Complete import mode for discovered spaces and add members.

    Args:
        migrator: The migrator instance providing API services and state.
        import_mode_spaces: List of (space_name, space_info) tuples.
    """
    log_with_context(
        logging.INFO,
        f"Found {len(import_mode_spaces)} spaces still in import mode."
        " Attempting to complete import.",
    )

    log_with_context(
        logging.INFO,
        f"Current channel_to_space mapping: {migrator.state.channel_to_space}",
    )
    log_with_context(
        logging.INFO,
        f"Current created_spaces mapping: {migrator.state.created_spaces}",
    )

    pbar = tqdm(import_mode_spaces, desc="Completing import mode for spaces")
    for space_name, space_info in pbar:
        log_with_context(
            logging.WARNING,
            f"Found space in import mode during cleanup: {space_name}",
        )

        try:
            _complete_single_space(migrator, space_name, space_info)
        except HttpError as http_e:
            log_with_context(
                logging.ERROR,
                f"HTTP error during cleanup for space {space_name}: {http_e}"
                f" (Status: {http_e.resp.status})",
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
                f"Failed to complete import mode for space"
                f" {space_name} during cleanup: {e}",
                space_name=space_name,
            )


def _complete_single_space(
    migrator: SlackToChatMigrator, space_name: str, space_info: dict
) -> None:
    """Complete import mode for a single space, preserve settings, and add members.

    Args:
        migrator: The migrator instance providing API services and state.
        space_name: The Google Chat space resource name (e.g. ``spaces/AAAA``).
        space_info: The space metadata dict from the API.
    """
    external_users_allowed = space_info.get("externalUserAllowed", False)

    if not external_users_allowed:
        external_users_allowed = migrator.state.spaces_with_external_users.get(
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
        if migrator.chat is None:
            raise RuntimeError("Chat API service not initialized")
        migrator.chat.spaces().completeImport(name=space_name).execute()
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
        if http_e.resp.status >= 500:
            log_with_context(
                logging.WARNING,
                "Server error completing import - this might be a temporary issue",
                space_name=space_name,
            )
        return
    except (RefreshError, TransportError) as e:
        log_with_context(
            logging.ERROR,
            f"Failed to complete import: {e}",
            space_name=space_name,
        )
        return

    # --- Preserve external user access ------------------------------------
    if external_users_allowed:
        try:
            if migrator.chat is None:
                raise RuntimeError("Chat API service not initialized")
            migrator.chat.spaces().patch(
                name=space_name,
                updateMask="externalUserAllowed",
                body={"externalUserAllowed": True},
            ).execute()
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

    # --- Resolve channel name and add members -----------------------------
    channel_name = _resolve_channel_name(migrator, space_name, space_info)

    if channel_name:
        log_with_context(
            logging.INFO,
            f"Step 5/6: Adding regular members to space for channel: {channel_name}",
        )
        try:
            add_regular_members(migrator, space_name, channel_name)
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


def _resolve_channel_name(
    migrator: SlackToChatMigrator, space_name: str, space_info: dict
) -> str | None:
    """Try to find the Slack channel name that corresponds to a space.

    First checks the ``channel_to_space`` mapping, then falls back to
    matching the space display name against known channel names.

    Args:
        migrator: The migrator instance providing state and helper methods.
        space_name: The Google Chat space resource name.
        space_info: The space metadata dict from the API.

    Returns:
        The channel name if found, or ``None``.
    """
    # Try channel_to_space mapping first
    for ch, sp in migrator.state.channel_to_space.items():
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

    for ch in migrator._get_all_channel_names():
        ch_name = migrator._get_space_name(ch)
        if ch_name in display_name:
            log_with_context(
                logging.INFO,
                f"Found channel {ch} for space {space_name} using display name",
            )
            return ch

    return None
