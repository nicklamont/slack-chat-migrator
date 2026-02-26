"""
Migration success/failure logging for the Slack-to-Google-Chat migration tool.

Extracted from ``migrator.py`` to keep the orchestrator focused on control flow.
Each function takes a ``migrator`` instance as its first argument, following the
same pattern used by ``space_creator.py`` and ``reaction_processor.py``.
"""

from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING, Any

from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.migrator import SlackToChatMigrator


def _collect_statistics(migrator: SlackToChatMigrator) -> dict[str, Any]:
    """Gather all migration statistics from migrator state into a flat dict.

    The returned dict is used both for structured log kwargs and for
    driving the human-readable summary lines.

    Args:
        migrator: The migrator instance whose state contains run statistics.

    Returns:
        Dict with keys: channels_processed, spaces_created, messages_created,
        reactions_created, files_created, channels_with_errors,
        incomplete_imports, unmapped_users.
    """
    unmapped_users = 0
    if (
        hasattr(migrator, "unmapped_user_tracker")
        and migrator.unmapped_user_tracker.has_unmapped_users()
    ):
        unmapped_users = migrator.unmapped_user_tracker.get_unmapped_count()

    summary = migrator.state.migration_summary
    return {
        "channels_processed": len(summary["channels_processed"]),
        "spaces_created": summary["spaces_created"],
        "messages_created": summary["messages_created"],
        "reactions_created": summary["reactions_created"],
        "files_created": summary["files_created"],
        "channels_with_errors": len(migrator.state.channels_with_errors),
        "incomplete_imports": len(migrator.state.incomplete_import_spaces),
        "unmapped_users": unmapped_users,
    }


def log_migration_success(migrator: SlackToChatMigrator, duration: float) -> None:
    """Log final migration success status with comprehensive summary.

    Logs are emitted as structured records: each call passes key statistics
    as kwargs so they appear as extra fields in JSON log output while
    remaining human-readable in the console formatter.

    Args:
        migrator: The migrator instance whose state contains run statistics.
        duration: Migration duration in seconds.
    """
    stats = _collect_statistics(migrator)
    duration_minutes = duration / 60
    is_dry_run = migrator.dry_run

    no_work_done = stats["spaces_created"] == 0 and stats["messages_created"] == 0
    interrupted_early = stats["channels_processed"] == 0

    # --- Outcome header ---------------------------------------------------
    if is_dry_run:
        log_with_context(
            logging.INFO,
            "DRY RUN VALIDATION COMPLETED SUCCESSFULLY",
            outcome="dry_run_complete",
        )
    elif no_work_done and interrupted_early:
        log_with_context(
            logging.WARNING,
            "MIGRATION WAS INTERRUPTED DURING INITIALIZATION - NO CHANNELS PROCESSED",
            outcome="interrupted_early",
        )
    elif no_work_done:
        log_with_context(
            logging.WARNING,
            "MIGRATION WAS INTERRUPTED BEFORE ANY SPACES WERE IMPORTED",
            outcome="no_work",
        )
    else:
        log_with_context(
            logging.INFO,
            "SLACK-TO-GOOGLE-CHAT MIGRATION COMPLETED SUCCESSFULLY",
            outcome="success",
        )

    # --- Statistics --------------------------------------------------------
    log_with_context(
        logging.INFO,
        f"Duration: {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
        duration_seconds=duration,
    )
    log_with_context(
        logging.INFO,
        f"Channels processed: {stats['channels_processed']}",
        stat="channels_processed",
        count=stats["channels_processed"],
    )
    if not is_dry_run:
        log_with_context(
            logging.INFO,
            f"Spaces created/updated: {stats['spaces_created']}",
            stat="spaces_created",
            count=stats["spaces_created"],
        )
        log_with_context(
            logging.INFO,
            f"Messages migrated: {stats['messages_created']}",
            stat="messages_created",
            count=stats["messages_created"],
        )
        log_with_context(
            logging.INFO,
            f"Reactions migrated: {stats['reactions_created']}",
            stat="reactions_created",
            count=stats["reactions_created"],
        )
        log_with_context(
            logging.INFO,
            f"Files migrated: {stats['files_created']}",
            stat="files_created",
            count=stats["files_created"],
        )

    # --- Issues -----------------------------------------------------------
    has_issues = False
    if stats["unmapped_users"] > 0:
        has_issues = True
        log_with_context(
            logging.WARNING,
            f"Unmapped users: {stats['unmapped_users']}",
            stat="unmapped_users",
            count=stats["unmapped_users"],
        )
    if stats["channels_with_errors"] > 0:
        has_issues = True
        log_with_context(
            logging.WARNING,
            f"Channels with errors: {stats['channels_with_errors']}",
            stat="channels_with_errors",
            count=stats["channels_with_errors"],
        )
    if stats["incomplete_imports"] > 0:
        has_issues = True
        log_with_context(
            logging.WARNING,
            f"Incomplete imports: {stats['incomplete_imports']}",
            stat="incomplete_imports",
            count=stats["incomplete_imports"],
        )
    if not has_issues:
        log_with_context(logging.INFO, "No issues detected")

    # --- Next-steps guidance ----------------------------------------------
    if is_dry_run:
        log_with_context(
            logging.INFO,
            "Validation complete. Review the logs and run without"
            " --dry_run to migrate.",
        )
    elif no_work_done:
        if interrupted_early:
            log_with_context(
                logging.WARNING,
                "Migration was interrupted during setup before any"
                " channels were processed.",
            )
            log_with_context(
                logging.INFO,
                "The migration may have been interrupted during"
                " channel filtering or initialization.",
            )
        else:
            log_with_context(
                logging.WARNING,
                "Migration was interrupted before any spaces were"
                " successfully imported.",
            )
        log_with_context(
            logging.INFO,
            "To complete the migration, run the command again.",
        )
        log_with_context(
            logging.INFO,
            "Check the migration report and logs for any issues"
            " that need to be addressed.",
        )
    elif has_issues:
        log_with_context(
            logging.WARNING,
            "Migration completed with some issues. Check the detailed logs and report.",
        )
    else:
        log_with_context(
            logging.INFO,
            "Migration completed successfully with no issues detected.",
        )


def log_migration_failure(
    migrator: SlackToChatMigrator, exception: BaseException, duration: float
) -> None:
    """Log final migration failure status with error details.

    Emits structured records with exception metadata and progress-before-failure
    statistics as extra kwargs for JSON log consumers.

    Args:
        migrator: The migrator instance whose state contains run statistics.
        exception: The exception that caused the failure.
        duration: Migration duration in seconds before failure.
    """
    duration_minutes = duration / 60
    is_interrupt = isinstance(exception, KeyboardInterrupt)
    is_dry_run = migrator.dry_run

    summary = migrator.state.migration_summary
    channels_processed = len(summary["channels_processed"])
    spaces_created = summary["spaces_created"]
    messages_created = summary["messages_created"]

    # --- Outcome header ---------------------------------------------------
    if is_interrupt:
        header = (
            "DRY RUN VALIDATION INTERRUPTED BY USER"
            if is_dry_run
            else "SLACK-TO-GOOGLE-CHAT MIGRATION INTERRUPTED BY USER"
        )
        log_with_context(
            logging.WARNING,
            header,
            outcome="interrupted",
            exception_type="KeyboardInterrupt",
        )
    else:
        header = (
            "DRY RUN VALIDATION FAILED"
            if is_dry_run
            else "SLACK-TO-GOOGLE-CHAT MIGRATION FAILED"
        )
        log_with_context(
            logging.ERROR,
            header,
            outcome="failed",
            exception_type=type(exception).__name__,
            exception_message=str(exception),
        )

    # --- Error / interruption details -------------------------------------
    if is_interrupt:
        log_with_context(
            logging.WARNING,
            f"User interruption (Ctrl+C) after"
            f" {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
            duration_seconds=duration,
            interruption_type="keyboard",
        )
    else:
        log_with_context(
            logging.ERROR,
            f"Exception: {type(exception).__name__}: {exception!s}",
            exception_type=type(exception).__name__,
            exception_message=str(exception),
            duration_seconds=duration,
        )
        log_with_context(
            logging.ERROR,
            f"Duration before failure:"
            f" {duration_minutes:.1f} minutes ({duration:.1f} seconds)",
            duration_seconds=duration,
        )

    # --- Progress before failure/interruption -----------------------------
    progress_level = logging.WARNING if is_interrupt else logging.ERROR
    progress_label = (
        "PROGRESS BEFORE INTERRUPTION" if is_interrupt else "PROGRESS BEFORE FAILURE"
    )

    log_with_context(
        progress_level,
        f"{progress_label}: Channels processed: {channels_processed}",
        stat="channels_processed",
        count=channels_processed,
    )
    if not is_dry_run:
        log_with_context(
            progress_level,
            f"Spaces created: {spaces_created}",
            stat="spaces_created",
            count=spaces_created,
        )
        log_with_context(
            progress_level,
            f"Messages migrated: {messages_created}",
            stat="messages_created",
            count=messages_created,
        )

    # --- Traceback (skip for interrupts — not useful) ---------------------
    if not is_interrupt:
        tb = traceback.format_exc()
        if tb and tb.strip() != "NoneType: None":
            log_with_context(logging.ERROR, f"Traceback:\n{tb}")

    # --- Recovery guidance ------------------------------------------------
    if is_interrupt:
        if is_dry_run:
            log_with_context(
                logging.WARNING,
                "Validation interrupted. You can restart the validation anytime.",
            )
        else:
            log_with_context(
                logging.WARNING,
                "Migration interrupted."
                " Use --update_mode to resume from where you left off.",
            )
    else:
        if is_dry_run:
            log_with_context(
                logging.ERROR,
                "Fix the validation issues above and try again.",
            )
        else:
            log_with_context(
                logging.ERROR,
                "Migration failed."
                " Check the error details and try --update_mode to resume.",
            )
