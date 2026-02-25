#!/usr/bin/env python3
"""
Main execution module for the Slack to Google Chat migration tool.

This module provides the command-line interface for the migration tool,
handling argument parsing, configuration loading, and executing the
migration process with appropriate error handling.
"""

import logging
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Callable, ClassVar, Optional

import click

if TYPE_CHECKING:
    from googleapiclient.errors import HttpError

import slack_migrator
from slack_migrator.core.cleanup import cleanup_channel_handlers, run_cleanup
from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.exceptions import (
    ConfigError,
    MigrationAbortedError,
    MigratorError,
    PermissionCheckError,
)
from slack_migrator.services.space_creator import cleanup_import_mode_spaces
from slack_migrator.utils.logging import log_with_context, setup_logger
from slack_migrator.utils.permissions import (
    check_permissions_standalone,
    validate_permissions,
)

# Create logger instance
logger = logging.getLogger("slack_migrator")


# ---------------------------------------------------------------------------
# Custom click.Group that defaults to ``migrate`` for backwards compatibility.
# When the first CLI token starts with ``-`` (i.e. a flag, not a subcommand)
# the group silently prepends ``migrate`` so that the old invocation style
#   ``slack-migrator --creds_path ... --export_path ...``
# continues to work.
# ---------------------------------------------------------------------------


class DefaultGroup(click.Group):
    """Click group that defaults to the ``migrate`` subcommand."""

    # Flags that belong to the group itself and should NOT trigger the
    # ``migrate`` default.
    _GROUP_FLAGS: ClassVar[set[str]] = {"--help", "--version", "-h"}

    def parse_args(self, ctx: click.Context, args: list[str]) -> list[str]:
        """Prepend ``migrate`` when the first token is a flag (backwards compat).

        Args:
            ctx: The current Click context.
            args: Raw CLI argument list.

        Returns:
            The (possibly modified) argument list for further parsing.
        """
        # If no args at all, let click show help as usual.
        if args and args[0].startswith("-") and args[0] not in self._GROUP_FLAGS:
            args = ["migrate", *args]
        return super().parse_args(ctx, args)


# ---------------------------------------------------------------------------
# Shared option decorator
# ---------------------------------------------------------------------------


def common_options(f: Callable[..., None]) -> Callable[..., None]:
    """Decorator that adds options shared across multiple subcommands.

    Args:
        f: The Click command function to decorate.

    Returns:
        The decorated function with common options attached.
    """
    f = click.option(
        "--creds_path",
        required=True,
        help="Path to service account credentials JSON",
    )(f)
    f = click.option(
        "--workspace_admin",
        required=True,
        help="Email of workspace admin to impersonate",
    )(f)
    f = click.option(
        "--config",
        default="config.yaml",
        show_default=True,
        help="Path to config YAML",
    )(f)
    f = click.option(
        "--verbose",
        "-v",
        is_flag=True,
        default=False,
        help="Enable verbose console logging (shows DEBUG level messages)",
    )(f)
    f = click.option(
        "--debug_api",
        is_flag=True,
        default=False,
        help="Enable detailed API request/response logging (creates very large log files)",
    )(f)
    return f


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group(
    cls=DefaultGroup,
    invoke_without_command=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.version_option(version=slack_migrator.__version__, prog_name="slack-migrator")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """Slack to Google Chat migration tool.

    Args:
        ctx: The Click context (injected by ``@click.pass_context``).
    """
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


# ---------------------------------------------------------------------------
# migrate subcommand
# ---------------------------------------------------------------------------


@cli.command()
@common_options
@click.option(
    "--export_path",
    required=True,
    help="Path to Slack export directory",
)
@click.option(
    "--dry_run",
    is_flag=True,
    default=False,
    help="Validation-only mode - performs comprehensive validation without making changes",
)
@click.option(
    "--update_mode",
    is_flag=True,
    default=False,
    help="Update mode - update existing spaces instead of creating new ones",
)
@click.option(
    "--skip_permission_check",
    is_flag=True,
    default=False,
    help="Skip permission checks (not recommended)",
)
def migrate(
    creds_path: str,
    export_path: str,
    workspace_admin: str,
    config: str,
    verbose: bool,
    debug_api: bool,
    dry_run: bool,
    update_mode: bool,
    skip_permission_check: bool,
) -> None:
    """Run the full Slack-to-Google-Chat migration.

    Args:
        creds_path: Path to service account credentials JSON.
        export_path: Path to Slack export directory.
        workspace_admin: Email of workspace admin to impersonate.
        config: Path to config YAML.
        verbose: Enable verbose console logging.
        debug_api: Enable detailed API request/response logging.
        dry_run: Validation-only mode.
        update_mode: Update existing spaces instead of creating new ones.
        skip_permission_check: Skip permission checks before migration.
    """
    args = SimpleNamespace(
        creds_path=creds_path,
        export_path=export_path,
        workspace_admin=workspace_admin,
        config=config,
        verbose=verbose,
        debug_api=debug_api,
        dry_run=dry_run,
        update_mode=update_mode,
        skip_permission_check=skip_permission_check,
    )

    # Create output directory early so all operations are logged to file
    output_dir = create_migration_output_directory()

    # Set up logger with output directory for file logging
    setup_logger(args.verbose, args.debug_api, output_dir)

    log_startup_info(args)
    log_with_context(logging.INFO, f"Output directory: {output_dir}")

    # Create orchestrator and run migration
    orchestrator = MigrationOrchestrator(args)
    orchestrator.output_dir = output_dir

    try:
        orchestrator.validate_prerequisites()
        orchestrator.run_migration()
    except Exception as e:
        handle_exception(e)
        sys.exit(1)
    finally:
        orchestrator.cleanup()
        show_security_warning()


# ---------------------------------------------------------------------------
# check-permissions subcommand
# ---------------------------------------------------------------------------


@cli.command("check-permissions")
@common_options
def check_permissions(
    creds_path: str, workspace_admin: str, config: str, verbose: bool, debug_api: bool
) -> None:
    """Validate API permissions without running a migration.

    Tests that the service account has all required scopes for the Chat and
    Drive APIs.  Does not require a Slack export directory.

    Args:
        creds_path: Path to service account credentials JSON.
        workspace_admin: Email of workspace admin to impersonate.
        config: Path to config YAML.
        verbose: Enable verbose console logging.
        debug_api: Enable detailed API request/response logging.
    """
    setup_logger(verbose, debug_api)

    try:
        check_permissions_standalone(
            creds_path=creds_path,
            workspace_admin=workspace_admin,
            config_path=config,
        )
    except Exception as e:
        handle_exception(e)
        sys.exit(1)


# ---------------------------------------------------------------------------
# validate subcommand
# ---------------------------------------------------------------------------


@cli.command()
@common_options
@click.option(
    "--export_path",
    required=True,
    help="Path to Slack export directory",
)
@click.option(
    "--dry_run",
    is_flag=True,
    default=False,
    hidden=True,
    help="(ignored — validate always runs in dry-run mode)",
)
def validate(
    creds_path: str,
    export_path: str,
    workspace_admin: str,
    config: str,
    verbose: bool,
    debug_api: bool,
    dry_run: bool,
) -> None:
    """Dry-run validation of export data, user mappings, and channels.

    Equivalent to ``migrate --dry_run`` but expressed as an explicit command.

    Args:
        creds_path: Path to service account credentials JSON.
        export_path: Path to Slack export directory.
        workspace_admin: Email of workspace admin to impersonate.
        config: Path to config YAML.
        verbose: Enable verbose console logging.
        debug_api: Enable detailed API request/response logging.
        dry_run: Ignored — validate always runs in dry-run mode.
    """
    if dry_run:
        log_with_context(
            logging.INFO,
            "Note: --dry_run is redundant with 'validate' (always dry-run).",
        )

    args = SimpleNamespace(
        creds_path=creds_path,
        export_path=export_path,
        workspace_admin=workspace_admin,
        config=config,
        verbose=verbose,
        debug_api=debug_api,
        dry_run=True,  # always dry run
        update_mode=False,
        skip_permission_check=False,
    )

    output_dir = create_migration_output_directory()
    setup_logger(args.verbose, args.debug_api, output_dir)

    log_startup_info(args)
    log_with_context(logging.INFO, f"Output directory: {output_dir}")

    orchestrator = MigrationOrchestrator(args)
    orchestrator.output_dir = output_dir

    try:
        orchestrator.validate_prerequisites()
        orchestrator.run_migration()
    except Exception as e:
        handle_exception(e)
        sys.exit(1)
    finally:
        orchestrator.cleanup()
        show_security_warning()


# ---------------------------------------------------------------------------
# cleanup subcommand
# ---------------------------------------------------------------------------


@cli.command()
@common_options
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
def cleanup(
    creds_path: str,
    workspace_admin: str,
    config: str,
    verbose: bool,
    debug_api: bool,
    yes: bool,
) -> None:
    """Complete import mode on spaces that are stuck.

    Lists all spaces visible to the service account and calls completeImport()
    on any that are still in import mode.  Does not add members — use
    ``migrate --update_mode`` for that.

    Args:
        creds_path: Path to service account credentials JSON.
        workspace_admin: Email of workspace admin to impersonate.
        config: Path to config YAML.
        verbose: Enable verbose console logging.
        debug_api: Enable detailed API request/response logging.
        yes: Skip confirmation prompt.
    """
    from slack_migrator.core.config import load_config
    from slack_migrator.utils.api import get_gcp_service

    setup_logger(verbose, debug_api)

    if not yes:
        if not click.confirm(
            "This will complete import mode on all stuck spaces. Continue?"
        ):
            click.echo("Cleanup cancelled.")
            sys.exit(0)

    cfg = load_config(Path(config))
    chat = get_gcp_service(
        creds_path,
        workspace_admin,
        "chat",
        "v1",
        max_retries=cfg.max_retries,
        retry_delay=cfg.retry_delay,
    )

    try:
        cleanup_import_mode_spaces(chat)
    except Exception as e:
        handle_exception(e)
        sys.exit(1)


# ---------------------------------------------------------------------------
# MigrationOrchestrator (unchanged from the argparse version)
# ---------------------------------------------------------------------------


class MigrationOrchestrator:
    """Orchestrates the migration process with validation and error handling."""

    def __init__(self, args: SimpleNamespace) -> None:
        self.args = args
        self.migrator: Optional[SlackToChatMigrator] = None
        self.dry_run_migrator: Optional[SlackToChatMigrator] = None
        self.output_dir: Optional[str] = None

    def create_migrator(self, force_dry_run: bool = False) -> SlackToChatMigrator:
        """Create a migrator instance with the given parameters.

        Args:
            force_dry_run: If True, override the CLI dry_run flag to True.

        Returns:
            A configured SlackToChatMigrator ready to run.
        """
        migrator = SlackToChatMigrator(
            self.args.creds_path,
            self.args.export_path,
            self.args.workspace_admin,
            self.args.config,
            dry_run=force_dry_run or self.args.dry_run,
            verbose=self.args.verbose,
            update_mode=self.args.update_mode,
            debug_api=self.args.debug_api,
        )

        # Set output directory if we have one
        if self.output_dir:
            migrator.state.output_dir = self.output_dir

        return migrator

    def validate_prerequisites(self) -> None:
        """Validate all prerequisites before migration."""
        # Check credentials file
        creds_path = Path(self.args.creds_path)
        if not creds_path.exists():
            raise ConfigError(
                f"Credentials file not found: {self.args.creds_path}. "
                "Make sure your service account JSON key file exists and has the correct path."
            )

        # Initialize main migrator
        self.migrator = self.create_migrator()

        # Run permission checks BEFORE any expensive operations
        if not self.args.skip_permission_check:
            log_with_context(logging.INFO, "Checking permissions before proceeding...")
            try:
                validate_permissions(self.migrator)
                log_with_context(logging.INFO, "Permission checks passed!")

                # Now that permissions are validated, initialize drive structures
                if (
                    hasattr(self.migrator, "file_handler")
                    and self.migrator.file_handler
                ):
                    self.migrator.file_handler.ensure_drive_initialized()
                    log_with_context(
                        logging.INFO, "Drive structures initialized successfully"
                    )

            except Exception as e:
                raise PermissionCheckError(
                    f"Permission checks failed: {e}. "
                    "Fix the issues or run with --skip_permission_check if you're sure."
                ) from e
        else:
            log_with_context(
                logging.WARNING,
                "Permission checks skipped. This may cause issues during migration.",
            )
            # Still initialize drive structures even if permission checks are skipped
            if hasattr(self.migrator, "file_handler") and self.migrator.file_handler:
                self.migrator.file_handler.ensure_drive_initialized()
                log_with_context(
                    logging.INFO, "Drive structures initialized successfully"
                )

    def check_unmapped_users(self, migrator_instance: SlackToChatMigrator) -> bool:
        """Check for unmapped users and return True if any found.

        Args:
            migrator_instance: The migrator to inspect.

        Returns:
            True if unmapped users were detected, False otherwise.
        """
        return (
            hasattr(migrator_instance, "unmapped_user_tracker")
            and migrator_instance.unmapped_user_tracker.has_unmapped_users()
        )

    def report_validation_issues(
        self, migrator_instance: SlackToChatMigrator, is_explicit_dry_run: bool = False
    ) -> bool:
        """Report validation issues and ask user if they want to proceed anyway.

        Args:
            migrator_instance: The migrator whose results are reported.
            is_explicit_dry_run: If True, skip the interactive confirmation prompt.

        Returns:
            True if the user chose to proceed despite issues, False otherwise.
        """
        log_with_context(logging.INFO, "")
        log_with_context(logging.INFO, "🚨 VALIDATION ISSUES DETECTED!")
        log_with_context(
            logging.INFO,
            f"Found {migrator_instance.unmapped_user_tracker.get_unmapped_count()} unmapped user(s).",
        )
        log_with_context(logging.INFO, "")
        log_with_context(
            logging.INFO, "⚠️  WARNING: If you proceed without fixing these mappings:"
        )
        log_with_context(
            logging.INFO,
            "   • Messages from unmapped users will be sent by the workspace admin",
        )
        log_with_context(
            logging.INFO, "   • Attribution prefixes will indicate the original sender"
        )
        log_with_context(
            logging.INFO,
            "   • Reactions from unmapped users will be skipped and logged",
        )
        log_with_context(logging.INFO, "")
        log_with_context(logging.INFO, "📋 Recommended steps to fix:")
        log_with_context(logging.INFO, "1. Review the unmapped users listed above")
        log_with_context(
            logging.INFO, "2. Add them to user_mapping_overrides in your config.yaml"
        )

        if is_explicit_dry_run:
            log_with_context(
                logging.INFO, "3. Run the migration again (without --dry_run)"
            )
            log_with_context(logging.INFO, "")
            return False  # In explicit dry run, just report and exit
        else:
            log_with_context(logging.INFO, "3. Run the migration again")
            log_with_context(logging.INFO, "")

            # Ask user if they want to proceed anyway
            try:
                if click.confirm(
                    "Proceed anyway despite unmapped users? (NOT RECOMMENDED)",
                    default=False,
                ):
                    log_with_context(
                        logging.WARNING,
                        "Proceeding with unmapped users - messages will be attributed to workspace admin",
                    )
                    return True
                else:
                    log_with_context(
                        logging.INFO,
                        "Migration cancelled. Please fix the user mappings and try again.",
                    )
                    return False
            except click.Abort:
                log_with_context(logging.INFO, "\nMigration cancelled by user.")
                return False

    def report_validation_success(self, is_explicit_dry_run: bool = False) -> None:
        """Report successful validation.

        Args:
            is_explicit_dry_run: If True, include a hint to re-run without dry_run.
        """
        log_with_context(logging.INFO, "")
        log_with_context(logging.INFO, "✅ Validation completed successfully!")
        log_with_context(logging.INFO, "   • All users mapped correctly")
        log_with_context(logging.INFO, "   • File attachments accessible")
        log_with_context(logging.INFO, "   • Channel structure validated")
        log_with_context(logging.INFO, "   • Migration scope confirmed")
        log_with_context(logging.INFO, "")

        if is_explicit_dry_run:
            log_with_context(
                logging.INFO,
                "You can now run the migration without --dry_run to perform the actual migration.",
            )

        log_with_context(logging.INFO, "")

    def run_validation(self) -> bool:
        """Run comprehensive validation.

        Returns:
            True if validation passes and the user elects to proceed.
        """
        log_with_context(logging.INFO, "")
        log_with_context(
            logging.INFO, "🔍 STEP 1: Running comprehensive validation (dry run)..."
        )
        log_with_context(
            logging.INFO, "   • Validating user mappings and detecting unmapped users"
        )
        log_with_context(logging.INFO, "   • Checking file attachments and permissions")
        log_with_context(
            logging.INFO, "   • Verifying channel structure and memberships"
        )
        log_with_context(logging.INFO, "   • Testing message formatting and content")
        log_with_context(
            logging.INFO, "   • Estimating migration scope and requirements"
        )
        log_with_context(logging.INFO, "")

        # Create and run dry run migrator
        self.dry_run_migrator = self.create_migrator(force_dry_run=True)

        try:
            self.dry_run_migrator.migrate()
        except Exception:
            log_with_context(
                logging.ERROR,
                "Please fix the issues identified during validation before proceeding.",
            )
            raise

        # Check validation results
        if self.check_unmapped_users(self.dry_run_migrator):
            return self.report_validation_issues(self.dry_run_migrator)

        return True

    def get_user_confirmation(self) -> bool:
        """Get user confirmation to proceed with migration.

        Returns:
            True if the user confirms, False otherwise.
        """
        log_with_context(
            logging.INFO, "🚀 STEP 2: Ready to proceed with actual migration"
        )
        log_with_context(logging.INFO, "")

        try:
            return click.confirm("Proceed with the actual migration?", default=False)
        except click.Abort:
            log_with_context(logging.INFO, "\nMigration cancelled by user.")
            return False

    def run_migration(self) -> None:
        """Execute the main migration logic."""
        if self.args.dry_run:
            # Explicit dry run mode
            assert self.migrator is not None
            self.migrator.migrate()

            if self.check_unmapped_users(self.migrator):
                self.report_validation_issues(self.migrator, is_explicit_dry_run=True)
                raise MigrationAbortedError(
                    "Dry run completed with unmapped users. "
                    "Use normal migration mode to proceed."
                )
            else:
                self.report_validation_success(is_explicit_dry_run=True)
        else:
            # Full migration with automatic validation
            if self.run_validation():
                self.report_validation_success()

                if self.get_user_confirmation():
                    assert self.migrator is not None
                    self.migrator.migrate()
                else:
                    log_with_context(logging.INFO, "Migration cancelled by user.")
                    return

    def cleanup(self) -> None:
        """Perform cleanup operations."""
        if self.migrator:
            try:
                log_with_context(logging.INFO, "Performing cleanup operations...")

                # Always clean up channel handlers, regardless of dry run mode
                try:
                    cleanup_channel_handlers(self.migrator)
                except Exception as handler_cleanup_e:
                    log_with_context(
                        logging.ERROR,
                        f"Failed to clean up channel handlers: {handler_cleanup_e}",
                        exc_info=True,
                    )

                # Only perform space cleanup if not in dry run mode
                if not self.args.dry_run:
                    try:
                        run_cleanup(self.migrator)
                    except Exception as space_cleanup_e:
                        log_with_context(
                            logging.ERROR,
                            f"Failed to clean up spaces: {space_cleanup_e}",
                            exc_info=True,
                        )
                        log_with_context(
                            logging.WARNING,
                            "Some spaces may still be in import mode and require manual cleanup",
                        )

                log_with_context(logging.INFO, "Cleanup completed successfully.")
            except Exception as cleanup_e:
                log_with_context(
                    logging.ERROR, f"Overall cleanup failed: {cleanup_e}", exc_info=True
                )
                log_with_context(
                    logging.INFO,
                    "You may need to manually clean up temporary resources.",
                )
                log_with_context(
                    logging.INFO,
                    "Check Google Chat admin console for spaces that may still be in import mode.",
                )


# ---------------------------------------------------------------------------
# Helper functions (unchanged)
# ---------------------------------------------------------------------------


def log_startup_info(args: SimpleNamespace) -> None:
    """Log startup information.

    Args:
        args: Parsed CLI arguments containing migration parameters.
    """
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path.cwd() / args.config

    log_with_context(logging.INFO, "Starting migration with the following parameters:")
    log_with_context(logging.INFO, f"- Export path: {args.export_path}")
    log_with_context(logging.INFO, f"- Workspace admin: {args.workspace_admin}")
    log_with_context(logging.INFO, f"- Config: {config_path}")
    log_with_context(logging.INFO, f"- Dry run: {args.dry_run}")
    log_with_context(logging.INFO, f"- Update mode: {args.update_mode}")
    log_with_context(logging.INFO, f"- Verbose logging: {args.verbose}")
    log_with_context(logging.INFO, f"- Debug API calls: {args.debug_api}")


def handle_http_error(e: "HttpError") -> None:
    """Handle HTTP errors with specific messages.

    Args:
        e: The Google API HTTP error to handle.
    """

    if e.resp.status == 403 and "PERMISSION_DENIED" in str(e):
        log_with_context(logging.ERROR, f"Permission denied error: {e}")
        log_with_context(
            logging.INFO,
            "\nThe service account doesn't have sufficient permissions. Please ensure:",
        )
        log_with_context(
            logging.INFO,
            "1. The service account has the 'Chat API Admin' role in your GCP project",
        )
        log_with_context(
            logging.INFO,
            "2. Domain-wide delegation is configured properly in your Google Workspace admin console",
        )
        log_with_context(
            logging.INFO, "3. The following scopes are granted to the service account:"
        )
        log_with_context(
            logging.INFO, "   - https://www.googleapis.com/auth/chat.import"
        )
        log_with_context(
            logging.INFO, "   - https://www.googleapis.com/auth/chat.spaces"
        )
        log_with_context(logging.INFO, "   - https://www.googleapis.com/auth/drive")
    elif e.resp.status == 429:
        log_with_context(logging.ERROR, f"Rate limit exceeded: {e}")
        log_with_context(
            logging.INFO,
            "The migration hit API rate limits. Consider using --update_mode to resume.",
        )
    elif e.resp.status >= 500:
        log_with_context(logging.ERROR, f"Server error from Google API: {e}")
        log_with_context(
            logging.INFO, "This is likely a temporary issue. Please try again later."
        )
    else:
        log_with_context(logging.ERROR, f"API error during migration: {e}")


def handle_exception(e: Exception) -> None:
    """Handle different types of exceptions.

    Args:
        e: The exception to handle.
    """
    from googleapiclient.errors import HttpError

    if isinstance(e, MigratorError):
        log_with_context(logging.ERROR, str(e))
    elif isinstance(e, HttpError):
        handle_http_error(e)
    elif isinstance(e, FileNotFoundError):
        log_with_context(logging.ERROR, f"File not found: {e}")
        log_with_context(
            logging.INFO,
            "Please check that all required files exist and paths are correct.",
        )
    elif isinstance(e, KeyboardInterrupt):
        log_with_context(logging.WARNING, "Migration interrupted by user.")
        log_with_context(
            logging.INFO,
            "📋 Check the partial migration report in the output directory.",
        )
        log_with_context(
            logging.INFO, "🔄 You can resume the migration with --update_mode."
        )
        log_with_context(
            logging.INFO, "📝 All progress and logs have been saved to disk."
        )
    else:
        log_with_context(logging.ERROR, f"Migration failed: {e}", exc_info=True)


def show_security_warning() -> None:
    """Show security warning about tokens in export files."""
    log_with_context(
        logging.WARNING,
        "\nSECURITY WARNING: Your Slack export files contain authentication tokens in the URLs.",
    )
    log_with_context(
        logging.WARNING,
        "Consider securing or deleting these files after the migration is complete.",
    )
    log_with_context(
        logging.WARNING,
        "See README.md for more information on security best practices.",
    )


def create_migration_output_directory() -> str:
    """Create output directory for migration with timestamp.

    Returns:
        The path to the newly created output directory.
    """
    import datetime
    import os

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"migration_logs/run_{timestamp}"

    # Create subdirectories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "channel_logs"), exist_ok=True)

    return output_dir


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the slack-migrator command."""
    cli()


if __name__ == "__main__":
    main()
