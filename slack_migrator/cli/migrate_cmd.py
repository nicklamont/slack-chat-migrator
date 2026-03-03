"""CLI command handler for the migrate and validate workflows."""

from __future__ import annotations

import datetime
import logging
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import click

from slack_migrator.cli.common import (
    cli,
    common_options,
    handle_exception,
    show_security_warning,
)
from slack_migrator.cli.report import generate_report, print_dry_run_summary
from slack_migrator.core.cleanup import cleanup_channel_handlers, run_cleanup
from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.exceptions import (
    ConfigError,
    MigrationAbortedError,
    PermissionCheckError,
)
from slack_migrator.utils.logging import log_with_context, setup_logger
from slack_migrator.utils.permissions import validate_permissions

# Create logger instance
logger = logging.getLogger("slack_migrator")


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
# MigrationOrchestrator (unchanged from the argparse version)
# ---------------------------------------------------------------------------


class MigrationOrchestrator:
    """Orchestrates the migration process with validation and error handling."""

    def __init__(self, args: SimpleNamespace) -> None:
        self.args = args
        self.migrator: SlackToChatMigrator | None = None
        self.dry_run_migrator: SlackToChatMigrator | None = None
        self.output_dir: str | None = None

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
            migrator.state.context.output_dir = self.output_dir

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
        except BaseException as e:
            # Generate report even on failure to show progress made
            try:
                m = self.dry_run_migrator
                report_file = generate_report(
                    m.ctx,
                    m.state,
                    m.user_resolver,
                    getattr(m, "file_handler", None),
                )
                if isinstance(e, (KeyboardInterrupt, SystemExit)):
                    log_with_context(
                        logging.INFO,
                        f"Partial migration report available at: {report_file}",
                    )
                    log_with_context(
                        logging.INFO,
                        "This report shows progress made before interruption.",
                    )
                else:
                    log_with_context(
                        logging.INFO,
                        f"Migration report (with partial results) available at: {report_file}",
                    )
            except Exception as report_error:
                log_with_context(
                    logging.WARNING,
                    f"Failed to generate migration report after failure: {report_error}",
                )
            log_with_context(
                logging.ERROR,
                "Please fix the issues identified during validation before proceeding.",
            )
            raise

        # Generate report after successful validation
        m = self.dry_run_migrator
        report_file = generate_report(
            m.ctx,
            m.state,
            m.user_resolver,
            getattr(m, "file_handler", None),
        )
        if m.dry_run:
            print_dry_run_summary(
                m.ctx,
                m.state,
                m.user_resolver,
                getattr(m, "file_handler", None),
                report_file,
            )

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
            if self.migrator is None:
                raise RuntimeError("Migrator not initialized")
            m = self.migrator
            try:
                m.migrate()
            except BaseException as e:
                # Generate report even on failure to show progress made
                try:
                    report_file = generate_report(
                        m.ctx,
                        m.state,
                        m.user_resolver,
                        getattr(m, "file_handler", None),
                    )
                    if isinstance(e, (KeyboardInterrupt, SystemExit)):
                        log_with_context(
                            logging.INFO,
                            f"Partial migration report available at: {report_file}",
                        )
                        log_with_context(
                            logging.INFO,
                            "This report shows progress made before interruption.",
                        )
                    else:
                        log_with_context(
                            logging.INFO,
                            f"Migration report (with partial results) available at: {report_file}",
                        )
                except Exception as report_error:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to generate migration report after failure: {report_error}",
                    )
                raise

            # Generate report after dry run migration
            report_file = generate_report(
                m.ctx,
                m.state,
                m.user_resolver,
                getattr(m, "file_handler", None),
            )
            print_dry_run_summary(
                m.ctx,
                m.state,
                m.user_resolver,
                getattr(m, "file_handler", None),
                report_file,
            )

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
                    if self.migrator is None:
                        raise RuntimeError("Migrator not initialized")
                    m = self.migrator
                    try:
                        m.migrate()
                    except BaseException as e:
                        # Generate report even on failure to show progress made
                        try:
                            report_file = generate_report(
                                m.ctx,
                                m.state,
                                m.user_resolver,
                                getattr(m, "file_handler", None),
                            )
                            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                                log_with_context(
                                    logging.INFO,
                                    f"Partial migration report available at: {report_file}",
                                )
                                log_with_context(
                                    logging.INFO,
                                    "This report shows progress made before interruption.",
                                )
                            else:
                                log_with_context(
                                    logging.INFO,
                                    f"Migration report (with partial results) available at: {report_file}",
                                )
                        except Exception as report_error:
                            log_with_context(
                                logging.WARNING,
                                f"Failed to generate migration report after failure: {report_error}",
                            )
                        raise
                    # Generate report after successful migration
                    generate_report(
                        m.ctx,
                        m.state,
                        m.user_resolver,
                        getattr(m, "file_handler", None),
                    )
                else:
                    log_with_context(logging.INFO, "Migration cancelled by user.")
                    return

    def cleanup(self) -> None:
        """Perform cleanup operations."""
        if self.migrator:
            m = self.migrator
            try:
                log_with_context(logging.INFO, "Performing cleanup operations...")

                # Always clean up channel handlers, regardless of dry run mode
                try:
                    cleanup_channel_handlers(m.state)
                except Exception as handler_cleanup_e:
                    log_with_context(
                        logging.ERROR,
                        f"Failed to clean up channel handlers: {handler_cleanup_e}",
                        exc_info=True,
                    )

                # Only perform space cleanup if not in dry run mode
                if not self.args.dry_run:
                    try:
                        run_cleanup(
                            m.ctx,
                            m.state,
                            m.chat,
                            m.user_resolver,
                            getattr(m, "file_handler", None),
                        )
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


def create_migration_output_directory() -> str:
    """Create output directory for migration with timestamp.

    Returns:
        The path to the newly created output directory.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"migration_logs/run_{timestamp}"

    # Create subdirectories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "channel_logs"), exist_ok=True)

    return output_dir
