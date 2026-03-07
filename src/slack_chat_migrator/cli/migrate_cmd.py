"""CLI command handler for the migrate and validate workflows."""

from __future__ import annotations

import contextlib
import datetime
import logging
import os
import sys
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import click

from slack_chat_migrator.cli.common import (
    InterruptHandler,
    cli,
    common_options,
    deprecated_option,
    handle_exception,
    show_security_warning,
)
from slack_chat_migrator.cli.report import (
    generate_report,
    print_dry_run_summary,
    print_rich_summary,
)
from slack_chat_migrator.core.cleanup import cleanup_channel_handlers, run_cleanup
from slack_chat_migrator.core.migrator import SlackToChatMigrator
from slack_chat_migrator.core.progress import ProgressTracker
from slack_chat_migrator.exceptions import (
    ConfigError,
    PermissionCheckError,
)
from slack_chat_migrator.utils.logging import log_with_context, setup_logger
from slack_chat_migrator.utils.permissions import validate_permissions

# Create logger instance
logger = logging.getLogger("slack_chat_migrator")


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
    "--resume",
    is_flag=True,
    default=False,
    help="Resume a previous migration - reuse existing spaces instead of creating new ones",
)
@deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
@click.option(
    "--complete",
    is_flag=True,
    default=False,
    help="Complete import mode on all spaces without migrating messages",
)
@click.option(
    "--skip_permission_check",
    is_flag=True,
    default=False,
    help="Skip permission checks (not recommended)",
)
def migrate(
    creds_path: str | None,
    export_path: str,
    workspace_admin: str | None,
    config: str,
    verbose: bool,
    debug_api: bool,
    dry_run: bool,
    resume: bool,
    complete: bool,
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
        resume: Resume a previous migration (reuse existing spaces).
        complete: Complete import mode on all spaces without migrating.
        skip_permission_check: Skip permission checks before migration.
    """
    if complete:
        _run_complete_mode(creds_path, workspace_admin, config, verbose, debug_api)
        return

    args = SimpleNamespace(
        creds_path=creds_path,
        export_path=export_path,
        workspace_admin=workspace_admin,
        config=config,
        verbose=verbose,
        debug_api=debug_api,
        dry_run=dry_run,
        update_mode=resume,
        skip_permission_check=skip_permission_check,
    )

    # Create output directory early so all operations are logged to file
    output_dir = create_migration_output_directory()

    # Set up logger with output directory for file logging
    # Suppress "Main log file created" from console on TTY (still goes to file)
    with _quiet_console() if sys.stdout.isatty() else contextlib.nullcontext():
        setup_logger(args.verbose, args.debug_api, output_dir)

    # Show config panel (Rich on TTY, log lines otherwise)
    _print_config_panel(args, output_dir)
    with _quiet_console() if sys.stdout.isatty() else contextlib.nullcontext():
        log_with_context(logging.INFO, f"Output directory: {output_dir}")

    # Create orchestrator and run migration
    orchestrator = MigrationOrchestrator(args)
    orchestrator.output_dir = output_dir

    with InterruptHandler(export_path=export_path):
        try:
            orchestrator.validate_prerequisites()
            orchestrator.run_migration()
        except Exception as e:
            handle_exception(e)
            sys.exit(1)
        finally:
            orchestrator.cleanup()
            show_security_warning()


def _run_complete_mode(
    creds_path: str | None,
    workspace_admin: str | None,
    config: str,
    verbose: bool,
    debug_api: bool,
) -> None:
    """Complete import mode on all spaces without migrating messages.

    This is equivalent to the standalone ``cleanup`` command but accessible
    via ``migrate --complete``.
    """
    from slack_chat_migrator.core.config import load_config
    from slack_chat_migrator.services.chat_adapter import ChatAdapter
    from slack_chat_migrator.services.spaces.space_creator import (
        cleanup_import_mode_spaces,
    )
    from slack_chat_migrator.utils.api import get_gcp_service

    setup_logger(verbose, debug_api)

    if not creds_path:
        raise click.UsageError("--creds_path is required for --complete")
    if not workspace_admin:
        raise click.UsageError("--workspace_admin is required for --complete")

    is_tty = sys.stdout.isatty()

    if is_tty:
        try:
            from slack_chat_migrator.cli.renderers import get_console

            console = get_console()
            console.print("\n[bold]Completing import mode on all spaces...[/bold]")
        except Exception:
            is_tty = False

    ctx = _quiet_console() if is_tty else contextlib.nullcontext()

    with ctx:
        log_with_context(logging.INFO, "Completing import mode on all spaces...")
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
            cleanup_import_mode_spaces(ChatAdapter(chat))
        except Exception as e:
            handle_exception(e)
            sys.exit(1)

    if is_tty:
        console.print("[green]\u2713[/green] All spaces completed successfully.")
    else:
        log_with_context(logging.INFO, "All spaces completed successfully.")


def _warn_import_mode_deadline() -> None:
    """Print a reminder about the 90-day import mode deadline."""
    if sys.stdout.isatty():
        try:
            from slack_chat_migrator.cli.renderers import get_console, warning_panel

            console = get_console()
            console.print(
                warning_panel(
                    "Import Mode Deadline",
                    "Spaces in import mode must be completed within "
                    "[bold]90 days[/bold] of creation.\n"
                    "Run [bold]slack-chat-migrator migrate --complete[/bold] "
                    "to finalize all spaces.",
                )
            )
            return
        except Exception:
            pass
    log_with_context(
        logging.WARNING,
        "Reminder: Spaces in import mode must be completed within 90 days "
        "of creation. Run 'migrate --complete' if any remain.",
    )


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
        # In dry-run mode, credentials and workspace_admin are optional
        if not self.args.dry_run:
            if not self.args.creds_path:
                raise click.UsageError("--creds_path is required for live migration")
            if not self.args.workspace_admin:
                raise click.UsageError(
                    "--workspace_admin is required for live migration"
                )
            # Check credentials file exists
            creds_path = Path(self.args.creds_path)
            if not creds_path.exists():
                raise ConfigError(
                    f"Credentials file not found: {self.args.creds_path}. "
                    "Make sure your service account JSON key file exists and has the correct path."
                )
        elif self.args.creds_path:
            # Dry-run with creds provided — still validate the file exists
            creds_path = Path(self.args.creds_path)
            if not creds_path.exists():
                raise ConfigError(
                    f"Credentials file not found: {self.args.creds_path}. "
                    "Make sure your service account JSON key file exists and has the correct path."
                )

        is_tty = sys.stdout.isatty()
        if is_tty:
            from slack_chat_migrator.cli.renderers import get_console

            console = get_console()
            console.print("\n[bold]Preflight Checks[/bold]")

        # Initialize main migrator (suppressing per-user warnings on console)
        with _quiet_console():
            self.migrator = self.create_migrator()

        # Report user mapping status
        mapped = len(self.migrator.ctx.user_map) if self.migrator.ctx.user_map else 0
        unmapped = 0
        if (
            hasattr(self.migrator, "unmapped_user_tracker")
            and self.migrator.unmapped_user_tracker.has_unmapped_users()
        ):
            unmapped = self.migrator.unmapped_user_tracker.get_unmapped_count()
        _print_preflight_status(
            f"User mappings loaded ({mapped} mapped, {unmapped} unmapped)",
            status="warn" if unmapped > 0 else "ok",
        )
        if unmapped > 0:
            _print_preflight_status(
                f"{unmapped} users without email \u2014 see log for details",
                status="warn",
            )

        # Run permission checks BEFORE any expensive operations
        if not self.args.skip_permission_check:
            has_creds = bool(self.args.creds_path and self.args.workspace_admin)
            if has_creds:
                log_with_context(
                    logging.INFO, "Checking permissions before proceeding..."
                )
                try:
                    with _quiet_console():
                        validate_permissions(self.migrator)
                    log_with_context(logging.INFO, "Permission checks passed!")
                    _print_preflight_status("Permissions verified")

                    # Now that permissions are validated, initialize drive structures
                    if (
                        hasattr(self.migrator, "file_handler")
                        and self.migrator.file_handler
                    ):
                        with _quiet_console():
                            self.migrator.file_handler.ensure_drive_initialized()
                        log_with_context(
                            logging.INFO, "Drive structures initialized successfully"
                        )
                        _print_preflight_status("Drive initialized")

                except Exception as e:
                    raise PermissionCheckError(
                        f"Permission checks failed: {e}. "
                        "Fix the issues or run with --skip_permission_check if you're sure."
                    ) from e
            else:
                _print_preflight_status(
                    "Permissions", status="skip", detail="no credentials"
                )
        else:
            log_with_context(
                logging.WARNING,
                "Permission checks skipped. This may cause issues during migration.",
            )
            _print_preflight_status("Permissions", status="skip", detail="skipped")
            # Still initialize drive structures even if permission checks are skipped
            if hasattr(self.migrator, "file_handler") and self.migrator.file_handler:
                with _quiet_console():
                    self.migrator.file_handler.ensure_drive_initialized()
                log_with_context(
                    logging.INFO, "Drive structures initialized successfully"
                )
                _print_preflight_status("Drive initialized")

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
        unmapped_count = migrator_instance.unmapped_user_tracker.get_unmapped_count()

        # Always log full detail to file
        with _quiet_console():
            log_with_context(logging.INFO, "")
            log_with_context(logging.INFO, "VALIDATION ISSUES DETECTED!")
            log_with_context(
                logging.INFO,
                f"Found {unmapped_count} unmapped user(s).",
            )
            log_with_context(logging.INFO, "")
            log_with_context(
                logging.INFO, "WARNING: If you proceed without fixing these mappings:"
            )
            log_with_context(
                logging.INFO,
                "   - Messages from unmapped users will be sent by the workspace admin",
            )
            log_with_context(
                logging.INFO,
                "   - Attribution prefixes will indicate the original sender",
            )
            log_with_context(
                logging.INFO,
                "   - Reactions from unmapped users will be skipped and logged",
            )
            log_with_context(logging.INFO, "")
            log_with_context(logging.INFO, "Recommended steps to fix:")
            log_with_context(logging.INFO, "1. Review the unmapped users listed above")
            log_with_context(
                logging.INFO,
                "2. Add them to user_mapping_overrides in your config.yaml",
            )
            log_with_context(logging.INFO, "3. Run the migration again")
            log_with_context(logging.INFO, "")

        # Show Rich panel on TTY
        if sys.stdout.isatty():
            from slack_chat_migrator.cli.renderers import get_console, warning_panel

            console = get_console()
            body = (
                f"[bold]{unmapped_count}[/bold] unmapped user(s) detected.\n\n"
                "Messages from unmapped users will be sent by the workspace admin.\n"
                "Fix: add them to [bold]user_mapping_overrides[/bold] in config.yaml."
            )
            console.print(warning_panel("Validation Issues", body))

        if is_explicit_dry_run:
            return False  # In explicit dry run, just report and exit

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
        log_with_context(logging.INFO, "Validation completed successfully!")
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
            logging.INFO, "STEP 1: Running comprehensive validation (dry run)..."
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
        log_with_context(logging.INFO, "STEP 2: Ready to proceed with actual migration")
        log_with_context(logging.INFO, "")

        try:
            return click.confirm("Proceed with the actual migration?", default=False)
        except click.Abort:
            log_with_context(logging.INFO, "\nMigration cancelled by user.")
            return False

    def _run_with_progress(self, m: SlackToChatMigrator) -> None:
        """Run ``m.migrate()`` with a ProgressTracker and renderer.

        On failure the report is still generated so partial results are
        available for inspection.
        """
        from slack_chat_migrator.cli.renderers import create_renderer

        total_channels = len(m.channels_meta) if m.channels_meta else 0
        tracker = ProgressTracker()
        renderer = create_renderer(
            tracker, total_channels=total_channels, dry_run=m.dry_run
        )
        renderer.start()
        try:
            m.migrate(progress_tracker=tracker)
        except BaseException as e:
            renderer.stop()
            self._generate_partial_report(m, e)
            raise
        renderer.stop()

    @staticmethod
    def _generate_partial_report(m: SlackToChatMigrator, exc: BaseException) -> None:
        """Generate a report after a failed or interrupted migration."""
        try:
            report_file = generate_report(
                m.ctx,
                m.state,
                m.user_resolver,
                getattr(m, "file_handler", None),
            )
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                log_with_context(
                    logging.INFO,
                    f"Partial migration report available at: {report_file}",
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

    def _print_summary(self, m: SlackToChatMigrator) -> None:
        """Generate the YAML report and print a console summary."""
        with _quiet_console() if sys.stdout.isatty() else contextlib.nullcontext():
            generate_report(
                m.ctx,
                m.state,
                m.user_resolver,
                getattr(m, "file_handler", None),
            )
        print_rich_summary(
            m.ctx,
            m.state,
            m.user_resolver,
            getattr(m, "file_handler", None),
        )
        if not m.dry_run:
            _warn_import_mode_deadline()

    def _confirm_start(self) -> bool:
        """Show a confirmation prompt before starting migration/validation."""
        if self.migrator is None:
            return True

        mode = "dry-run validation" if self.args.dry_run else "migration"
        channel_count = (
            len(self.migrator.channels_meta) if self.migrator.channels_meta else 0
        )
        msg = f"Ready to begin {mode} of {channel_count} channels."

        if sys.stdout.isatty():
            from slack_chat_migrator.cli.renderers import get_console

            console = get_console()
            console.print(f"\n{msg}")
            if not click.confirm("Continue?", default=True):
                click.echo("Aborted.")
                return False
        else:
            log_with_context(logging.INFO, msg)
        return True

    def run_migration(self) -> None:
        """Execute the main migration logic."""
        if self.args.dry_run:
            # Explicit dry run mode
            if self.migrator is None:
                raise RuntimeError("Migrator not initialized")

            if not self._confirm_start():
                return

            m = self.migrator
            self._run_with_progress(m)
            self._print_summary(m)

            if self.check_unmapped_users(self.migrator):
                self.report_validation_issues(self.migrator, is_explicit_dry_run=True)
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
                    self._run_with_progress(m)
                    self._print_summary(m)
                else:
                    log_with_context(logging.INFO, "Migration cancelled by user.")
                    return

    def cleanup(self) -> None:
        """Perform cleanup operations."""
        if self.migrator:
            m = self.migrator
            # Suppress cleanup log lines on TTY (detail goes to log file)
            ctx_mgr = (
                _quiet_console() if sys.stdout.isatty() else contextlib.nullcontext()
            )
            with ctx_mgr:
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
                        logging.ERROR,
                        f"Overall cleanup failed: {cleanup_e}",
                        exc_info=True,
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


@contextlib.contextmanager
def _quiet_console() -> Iterator[None]:
    """Suppress INFO/WARNING from console output (still goes to log file)."""
    root = logging.getLogger()
    restored: list[tuple[logging.Handler, int]] = []
    for handler in root.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, logging.FileHandler
        ):
            restored.append((handler, handler.level))
            handler.setLevel(logging.ERROR)
    # Also check the package logger
    pkg = logging.getLogger("slack_chat_migrator")
    for handler in pkg.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, logging.FileHandler
        ):
            restored.append((handler, handler.level))
            handler.setLevel(logging.ERROR)
    try:
        yield
    finally:
        for handler, old_level in restored:
            handler.setLevel(old_level)


def _print_config_panel(args: SimpleNamespace, output_dir: str) -> None:
    """Print startup config as Rich panel on TTY, or log lines otherwise."""
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path.cwd() / args.config

    is_tty = sys.stdout.isatty()

    # Log to file (suppress console on TTY since the Rich panel replaces it)
    with _quiet_console() if is_tty else contextlib.nullcontext():
        log_with_context(
            logging.INFO, "Starting migration with the following parameters:"
        )
        log_with_context(logging.INFO, f"- Export path: {args.export_path}")
        log_with_context(logging.INFO, f"- Workspace admin: {args.workspace_admin}")
        log_with_context(logging.INFO, f"- Config: {config_path}")
        log_with_context(logging.INFO, f"- Dry run: {args.dry_run}")
        log_with_context(logging.INFO, f"- Resume mode: {args.update_mode}")
        log_with_context(logging.INFO, f"- Verbose logging: {args.verbose}")
        log_with_context(logging.INFO, f"- Debug API calls: {args.debug_api}")

    if not is_tty:
        return

    from rich.table import Table

    from slack_chat_migrator.cli.renderers import get_console

    console = get_console()
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Key", style="dim")
    table.add_column("Value")

    table.add_row("Export", str(args.export_path))
    table.add_row("Config", str(config_path))
    table.add_row("Admin", str(args.workspace_admin or "(none)"))
    mode = "Dry Run" if args.dry_run else "Live Migration"
    if getattr(args, "update_mode", False):
        mode += " (resume)"
    table.add_row("Mode", mode)
    table.add_row("Log dir", output_dir)

    from rich.panel import Panel

    console.print(
        Panel(
            table,
            title="Configuration",
            border_style="blue" if args.dry_run else "green",
        )
    )


def _print_preflight_status(label: str, status: str = "ok", detail: str = "") -> None:
    """Print a single preflight check result on TTY."""
    if not sys.stdout.isatty():
        return

    from slack_chat_migrator.cli.renderers import get_console

    console = get_console()
    if status == "ok":
        icon = "[green]\u2713[/green]"
    elif status == "warn":
        icon = "[yellow]\u26a0[/yellow]"
    elif status == "skip":
        icon = "[dim]\u2013[/dim]"
    else:
        icon = "[red]\u2717[/red]"

    msg = f"  {icon} {label}"
    if detail:
        msg += f" [dim]({detail})[/dim]"
    console.print(msg)


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
