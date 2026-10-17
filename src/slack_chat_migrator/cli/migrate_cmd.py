"""CLI command handler for the migrate and validate workflows."""

from __future__ import annotations

import contextlib
import logging
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import click

from slack_chat_migrator.cli.common import (
    EXIT_FAILURE,
    EXIT_INTERRUPTED,
    EXIT_OK,
    YES_ENVVAR,
    InterruptHandler,
    cli,
    common_options,
    config_was_given,
    confirm_action,
    deprecated_option,
    handle_exception,
    show_security_warning,
    stdin_is_interactive,
    yes_option,
)
from slack_chat_migrator.cli.report import (
    generate_report,
    print_dry_run_summary,
    print_rich_summary,
)
from slack_chat_migrator.core.cleanup import cleanup_channel_handlers, run_cleanup
from slack_chat_migrator.core.config import MigrationConfig, load_config
from slack_chat_migrator.core.migrator import SlackToChatMigrator
from slack_chat_migrator.core.progress import ProgressTracker
from slack_chat_migrator.exceptions import (
    ConfigError,
    ConfirmationRequiredError,
    PermissionCheckError,
)
from slack_chat_migrator.utils.api import scopes_for
from slack_chat_migrator.utils.logging import (
    create_output_directory,
    log_with_context,
    setup_logger,
)
from slack_chat_migrator.utils.permissions import (
    check_permissions_standalone,
    validate_permissions,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.spaces.space_creator import (
        ImportCompletionResult,
    )

# Create logger instance
logger = logging.getLogger("slack_chat_migrator")


# ---------------------------------------------------------------------------
# migrate subcommand
# ---------------------------------------------------------------------------


@cli.command()
@common_options
@click.option(
    "--export_path",
    default=None,
    help="Path to Slack export directory (required unless --complete)",
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
    help="Complete import mode on this tool's spaces (named 'Slack #...') "
    "without migrating messages. Lists them and asks first.",
)
@click.option(
    "--skip_permission_check",
    is_flag=True,
    default=False,
    help="Skip permission checks (not recommended)",
)
@yes_option
def migrate(
    creds_path: str | None,
    export_path: str | None,
    workspace_admin: str | None,
    config: str,
    verbose: bool,
    debug_api: bool,
    dry_run: bool,
    resume: bool,
    complete: bool,
    skip_permission_check: bool,
    yes: bool,
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
        complete: Complete import mode on this tool's spaces without migrating.
        skip_permission_check: Skip permission checks before migration.
        yes: Answer yes to confirmation prompts.
    """
    if complete:
        try:
            exit_code = _run_complete_mode(
                creds_path, workspace_admin, config, verbose, debug_api, yes
            )
        except KeyboardInterrupt:
            click.echo("\nInterrupted.", err=True)
            exit_code = EXIT_INTERRUPTED
        if exit_code:
            sys.exit(exit_code)
        return
    if not export_path:
        raise click.MissingParameter(param_type="option", param_hint="'--export_path'")

    args = SimpleNamespace(
        creds_path=creds_path,
        export_path=export_path,
        workspace_admin=workspace_admin,
        config=config,
        config_required=config_was_given(),
        verbose=verbose,
        debug_api=debug_api,
        dry_run=dry_run,
        update_mode=resume,
        skip_permission_check=skip_permission_check,
        assume_yes=yes,
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

    exit_code = run_orchestrator(orchestrator, export_path)
    if exit_code:
        sys.exit(exit_code)


def run_orchestrator(orchestrator: MigrationOrchestrator, export_path: str) -> int:
    """Run prerequisites, the migration and cleanup, and return the exit code.

    Exit codes: 0 success with no errors; 1 failed, aborted, cancelled or
    completed with errors; 130 interrupted (Ctrl-C).  Click usage errors
    propagate (exit 2).

    Args:
        orchestrator: The configured orchestrator.
        export_path: The export path (for the resume hint after Ctrl-C).

    Returns:
        The process exit code.
    """
    exit_code = EXIT_OK
    try:
        with InterruptHandler(export_path=export_path):
            try:
                orchestrator.validate_prerequisites()
                exit_code = orchestrator.run_migration()
            except click.ClickException:
                raise
            except Exception as e:
                handle_exception(e)
                exit_code = EXIT_FAILURE
            finally:
                orchestrator.cleanup()
                show_security_warning()
    except KeyboardInterrupt:
        return EXIT_INTERRUPTED

    cleanup_result = orchestrator.cleanup_result
    if cleanup_result is not None and cleanup_result.failed:
        log_with_context(
            logging.ERROR,
            "Post-migration import completion failed for"
            f" {len(cleanup_result.failed)} space(s): "
            + ", ".join(sorted(cleanup_result.failed)),
        )
        exit_code = EXIT_FAILURE
    return exit_code


def _run_complete_mode(
    creds_path: str | None,
    workspace_admin: str | None,
    config: str,
    verbose: bool,
    debug_api: bool,
    assume_yes: bool = False,
) -> int:
    """Complete import mode on this tool's spaces without migrating messages.

    This is equivalent to the standalone ``cleanup`` command but accessible
    via ``migrate --complete``.  Only spaces named ``Slack #...`` are
    considered; they are listed and the user confirms first (``--yes``
    skips the prompt).

    Returns:
        The exit code: 0 if every listed space was completed (or none were
        found), 1 if anything failed or the user cancelled.
    """
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

    # Load (and validate) the config before any API call, outside the quiet
    # console so its warnings are visible.
    try:
        cfg = load_config(Path(config), required=config_was_given())
    except Exception as e:
        handle_exception(e)
        return EXIT_FAILURE

    is_tty = sys.stdout.isatty()
    if is_tty:
        try:
            from slack_chat_migrator.cli.renderers import get_console

            get_console().print("\n[bold]Finding spaces still in import mode...[/bold]")
        except Exception:
            is_tty = False

    with _quiet_console() if is_tty else contextlib.nullcontext():
        log_with_context(logging.INFO, "Finding spaces still in import mode...")
        try:
            chat = get_gcp_service(
                creds_path,
                workspace_admin,
                "chat",
                "v1",
                max_retries=cfg.max_retries,
                retry_delay=cfg.retry_delay,
            )
            result = cleanup_import_mode_spaces(
                ChatAdapter(chat), confirm=completion_confirmer(assume_yes)
            )
        except Exception as e:
            handle_exception(e)
            return EXIT_FAILURE

    return report_import_completion(result)


def completion_confirmer(
    assume_yes: bool,
) -> Callable[[list[dict[str, Any]]], bool]:
    """Build the callback that lists the spaces to complete and asks first.

    Args:
        assume_yes: True when ``--yes`` was given (no prompt).

    Returns:
        A callback for ``cleanup_import_mode_spaces(confirm=...)``.
    """

    def _confirm(spaces: list[dict[str, Any]]) -> bool:
        lines = [
            f"  {space.get('displayName', '?')}  ({space.get('name', '?')})"
            for space in spaces
        ]
        message = (
            f"{len(spaces)} space(s) are still in import mode:\n"
            + "\n".join(lines)
            + "\nCompleting import mode is irreversible: imported history can"
            " no longer be added afterwards."
        )
        # Show the list even when INFO is hidden on the console.
        click.echo(message)
        log_with_context(logging.DEBUG, message)
        return confirm_action(
            f"Complete import mode on these {len(spaces)} space(s)?",
            assume_yes=assume_yes,
        )

    return _confirm


def report_import_completion(result: ImportCompletionResult) -> int:
    """Print an accurate summary of an import-completion run.

    Args:
        result: The outcome from ``cleanup_import_mode_spaces``.

    Returns:
        The exit code: 0 when everything listed was completed, else 1.
    """
    if result.cancelled:
        click.echo("Import completion cancelled; no spaces were changed.")
        return EXIT_FAILURE

    lines: list[str] = []
    if not result.candidates and not result.errors:
        lines.append(
            "No spaces named 'Slack #...' are in import mode (only spaces the"
            " workspace admin can see are listed)."
        )
    if result.completed:
        lines.append(f"Completed import mode on {len(result.completed)} space(s).")
    if result.failed:
        lines.append(f"Failed to complete {len(result.failed)} space(s):")
        lines.extend(f"  {name}: {error}" for name, error in result.failed.items())
    if result.errors:
        lines.append(f"{len(result.errors)} error(s) while finding spaces:")
        lines.extend(f"  {error}" for error in result.errors)

    for line in lines:
        click.echo(line)
    level = logging.INFO if result.ok else logging.ERROR
    log_with_context(level, " ".join(line.strip() for line in lines))
    return EXIT_OK if result.ok else EXIT_FAILURE


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
        self.config: MigrationConfig | None = None
        # Set only when a live migrate() returned normally; gates the
        # irreversible post-migration import completion in cleanup().
        self.live_migration_finished = False
        self.cleanup_result: ImportCompletionResult | None = None

    def load_config(self) -> MigrationConfig:
        """Load and validate the config file once for this run.

        A ``--config`` path given explicitly must exist; the default
        ``config.yaml`` may be missing (defaults are used, with a warning).

        Returns:
            The validated MigrationConfig.

        Raises:
            ConfigError: If the config is required but missing, or invalid.
        """
        if self.config is None:
            self.config = load_config(
                Path(self.args.config),
                required=bool(getattr(self.args, "config_required", False)),
            )
        return self.config

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
            config=self.load_config(),
        )

        # Set output directory if we have one
        if self.output_dir:
            migrator.state.context.output_dir = self.output_dir

        return migrator

    def _check_credential_args(self) -> None:
        """Check the credential options needed for this run.

        Raises:
            click.UsageError: If a live run lacks ``--creds_path`` or
                ``--workspace_admin``.
            ConfigError: If a given credentials file doesn't exist.
        """
        # In dry-run mode, credentials and workspace_admin are optional
        if not self.args.dry_run:
            if not self.args.creds_path:
                raise click.UsageError("--creds_path is required for live migration")
            if not self.args.workspace_admin:
                raise click.UsageError(
                    "--workspace_admin is required for live migration"
                )
        # Check the credentials file exists (dry runs too, when one is given)
        if self.args.creds_path and not Path(self.args.creds_path).exists():
            raise ConfigError(
                f"Credentials file not found: {self.args.creds_path}. "
                "Make sure your service account JSON key file exists and has the correct path."
            )

    def _load_and_report_config(self) -> None:
        """Load the config (warnings visible) and show its status."""
        self.load_config()
        config_path = Path(self.args.config)
        if config_path.exists():
            _print_preflight_status("Config loaded", detail=str(config_path))
        else:
            _print_preflight_status(
                "Config not found, using defaults",
                status="warn",
                detail=str(config_path),
            )

    def _check_can_confirm(self) -> None:
        """Fail fast when a live run would need a prompt nobody can answer.

        Raises:
            ConfirmationRequiredError: For a live migration without
                ``--yes`` when stdin isn't an interactive terminal.
        """
        if (
            not self.args.dry_run
            and not getattr(self.args, "assume_yes", False)
            and not stdin_is_interactive()
        ):
            raise ConfirmationRequiredError(
                "A live migration asks for confirmation before it starts, but"
                " stdin is not an interactive terminal. Re-run with --yes (or"
                f" set {YES_ENVVAR}=1) to run unattended."
            )

    def validate_prerequisites(self) -> None:
        """Validate all prerequisites before migration."""
        self._check_credential_args()
        self._check_can_confirm()

        is_tty = sys.stdout.isatty()
        if is_tty:
            from slack_chat_migrator.cli.renderers import get_console

            console = get_console()
            console.print("\n[bold]Preflight Checks[/bold]")

        # Load and validate the config before anything else
        self._load_and_report_config()

        # Initialize main migrator (INFO noise hidden; warnings still shown)
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
                self._run_permission_checks()
            else:
                log_with_context(
                    logging.INFO,
                    "Permission checks skipped: no credentials (pass --creds_path"
                    " and --workspace_admin to check them).",
                )
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
            self._initialize_drive()

    def _run_permission_checks(self) -> None:
        """Test the permissions the migration needs against the real APIs.

        Data validation in a dry run (``validate``, ``migrate --dry_run``)
        uses no-op services, but the permission checks always build real
        Chat and Drive clients from ``--creds_path``, so an invalid key,
        missing scopes or broken delegation fail here.

        Raises:
            PermissionCheckError: If any check fails (including an unusable
                credentials file).
        """
        log_with_context(
            logging.INFO, "Checking permissions against the Google APIs..."
        )
        try:
            with _quiet_console():
                if self.args.dry_run:
                    cfg = self.load_config()
                    check_permissions_standalone(
                        creds_path=self.args.creds_path,
                        workspace_admin=self.args.workspace_admin,
                        max_retries=cfg.max_retries,
                        retry_delay=cfg.retry_delay,
                        scopes=scopes_for(cfg),
                    )
                else:
                    validate_permissions(self.migrator)
        except Exception as e:
            raise PermissionCheckError(
                f"Permission checks failed: {e}. "
                "Fix the issues or run with --skip_permission_check if you're sure."
            ) from e
        log_with_context(logging.INFO, "Permission checks passed!")
        _print_preflight_status("Permissions verified")

        # Now that permissions are validated, initialize drive structures
        # (live runs only; a dry run's migrator has no real services).
        if not self.args.dry_run:
            self._initialize_drive()

    def _initialize_drive(self) -> None:
        """Initialize the shared drive and folders, if the file handler exists."""
        file_handler = getattr(self.migrator, "file_handler", None)
        if file_handler:
            with _quiet_console():
                file_handler.ensure_drive_initialized()
            log_with_context(logging.INFO, "Drive structures initialized successfully")
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
        if confirm_action(
            "Proceed anyway despite unmapped users? (NOT RECOMMENDED)",
            assume_yes=bool(getattr(self.args, "assume_yes", False)),
        ):
            log_with_context(
                logging.WARNING,
                "Proceeding with unmapped users - messages will be attributed to workspace admin",
            )
            return True
        log_with_context(
            logging.WARNING,
            "Migration cancelled. Please fix the user mappings and try again.",
        )
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
            validation_clean = self.dry_run_migrator.migrate()
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

        if not validation_clean:
            self._report_run_errors(m, "Validation")

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

        return confirm_action(
            "Proceed with the actual migration?",
            assume_yes=bool(getattr(self.args, "assume_yes", False)),
        )

    @staticmethod
    def _report_run_errors(m: SlackToChatMigrator, label: str) -> None:
        """Log which channels had errors or were left in import mode."""
        errors = m.state.errors
        if errors.channels_with_errors:
            log_with_context(
                logging.ERROR,
                f"{label} completed with errors in"
                f" {len(errors.channels_with_errors)} channel(s): "
                + ", ".join(errors.channels_with_errors),
            )
        if errors.incomplete_import_spaces:
            log_with_context(
                logging.ERROR,
                f"{len(errors.incomplete_import_spaces)} space(s) were left in"
                " import mode: "
                + ", ".join(
                    f"#{channel} ({space})"
                    for space, channel in errors.incomplete_import_spaces
                ),
            )

    def _run_with_progress(self, m: SlackToChatMigrator) -> bool:
        """Run ``m.migrate()`` with a ProgressTracker and renderer.

        On failure the report is still generated so partial results are
        available for inspection.

        Returns:
            ``m.migrate()``'s result: True if no channel had errors.
        """
        from slack_chat_migrator.cli.renderers import create_renderer

        total_channels = len(m.channels_meta) if m.channels_meta else 0
        tracker = ProgressTracker()
        renderer = create_renderer(
            tracker, total_channels=total_channels, dry_run=m.dry_run
        )
        renderer.start()
        try:
            succeeded = m.migrate(progress_tracker=tracker)
        except BaseException as e:
            renderer.stop()
            self._generate_partial_report(m, e)
            raise
        renderer.stop()
        return bool(succeeded)

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
        """Show a confirmation prompt before starting a dry-run validation.

        A dry run changes nothing, so there's no prompt when nobody can
        answer it (no terminal) or ``--yes`` was given.
        """
        if self.migrator is None:
            return True

        mode = "dry-run validation" if self.args.dry_run else "migration"
        channel_count = (
            len(self.migrator.channels_meta) if self.migrator.channels_meta else 0
        )
        msg = f"Ready to begin {mode} of {channel_count} channels."

        interactive = sys.stdout.isatty() and stdin_is_interactive()
        if not interactive or getattr(self.args, "assume_yes", False):
            log_with_context(logging.INFO, msg)
            return True

        from slack_chat_migrator.cli.renderers import get_console

        get_console().print(f"\n{msg}")
        if not confirm_action("Continue?", assume_yes=False, default=True):
            click.echo("Aborted.")
            return False
        return True

    def run_migration(self) -> int:
        """Execute the main migration logic.

        Returns:
            The exit code: 0 when the run succeeded with no errors; 1 when it
            was cancelled, completed with errors, or (validation) found
            unmapped users.
        """
        if self.args.dry_run:
            # Explicit dry run mode
            if self.migrator is None:
                raise RuntimeError("Migrator not initialized")

            if not self._confirm_start():
                log_with_context(logging.WARNING, "Validation cancelled.")
                return EXIT_FAILURE

            m = self.migrator
            succeeded = self._run_with_progress(m)
            self._print_summary(m)

            exit_code = EXIT_OK
            if not succeeded:
                self._report_run_errors(m, "Validation")
                exit_code = EXIT_FAILURE
            if self.check_unmapped_users(self.migrator):
                self.report_validation_issues(self.migrator, is_explicit_dry_run=True)
                exit_code = EXIT_FAILURE
            elif succeeded:
                self.report_validation_success(is_explicit_dry_run=True)
            return exit_code

        # Full migration with automatic validation
        if not self.run_validation():
            return EXIT_FAILURE
        self.report_validation_success()

        if not self.get_user_confirmation():
            log_with_context(logging.WARNING, "Migration cancelled by user.")
            return EXIT_FAILURE

        if self.migrator is None:
            raise RuntimeError("Migrator not initialized")
        m = self.migrator
        succeeded = self._run_with_progress(m)
        self.live_migration_finished = True
        self._print_summary(m)
        if not succeeded:
            self._report_run_errors(m, "Migration")
            return EXIT_FAILURE
        return EXIT_OK

    def cleanup(self) -> None:
        """Perform cleanup operations.

        Channel log handlers are always closed.  The irreversible
        post-migration import completion runs only after a live migration
        actually finished: never on cancel, Ctrl-C, an exception, or failed
        validation or permission checks.
        """
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

                    # Only complete imports after a finished live migration
                    if self.live_migration_finished and not self.args.dry_run:
                        try:
                            self.cleanup_result = run_cleanup(
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
                    elif not self.args.dry_run and m.state.spaces.created_spaces:
                        log_with_context(
                            logging.WARNING,
                            "The migration did not finish, so import mode was not"
                            f" completed on the {len(m.state.spaces.created_spaces)}"
                            " space(s) this run worked on. Finish them with"
                            " 'migrate --resume' or 'migrate --complete' within"
                            " 90 days.",
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
    """Suppress INFO/DEBUG noise on the console (it still goes to the log file).

    Warnings and errors stay visible, so problems such as an ignored config
    option are never hidden.
    """
    restored: list[tuple[logging.Handler, int]] = []
    for lgr in (logging.getLogger(), logging.getLogger("slack_chat_migrator")):
        for handler in lgr.handlers:
            if (
                isinstance(handler, logging.StreamHandler)
                and not isinstance(handler, logging.FileHandler)
                and handler.level < logging.WARNING
            ):
                restored.append((handler, handler.level))
                handler.setLevel(logging.WARNING)
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
    config_label = str(config_path)
    if not config_path.exists():
        config_label += " [yellow](not found, using defaults)[/yellow]"
    table.add_row("Config", config_label)
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

    The run directory (``migration_logs/run_<timestamp>``) and its
    ``channel_logs`` folder are private to the current user (mode ``0700``).

    Returns:
        The path to the newly created output directory.
    """
    return create_output_directory("migration_logs")
