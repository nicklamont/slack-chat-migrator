"""CLI command handler for dry-run validation."""

from __future__ import annotations

import logging
import sys
from types import SimpleNamespace

import click

from slack_migrator.cli.common import (
    cli,
    common_options,
    handle_exception,
    show_security_warning,
)
from slack_migrator.cli.migrate_cmd import (
    MigrationOrchestrator,
    create_migration_output_directory,
    log_startup_info,
)
from slack_migrator.utils.logging import log_with_context, setup_logger

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
