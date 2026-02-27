"""CLI command handler for import mode cleanup."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from slack_migrator.cli.common import cli, common_options, handle_exception
from slack_migrator.core.config import load_config
from slack_migrator.services.space_creator import cleanup_import_mode_spaces
from slack_migrator.utils.api import get_gcp_service
from slack_migrator.utils.logging import setup_logger

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
