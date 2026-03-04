"""CLI command handler for permission checking."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from slack_chat_migrator.cli.common import cli, common_options, handle_exception
from slack_chat_migrator.core.config import load_config
from slack_chat_migrator.utils.logging import setup_logger
from slack_chat_migrator.utils.permissions import check_permissions_standalone

# ---------------------------------------------------------------------------
# check-permissions subcommand
# ---------------------------------------------------------------------------


@cli.command("check-permissions")
@common_options
def check_permissions(
    creds_path: str | None,
    workspace_admin: str | None,
    config: str,
    verbose: bool,
    debug_api: bool,
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

    if not creds_path:
        raise click.UsageError("--creds_path is required for check-permissions")
    if not workspace_admin:
        raise click.UsageError("--workspace_admin is required for check-permissions")

    try:
        cfg = load_config(Path(config))
        check_permissions_standalone(
            creds_path=creds_path,
            workspace_admin=workspace_admin,
            max_retries=cfg.max_retries,
            retry_delay=cfg.retry_delay,
        )
    except Exception as e:
        handle_exception(e)
        sys.exit(1)
