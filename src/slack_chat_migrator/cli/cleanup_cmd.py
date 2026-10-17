"""CLI command handler for import mode cleanup."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from slack_chat_migrator.cli.common import (
    EXIT_FAILURE,
    EXIT_INTERRUPTED,
    PromptCancelled,
    cli,
    common_options,
    config_was_given,
    deprecated_command,
    handle_exception,
    report_prompt_cancel,
    yes_option,
)
from slack_chat_migrator.cli.migrate_cmd import (
    completion_confirmer,
    report_import_completion,
)
from slack_chat_migrator.core.config import load_config
from slack_chat_migrator.services.chat_adapter import ChatAdapter
from slack_chat_migrator.services.spaces.space_creator import cleanup_import_mode_spaces
from slack_chat_migrator.utils.api import get_gcp_service
from slack_chat_migrator.utils.logging import setup_logger

# ---------------------------------------------------------------------------
# cleanup subcommand
# ---------------------------------------------------------------------------


@cli.command()
@common_options
@yes_option
@deprecated_command("cleanup", "Use 'migrate --complete' instead.")
def cleanup(
    creds_path: str | None,
    workspace_admin: str | None,
    config: str,
    verbose: bool,
    debug_api: bool,
    yes: bool,
) -> None:
    """(Deprecated: use 'migrate --complete') Complete import mode on stuck spaces.

    Lists this tool's spaces (named "Slack #...") that are still in import
    mode, asks for confirmation (skip with --yes), and calls completeImport()
    on each.  Does not add members — use ``migrate --resume`` for that.
    Exits 1 if the config, the credentials or the list of spaces can't be
    loaded, if the user declines at the prompt, if a confirmation is
    needed without a terminal (use --yes), or if any space could not be
    completed.  Exits 130 on Ctrl-C (also at the prompt) or end of input
    at the prompt.

    Args:
        creds_path: Path to service account credentials JSON.
        workspace_admin: Email of workspace admin to impersonate.
        config: Path to config YAML.
        verbose: Enable verbose console logging.
        debug_api: Enable detailed API request/response logging.
        yes: Skip confirmation prompt.
    """
    setup_logger(verbose, debug_api)

    if not creds_path:
        raise click.UsageError("--creds_path is required for cleanup")
    if not workspace_admin:
        raise click.UsageError("--workspace_admin is required for cleanup")

    try:
        cfg = load_config(Path(config), required=config_was_given())
        chat = get_gcp_service(
            creds_path,
            workspace_admin,
            "chat",
            "v1",
            max_retries=cfg.max_retries,
            retry_delay=cfg.retry_delay,
        )
        result = cleanup_import_mode_spaces(
            ChatAdapter(chat), confirm=completion_confirmer(yes)
        )
    except PromptCancelled:
        report_prompt_cancel("No spaces were changed.")
        raise click.exceptions.Exit(EXIT_INTERRUPTED) from None
    except Exception as e:  # other Ctrl-C is left to the CLI group (exit 130)
        handle_exception(e)
        sys.exit(EXIT_FAILURE)

    exit_code = report_import_completion(result)
    if exit_code:
        sys.exit(exit_code)
