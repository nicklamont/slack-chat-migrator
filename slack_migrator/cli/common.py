"""Shared CLI infrastructure: option decorators, error handlers, and the CLI group."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, ClassVar

import click

if TYPE_CHECKING:
    from googleapiclient.errors import HttpError

import slack_migrator
from slack_migrator.constants import (
    HTTP_FORBIDDEN,
    HTTP_RATE_LIMIT,
    HTTP_SERVER_ERROR_MIN,
    PERMISSION_DENIED_ERROR,
)
from slack_migrator.exceptions import MigratorError
from slack_migrator.utils.logging import log_with_context

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
# Error handlers
# ---------------------------------------------------------------------------


def handle_http_error(e: HttpError) -> None:
    """Handle HTTP errors with specific messages.

    Args:
        e: The Google API HTTP error to handle.
    """

    if e.resp.status == HTTP_FORBIDDEN and PERMISSION_DENIED_ERROR in str(e):
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
    elif e.resp.status == HTTP_RATE_LIMIT:
        log_with_context(logging.ERROR, f"Rate limit exceeded: {e}")
        log_with_context(
            logging.INFO,
            "The migration hit API rate limits. Consider using --update_mode to resume.",
        )
    elif e.resp.status >= HTTP_SERVER_ERROR_MIN:
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
