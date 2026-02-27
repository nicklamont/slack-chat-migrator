"""
Main execution module for the Slack to Google Chat migration tool.

This module is a thin facade that imports and re-exports the CLI group,
all subcommands, and shared helpers so that existing import paths
(``from slack_migrator.cli.commands import cli``) continue to work.
"""

from __future__ import annotations

# --- subcommand registrations (importing these modules registers
#     each @cli.command() on the ``cli`` group) ---
from slack_migrator.cli.cleanup_cmd import cleanup  # noqa: F401

# --- shared infrastructure (group, decorators, error handlers) ---
from slack_migrator.cli.common import (  # noqa: F401
    DefaultGroup,
    cli,
    common_options,
    handle_exception,
    handle_http_error,
    show_security_warning,
)
from slack_migrator.cli.migrate_cmd import (  # noqa: F401
    MigrationOrchestrator,
    create_migration_output_directory,
    log_startup_info,
    migrate,
)
from slack_migrator.cli.permissions_cmd import check_permissions  # noqa: F401
from slack_migrator.cli.validate_cmd import validate  # noqa: F401

# --- re-export names that tests patch via ``slack_migrator.cli.commands.X`` ---
from slack_migrator.core.config import load_config  # noqa: F401
from slack_migrator.services.space_creator import (
    cleanup_import_mode_spaces,  # noqa: F401
)
from slack_migrator.utils.api import get_gcp_service  # noqa: F401
from slack_migrator.utils.logging import log_with_context, setup_logger  # noqa: F401
from slack_migrator.utils.permissions import check_permissions_standalone  # noqa: F401

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point for the slack-migrator command."""
    cli()


if __name__ == "__main__":
    main()
