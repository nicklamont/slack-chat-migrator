"""CLI command handler for interactive config.yaml generation."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click
import yaml

from slack_chat_migrator.cli.common import cli
from slack_chat_migrator.services.export_inspector import ExportInspector

# ---------------------------------------------------------------------------
# init subcommand
# ---------------------------------------------------------------------------


@cli.command("init")
@click.option(
    "--export_path",
    required=True,
    help="Path to Slack export directory",
)
@click.option(
    "--output",
    default="config.yaml",
    show_default=True,
    help="Output path for generated config file",
)
@click.pass_context
def init(ctx: click.Context, export_path: str, output: str) -> None:
    """Generate a config.yaml from a Slack export directory.

    Analyzes the export structure, shows a summary, and interactively
    builds a configuration file with channel selection, user mapping
    hints, and error handling preferences.

    Args:
        export_path: Path to Slack export directory.
        output: Output path for generated config file.
    """
    output_path = Path(output)
    if not output_path.parent.exists():
        click.echo(f"Error: Output directory does not exist: {output_path.parent}")
        sys.exit(1)
    if output_path.exists():
        if not click.confirm(
            f"{output_path} already exists. Overwrite?", default=False
        ):
            click.echo("Aborted.")
            sys.exit(0)

    export = Path(export_path)
    if not export.is_dir():
        click.echo(f"Error: Export path does not exist or is not a directory: {export}")
        sys.exit(1)

    inspector = ExportInspector(export)

    # Validate export structure
    issues = inspector.get_structure_issues()
    if issues:
        click.echo("Export structure issues found:")
        for issue in issues:
            click.echo(f"  - {issue}")
        if not click.confirm("Continue anyway?", default=False):
            sys.exit(1)

    # Show export summary
    _print_export_summary(inspector)

    # Build config interactively
    config = _build_config(inspector)

    # Write config file
    with open(output_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

    click.echo(f"\nConfig written to {output_path}")

    if click.confirm("Run validation now?", default=True):
        from slack_chat_migrator.cli.validate_cmd import validate

        click.echo("")
        ctx.invoke(
            validate,
            export_path=export_path,
            creds_path=None,
            workspace_admin=None,
            config=str(output_path),
            verbose=False,
            debug_api=False,
            dry_run=False,
        )
    else:
        quoted = f'"{export_path}"' if " " in export_path else export_path
        click.echo(
            "Next step: run 'slack-chat-migrator validate --export_path "
            f"{quoted}' to verify"
        )


def _print_export_summary(inspector: ExportInspector) -> None:
    """Print a summary of the export contents."""
    click.echo("")
    click.echo("Export summary:")
    click.echo(f"  Channels: {inspector.get_channel_count()}")
    click.echo(f"  Users: {inspector.get_user_count()}")
    click.echo(f"  Messages: {inspector.get_total_message_count()}")
    click.echo(f"  Files: {inspector.get_total_file_count()}")

    date_range = inspector.get_export_date_range()
    if date_range:
        click.echo(f"  Date range: {date_range[0]} to {date_range[1]}")

    bots = inspector.get_bot_users()
    if bots:
        click.echo(f"  Bot users: {len(bots)}")

    no_email = inspector.get_users_without_email()
    if no_email:
        click.echo(f"  Users without email: {len(no_email)}")

    click.echo("")


def _build_config(inspector: ExportInspector) -> dict[str, Any]:
    """Interactively build the config dictionary."""
    config: dict[str, Any] = {}

    # --- Channel selection ---
    config.update(_ask_channel_selection(inspector))

    # --- Bot handling ---
    bots = inspector.get_bot_users()
    if bots:
        bot_names = [b.get("name", b.get("id", "?")) for b in bots]
        click.echo(f"Found {len(bots)} bot users: {', '.join(bot_names)}")
        if click.confirm("Ignore bot users during migration?", default=True):
            config["ignore_bots"] = True

    # --- User mapping hints ---
    config.update(_ask_user_mapping(inspector))

    # --- Error handling ---
    config.update(_ask_error_handling())

    # --- Shared drive ---
    drive_name = click.prompt(
        "Shared Drive name for file attachments",
        default="Imported Slack Attachments",
    )
    config["shared_drive"] = {"name": drive_name}

    return config


def _ask_channel_selection(inspector: ExportInspector) -> dict[str, Any]:
    """Ask the user about channel inclusion/exclusion."""
    result: dict[str, Any] = {}
    channel_dirs = inspector.get_channel_dirs()
    channel_names = [d.name for d in channel_dirs]

    click.echo(f"Channels found ({len(channel_names)}):")
    for name in channel_names:
        click.echo(f"  - {name}")
    click.echo("")

    mode = click.prompt(
        "Channel selection mode",
        type=click.Choice(["all", "include", "exclude"]),
        default="all",
    )

    if mode == "include":
        selected = click.prompt(
            "Channels to include (comma-separated)",
            default="",
        )
        if selected.strip():
            result["include_channels"] = [
                c.strip() for c in selected.split(",") if c.strip()
            ]
    elif mode == "exclude":
        excluded = click.prompt(
            "Channels to exclude (comma-separated)",
            default="",
        )
        if excluded.strip():
            result["exclude_channels"] = [
                c.strip() for c in excluded.split(",") if c.strip()
            ]

    return result


def _ask_user_mapping(inspector: ExportInspector) -> dict[str, Any]:
    """Ask about user mapping overrides."""
    result: dict[str, Any] = {}
    no_email = inspector.get_users_without_email()

    if not no_email:
        return result

    click.echo(f"\n{len(no_email)} users lack email addresses and need manual mapping:")
    for u in no_email:
        uid = u.get("id", "?")
        name = u.get("name", "?")
        real_name = u.get("real_name", "")
        suffix = f" ({real_name})" if real_name else ""
        click.echo(f"  {uid}: {name}{suffix}")

    click.echo("")
    click.echo("You can add mappings now or edit config.yaml later.")

    if click.confirm("Add user mappings now?", default=False):
        overrides: dict[str, str] = {}
        for u in no_email:
            uid = u.get("id", "?")
            name = u.get("name", "?")
            email = click.prompt(
                f"  Google email for {name} ({uid})",
                default="",
            )
            if email.strip():
                overrides[uid] = email.strip()
        if overrides:
            result["user_mapping_overrides"] = overrides

    return result


def _ask_error_handling() -> dict[str, Any]:
    """Ask about error handling preferences."""
    result: dict[str, Any] = {}

    click.echo("")
    if click.confirm("Abort migration on first error?", default=False):
        result["abort_on_error"] = True
    else:
        pct = click.prompt(
            "Max failure percentage before aborting a channel",
            type=int,
            default=10,
        )
        result["max_failure_percentage"] = pct

    strategy = click.prompt(
        "Import completion strategy on error",
        type=click.Choice(["skip_on_error", "force_complete"]),
        default="skip_on_error",
    )
    result["import_completion_strategy"] = strategy

    return result
