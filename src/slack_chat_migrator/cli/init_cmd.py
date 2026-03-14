"""CLI command handler for interactive config.yaml generation."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click
import yaml

from slack_chat_migrator.cli.common import cli
from slack_chat_migrator.cli.renderers import (
    error_panel,
    get_console,
    next_step_panel,
    success_panel,
    warning_panel,
)
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
    console = get_console()
    output_path = Path(output)

    if not output_path.parent.exists():
        console.print(
            error_panel(
                "Invalid path", f"Output directory does not exist: {output_path.parent}"
            )
        )
        sys.exit(1)
    if output_path.exists():
        if not click.confirm(
            f"{output_path} already exists. Overwrite?", default=False
        ):
            click.echo("Aborted.")
            sys.exit(0)

    export = Path(export_path)
    if not export.is_dir():
        console.print(
            error_panel(
                "Invalid export path",
                f"Export path does not exist or is not a directory: {export}",
            )
        )
        sys.exit(1)

    inspector = ExportInspector(export)

    # Validate export structure
    issues = inspector.get_structure_issues()
    if issues:
        issue_list = "\n".join(f"  - {issue}" for issue in issues)
        console.print(warning_panel("Export structure issues", issue_list))
        if not click.confirm("Continue anyway?", default=False):
            sys.exit(1)

    # Show export summary
    _print_export_summary(inspector)

    # Build config interactively
    config = _build_config(inspector)

    # Write config file
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

    console.print()
    console.print(
        success_panel("Config created", f"Written to [bold]{output_path}[/bold]")
    )

    if click.confirm("Run validation now?", default=True):
        from slack_chat_migrator.cli.validate_cmd import validate

        console.print()
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
        console.print(
            next_step_panel(f"slack-chat-migrator validate --export_path {quoted}")
        )


def _print_export_summary(inspector: ExportInspector) -> None:
    """Print a Rich table summarizing the export contents."""
    from rich.table import Table

    console = get_console()
    table = Table(show_header=False, expand=True, box=None, padding=(0, 2))
    table.add_column("Metric", style="cyan", min_width=18)
    table.add_column("Value", justify="right", style="green", min_width=10)

    table.add_row("Channels", str(inspector.get_channel_count()))
    table.add_row("Users", str(inspector.get_user_count()))
    table.add_row("Messages", f"{inspector.get_total_message_count():,}")
    table.add_row("Files", f"{inspector.get_total_file_count():,}")

    date_range = inspector.get_export_date_range()
    if date_range:
        table.add_row("Date range", f"{date_range[0]} to {date_range[1]}")

    bots = inspector.get_bot_users()
    if bots:
        table.add_row("Bot users", str(len(bots)))

    no_email = inspector.get_users_without_email()
    if no_email:
        table.add_row(
            "[yellow]Users without email[/yellow]", f"[yellow]{len(no_email)}[/yellow]"
        )

    from rich.panel import Panel

    console.print()
    console.print(Panel(table, title="Export Summary", border_style="blue"))


def _build_config(inspector: ExportInspector) -> dict[str, Any]:
    """Interactively build the config dictionary."""
    config: dict[str, Any] = {}

    # --- Channel selection ---
    config.update(_ask_channel_selection(inspector))

    # --- Bot handling ---
    bots = inspector.get_bot_users()
    if bots:
        bot_names = [b.get("name", b.get("id", "?")) for b in bots]
        console = get_console()
        console.print(
            f"\nFound [bold]{len(bots)}[/bold] bot users: {', '.join(bot_names)}"
        )
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
    from rich.columns import Columns
    from rich.text import Text

    result: dict[str, Any] = {}
    channel_dirs = inspector.get_channel_dirs()
    channel_names = [d.name for d in channel_dirs]
    valid_names = set(channel_names)

    console = get_console()
    items = [Text(f"#{name}", style="cyan") for name in channel_names]
    console.print(f"\nChannels found ([bold]{len(channel_names)}[/bold]):")
    console.print(Columns(items, padding=(0, 3)))
    console.print()

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
            parsed = _parse_channel_names(selected)
            _warn_unrecognized_channels(parsed, valid_names, console)
            result["include_channels"] = parsed
    elif mode == "exclude":
        excluded = click.prompt(
            "Channels to exclude (comma-separated)",
            default="",
        )
        if excluded.strip():
            parsed = _parse_channel_names(excluded)
            _warn_unrecognized_channels(parsed, valid_names, console)
            result["exclude_channels"] = parsed

    return result


def _parse_channel_names(raw: str) -> list[str]:
    """Parse comma-separated channel names, stripping '#' prefixes and whitespace."""
    return [c.strip().lstrip("#") for c in raw.split(",") if c.strip()]


def _warn_unrecognized_channels(
    parsed: list[str], valid_names: set[str], console: Any
) -> None:
    """Warn about channel names that don't match any export directory."""
    unrecognized = [name for name in parsed if name not in valid_names]
    if unrecognized:
        console.print(
            warning_panel(
                "Unrecognized channels",
                "The following channels were not found in the export:\n"
                + "\n".join(f"  - {name}" for name in unrecognized),
            )
        )


def _ask_user_mapping(inspector: ExportInspector) -> dict[str, Any]:
    """Ask about user mapping overrides."""
    from rich.table import Table

    result: dict[str, Any] = {}
    no_email = inspector.get_users_without_email()

    if not no_email:
        return result

    console = get_console()
    table = Table(title=f"{len(no_email)} Users Need Manual Mapping", expand=True)
    table.add_column("User ID", style="dim")
    table.add_column("Name", style="bold")
    table.add_column("Real Name")

    for u in no_email:
        table.add_row(
            u.get("id", "?"),
            u.get("name", "?"),
            u.get("real_name", ""),
        )

    console.print()
    console.print(table)
    console.print("[dim]You can add mappings now or edit config.yaml later.[/dim]")

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
