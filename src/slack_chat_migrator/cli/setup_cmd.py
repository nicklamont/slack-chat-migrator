"""CLI command handler for interactive GCP setup wizard."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from slack_chat_migrator.cli.common import cli
from slack_chat_migrator.cli.renderers import (
    error_panel,
    get_console,
    next_step_panel,
    success_panel,
    warning_panel,
)


def _check_setup_deps() -> bool:
    """Check if optional setup dependencies are installed."""
    try:
        __import__("google.auth")
        __import__("google.cloud.iam_admin_v1")
        __import__("google.cloud.resourcemanager_v3")
        __import__("google.cloud.servicemanagement_v1")
        return True
    except ImportError:
        return False


@cli.command("setup")
def setup() -> None:
    """Interactive GCP setup wizard for migration prerequisites.

    Walks through project selection/creation, API enablement,
    service account setup, and delegation verification.
    Requires: pip install slack-chat-migrator[setup]
    """
    console = get_console()

    if not _check_setup_deps():
        console.print(
            error_panel(
                "Missing dependencies",
                "Install setup extras with:\n"
                "  [bold]pip install slack-chat-migrator\\[setup][/bold]",
            )
        )
        sys.exit(1)

    from rich.panel import Panel

    from slack_chat_migrator.services.setup.setup_service import (
        StepStatus,
        get_credentials,
        load_state,
        save_state,
    )

    # Load persistent state
    state = load_state()

    console.print()
    console.print(
        Panel(
            "[bold]GCP Setup Wizard[/bold]",
            border_style="blue",
            subtitle="5 steps to configure Google Chat migration",
        )
    )

    if any(s == StepStatus.COMPLETE.value for s in state.steps.values()):
        console.print("[dim]Resuming from previous session.[/dim]")
        console.print()

    try:
        credentials = get_credentials()
    except RuntimeError as e:
        console.print(error_panel("Authentication failed", str(e)))
        sys.exit(1)

    # Step 1: Project selection
    if state.step_status("project") != StepStatus.COMPLETE:
        state = _step_project(credentials, state)
        save_state(state)
    else:
        console.print(
            f"[green]\u2713[/green] [1/5] Project: [bold]{state.project_id}[/bold]"
        )

    if not state.project_id:
        console.print(error_panel("No project", "No project selected. Aborting."))
        sys.exit(1)

    # Step 2: Enable APIs
    if state.step_status("apis") != StepStatus.COMPLETE:
        state = _step_apis(credentials, state)
        save_state(state)
    else:
        console.print("[green]\u2713[/green] [2/5] APIs: enabled")

    # Step 3: Service account
    if state.step_status("service_account") != StepStatus.COMPLETE:
        state = _step_service_account(credentials, state)
        save_state(state)
    else:
        console.print(
            f"[green]\u2713[/green] [3/5] Service account: "
            f"[bold]{state.service_account_email}[/bold]"
        )

    # Step 4: Download key
    if state.step_status("key") != StepStatus.COMPLETE:
        state = _step_download_key(credentials, state)
        save_state(state)
    else:
        console.print(f"[green]\u2713[/green] [4/5] Key: [bold]{state.key_path}[/bold]")

    # Step 5: Test delegation
    if state.step_status("delegation") != StepStatus.COMPLETE:
        state = _step_delegation(state)
        save_state(state)
    else:
        console.print("[green]\u2713[/green] [5/5] Delegation: verified")

    # Completion summary
    console.print()
    lines = ["[bold]Setup complete![/bold]"]
    if state.key_path:
        lines.append(f"\nCredentials file: [bold]{state.key_path}[/bold]")
    console.print(success_panel("GCP Setup", "\n".join(lines)))
    console.print(next_step_panel("slack-chat-migrator init --export_path <dir>"))


def _step_project(credentials, state):  # type: ignore[no-untyped-def]
    """Step 1: Select or create a GCP project."""
    from slack_chat_migrator.services.setup.gcp_project import (
        create_project,
        list_projects,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][1/5] GCP Project[/bold]")

    if click.confirm("Use an existing project?", default=True):
        projects = list_projects(credentials)
        if not projects:
            console.print("[yellow]No accessible projects found.[/yellow]")
        else:
            from rich.table import Table

            table = Table(title="Available Projects")
            table.add_column("#", style="dim", width=4)
            table.add_column("Project ID", style="bold")
            table.add_column("Display Name")
            for i, p in enumerate(projects[:20], 1):
                table.add_row(str(i), p["project_id"], p["display_name"])
            console.print(table)

        project_id = click.prompt("Enter project ID")
        state.project_id = project_id
    else:
        project_id = click.prompt("New project ID")
        display_name = click.prompt("Display name", default=project_id)
        try:
            state.project_id = create_project(credentials, project_id, display_name)
            console.print(f"[green]Created project: {state.project_id}[/green]")
        except Exception as e:
            console.print(f"[red]Failed to create project: {e}[/red]")
            if click.confirm("Enter an existing project ID instead?", default=True):
                state.project_id = click.prompt("Project ID")
            else:
                return state

    state.mark_step("project", StepStatus.COMPLETE)
    return state


def _step_apis(credentials, state):  # type: ignore[no-untyped-def]
    """Step 2: Enable required APIs."""
    from slack_chat_migrator.services.setup.api_enablement import (
        REQUIRED_APIS,
        enable_required_apis,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][2/5] Enable APIs[/bold]")
    console.print(f"Required: {', '.join(REQUIRED_APIS)}")

    if click.confirm("Enable required APIs?", default=True):
        try:

            def progress(api: str, status: str) -> None:
                mark = (
                    "[green]\u2713[/green]"
                    if status == "enabled"
                    else "[dim]\u2022[/dim]"
                )
                console.print(f"  {mark} {api}: {status}")

            newly = enable_required_apis(
                credentials, state.project_id, on_progress=progress
            )
            state.apis_enabled = newly
            console.print(f"  [green]{len(newly)} API(s) newly enabled.[/green]")
        except Exception as e:
            console.print(f"[red]Failed to enable APIs: {e}[/red]")
            console.print("[dim]You can enable them manually in the GCP Console.[/dim]")
    else:
        console.print("[dim]Skipped. Enable APIs manually before migrating.[/dim]")

    state.mark_step("apis", StepStatus.COMPLETE)
    return state


def _step_service_account(credentials, state):  # type: ignore[no-untyped-def]
    """Step 3: Create service account and grant roles."""
    from slack_chat_migrator.services.setup.service_account import (
        create_service_account,
        grant_roles,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][3/5] Service Account[/bold]")

    account_id = click.prompt("Service account ID", default="slack-chat-migrator")
    display_name = click.prompt("Display name", default="Slack Chat Migrator")

    try:
        result = create_service_account(
            credentials, state.project_id, account_id, display_name
        )
        state.service_account_email = result["email"]
        console.print(f"[green]Created: {state.service_account_email}[/green]")

        roles = grant_roles(credentials, state.project_id, state.service_account_email)
        console.print(f"Granted roles: {', '.join(roles)}")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            email = f"{account_id}@{state.project_id}.iam.gserviceaccount.com"
            console.print(f"[yellow]Service account already exists: {email}[/yellow]")
            state.service_account_email = email
        else:
            console.print(f"[red]Failed: {e}[/red]")
            sa_email = click.prompt(
                "Enter existing service account email (or leave empty to skip)",
                default="",
            )
            if sa_email.strip():
                state.service_account_email = sa_email.strip()

    if state.service_account_email:
        state.mark_step("service_account", StepStatus.COMPLETE)
    else:
        state.mark_step("service_account", StepStatus.SKIPPED)
    return state


def _step_download_key(credentials, state):  # type: ignore[no-untyped-def]
    """Step 4: Download service account key."""
    from slack_chat_migrator.services.setup.service_account import download_key
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][4/5] Service Account Key[/bold]")

    if not state.service_account_email:
        console.print(
            "[yellow]No service account configured. Skipping key download.[/yellow]"
        )
        state.mark_step("key", StepStatus.SKIPPED)
        return state

    default_path = f"{state.project_id}-credentials.json"
    key_path = click.prompt("Key file output path", default=default_path)

    try:
        path = download_key(credentials, state.service_account_email, Path(key_path))
        state.key_path = str(path)
        console.print(f"[green]Key saved to: {path}[/green]")
        console.print(
            warning_panel(
                "Security",
                "Keep this file secure and [bold]do not commit it to git[/bold].",
            )
        )
        state.mark_step("key", StepStatus.COMPLETE)
    except Exception as e:
        console.print(f"[red]Failed to download key: {e}[/red]")
        console.print("[dim]You can create a key manually in the GCP Console.[/dim]")
        state.mark_step("key", StepStatus.SKIPPED)
    return state


def _step_delegation(state):  # type: ignore[no-untyped-def]
    """Step 5: Test domain-wide delegation."""
    from rich.panel import Panel

    from slack_chat_migrator.services.setup.delegation import test_delegation
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][5/5] Domain-Wide Delegation[/bold]")

    if not state.key_path:
        console.print(
            "[yellow]No key file available. Skipping delegation test.[/yellow]"
        )
        console.print(
            "[dim]Configure domain-wide delegation manually in the "
            "Google Workspace Admin Console.[/dim]"
        )
        state.mark_step("delegation", StepStatus.SKIPPED)
        return state

    console.print(
        Panel(
            "Before testing, ensure you have:\n"
            "  1. Enabled domain-wide delegation for the service account\n"
            "  2. Added the required OAuth scopes in the Admin Console\n"
            "  3. Granted the scopes to the service account client ID",
            title="Prerequisites",
            border_style="blue",
        )
    )

    if not click.confirm("Ready to test delegation?", default=True):
        console.print(
            "[dim]Skipped. Test delegation later with 'validate --creds_path'.[/dim]"
        )
        state.mark_step("delegation", StepStatus.SKIPPED)
        return state

    workspace_admin = click.prompt("Workspace admin email to impersonate")
    result = test_delegation(Path(state.key_path), workspace_admin)

    if result["success"]:
        console.print("[green]Delegation verified successfully.[/green]")
        state.delegation_verified = True
    else:
        console.print(f"[red]Delegation test failed: {result['detail']}[/red]")
        console.print(
            "[dim]Check the Admin Console delegation settings and try again.[/dim]"
        )

    state.mark_step("delegation", StepStatus.COMPLETE)
    return state
