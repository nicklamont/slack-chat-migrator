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
    """Check if optional setup dependencies are installed.

    All setup operations now use the REST discovery API (googleapiclient)
    which is already a core dependency, so this just verifies google.auth.
    """
    try:
        __import__("google.auth")
        __import__("googleapiclient")
        return True
    except ImportError:
        return False


def _get_and_validate_credentials(console):  # type: ignore[no-untyped-def]
    """Authenticate and validate the ADC quota project."""
    from slack_chat_migrator.services.setup.setup_service import (
        get_adc_quota_project,
        get_credentials,
        verify_quota_project,
    )

    try:
        credentials = get_credentials()
    except RuntimeError as e:
        console.print(error_panel("Authentication failed", str(e)))
        sys.exit(1)

    quota_project = get_adc_quota_project()
    if not quota_project:
        console.print(
            warning_panel(
                "No quota project",
                "Your application-default credentials don't have a quota project set.\n"
                "API calls may fail. If you have an existing GCP project, run:\n"
                "  [bold]gcloud auth application-default set-quota-project PROJECT_ID[/bold]",
            )
        )
        if not click.confirm("Continue anyway?", default=True):
            sys.exit(0)
        return credentials

    result = verify_quota_project(credentials, quota_project)
    if result is not None:
        severity, message = result
        if severity == "error":
            console.print(
                error_panel(
                    "Invalid quota project",
                    f"{message}\n\n"
                    "Fix by setting an active project:\n"
                    "  [bold]gcloud auth application-default "
                    "set-quota-project PROJECT_ID[/bold]\n\n"
                    "If you don't have a project yet, create one at\n"
                    "  [bold]console.cloud.google.com[/bold] and then "
                    "run the command above.",
                )
            )
            sys.exit(1)
        else:
            console.print(
                warning_panel("Quota project", f"{message}\nContinuing anyway.")
            )

    return credentials


@cli.command("setup")
def setup() -> None:
    """Interactive GCP setup wizard for migration prerequisites.

    Walks through project selection/creation, API enablement,
    service account setup, and delegation verification.
    Requires: pip install "slack-chat-migrator[setup]"
    """
    console = get_console()

    if not _check_setup_deps():
        console.print(
            error_panel(
                "Missing dependencies",
                "Install setup extras with:\n"
                '  [bold]pip install "slack-chat-migrator\\[setup]"[/bold]',
            )
        )
        sys.exit(1)

    from rich.panel import Panel

    from slack_chat_migrator.services.setup.setup_service import (
        SetupState,
        StepStatus,
        load_state,
        save_state,
    )

    # Load persistent state
    saved_state = load_state()

    console.print()
    console.print(
        Panel(
            "[bold]GCP Setup Wizard[/bold]",
            border_style="blue",
            subtitle="6 steps to configure Google Chat migration",
        )
    )

    if saved_state and any(
        s == StepStatus.COMPLETE.value for s in saved_state.steps.values()
    ):
        completed = [
            name
            for name, s in saved_state.steps.items()
            if s == StepStatus.COMPLETE.value
        ]
        console.print(
            f"[dim]Previous session found ({', '.join(completed)} done).[/dim]"
        )
        if click.confirm("Resume from previous session?", default=True):
            state = saved_state
        else:
            state = SetupState()
            console.print("[dim]Starting fresh.[/dim]")
    else:
        state = SetupState()
    console.print()

    credentials = _get_and_validate_credentials(console)

    # Step 1: Project selection
    if state.step_status("project") != StepStatus.COMPLETE:
        state = _step_project(credentials, state)
        save_state(state)
    else:
        console.print(
            f"[green]\u2713[/green] [1/6] Project: [bold]{state.project_id}[/bold]"
        )

    if not state.project_id:
        console.print(error_panel("No project", "No project selected. Aborting."))
        sys.exit(1)

    # Rebind credentials to bill against the target project, not the ADC
    # quota project. This prevents 403 errors when the quota project doesn't
    # have the required APIs enabled.
    if hasattr(credentials, "with_quota_project"):
        credentials = credentials.with_quota_project(state.project_id)

    # Step 2: Enable APIs
    if state.step_status("apis") != StepStatus.COMPLETE:
        state = _step_apis(credentials, state)
        save_state(state)
    else:
        console.print("[green]\u2713[/green] [2/6] APIs: enabled")

    # Step 3: Service account
    if state.step_status("service_account") != StepStatus.COMPLETE:
        state = _step_service_account(credentials, state)
        save_state(state)
    else:
        console.print(
            f"[green]\u2713[/green] [3/6] Service account: "
            f"[bold]{state.service_account_email}[/bold]"
        )

    # Step 4: Download key
    if state.step_status("key") != StepStatus.COMPLETE:
        state = _step_download_key(credentials, state)
        save_state(state)
    else:
        console.print(f"[green]\u2713[/green] [4/6] Key: [bold]{state.key_path}[/bold]")

    # Step 5: Configure Chat app (verified via API call)
    if state.step_status("chat_app") != StepStatus.COMPLETE:
        state = _step_chat_app(state)
        save_state(state)
    else:
        console.print("[green]\u2713[/green] [5/6] Chat app: configured")

    # Step 6: Test delegation
    # Re-run if delegation wasn't actually verified (e.g. failed on a previous run)
    if state.delegation_verified:
        console.print("[green]\u2713[/green] [6/6] Delegation: verified")
    else:
        state = _step_delegation(state)
        save_state(state)

    # Completion summary
    console.print()
    if state.delegation_verified:
        lines = ["[bold]Setup complete![/bold]"]
        if state.key_path:
            lines.append(f"\nCredentials file: [bold]{state.key_path}[/bold]")
        console.print(success_panel("GCP Setup", "\n".join(lines)))
        console.print(next_step_panel("slack-chat-migrator init --export_path <dir>"))
    else:
        lines = [
            "[bold]GCP project and service account are configured.[/bold]",
            "\nDomain-wide delegation is still needed before migrating.",
            "Re-run [bold]slack-chat-migrator setup[/bold] after configuring delegation.",
        ]
        if state.key_path:
            lines.insert(1, f"\nCredentials file: [bold]{state.key_path}[/bold]")
        console.print(warning_panel("GCP Setup — Almost Done", "\n".join(lines)))


def _step_project(credentials, state):  # type: ignore[no-untyped-def]
    """Step 1: Select or create a GCP project."""
    from slack_chat_migrator.services.setup.gcp_project import (
        create_project,
        list_projects,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][1/6] GCP Project[/bold]")

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
        from slack_chat_migrator.services.setup.gcp_project import check_project_id

        project_id = click.prompt("New project ID")
        display_name = click.prompt("Display name", default=project_id)
        try:
            state.project_id = create_project(credentials, project_id, display_name)
            console.print(f"[green]Created project: {state.project_id}[/green]")
        except Exception as e:
            error_msg = str(e)
            if "already exists" in error_msg.lower():
                # Check if it's our own deleted project or globally taken
                reason = check_project_id(credentials, project_id)
                if reason:
                    console.print(warning_panel("Project ID unavailable", reason))
                else:
                    console.print(
                        error_panel(
                            "Project ID taken",
                            f"[bold]{project_id}[/bold] is taken by another Google Cloud user.\n"
                            "Project IDs are globally unique. Try a more specific name\n"
                            "(e.g. [bold]yourcompany-chat-migration[/bold]).",
                        )
                    )
            else:
                console.print(error_panel("Failed to create project", error_msg))
            if click.confirm("Enter an existing project ID instead?", default=True):
                state.project_id = click.prompt("Project ID")
            else:
                return state

    if state.project_id:
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
    console.print("\n[bold][2/6] Enable APIs[/bold]")
    console.print(f"Required: {', '.join(REQUIRED_APIS)}")

    if click.confirm("Enable required APIs?", default=True):
        try:

            def progress(api: str, status: str) -> None:
                if status == "enabled":
                    console.print(f"  [green]\u2713[/green] {api}")
                else:
                    console.print(f"  [dim]\u2713 {api} (already enabled)[/dim]")

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
    """Step 3: Create service account."""
    from slack_chat_migrator.services.setup.service_account import (
        list_service_accounts,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][3/6] Service Account[/bold]")

    # Check for existing service accounts in the project
    existing = list_service_accounts(credentials, state.project_id)
    if existing:
        console.print("[dim]Existing service accounts:[/dim]")
        for sa in existing:
            console.print(f"  [dim]\u2022[/dim] {sa['email']}")

        if click.confirm("Use an existing service account?", default=True):
            if len(existing) == 1:
                state.service_account_email = existing[0]["email"]
                console.print(f"[green]Using: {state.service_account_email}[/green]")
            else:
                sa_email = click.prompt(
                    "Service account email",
                    default=existing[0]["email"],
                )
                state.service_account_email = sa_email
                console.print(f"[green]Using: {state.service_account_email}[/green]")
        else:
            state = _create_new_service_account(credentials, state, console)
    else:
        state = _create_new_service_account(credentials, state, console)

    if state.service_account_email:
        state.mark_step("service_account", StepStatus.COMPLETE)
    else:
        state.mark_step("service_account", StepStatus.SKIPPED)
    return state


def _create_new_service_account(credentials, state, console):  # type: ignore[no-untyped-def]
    """Prompt for details and create a new service account."""
    from slack_chat_migrator.services.setup.service_account import (
        create_service_account,
    )

    account_id = click.prompt("Service account ID", default="slack-chat-migrator")
    display_name = click.prompt("Display name", default="Slack Chat Migrator")

    try:
        result = create_service_account(
            credentials, state.project_id, account_id, display_name
        )
        state.service_account_email = result["email"]
        console.print(f"[green]Created: {state.service_account_email}[/green]")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            email = f"{account_id}@{state.project_id}.iam.gserviceaccount.com"
            console.print(f"[dim]Already exists: {email}[/dim]")
            state.service_account_email = email
        else:
            console.print(f"[red]Failed: {e}[/red]")
            sa_email = click.prompt(
                "Enter existing service account email (or leave empty to skip)",
                default="",
            )
            if sa_email.strip():
                state.service_account_email = sa_email.strip()
    return state


def _step_download_key(credentials, state):  # type: ignore[no-untyped-def]
    """Step 4: Download service account key."""
    from slack_chat_migrator.services.setup.service_account import download_key
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][4/6] Service Account Key[/bold]")

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


def _copy_to_clipboard(text: str) -> bool:
    """Try to copy text to the system clipboard. Returns True on success."""
    import platform
    import shutil
    import subprocess

    if platform.system() == "Darwin":
        cmd = "pbcopy"
    elif shutil.which("xclip"):
        cmd = "xclip"
    elif shutil.which("xsel"):
        cmd = "xsel"
    else:
        return False

    try:
        subprocess.run(  # noqa: S603
            [cmd], input=text.encode(), check=True, capture_output=True
        )
        return True
    except Exception:
        return False


def _read_client_id(key_path: str) -> str | None:
    """Read the client_id (OAuth2 client ID) from a service account key file."""
    import json

    try:
        data = json.loads(Path(key_path).read_text())
        result: str | None = data.get("client_id")
        return result
    except Exception:
        return None


def _get_required_scopes() -> list[str]:
    """Return the scopes needed for domain-wide delegation.

    Uses the canonical list from the migration runtime so the setup wizard
    and actual migration always stay in sync.
    """
    from slack_chat_migrator.utils.api import REQUIRED_SCOPES

    return list(REQUIRED_SCOPES)


def _prompt_email(prompt_text: str, default: str = "") -> str:
    """Prompt for a valid email address, looping until one is provided."""
    console = get_console()
    while True:
        value = click.prompt(prompt_text, default=default)
        if value and "@" in value:
            return value
        console.print("[red]A valid email address is required.[/red]")


def _step_chat_app(state):  # type: ignore[no-untyped-def]
    """Step 5: Configure Google Chat app in GCP Console (manual).

    This is a manual configuration step — we show instructions and ask
    the user to confirm. Actual verification happens in Step 6 (delegation),
    which can detect ``chat_app_missing`` once delegation credentials work.
    """
    from rich.panel import Panel

    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][5/6] Configure Chat App[/bold]")

    config_url = (
        "https://console.cloud.google.com/apis/api/chat.googleapis.com/hangouts-chat"
    )
    if state.project_id:
        config_url += f"?project={state.project_id}"

    console.print(
        Panel(
            "The Google Chat API requires a Chat app to be configured\n"
            "in your GCP project before migration can work.\n\n"
            f"  1. Open: [bold]{config_url}[/bold]\n"
            '  2. Set App name (e.g. "Slack Migrator")\n'
            "  3. Set Avatar URL (e.g. https://developers.google.com/chat/images/quickstart-app-avatar.png)\n"
            '  4. Set Description (e.g. "Slack to Google Chat migration")\n'
            "  5. Leave Interactive features [bold]disabled[/bold]\n"
            "  6. Click [bold]Save[/bold]\n\n"
            "[dim]This cannot be automated — it requires the GCP Console.\n"
            "The next step (delegation) will verify the Chat app is working.[/dim]",
            title="Instructions",
            border_style="blue",
        )
    )

    click.echo(config_url)
    console.print()

    if click.confirm("Have you configured the Chat app?", default=True):
        state.mark_step("chat_app", StepStatus.COMPLETE)
    else:
        console.print(
            "[dim]Skipped. Configure the Chat app before running the next step.[/dim]"
        )
        state.mark_step("chat_app", StepStatus.SKIPPED)
    return state


def _step_delegation(state):  # type: ignore[no-untyped-def]
    """Step 6: Test domain-wide delegation."""
    from rich.panel import Panel

    from slack_chat_migrator.services.setup.delegation import test_delegation
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    console = get_console()
    console.print("\n[bold][6/6] Domain-Wide Delegation[/bold]")

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

    client_id = _read_client_id(state.key_path)
    scopes_csv = ",".join(_get_required_scopes())

    prereq_lines = [
        "In the [bold]Google Admin Console[/bold], authorize the service account:\n",
        "  1. Go to https://admin.google.com/ac/owl/domainwidedelegation",
        "  2. Click [bold]Add new[/bold] and enter:",
    ]
    if client_id:
        prereq_lines.append(f"     Client ID: [bold]{client_id}[/bold]")
    else:
        prereq_lines.append(
            "     Client ID: [dim](find in your key file under 'client_id')[/dim]"
        )
    # Try to copy scopes to clipboard
    copied = _copy_to_clipboard(scopes_csv)

    if copied:
        prereq_lines.append("  3. Paste the OAuth scopes (copied to clipboard)")
    else:
        prereq_lines.append("  3. Add the OAuth scopes printed below")

    prereq_lines.append("  4. Click [bold]Authorize[/bold]")

    console.print(
        Panel(
            "\n".join(prereq_lines),
            title="Prerequisites",
            border_style="blue",
        )
    )

    # Print scopes outside the panel so they're easy to select/copy
    if copied:
        console.print("[dim]Scopes (already on clipboard):[/dim]")
    else:
        console.print("[dim]Scopes (copy this entire line):[/dim]")
    click.echo(scopes_csv)
    console.print()

    if not click.confirm("Ready to test delegation?", default=True):
        console.print(
            "[dim]Skipped. Test delegation later with 'validate --creds_path'.[/dim]"
        )
        state.mark_step("delegation", StepStatus.SKIPPED)
        return state

    workspace_admin = _prompt_email(
        "Workspace admin email to impersonate",
        default=state.workspace_admin or "",
    )
    state.workspace_admin = workspace_admin

    while True:
        result = test_delegation(Path(state.key_path), workspace_admin)

        if result["success"] and not result.get("chat_app_missing"):
            # Full success: delegation works and Chat app is configured
            state.delegation_verified = True
            state.mark_step("delegation", StepStatus.COMPLETE)
            console.print("[green]Delegation verified successfully.[/green]")
            break

        if result.get("chat_app_missing"):
            # Delegation works but Chat app isn't configured yet
            console.print("[green]\u2713 Delegation works![/green]")
            project_id = result.get("key_info", {}).get("project_id", "")
            config_url = (
                "https://console.cloud.google.com/apis/api/"
                "chat.googleapis.com/hangouts-chat"
            )
            if project_id:
                config_url += f"?project={project_id}"

            console.print(
                warning_panel(
                    "Chat app not configured",
                    "The Google Chat API requires a Chat app to be configured\n"
                    "in your GCP project before migration can work.\n\n"
                    f"  1. Open: [bold]{config_url}[/bold]\n"
                    '  2. Set App name (e.g. "Slack Migrator")\n'
                    "  3. Set Avatar URL (e.g. https://developers.google.com/chat/images/quickstart-app-avatar.png)\n"
                    "  4. Under Functionality, check both boxes\n"
                    "  5. Under Visibility, select your domain\n"
                    "  6. Click [bold]Save[/bold]\n\n"
                    "[dim]This cannot be automated — it requires the GCP Console.[/dim]",
                )
            )
            click.echo(config_url)
            console.print()

            if click.confirm("Retry after configuring?", default=True):
                continue
            state.mark_step("delegation", StepStatus.SKIPPED)
            break

        # Delegation failed
        detail = result["detail"]
        key_info = result.get("key_info", {})

        diag_lines = ["[bold]Verify these match your Admin Console entry:[/bold]"]
        if key_info.get("client_id"):
            diag_lines.append(f"  Client ID:    [bold]{key_info['client_id']}[/bold]")
        if key_info.get("client_email"):
            diag_lines.append(f"  SA Email:     {key_info['client_email']}")
        if key_info.get("project_id"):
            diag_lines.append(f"  Project:      {key_info['project_id']}")
        diag_lines.append(f"  Impersonating: {workspace_admin}")
        diag_lines.append("  Test scope:   chat.spaces.readonly")
        diag_section = "\n".join(diag_lines)

        console.print(
            warning_panel(
                "Delegation not configured",
                f"[dim]{detail}[/dim]\n\n"
                f"{diag_section}\n\n"
                "Common causes:\n"
                "  \u2022 Client ID in Admin Console doesn't match the one above\n"
                "  \u2022 Scopes were pasted with extra spaces or got truncated\n"
                "  \u2022 The admin email isn't a super admin in your Workspace\n\n"
                "To fix:\n"
                "  1. Go to https://admin.google.com/ac/owl/domainwidedelegation\n"
                + (
                    f"  2. Verify client ID [bold]{key_info.get('client_id', client_id or '?')}[/bold] "
                    "with the scopes above\n"
                    if key_info.get("client_id") or client_id
                    else "  2. Verify the service account client ID with the scopes above\n"
                )
                + "  3. Re-run [bold]slack-chat-migrator setup[/bold] to verify",
            )
        )
        state.mark_step("delegation", StepStatus.SKIPPED)
        break

    return state
