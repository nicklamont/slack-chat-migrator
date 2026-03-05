"""CLI command handler for interactive GCP setup wizard."""

from __future__ import annotations

import sys
from pathlib import Path

import click

from slack_chat_migrator.cli.common import cli


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
    if not _check_setup_deps():
        click.echo("Missing required dependencies for setup wizard.")
        click.echo("Install with: pip install slack-chat-migrator[setup]")
        sys.exit(1)

    from slack_chat_migrator.services.setup.setup_service import (
        StepStatus,
        get_credentials,
        load_state,
        save_state,
    )

    # Load persistent state
    state = load_state()

    click.echo("")
    click.echo("GCP Setup Wizard")
    click.echo("=" * 40)

    if any(s == StepStatus.COMPLETE.value for s in state.steps.values()):
        click.echo("Resuming from previous session.")
        click.echo("")

    try:
        credentials = get_credentials()
    except RuntimeError as e:
        click.echo(f"Error: {e}")
        sys.exit(1)

    # Step 1: Project selection
    if state.step_status("project") != StepStatus.COMPLETE:
        state = _step_project(credentials, state)
        save_state(state)
    else:
        click.echo(f"\n[1/5] Project: {state.project_id} (done)")

    if not state.project_id:
        click.echo("No project selected. Aborting.")
        sys.exit(1)

    # Step 2: Enable APIs
    if state.step_status("apis") != StepStatus.COMPLETE:
        state = _step_apis(credentials, state)
        save_state(state)
    else:
        click.echo("[2/5] APIs: enabled (done)")

    # Step 3: Service account
    if state.step_status("service_account") != StepStatus.COMPLETE:
        state = _step_service_account(credentials, state)
        save_state(state)
    else:
        click.echo(f"[3/5] Service account: {state.service_account_email} (done)")

    # Step 4: Download key
    if state.step_status("key") != StepStatus.COMPLETE:
        state = _step_download_key(credentials, state)
        save_state(state)
    else:
        click.echo(f"[4/5] Key: {state.key_path} (done)")

    # Step 5: Test delegation
    if state.step_status("delegation") != StepStatus.COMPLETE:
        state = _step_delegation(state)
        save_state(state)
    else:
        click.echo("[5/5] Delegation: verified (done)")

    click.echo("")
    click.echo("Setup complete!")
    if state.key_path:
        click.echo(f"Credentials file: {state.key_path}")
    click.echo(
        "Next: run 'slack-chat-migrator init --export_path <dir>' "
        "to generate config.yaml"
    )


def _step_project(credentials, state):  # type: ignore[no-untyped-def]
    """Step 1: Select or create a GCP project."""
    from slack_chat_migrator.services.setup.gcp_project import (
        create_project,
        list_projects,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    click.echo("\n[1/5] GCP Project")

    if click.confirm("Use an existing project?", default=True):
        projects = list_projects(credentials)
        if not projects:
            click.echo("No accessible projects found.")
        else:
            click.echo("Available projects:")
            for i, p in enumerate(projects[:20], 1):
                click.echo(f"  {i}. {p['project_id']} ({p['display_name']})")

        project_id = click.prompt("Enter project ID")
        state.project_id = project_id
    else:
        project_id = click.prompt("New project ID")
        display_name = click.prompt("Display name", default=project_id)
        try:
            state.project_id = create_project(credentials, project_id, display_name)
            click.echo(f"Created project: {state.project_id}")
        except Exception as e:
            click.echo(f"Failed to create project: {e}")
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

    click.echo("\n[2/5] Enable APIs")
    click.echo(f"Required APIs: {', '.join(REQUIRED_APIS)}")

    if click.confirm("Enable required APIs?", default=True):
        try:

            def progress(api: str, status: str) -> None:
                click.echo(f"  {api}: {status}")

            newly = enable_required_apis(
                credentials, state.project_id, on_progress=progress
            )
            state.apis_enabled = newly
            click.echo(f"  {len(newly)} API(s) newly enabled.")
        except Exception as e:
            click.echo(f"Failed to enable APIs: {e}")
            click.echo("You can enable them manually in the GCP Console.")
    else:
        click.echo("Skipped. Enable APIs manually before migrating.")

    state.mark_step("apis", StepStatus.COMPLETE)
    return state


def _step_service_account(credentials, state):  # type: ignore[no-untyped-def]
    """Step 3: Create service account and grant roles."""
    from slack_chat_migrator.services.setup.service_account import (
        create_service_account,
        grant_roles,
    )
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    click.echo("\n[3/5] Service Account")

    account_id = click.prompt("Service account ID", default="slack-chat-migrator")
    display_name = click.prompt("Display name", default="Slack Chat Migrator")

    try:
        result = create_service_account(
            credentials, state.project_id, account_id, display_name
        )
        state.service_account_email = result["email"]
        click.echo(f"Created: {state.service_account_email}")

        roles = grant_roles(credentials, state.project_id, state.service_account_email)
        click.echo(f"Granted roles: {', '.join(roles)}")
    except Exception as e:
        error_msg = str(e)
        if "already exists" in error_msg.lower():
            email = f"{account_id}@{state.project_id}.iam.gserviceaccount.com"
            click.echo(f"Service account already exists: {email}")
            state.service_account_email = email
        else:
            click.echo(f"Failed: {e}")
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

    click.echo("\n[4/5] Service Account Key")

    if not state.service_account_email:
        click.echo("No service account configured. Skipping key download.")
        state.mark_step("key", StepStatus.SKIPPED)
        return state

    default_path = f"{state.project_id}-credentials.json"
    key_path = click.prompt("Key file output path", default=default_path)

    try:
        path = download_key(credentials, state.service_account_email, Path(key_path))
        state.key_path = str(path)
        click.echo(f"Key saved to: {path}")
        click.echo("IMPORTANT: Keep this file secure and do not commit it to git.")
        state.mark_step("key", StepStatus.COMPLETE)
    except Exception as e:
        click.echo(f"Failed to download key: {e}")
        click.echo("You can create a key manually in the GCP Console.")
        state.mark_step("key", StepStatus.SKIPPED)
    return state


def _step_delegation(state):  # type: ignore[no-untyped-def]
    """Step 5: Test domain-wide delegation."""
    from slack_chat_migrator.services.setup.delegation import test_delegation
    from slack_chat_migrator.services.setup.setup_service import StepStatus

    click.echo("\n[5/5] Domain-Wide Delegation")

    if not state.key_path:
        click.echo("No key file available. Skipping delegation test.")
        click.echo(
            "Configure domain-wide delegation manually in the "
            "Google Workspace Admin Console."
        )
        state.mark_step("delegation", StepStatus.SKIPPED)
        return state

    click.echo("Before testing, ensure you have:")
    click.echo("  1. Enabled domain-wide delegation for the service account")
    click.echo("  2. Added the required OAuth scopes in the Admin Console")
    click.echo("  3. Granted the scopes to the service account client ID")
    click.echo("")

    if not click.confirm("Ready to test delegation?", default=True):
        click.echo("Skipped. Test delegation later with 'validate --creds_path'.")
        state.mark_step("delegation", StepStatus.SKIPPED)
        return state

    workspace_admin = click.prompt("Workspace admin email to impersonate")
    result = test_delegation(Path(state.key_path), workspace_admin)

    if result["success"]:
        click.echo("Delegation verified successfully.")
        state.delegation_verified = True
    else:
        click.echo(f"Delegation test failed: {result['detail']}")
        click.echo("Check the Admin Console delegation settings and try again.")

    state.mark_step("delegation", StepStatus.COMPLETE)
    return state
