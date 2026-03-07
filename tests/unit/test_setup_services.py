"""Tests for the setup service layer and CLI command."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from slack_chat_migrator.services.setup.setup_service import (
    SetupState,
    StepStatus,
    load_state,
    save_state,
)


class TestSetupState:
    """Tests for SetupState dataclass."""

    def test_default_state(self) -> None:
        state = SetupState()
        assert state.project_id is None
        assert state.apis_enabled == []
        assert state.service_account_email is None
        assert state.delegation_verified is False
        assert state.steps == {}

    def test_step_status_default(self) -> None:
        state = SetupState()
        assert state.step_status("project") == StepStatus.PENDING

    def test_mark_step(self) -> None:
        state = SetupState()
        state.mark_step("project", StepStatus.COMPLETE)
        assert state.step_status("project") == StepStatus.COMPLETE

    def test_mark_step_skipped(self) -> None:
        state = SetupState()
        state.mark_step("delegation", StepStatus.SKIPPED)
        assert state.step_status("delegation") == StepStatus.SKIPPED


class TestStatePersistence:
    """Tests for state load/save."""

    def test_save_and_load(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state = SetupState(project_id="my-project")
        state.mark_step("project", StepStatus.COMPLETE)

        save_state(state, path=state_file)
        assert state_file.exists()

        loaded = load_state(path=state_file)
        assert loaded is not None
        assert loaded.project_id == "my-project"
        assert loaded.step_status("project") == StepStatus.COMPLETE

    def test_load_missing_file(self, tmp_path: Path) -> None:
        state_file = tmp_path / "nonexistent.json"
        assert load_state(path=state_file) is None

    def test_load_empty_file(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text("")
        assert load_state(path=state_file) is None

    def test_load_invalid_json(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text("{bad json")
        assert load_state(path=state_file) is None

    def test_load_non_dict_json(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state_file.write_text('"just a string"')
        assert load_state(path=state_file) is None

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        state_file = tmp_path / "sub" / "dir" / "state.json"
        save_state(SetupState(), path=state_file)
        assert state_file.exists()

    def test_roundtrip_preserves_all_fields(self, tmp_path: Path) -> None:
        state_file = tmp_path / "state.json"
        state = SetupState(
            project_id="test-proj",
            apis_enabled=["chat.googleapis.com"],
            service_account_email="sa@test.iam.gserviceaccount.com",
            key_path="/tmp/key.json",
            delegation_verified=True,
            steps={"project": "complete", "apis": "complete"},
        )
        save_state(state, path=state_file)
        loaded = load_state(path=state_file)
        assert loaded is not None
        assert loaded.project_id == state.project_id
        assert loaded.apis_enabled == state.apis_enabled
        assert loaded.service_account_email == state.service_account_email
        assert loaded.key_path == state.key_path
        assert loaded.delegation_verified is True


class TestApiEnablement:
    """Tests for API enablement functions."""

    def test_enable_required_apis_skips_enabled(self) -> None:
        from slack_chat_migrator.services.setup.api_enablement import (
            REQUIRED_APIS,
            enable_required_apis,
        )

        # Mock the discovery-based service
        mock_service = MagicMock()
        mock_list = mock_service.services().list
        mock_list.return_value.execute.return_value = {
            "services": [{"config": {"name": api}} for api in REQUIRED_APIS],
        }
        # No next page
        mock_service.services().list_next.return_value = None

        progress_calls: list[tuple[str, str]] = []
        with patch(
            "slack_chat_migrator.services.setup.api_enablement._build_service",
            return_value=mock_service,
        ):
            newly = enable_required_apis(
                MagicMock(),
                "test-proj",
                on_progress=lambda api, status: progress_calls.append((api, status)),
            )
        assert newly == []
        assert all(s == "already_enabled" for _, s in progress_calls)


class TestSetupCommand:
    """Tests for the setup CLI command."""

    def test_setup_registered_in_cli(self) -> None:
        from slack_chat_migrator.cli.commands import cli

        assert "setup" in cli.commands

    @patch(
        "slack_chat_migrator.cli.setup_cmd._check_setup_deps",
        return_value=False,
    )
    def test_setup_missing_deps(self, mock_deps: MagicMock) -> None:
        from click.testing import CliRunner

        from slack_chat_migrator.cli.commands import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["setup"])
        assert result.exit_code == 1
        assert "pip install" in result.output

    @patch(
        "slack_chat_migrator.cli.setup_cmd._check_setup_deps",
        return_value=True,
    )
    @patch("slack_chat_migrator.services.setup.setup_service.get_credentials")
    def test_setup_no_credentials(
        self, mock_creds: MagicMock, mock_deps: MagicMock
    ) -> None:
        from click.testing import CliRunner

        from slack_chat_migrator.cli.commands import cli

        mock_creds.side_effect = RuntimeError("No credentials found")
        runner = CliRunner()
        result = runner.invoke(cli, ["setup"])
        assert result.exit_code == 1
        assert "No credentials found" in result.output


class TestServiceAccount:
    """Tests for service account REST operations."""

    @patch(
        "slack_chat_migrator.services.setup.service_account._build_iam_service",
    )
    def test_create_service_account(self, mock_build: MagicMock) -> None:
        from slack_chat_migrator.services.setup.service_account import (
            create_service_account,
        )

        mock_service = mock_build.return_value
        mock_service.projects().serviceAccounts().create().execute.return_value = {
            "email": "sa@proj.iam.gserviceaccount.com",
            "name": "projects/proj/serviceAccounts/sa@proj.iam.gserviceaccount.com",
        }

        result = create_service_account(MagicMock(), "proj", "sa", "SA Display")
        assert result["email"] == "sa@proj.iam.gserviceaccount.com"


class TestGcpProject:
    """Tests for GCP project REST operations."""

    @patch(
        "slack_chat_migrator.services.setup.gcp_project._build_crm_service",
    )
    def test_list_projects(self, mock_build: MagicMock) -> None:
        from slack_chat_migrator.services.setup.gcp_project import list_projects

        mock_service = mock_build.return_value
        mock_service.projects().list().execute.return_value = {
            "projects": [
                {"projectId": "proj-1", "name": "Project One"},
                {"projectId": "proj-2", "name": "Project Two"},
            ],
        }
        mock_service.projects().list_next.return_value = None

        projects = list_projects(MagicMock())
        assert len(projects) == 2
        assert projects[0]["project_id"] == "proj-1"
        assert projects[0]["display_name"] == "Project One"

    @patch(
        "slack_chat_migrator.services.setup.gcp_project._build_crm_service",
    )
    def test_create_project(self, mock_build: MagicMock) -> None:
        from slack_chat_migrator.services.setup.gcp_project import create_project

        mock_service = mock_build.return_value
        mock_service.projects().create().execute.return_value = {"done": True}

        result = create_project(MagicMock(), "my-proj", "My Project")
        assert result == "my-proj"


class TestDelegation:
    """Tests for delegation verification."""

    @patch("google.oauth2.service_account.Credentials")
    @patch("googleapiclient.discovery.build")
    def test_delegation_success(
        self, mock_build: MagicMock, mock_sa_creds: MagicMock, tmp_path: Path
    ) -> None:
        from slack_chat_migrator.services.setup.delegation import test_delegation

        key_file = tmp_path / "key.json"
        key_file.write_text(json.dumps({"type": "service_account"}))

        mock_creds = MagicMock()
        mock_sa_creds.from_service_account_file.return_value = mock_creds
        mock_creds.with_subject.return_value = mock_creds

        mock_service = MagicMock()
        mock_build.return_value = mock_service

        result = test_delegation(key_file, "admin@example.com")
        assert result["success"] is True

    @patch("google.oauth2.service_account.Credentials")
    @patch("googleapiclient.discovery.build")
    def test_delegation_failure(
        self, mock_build: MagicMock, mock_sa_creds: MagicMock, tmp_path: Path
    ) -> None:
        from slack_chat_migrator.services.setup.delegation import test_delegation

        key_file = tmp_path / "key.json"
        key_file.write_text(json.dumps({"type": "service_account"}))

        mock_creds = MagicMock()
        mock_sa_creds.from_service_account_file.return_value = mock_creds
        mock_creds.with_subject.return_value = mock_creds

        mock_build.side_effect = Exception("Delegation not configured")

        result = test_delegation(key_file, "admin@example.com")
        assert result["success"] is False
        assert "Delegation not configured" in result["detail"]
