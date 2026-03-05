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
        assert loaded.project_id == "my-project"
        assert loaded.step_status("project") == StepStatus.COMPLETE

    def test_load_missing_file(self, tmp_path: Path) -> None:
        state_file = tmp_path / "nonexistent.json"
        state = load_state(path=state_file)
        assert state.project_id is None
        assert state.steps == {}

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
        assert loaded.project_id == state.project_id
        assert loaded.apis_enabled == state.apis_enabled
        assert loaded.service_account_email == state.service_account_email
        assert loaded.key_path == state.key_path
        assert loaded.delegation_verified is True


class TestApiEnablement:
    """Tests for API enablement functions."""

    def test_enable_required_apis_skips_enabled(self) -> None:
        from slack_chat_migrator.services.setup.api_enablement import (
            enable_required_apis,
        )

        mock_sm = MagicMock()
        mock_client = MagicMock()
        mock_sm.ServiceManagerClient.return_value = mock_client

        # Simulate all APIs already enabled
        mock_service1 = MagicMock()
        mock_service1.service_name = "chat.googleapis.com"
        mock_service2 = MagicMock()
        mock_service2.service_name = "drive.googleapis.com"
        mock_service3 = MagicMock()
        mock_service3.service_name = "admin.googleapis.com"
        mock_client.list_services.return_value = [
            mock_service1,
            mock_service2,
            mock_service3,
        ]

        progress_calls: list[tuple[str, str]] = []
        with patch.dict("sys.modules", {"google.cloud.servicemanagement_v1": mock_sm}):
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
