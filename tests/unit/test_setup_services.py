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


# ---------------------------------------------------------------------------
# Issue #121: delegation scopes, API step status, clipboard, key handling
# ---------------------------------------------------------------------------


class TestDelegationScopes:
    """The delegation test requests every scope the migration requests."""

    @patch("google.oauth2.service_account.Credentials")
    @patch("googleapiclient.discovery.build")
    def test_requests_all_required_scopes(
        self, mock_build: MagicMock, mock_sa_creds: MagicMock, tmp_path: Path
    ) -> None:
        from slack_chat_migrator.services.setup.delegation import test_delegation
        from slack_chat_migrator.utils.api import REQUIRED_SCOPES

        key_file = tmp_path / "key.json"
        key_file.write_text("{}")
        test_delegation(key_file, "admin@example.com")

        scopes = mock_sa_creds.from_service_account_file.call_args.kwargs["scopes"]
        assert scopes == REQUIRED_SCOPES

    @patch("google.oauth2.service_account.Credentials")
    @patch("googleapiclient.discovery.build")
    def test_missing_scope_fails(
        self, mock_build: MagicMock, mock_sa_creds: MagicMock, tmp_path: Path
    ) -> None:
        """A delegation entry missing a scope makes the token request fail."""
        from google.auth.exceptions import RefreshError

        from slack_chat_migrator.services.setup.delegation import test_delegation

        key_file = tmp_path / "key.json"
        key_file.write_text("{}")
        mock_build.return_value.spaces.return_value.list.return_value.execute.side_effect = RefreshError(
            "unauthorized_client: Client is unauthorized to retrieve access tokens"
        )

        result = test_delegation(key_file, "admin@example.com")

        assert result["success"] is False
        assert "every required scope" in result["detail"]


class TestApisStep:
    """The APIs step is complete only when enabling succeeded."""

    def _run(self, confirm: bool, side_effect=None):
        from slack_chat_migrator.cli.setup_cmd import _step_apis

        state = SetupState(project_id="proj")
        with (
            patch(
                "slack_chat_migrator.cli.setup_cmd.click.confirm", return_value=confirm
            ),
            patch(
                "slack_chat_migrator.services.setup.api_enablement.enable_required_apis",
                side_effect=side_effect,
                return_value=["chat.googleapis.com"],
            ),
        ):
            return _step_apis(MagicMock(), state)

    def test_success_is_complete(self) -> None:
        assert self._run(True).step_status("apis") == StepStatus.COMPLETE

    def test_failure_is_not_complete(self) -> None:
        state = self._run(True, side_effect=RuntimeError("permission denied"))
        assert state.step_status("apis") == StepStatus.SKIPPED

    def test_skip_is_not_complete(self) -> None:
        assert self._run(False).step_status("apis") == StepStatus.SKIPPED


class TestClipboard:
    """xclip/xsel write the CLIPBOARD selection, and output isn't captured."""

    @staticmethod
    def _copy(available: set[str]):
        import subprocess

        from slack_chat_migrator.cli import setup_cmd

        with (
            patch("platform.system", return_value="Linux"),
            patch("shutil.which", side_effect=lambda c: c if c in available else None),
            patch("subprocess.run") as run,
        ):
            ok = setup_cmd._copy_to_clipboard("scopes")
        return ok, run, subprocess

    def test_xclip_uses_clipboard_selection(self) -> None:
        ok, run, subprocess = self._copy({"xclip"})
        assert ok
        assert run.call_args.args[0] == ["xclip", "-selection", "clipboard"]
        kwargs = run.call_args.kwargs
        assert "capture_output" not in kwargs
        assert kwargs["stdout"] is subprocess.DEVNULL

    def test_xsel_uses_clipboard(self) -> None:
        ok, run, _ = self._copy({"xsel"})
        assert ok
        assert run.call_args.args[0] == ["xsel", "--clipboard", "--input"]

    def test_wl_copy_preferred(self) -> None:
        ok, run, _ = self._copy({"wl-copy", "xclip"})
        assert ok
        assert run.call_args_list[0].args[0] == ["wl-copy"]

    def test_none_available(self) -> None:
        ok, run, _ = self._copy(set())
        assert ok is False
        run.assert_not_called()


def _iam_with_key(key_id: str = "kid1") -> MagicMock:
    import base64

    service = MagicMock()
    keys = service.projects.return_value.serviceAccounts.return_value.keys.return_value
    keys.create.return_value.execute.return_value = {
        "privateKeyData": base64.b64encode(
            json.dumps(
                {
                    "client_email": "sa@p.iam.gserviceaccount.com",
                    "private_key_id": "new",
                    "private_key": "PK",
                }
            ).encode()
        ).decode()
    }
    keys.list.return_value.execute.return_value = {
        "keys": [{"name": f"projects/p/serviceAccounts/sa/keys/{key_id}"}]
    }
    return service


class TestKeyFiles:
    """download_key and key reuse."""

    def test_overwrite_makes_file_private(self, tmp_path: Path) -> None:
        import os
        import stat

        from slack_chat_migrator.services.setup import service_account as sa

        key = tmp_path / "key.json"
        key.write_text("{}")
        os.chmod(key, 0o644)
        with patch.object(sa, "_build_iam_service", return_value=_iam_with_key()):
            sa.download_key(None, "sa@p.iam.gserviceaccount.com", key, overwrite=True)
        assert stat.S_IMODE(key.stat().st_mode) == 0o600
        assert "PK" in key.read_text()

    def test_refuses_to_overwrite_by_default(self, tmp_path: Path) -> None:
        import pytest

        from slack_chat_migrator.services.setup import service_account as sa

        key = tmp_path / "key.json"
        key.write_text("{}")
        service = _iam_with_key()
        with patch.object(sa, "_build_iam_service", return_value=service):
            with pytest.raises(FileExistsError):
                sa.download_key(None, "sa@p.iam.gserviceaccount.com", key)
        keys = (
            service.projects.return_value.serviceAccounts.return_value.keys.return_value
        )
        keys.create.assert_not_called()

    def test_existing_key_is_valid(self, tmp_path: Path) -> None:
        from slack_chat_migrator.services.setup import service_account as sa

        key = tmp_path / "key.json"
        key.write_text(
            json.dumps(
                {
                    "client_email": "sa@p.iam.gserviceaccount.com",
                    "private_key_id": "kid1",
                }
            )
        )
        with patch.object(sa, "_build_iam_service", return_value=_iam_with_key("kid1")):
            assert sa.existing_key_is_valid(None, "sa@p.iam.gserviceaccount.com", key)
        with patch.object(
            sa, "_build_iam_service", return_value=_iam_with_key("other")
        ):
            assert not sa.existing_key_is_valid(
                None, "sa@p.iam.gserviceaccount.com", key
            )
        assert not sa.existing_key_is_valid(None, "someone@else.com", key)
        (tmp_path / "bad.json").write_text("not json")
        assert not sa.existing_key_is_valid(None, "x", tmp_path / "bad.json")

    def test_key_step_reuses_valid_key(self, tmp_path: Path) -> None:
        from slack_chat_migrator.cli.setup_cmd import _step_download_key

        key = tmp_path / "key.json"
        key.write_text("{}")
        state = SetupState(
            project_id="p", service_account_email="sa@p.iam.gserviceaccount.com"
        )
        with (
            patch(
                "slack_chat_migrator.cli.setup_cmd.click.prompt", return_value=str(key)
            ),
            patch(
                "slack_chat_migrator.services.setup.service_account.existing_key_is_valid",
                return_value=True,
            ),
            patch(
                "slack_chat_migrator.services.setup.service_account.download_key"
            ) as download,
        ):
            state = _step_download_key(MagicMock(), state)
        download.assert_not_called()
        assert state.key_path == str(key)
        assert state.step_status("key") == StepStatus.COMPLETE

    def test_key_step_asks_before_overwriting(self, tmp_path: Path) -> None:
        from slack_chat_migrator.cli.setup_cmd import _step_download_key

        key = tmp_path / "key.json"
        key.write_text("{}")
        state = SetupState(
            project_id="p", service_account_email="sa@p.iam.gserviceaccount.com"
        )
        with (
            patch(
                "slack_chat_migrator.cli.setup_cmd.click.prompt", return_value=str(key)
            ),
            patch(
                "slack_chat_migrator.cli.setup_cmd.click.confirm", return_value=False
            ),
            patch(
                "slack_chat_migrator.services.setup.service_account.existing_key_is_valid",
                return_value=False,
            ),
            patch(
                "slack_chat_migrator.services.setup.service_account.download_key"
            ) as download,
        ):
            state = _step_download_key(MagicMock(), state)
        download.assert_not_called()
        assert state.step_status("key") == StepStatus.SKIPPED
        assert key.read_text() == "{}"
