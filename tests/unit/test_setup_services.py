"""Tests for the setup service layer and CLI command."""

from __future__ import annotations

import http.client
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httplib2
import pytest

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


def _iam_with_key(key_id: str = "kid1", **fields: object) -> MagicMock:
    import base64

    service = MagicMock()
    keys = service.projects.return_value.serviceAccounts.return_value.keys.return_value
    keys.create.return_value.execute.return_value = {
        "name": NEW_KEY_NAME,
        "privateKeyData": base64.b64encode(
            json.dumps(
                {
                    "client_email": "sa@p.iam.gserviceaccount.com",
                    "private_key_id": "new",
                    "private_key": "PK",  # pragma: allowlist secret
                }
            ).encode()
        ).decode(),
    }
    keys.list.return_value.execute.return_value = {
        "keys": [{"name": f"projects/p/serviceAccounts/sa/keys/{key_id}", **fields}]
    }
    return service


def _iam_failing(error: Exception) -> MagicMock:
    service = MagicMock()
    keys = service.projects.return_value.serviceAccounts.return_value.keys.return_value
    keys.list.return_value.execute.side_effect = error
    return service


def _http_error(status: int, reason: str = "denied") -> Exception:
    from googleapiclient.errors import HttpError
    from httplib2 import Response

    body = json.dumps({"error": {"code": status, "message": reason}}).encode()
    return HttpError(Response({"status": str(status)}), body)


def _refresh_error() -> Exception:
    from google.auth.exceptions import RefreshError

    return RefreshError("invalid_grant")


def _transport_error() -> Exception:
    from google.auth.exceptions import TransportError

    return TransportError("connection reset during token refresh")


NEW_KEY_NAME = "projects/p/serviceAccounts/sa/keys/new"


SA = "sa@p.iam.gserviceaccount.com"


def _key_check(status: str, reason: str = ""):  # type: ignore[no-untyped-def]
    from slack_chat_migrator.services.setup.service_account import KeyCheck, KeyStatus

    return KeyCheck(KeyStatus[status], reason)


class TestDownloadKey:
    """download_key reserves the file first and never leaves an unsaved key."""

    @staticmethod
    def _keys(service: MagicMock) -> MagicMock:
        return (
            service.projects.return_value.serviceAccounts.return_value.keys.return_value
        )

    def _download(self, service: MagicMock, path: Path, **kwargs: Any) -> Path:
        from slack_chat_migrator.services.setup import service_account as sa

        with patch.object(sa, "_build_iam_service", return_value=service):
            return sa.download_key(None, SA, path, **kwargs)

    def test_overwrite_makes_file_private(self, tmp_path: Path) -> None:
        import os
        import stat

        key = tmp_path / "key.json"
        key.write_text("{" + " " * 500 + "}")
        os.chmod(key, 0o644)
        self._download(_iam_with_key(), key, overwrite=True)
        assert stat.S_IMODE(key.stat().st_mode) == 0o600
        # The old, longer content is truncated, not left after the new JSON.
        written = json.loads(key.read_text())
        assert written["private_key_id"] == "new"

    def test_new_file_is_private(self, tmp_path: Path) -> None:
        import stat

        key = self._download(_iam_with_key(), tmp_path / "key.json")
        assert stat.S_IMODE(key.stat().st_mode) == 0o600

    def test_refuses_to_overwrite_by_default(self, tmp_path: Path) -> None:
        key = tmp_path / "key.json"
        key.write_text("{}")
        service = _iam_with_key()
        with pytest.raises(FileExistsError):
            self._download(service, key)
        self._keys(service).create.assert_not_called()
        assert key.read_text() == "{}"

    @pytest.mark.parametrize("where", ["directory", "missing_parent"])
    def test_unwritable_path_fails_before_a_key_is_created(
        self, tmp_path: Path, where: str
    ) -> None:
        path = tmp_path / "dir" if where == "directory" else tmp_path / "no" / "k.json"
        if where == "directory":
            path.mkdir()
        service = _iam_with_key()
        with pytest.raises(OSError):
            self._download(service, path, overwrite=True)
        self._keys(service).create.assert_not_called()

    def test_failed_create_leaves_no_file_and_keeps_an_old_one(
        self, tmp_path: Path
    ) -> None:
        from googleapiclient.errors import HttpError

        service = _iam_with_key()
        self._keys(service).create.return_value.execute.side_effect = _http_error(403)
        new = tmp_path / "new.json"
        with pytest.raises(HttpError):
            self._download(service, new)
        assert not new.exists()
        old = tmp_path / "old.json"
        old.write_text('{"old": true}')
        with pytest.raises(HttpError):
            self._download(service, old, overwrite=True)
        assert old.read_text() == '{"old": true}'

    def test_unsaved_key_is_deleted_again(self, tmp_path: Path) -> None:
        from slack_chat_migrator.services.setup.service_account import (
            KeyNotSavedError,
        )

        service = _iam_with_key()
        key = tmp_path / "key.json"
        with patch("json.dump", side_effect=OSError(28, "No space left on device")):
            with pytest.raises(KeyNotSavedError) as raised:
                self._download(service, key)
        assert raised.value.deleted is True
        assert "deleted again" in str(raised.value)
        self._keys(service).delete.assert_called_once_with(name=NEW_KEY_NAME)
        assert not key.exists()

    def test_key_that_cannot_be_deleted_is_named(self, tmp_path: Path) -> None:
        from slack_chat_migrator.services.setup.service_account import (
            KeyNotSavedError,
        )

        service = _iam_with_key()
        self._keys(service).delete.return_value.execute.side_effect = _http_error(403)
        with patch("json.dump", side_effect=OSError(28, "No space left on device")):
            with pytest.raises(KeyNotSavedError) as raised:
                self._download(service, tmp_path / "key.json")
        assert raised.value.deleted is False
        assert raised.value.key_name == NEW_KEY_NAME
        assert NEW_KEY_NAME in str(raised.value)
        assert "still active" in str(raised.value)

    def test_ctrl_c_while_saving_deletes_the_key_and_stays_ctrl_c(
        self, tmp_path: Path
    ) -> None:
        service = _iam_with_key()
        with patch("json.dump", side_effect=KeyboardInterrupt):
            with pytest.raises(KeyboardInterrupt):
                self._download(service, tmp_path / "key.json")
        self._keys(service).delete.assert_called_once_with(name=NEW_KEY_NAME)


class TestCheckExistingKey:
    """check_existing_key: reuse a key file only when IAM says it works."""

    @staticmethod
    def _key_file(tmp_path: Path, email: str = SA, key_id: str = "kid1") -> Path:
        key = tmp_path / "key.json"
        key.write_text(json.dumps({"client_email": email, "private_key_id": key_id}))
        return key

    def _check(self, key: Path, service: MagicMock):  # type: ignore[no-untyped-def]
        from datetime import datetime, timezone

        from slack_chat_migrator.services.setup import service_account as sa

        now = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)
        with patch.object(sa, "_build_iam_service", return_value=service):
            return sa.check_existing_key(None, SA, key, now=now)

    def test_listed_key_is_active(self, tmp_path: Path) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        check = self._check(self._key_file(tmp_path), _iam_with_key("kid1"))
        assert check.status is KeyStatus.ACTIVE
        assert check.reason == ""

    @pytest.mark.parametrize(
        ("listed", "reason"),
        [
            ({"key_id": "other"}, "deleted"),
            ({"disabled": True}, "disabled"),
            ({"validBeforeTime": "2026-01-01T00:00:00Z"}, "expired on 2026-01-01"),
            ({"validBeforeTime": "2026-10-17T12:00:00Z"}, "expired"),
            ({"validBeforeTime": "2026-10-17T11:59:59.123456789Z"}, "expired"),
        ],
    )
    def test_unusable_key_is_inactive(
        self, tmp_path: Path, listed: dict, reason: str
    ) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        check = self._check(self._key_file(tmp_path), _iam_with_key(**listed))
        assert check.status is KeyStatus.INACTIVE
        assert reason in check.reason

    @pytest.mark.parametrize(
        "listed",
        [
            {"disabled": False, "validBeforeTime": "2027-01-01T00:00:00Z"},
            {"validBeforeTime": "9999-12-31T23:59:59Z"},
            {"validBeforeTime": ""},
            {},
            # Clock skew: IAM's creation time can be ahead of the local clock.
            {"validAfterTime": "2026-10-17T12:30:00Z"},
        ],
    )
    def test_key_that_still_works_is_active(self, tmp_path: Path, listed: dict) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        check = self._check(self._key_file(tmp_path), _iam_with_key(**listed))
        assert check.status is KeyStatus.ACTIVE

    @pytest.mark.parametrize(
        "fraction", ["", ".1", ".12", ".123", ".12345", ".123456", ".123456789"]
    )
    def test_expiry_with_any_fraction_length_parses(
        self, tmp_path: Path, fraction: str
    ) -> None:
        """Python 3.10's fromisoformat() takes only 0, 3 or 6 digits."""
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        expired = _iam_with_key(validBeforeTime=f"2026-10-17T11:59:58{fraction}Z")
        valid = _iam_with_key(validBeforeTime=f"2026-10-17T12:00:01{fraction}Z")
        key = self._key_file(tmp_path)
        assert self._check(key, expired).status is KeyStatus.INACTIVE
        assert self._check(key, valid).status is KeyStatus.ACTIVE

    @pytest.mark.parametrize("value", ["tomorrow", "2026-13-45T00:00:00Z", 12])
    def test_unparseable_expiry_is_unknown(self, tmp_path: Path, value: Any) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        check = self._check(
            self._key_file(tmp_path), _iam_with_key(validBeforeTime=value)
        )
        assert check.status is KeyStatus.UNKNOWN
        assert "couldn't be parsed" in check.reason

    def test_files_that_are_not_keys_of_the_account(self, tmp_path: Path) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        service = _iam_with_key("kid1")
        other = self._check(self._key_file(tmp_path, email="x@else.com"), service)
        assert other.status is KeyStatus.NOT_A_KEY
        assert "x@else.com" in other.reason
        bad = tmp_path / "bad.json"
        bad.write_text("not json")
        not_json = self._check(bad, service)
        assert not_json.status is KeyStatus.NOT_A_KEY
        assert "isn't valid JSON" in not_json.reason
        bad.write_bytes(b"\xff\xfe binary")
        assert self._check(bad, service).status is KeyStatus.NOT_A_KEY
        bad.write_text(json.dumps({"client_email": SA}))
        assert self._check(bad, service).status is KeyStatus.NOT_A_KEY

    def test_directory_is_unreadable(self, tmp_path: Path) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        service = _iam_with_key("kid1")
        check = self._check(tmp_path, service)
        assert check.status is KeyStatus.UNREADABLE
        assert "can't be read" in check.reason
        self._keys_list(service).assert_not_called()

    @pytest.mark.parametrize(
        "error",
        [PermissionError(13, "Permission denied"), OSError(5, "Input/output error")],
    )
    def test_read_errors_are_unreadable(self, tmp_path: Path, error: OSError) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        key = self._key_file(tmp_path)
        with patch.object(Path, "read_text", side_effect=error):
            check = self._check(key, _iam_with_key("kid1"))
        assert check.status is KeyStatus.UNREADABLE
        assert str(error.strerror) in check.reason

    @staticmethod
    def _keys_list(service: MagicMock) -> MagicMock:
        keys = (
            service.projects.return_value.serviceAccounts.return_value.keys.return_value
        )
        return keys.list

    @pytest.mark.parametrize(
        ("error", "text"),
        [
            (_http_error(403), "HTTP 403"),
            (ConnectionResetError("reset"), "couldn't be reached"),
            (OSError(101, "Network is unreachable"), "couldn't be reached"),
            (
                httplib2.ServerNotFoundError(
                    "Unable to find the server at iam.googleapis.com"
                ),
                "couldn't be reached",
            ),
            (http.client.IncompleteRead(b"partial"), "couldn't be reached"),
            (http.client.BadStatusLine("garbage"), "couldn't be reached"),
            (_transport_error(), "couldn't be reached"),
            (_refresh_error(), "application-default login"),
        ],
    )
    def test_failed_listing_is_unknown_not_inactive(
        self, tmp_path: Path, error: Exception, text: str
    ) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyStatus

        check = self._check(self._key_file(tmp_path), _iam_failing(error))
        assert check.status is KeyStatus.UNKNOWN
        assert text in check.reason

    @pytest.mark.parametrize("status", [400, 401, 403])
    def test_permission_errors_point_at_iam_permission(
        self, tmp_path: Path, status: int
    ) -> None:
        error = _http_error(status)
        check = self._check(self._key_file(tmp_path), _iam_failing(error))
        assert "IAM API is enabled" in check.reason
        assert "iam.serviceAccountKeys.list" in check.reason

    def test_404_says_the_service_account_does_not_exist(self, tmp_path: Path) -> None:
        error = _http_error(404, "Service account does not exist.")
        check = self._check(self._key_file(tmp_path), _iam_failing(error))
        assert f"The service account {SA} doesn't exist" in check.reason
        assert "IAM API is enabled" not in check.reason

    def test_programming_errors_are_not_swallowed(self, tmp_path: Path) -> None:
        with pytest.raises(TypeError):
            self._check(self._key_file(tmp_path), _iam_failing(TypeError("bug")))


class TestKeyCheck:
    """KeyCheck: a reason exactly when the key isn't active."""

    @pytest.mark.parametrize(
        ("status", "reason"),
        [
            ("ACTIVE", "boom"),
            ("INACTIVE", ""),
            ("NOT_A_KEY", ""),
            ("UNREADABLE", ""),
            ("UNKNOWN", ""),
        ],
    )
    def test_reason_iff_not_active(self, status: str, reason: str) -> None:
        from slack_chat_migrator.services.setup.service_account import (
            KeyCheck,
            KeyStatus,
        )

        with pytest.raises(ValueError, match="reason"):
            KeyCheck(KeyStatus[status], reason)

    def test_status_must_be_a_key_status(self) -> None:
        from slack_chat_migrator.services.setup.service_account import KeyCheck

        with pytest.raises(ValueError, match="KeyStatus"):
            KeyCheck("active")  # type: ignore[arg-type]


class TestKeyStep:
    """The wizard's key step acts on each KeyCheck status."""

    def _run_key_step(
        self,
        tmp_path: Path,
        check,  # type: ignore[no-untyped-def]
        confirm_answer: bool = False,
        download_error: Exception | None = None,
    ):  # type: ignore[no-untyped-def]
        from slack_chat_migrator.cli.setup_cmd import _step_download_key

        key = tmp_path / "key.json"
        key.write_text("{}")
        state = SetupState(project_id="p", service_account_email=SA)
        with (
            patch(
                "slack_chat_migrator.cli.setup_cmd.click.prompt", return_value=str(key)
            ),
            patch(
                "slack_chat_migrator.cli.setup_cmd.click.confirm",
                return_value=confirm_answer,
            ) as confirm,
            patch(
                "slack_chat_migrator.services.setup.service_account.check_existing_key",
                return_value=check,
            ),
            patch(
                "slack_chat_migrator.services.setup.service_account.download_key",
                return_value=key,
                side_effect=download_error,
            ) as download,
        ):
            state = _step_download_key(MagicMock(), state)
        return state, key, confirm, download

    def test_reuses_valid_key(self, tmp_path: Path) -> None:
        state, key, confirm, download = self._run_key_step(
            tmp_path, _key_check("ACTIVE")
        )
        confirm.assert_not_called()
        download.assert_not_called()
        assert state.key_path == str(key)
        assert state.step_status("key") == StepStatus.COMPLETE

    def test_asks_before_overwriting_an_inactive_key(self, tmp_path: Path) -> None:
        state, key, confirm, download = self._run_key_step(
            tmp_path, _key_check("INACTIVE", "it is disabled")
        )
        assert "it is disabled" in confirm.call_args.args[0]
        download.assert_not_called()
        assert state.step_status("key") == StepStatus.SKIPPED
        assert key.read_text() == "{}"

    def test_never_overwrites_a_key_it_could_not_check(
        self, tmp_path: Path, capsys
    ) -> None:
        state, key, confirm, download = self._run_key_step(
            tmp_path, _key_check("UNKNOWN", "listing the keys failed with HTTP 403.")
        )
        confirm.assert_not_called()
        download.assert_not_called()
        assert key.read_text() == "{}"
        assert state.step_status("key") == StepStatus.SKIPPED
        assert state.key_path == str(key)
        out = capsys.readouterr().out
        assert "Couldn't check" in out
        assert "HTTP 403" in out
        assert "Kept the existing file unchanged" in out

    def test_never_overwrites_a_file_it_could_not_read(
        self, tmp_path: Path, capsys
    ) -> None:
        state, key, confirm, download = self._run_key_step(
            tmp_path, _key_check("UNREADABLE", "it can't be read (Permission denied)")
        )
        confirm.assert_not_called()
        download.assert_not_called()
        assert key.read_text() == "{}"
        assert state.step_status("key") == StepStatus.SKIPPED
        assert state.key_path is None
        out = capsys.readouterr().out
        assert "Can't read" in out
        assert "Permission denied" in out

    def test_names_another_accounts_key(self, tmp_path: Path) -> None:
        state, _, confirm, download = self._run_key_step(
            tmp_path,
            _key_check("NOT_A_KEY", "it is a key of x@else.com"),
            confirm_answer=True,
        )
        question = confirm.call_args.args[0]
        assert "isn't a key for" in question
        assert "x@else.com" in question
        assert download.call_args.kwargs["overwrite"] is True
        assert state.step_status("key") == StepStatus.COMPLETE

    def test_reports_a_key_left_in_iam(self, tmp_path: Path, capsys) -> None:
        from slack_chat_migrator.services.setup.service_account import (
            KeyNotSavedError,
        )

        error = KeyNotSavedError(f"delete {NEW_KEY_NAME} by hand", NEW_KEY_NAME, False)
        state, _, _, _ = self._run_key_step(
            tmp_path,
            _key_check("INACTIVE", "it was deleted"),
            confirm_answer=True,
            download_error=error,
        )
        out = capsys.readouterr().out
        assert NEW_KEY_NAME in out
        assert "No key was created" not in out
        assert state.step_status("key") == StepStatus.SKIPPED

    def test_failed_download_says_no_key_was_created(
        self, tmp_path: Path, capsys
    ) -> None:
        state, _, _, _ = self._run_key_step(
            tmp_path,
            _key_check("INACTIVE", "it was deleted"),
            confirm_answer=True,
            download_error=IsADirectoryError(21, "Is a directory"),
        )
        assert "No key was created" in capsys.readouterr().out
        assert state.step_status("key") == StepStatus.SKIPPED
