"""CLI-level validation tests using ``click.testing.CliRunner``.

The ``validate`` subcommand always runs in dry-run mode, so these tests
need no Google credentials.  They verify that the CLI surfaces clear
errors for malformed or missing export data.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner, Result

from slack_chat_migrator.cli.commands import cli
from tests.integration.conftest import GENERAL_CHANNEL, USERS

pytestmark = pytest.mark.integration


def _invoke_validate(
    tmp_path: Path,
    export_path: str | Path,
    extra_args: list[str] | None = None,
    creds_text: str | None = None,
) -> Result:
    """Invoke ``validate`` via CliRunner with sensible defaults.

    Without *creds_text* no credentials are passed (data validation only).
    With it, a ``creds.json`` with that content is passed together with
    ``--workspace_admin``, which makes ``validate`` run the real
    permission checks.

    Uses ``catch_exceptions=True`` so that ``sys.exit(1)`` from the CLI
    error handler is captured as a non-zero exit code rather than
    raising ``SystemExit``.
    """
    config_file = tmp_path / "config.yaml"
    if not config_file.exists():
        config_file.write_text("{}")

    args = [
        "validate",
        "--export_path",
        str(export_path),
        "--config",
        str(tmp_path / "config.yaml"),
    ]
    if creds_text is not None:
        creds_file = tmp_path / "creds.json"
        creds_file.write_text(creds_text)
        args += [
            "--creds_path",
            str(creds_file),
            "--workspace_admin",
            "admin@example.com",
        ]
    if extra_args:
        args.extend(extra_args)

    runner = CliRunner()
    return runner.invoke(cli, args, catch_exceptions=True)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestValidateValidExport:
    """validate subcommand exits 0 with a valid export."""

    def test_exit_code_zero(self, single_channel_export: Path) -> None:
        result = _invoke_validate(single_channel_export, single_channel_export)
        assert result.exit_code == 0, result.output


class TestValidateMissingUsersJson:
    """validate exits non-zero when users.json is absent."""

    def test_exit_code_nonzero(self, tmp_path: Path) -> None:
        # Create minimal export WITHOUT users.json
        (tmp_path / "channels.json").write_text(json.dumps([GENERAL_CHANNEL]))
        (tmp_path / "config.yaml").write_text("{}")
        ch_dir = tmp_path / "general"
        ch_dir.mkdir()

        result = _invoke_validate(tmp_path, tmp_path)
        assert result.exit_code != 0


class TestValidateMissingChannelsJson:
    """validate still runs (with warning) when channels.json is missing.

    The migrator warns but does not abort when channels.json is absent —
    it falls back to discovering channels from directories.
    """

    def test_runs_without_crash(self, tmp_path: Path) -> None:
        # Create export with users.json but NO channels.json
        (tmp_path / "users.json").write_text(json.dumps(USERS))
        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        # Put a dummy message so the channel dir has a JSON file
        (ch_dir / "2021-01-01.json").write_text(json.dumps([]))

        result = _invoke_validate(tmp_path, tmp_path)
        # Should still succeed — missing channels.json is non-fatal
        assert result.exit_code == 0, result.output


class TestValidateEmptyExportDir:
    """validate exits non-zero on an empty directory."""

    def test_exit_code_nonzero(self, tmp_path: Path) -> None:
        # Separate export dir so creds.json written by _invoke_validate
        # doesn't pollute the "empty" export directory.
        export_dir = tmp_path / "export"
        export_dir.mkdir()
        (tmp_path / "config.yaml").write_text("{}")
        result = _invoke_validate(tmp_path, export_dir)
        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Permission checks (issue #109)
# ---------------------------------------------------------------------------


class TestValidatePermissionChecks:
    """With credentials, validate checks permissions against the real APIs."""

    def test_without_credentials_permissions_are_skipped(
        self, single_channel_export: Path
    ) -> None:
        result = _invoke_validate(single_channel_export, single_channel_export)
        assert result.exit_code == 0, result.output
        assert "Permission checks skipped: no credentials" in result.output
        assert "Permission checks passed" not in result.output

    def test_invalid_key_file_fails(self, single_channel_export: Path) -> None:
        result = _invoke_validate(
            single_channel_export,
            single_channel_export,
            creds_text="this is not a service account key",
        )
        assert result.exit_code == 1, result.output
        assert "Invalid credential file format" in result.output
        assert "Permission checks passed" not in result.output

    def test_valid_credentials_use_real_services(
        self, single_channel_export: Path
    ) -> None:
        """The checks go through get_gcp_service (real clients), not dry-run fakes."""
        from unittest.mock import MagicMock, patch

        services: dict[str, MagicMock] = {}

        def fake_service(creds_path, user, api, version, **kwargs):
            services[api] = MagicMock(name=f"{api}-service")
            services[
                api
            ].spaces.return_value.create.return_value.execute.return_value = {
                "name": "spaces/perm-test"
            }
            services[
                api
            ].files.return_value.create.return_value.execute.return_value = {
                "id": "file-1"
            }
            return services[api]

        with patch(
            "slack_chat_migrator.utils.permissions.get_gcp_service",
            side_effect=fake_service,
        ):
            result = _invoke_validate(
                single_channel_export, single_channel_export, creds_text="{}"
            )

        assert result.exit_code == 0, result.output
        assert set(services) == {"chat", "drive"}
        services["chat"].spaces.return_value.create.assert_called_once()
        services["drive"].files.return_value.create.assert_called_once()
        assert "Permission checks passed" in result.output

    def test_failed_real_check_fails_validate(
        self, single_channel_export: Path
    ) -> None:
        from unittest.mock import MagicMock, patch

        import httplib2
        from googleapiclient.errors import HttpError

        def fake_service(creds_path, user, api, version, **kwargs):
            svc = MagicMock(name=f"{api}-service")
            resp = httplib2.Response({"status": 403})
            resp.reason = "Forbidden"
            svc.spaces.return_value.create.return_value.execute.side_effect = HttpError(
                resp, b'{"error": {"message": "insufficient authentication scopes"}}'
            )
            return svc

        with patch(
            "slack_chat_migrator.utils.permissions.get_gcp_service",
            side_effect=fake_service,
        ):
            result = _invoke_validate(
                single_channel_export, single_channel_export, creds_text="{}"
            )

        assert result.exit_code == 1, result.output
        assert "Permission checks failed" in result.output
