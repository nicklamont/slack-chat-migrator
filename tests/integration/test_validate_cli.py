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
) -> Result:
    """Invoke ``validate`` via CliRunner with sensible defaults.

    A dummy ``creds.json`` is created inside *tmp_path* so the
    prerequisite check for the credentials file passes.

    Uses ``catch_exceptions=True`` so that ``sys.exit(1)`` from the CLI
    error handler is captured as a non-zero exit code rather than
    raising ``SystemExit``.
    """
    creds_file = tmp_path / "creds.json"
    if not creds_file.exists():
        creds_file.write_text("{}")

    args = [
        "validate",
        "--creds_path",
        str(creds_file),
        "--export_path",
        str(export_path),
        "--workspace_admin",
        "admin@example.com",
        "--config",
        str(tmp_path / "config.yaml"),
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
        result = _invoke_validate(tmp_path, export_dir)
        assert result.exit_code != 0
