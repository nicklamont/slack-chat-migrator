"""Exit codes and non-interactive behaviour of migrate/validate (issue #110).

These drive the real CLI on generated exports.  Live runs never reach the
Google APIs: they stop at a prompt, or the final migration step is stubbed.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner, Result

from slack_chat_migrator.cli.commands import cli
from slack_chat_migrator.cli.migrate_cmd import MigrationOrchestrator
from slack_chat_migrator.services.spaces.space_creator import ImportCompletionResult
from tests.integration.conftest import (
    GENERAL_CHANNEL,
    RANDOM_CHANNEL,
    USERS,
    build_export,
    make_messages,
)

pytestmark = pytest.mark.integration


@contextlib.contextmanager
def _interactive(value: bool) -> Iterator[None]:
    """Pretend stdin is (or isn't) an interactive terminal."""
    with (
        patch(
            "slack_chat_migrator.cli.common.stdin_is_interactive", return_value=value
        ),
        patch(
            "slack_chat_migrator.cli.migrate_cmd.stdin_is_interactive",
            return_value=value,
        ),
        patch(
            "slack_chat_migrator.cli.validate_cmd.stdin_is_interactive",
            return_value=value,
        ),
    ):
        yield


#: A user with no email who posts in #general, so validation finds an
#: unmapped user.
UNMAPPED_USER = {
    "id": "U009",
    "name": "nomail",
    "real_name": "No Mail",
    "profile": {"real_name": "No Mail"},
    "is_bot": False,
    "deleted": False,
}


def _export(tmp_path: Path, *, unmapped: bool = False) -> Path:
    export = tmp_path / "export"
    export.mkdir()
    messages = make_messages(3)
    if unmapped:
        messages += make_messages(1, user="U009", start_ts=1609460000.0)
    return build_export(
        export,
        users=[*USERS, UNMAPPED_USER] if unmapped else USERS,
        channels=[GENERAL_CHANNEL, RANDOM_CHANNEL],
        messages_by_channel={"general": messages, "random": make_messages(1)},
    )


def _config(tmp_path: Path, text: str = "exclude_channels: []\n") -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return path


def _live(
    tmp_path: Path,
    export: Path,
    *extra: str,
    stdin: str = "",
    interactive: bool = True,
    config_text: str = "exclude_channels: []\n",
) -> Result:
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    args = [
        "migrate",
        "--export_path",
        str(export),
        "--config",
        str(_config(tmp_path, config_text)),
        "--creds_path",
        str(creds),
        "--workspace_admin",
        "admin@example.com",
        "--skip_permission_check",
        *extra,
    ]
    with (
        _interactive(interactive),
        patch("slack_chat_migrator.cli.migrate_cmd.run_cleanup") as mock_cleanup,
    ):
        result = CliRunner().invoke(cli, args, input=stdin)
    mock_cleanup.assert_not_called()
    return result


def _dry_run(tmp_path: Path, export: Path, config_text: str, **kwargs: Any) -> Result:
    from slack_chat_migrator.core.migrator import SlackToChatMigrator

    schedule = kwargs.pop("message_error_schedule", None)

    class Injected(SlackToChatMigrator):
        def __init__(self, *a: Any, **kw: Any) -> None:
            kw.setdefault("message_error_schedule", schedule)
            super().__init__(*a, **kw)

    args = [
        "migrate",
        "--dry_run",
        "--export_path",
        str(export),
        "--config",
        str(_config(tmp_path, config_text)),
    ]
    with patch("slack_chat_migrator.cli.migrate_cmd.SlackToChatMigrator", Injected):
        return CliRunner().invoke(cli, args)


class TestLivePrompts:
    """Declining a prompt, or having no one to answer it, exits 1."""

    def test_decline_unmapped_prompt_exits_1(self, tmp_path: Path) -> None:
        result = _live(tmp_path, _export(tmp_path, unmapped=True), stdin="n\n")
        assert result.exit_code == 1, result.output
        assert "Migration cancelled" in result.output

    def test_decline_final_confirmation_exits_1(self, tmp_path: Path) -> None:
        result = _live(tmp_path, _export(tmp_path), stdin="n\n")
        assert result.exit_code == 1, result.output
        assert "Migration cancelled" in result.output

    def test_eof_at_prompt_exits_1(self, tmp_path: Path) -> None:
        result = _live(tmp_path, _export(tmp_path), stdin="")
        assert result.exit_code == 1, result.output

    def test_no_tty_without_yes_exits_1_before_doing_anything(
        self, tmp_path: Path
    ) -> None:
        with patch.object(MigrationOrchestrator, "run_validation") as validation:
            result = _live(tmp_path, _export(tmp_path), interactive=False)
        assert result.exit_code == 1, result.output
        assert "--yes" in result.output
        validation.assert_not_called()

    def test_yes_runs_without_prompts(self, tmp_path: Path) -> None:
        """--yes answers every prompt; the live step itself is stubbed."""
        with (
            patch("click.confirm", side_effect=AssertionError("prompted")),
            patch.object(
                MigrationOrchestrator, "_run_with_progress", return_value=True
            ),
            patch.object(MigrationOrchestrator, "_print_summary"),
            patch(
                "slack_chat_migrator.cli.migrate_cmd.run_cleanup",
                return_value=ImportCompletionResult(),
            ) as cleanup,
            _interactive(False),
        ):
            creds = tmp_path / "creds.json"
            creds.write_text("{}")
            result = CliRunner().invoke(
                cli,
                [
                    "migrate",
                    "--export_path",
                    str(_export(tmp_path, unmapped=True)),
                    "--config",
                    str(_config(tmp_path)),
                    "--creds_path",
                    str(creds),
                    "--workspace_admin",
                    "admin@example.com",
                    "--skip_permission_check",
                    "--yes",
                ],
            )
        assert result.exit_code == 0, result.output
        # The live run finished, so the post-migration completion ran
        cleanup.assert_called_once()

    def test_failed_post_migration_completion_exits_1(self, tmp_path: Path) -> None:
        failed = ImportCompletionResult(failed={"spaces/a": "HTTP 500"})
        with (
            patch.object(
                MigrationOrchestrator, "_run_with_progress", return_value=True
            ),
            patch.object(MigrationOrchestrator, "_print_summary"),
            patch(
                "slack_chat_migrator.cli.migrate_cmd.run_cleanup", return_value=failed
            ),
            _interactive(False),
        ):
            creds = tmp_path / "creds.json"
            creds.write_text("{}")
            result = CliRunner().invoke(
                cli,
                [
                    "migrate",
                    "--export_path",
                    str(_export(tmp_path)),
                    "--config",
                    str(_config(tmp_path)),
                    "--creds_path",
                    str(creds),
                    "--workspace_admin",
                    "admin@example.com",
                    "--skip_permission_check",
                    "--yes",
                ],
            )
        assert result.exit_code == 1, result.output
        assert "spaces/a" in result.output

    def test_live_run_with_errors_exits_1(self, tmp_path: Path) -> None:
        with (
            patch.object(
                MigrationOrchestrator, "_run_with_progress", return_value=False
            ),
            patch.object(MigrationOrchestrator, "_print_summary"),
            patch(
                "slack_chat_migrator.cli.migrate_cmd.run_cleanup",
                return_value=ImportCompletionResult(),
            ),
            _interactive(False),
        ):
            creds = tmp_path / "creds.json"
            creds.write_text("{}")
            result = CliRunner().invoke(
                cli,
                [
                    "migrate",
                    "--export_path",
                    str(_export(tmp_path)),
                    "--config",
                    str(_config(tmp_path)),
                    "--creds_path",
                    str(creds),
                    "--workspace_admin",
                    "admin@example.com",
                    "--skip_permission_check",
                    "--yes",
                ],
            )
        assert result.exit_code == 1, result.output

    def test_ctrl_c_exits_130(self, tmp_path: Path) -> None:
        with patch.object(
            MigrationOrchestrator, "run_validation", side_effect=KeyboardInterrupt
        ):
            result = _live(tmp_path, _export(tmp_path), "--yes")
        assert result.exit_code == 130, result.output

    def test_usage_error_exits_2(self, tmp_path: Path) -> None:
        result = CliRunner().invoke(
            cli, ["migrate", "--export_path", str(_export(tmp_path))]
        )
        assert result.exit_code == 2
        assert "--creds_path is required" in result.output


class TestDryRunExitCodes:
    """Dry runs and validate exit 1 when they find problems."""

    def test_clean_dry_run_exits_0(self, tmp_path: Path) -> None:
        result = _dry_run(tmp_path, _export(tmp_path), "exclude_channels: []\n")
        assert result.exit_code == 0, result.output

    def test_failed_message_exits_1_and_reports_channel(self, tmp_path: Path) -> None:
        result = _dry_run(
            tmp_path,
            _export(tmp_path),
            "exclude_channels: []\n",
            message_error_schedule={1: 400},
        )
        assert result.exit_code == 1, result.output
        assert "Channels with errors: 1" in result.output
        assert "COMPLETED WITH ERRORS" in result.output
        assert "COMPLETED SUCCESSFULLY" not in result.output

    def test_abort_on_error_exits_1(self, tmp_path: Path) -> None:
        result = _dry_run(
            tmp_path,
            _export(tmp_path),
            "abort_on_error: true\n",
            message_error_schedule={1: 400},
        )
        assert result.exit_code == 1, result.output
        assert "aborted" in result.output.lower()
        assert "COMPLETED SUCCESSFULLY" not in result.output

    def test_validate_with_unmapped_users_exits_1(self, tmp_path: Path) -> None:
        export = _export(tmp_path, unmapped=True)
        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--export_path",
                str(export),
                "--config",
                str(_config(tmp_path)),
            ],
        )
        assert result.exit_code == 1, result.output

    def test_validate_clean_export_exits_0(self, tmp_path: Path) -> None:
        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--export_path",
                str(_export(tmp_path)),
                "--config",
                str(_config(tmp_path)),
            ],
        )
        assert result.exit_code == 0, result.output

    def test_validate_missing_config_non_interactive_exits_1(
        self, tmp_path: Path
    ) -> None:
        result = CliRunner().invoke(
            cli,
            [
                "validate",
                "--export_path",
                str(_export(tmp_path)),
                "--config",
                str(tmp_path / "missing.yaml"),
            ],
        )
        assert result.exit_code == 1
