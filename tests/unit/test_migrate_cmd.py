"""Tests for migrate_cmd: --complete, cleanup gating and confirmations."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import httplib2
import pytest
from click.testing import CliRunner
from googleapiclient.errors import HttpError

from slack_chat_migrator.cli.commands import cli
from slack_chat_migrator.cli.common import confirm_action
from slack_chat_migrator.cli.migrate_cmd import MigrationOrchestrator
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.exceptions import (
    ConfirmationRequiredError,
    PermissionCheckError,
)

INTERACTIVE = "slack_chat_migrator.cli.common.stdin_is_interactive"


def _http_error(status: int) -> HttpError:
    resp = httplib2.Response({"status": status})
    resp.reason = "error"
    return HttpError(resp, b"{}")


class FakeChat:
    """A ChatAdapter stand-in listing a few spaces."""

    def __init__(
        self, spaces: list[dict[str, Any]], fail: dict[str, int] | None = None
    ):
        self.spaces = spaces
        self.fail = fail or {}
        self.completed: list[str] = []

    def list_spaces(self, page_size: int = 100, page_token: str | None = None) -> dict:
        return {"spaces": self.spaces}

    def get_space(self, name: str) -> dict:
        return next(s for s in self.spaces if s["name"] == name)

    def complete_import(self, name: str) -> dict:
        if name in self.fail:
            raise _http_error(self.fail[name])
        self.completed.append(name)
        return {}

    def patch_space(self, **kwargs: Any) -> dict:
        return {}


SPACES = [
    {"name": "spaces/run", "displayName": "Slack #general", "importMode": True},
    {"name": "spaces/eng", "displayName": "Slack #eng", "importMode": True},
    {"name": "spaces/done", "displayName": "Slack #random", "importMode": False},
    {"name": "spaces/perm", "displayName": "Permission Test Space", "importMode": True},
    {"name": "spaces/other", "displayName": "Other tool", "importMode": True},
]

COMPLETE_ARGS = [
    "migrate",
    "--export_path",
    "unused",
    "--complete",
    "--creds_path",
    "fake.json",
    "--workspace_admin",
    "admin@example.com",
]


def _run_complete(
    fake: FakeChat, args: list[str], *, interactive: bool = False, stdin: str = ""
) -> Any:
    with (
        patch("slack_chat_migrator.utils.api.get_gcp_service", return_value=object()),
        patch(
            "slack_chat_migrator.services.chat_adapter.ChatAdapter", lambda raw: fake
        ),
        patch(INTERACTIVE, return_value=interactive),
    ):
        return CliRunner().invoke(cli, args, input=stdin)


class TestMigrateComplete:
    """migrate --complete only touches this tool's spaces, after confirmation."""

    def test_yes_completes_only_prefixed_import_mode_spaces(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES])
        result = _run_complete(fake, [*COMPLETE_ARGS, "--yes"])
        assert result.exit_code == 0, result.output
        assert sorted(fake.completed) == ["spaces/eng", "spaces/run"]
        assert "Completed import mode on 2 space(s)" in result.output

    def test_prompt_lists_spaces_and_decline_changes_nothing(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES])
        result = _run_complete(fake, COMPLETE_ARGS, interactive=True, stdin="n\n")
        assert result.exit_code == 1
        assert "Slack #general" in result.output
        assert "Slack #eng" in result.output
        assert "Permission Test Space" not in result.output
        assert "irreversible" in result.output
        assert fake.completed == []

    def test_prompt_accept_completes(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES])
        result = _run_complete(fake, COMPLETE_ARGS, interactive=True, stdin="y\n")
        assert result.exit_code == 0, result.output
        assert sorted(fake.completed) == ["spaces/eng", "spaces/run"]

    def test_non_interactive_without_yes_fails(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES])
        result = _run_complete(fake, COMPLETE_ARGS, interactive=False)
        assert result.exit_code == 1
        assert "--yes" in result.output
        assert fake.completed == []

    def test_env_var_counts_as_yes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SLACK_CHAT_MIGRATOR_YES", "1")
        fake = FakeChat([dict(s) for s in SPACES])
        result = _run_complete(fake, COMPLETE_ARGS)
        assert result.exit_code == 0, result.output
        assert len(fake.completed) == 2

    def test_failed_completions_listed_and_exit_non_zero(self) -> None:
        fake = FakeChat(
            [dict(s) for s in SPACES], fail={"spaces/run": 500, "spaces/eng": 403}
        )
        result = _run_complete(fake, [*COMPLETE_ARGS, "--yes"])
        assert result.exit_code == 1
        assert "Failed to complete 2 space(s)" in result.output
        assert "spaces/run" in result.output and "spaces/eng" in result.output
        assert "completed successfully" not in result.output.lower()

    def test_partial_failure_exit_non_zero(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES], fail={"spaces/eng": 500})
        result = _run_complete(fake, [*COMPLETE_ARGS, "--yes"])
        assert result.exit_code == 1
        assert "Completed import mode on 1 space(s)" in result.output
        assert "spaces/eng" in result.output

    def test_export_path_not_needed(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES])
        args = [a for a in COMPLETE_ARGS if a not in ("--export_path", "unused")]
        result = _run_complete(fake, [*args, "--yes"])
        assert result.exit_code == 0, result.output

    def test_export_path_still_required_without_complete(self) -> None:
        result = CliRunner().invoke(cli, ["migrate", "--dry_run"])
        assert result.exit_code == 2
        assert "Missing option '--export_path'" in result.output

    def test_nothing_to_do_exits_zero(self) -> None:
        fake = FakeChat([SPACES[2], SPACES[3]])
        result = _run_complete(fake, COMPLETE_ARGS)
        assert result.exit_code == 0, result.output
        assert "No spaces named 'Slack #...' are in import mode" in result.output

    def test_listing_failure_exits_non_zero(self) -> None:
        fake = FakeChat([])
        fake.list_spaces = MagicMock(side_effect=_http_error(503))  # type: ignore[method-assign]
        result = _run_complete(fake, [*COMPLETE_ARGS, "--yes"])
        assert result.exit_code == 1

    def test_deprecated_cleanup_uses_same_flow(self) -> None:
        fake = FakeChat([dict(s) for s in SPACES])
        with (
            patch(
                "slack_chat_migrator.cli.cleanup_cmd.get_gcp_service",
                return_value=object(),
            ),
            patch("slack_chat_migrator.cli.cleanup_cmd.ChatAdapter", lambda raw: fake),
            patch(INTERACTIVE, return_value=False),
        ):
            result = CliRunner().invoke(
                cli,
                [
                    "cleanup",
                    "--creds_path",
                    "f.json",
                    "--workspace_admin",
                    "a@b.com",
                    "-y",
                ],
            )
        assert result.exit_code == 0, result.output
        assert sorted(fake.completed) == ["spaces/eng", "spaces/run"]


class TestConfirmAction:
    """The shared confirmation helper."""

    def test_assume_yes_skips_prompt(self) -> None:
        with patch("click.confirm") as prompt:
            assert confirm_action("Go?", assume_yes=True) is True
        prompt.assert_not_called()

    def test_non_interactive_raises(self) -> None:
        with patch(INTERACTIVE, return_value=False):
            with pytest.raises(ConfirmationRequiredError, match="--yes"):
                confirm_action("Go?", assume_yes=False)

    def test_eof_at_prompt_is_a_no(self) -> None:
        import click

        with (
            patch(INTERACTIVE, return_value=True),
            patch("click.confirm", side_effect=click.Abort()),
        ):
            assert confirm_action("Go?", assume_yes=False) is False


# ---------------------------------------------------------------------------
# Cleanup gating (issue #85)
# ---------------------------------------------------------------------------


def _orchestrator(dry_run: bool = False) -> MigrationOrchestrator:
    args = SimpleNamespace(
        creds_path="c.json",
        export_path="exp",
        workspace_admin="a@b.com",
        config="config.yaml",
        config_required=False,
        verbose=False,
        debug_api=False,
        dry_run=dry_run,
        update_mode=False,
        skip_permission_check=True,
        assume_yes=True,
    )
    orch = MigrationOrchestrator(args)
    orch.migrator = MagicMock()
    orch.migrator.state.spaces.created_spaces = {"general": "spaces/run"}
    return orch


@patch("slack_chat_migrator.cli.migrate_cmd.cleanup_channel_handlers")
@patch("slack_chat_migrator.cli.migrate_cmd.run_cleanup")
class TestCleanupGating:
    """Import completion runs only after a finished live migration."""

    def test_runs_after_finished_live_migration(self, mock_run, mock_handlers) -> None:
        orch = _orchestrator()
        orch.live_migration_finished = True
        orch.cleanup()
        mock_run.assert_called_once()
        mock_handlers.assert_called_once()

    def test_not_run_when_migration_did_not_finish(
        self, mock_run, mock_handlers
    ) -> None:
        orch = _orchestrator()
        orch.cleanup()
        mock_run.assert_not_called()
        # Channel log handlers are always closed
        mock_handlers.assert_called_once()

    def test_not_run_in_dry_run(self, mock_run, mock_handlers) -> None:
        orch = _orchestrator(dry_run=True)
        orch.live_migration_finished = True
        orch.cleanup()
        mock_run.assert_not_called()


def _live_cli(tmp_path: Any, stdin: str = "", extra: list[str] | None = None) -> Any:
    creds = tmp_path / "creds.json"
    creds.write_text("{}")
    return CliRunner().invoke(
        cli,
        [
            "migrate",
            "--export_path",
            str(tmp_path),
            "--creds_path",
            str(creds),
            "--workspace_admin",
            "admin@example.com",
            "--skip_permission_check",
            *(extra or []),
        ],
        input=stdin,
    )


@patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
@patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
@patch("slack_chat_migrator.cli.migrate_cmd.run_cleanup")
class TestLiveExitsNeverCompleteImports:
    """Cancel, Ctrl-C, exceptions and permission failures make no completeImport."""

    @staticmethod
    def _stub_prereqs(orch: MigrationOrchestrator) -> None:
        orch.migrator = MagicMock()
        orch.migrator.state.spaces.created_spaces = {"general": "spaces/partial"}

    def _patch(self, **behaviour: Any) -> Any:
        def validate(orch: MigrationOrchestrator) -> None:
            self._stub_prereqs(orch)
            if "validate" in behaviour:
                raise behaviour["validate"]

        def run(orch: MigrationOrchestrator) -> int:
            if "run" in behaviour:
                raise behaviour["run"]
            return behaviour.get("code", 0)

        return (
            patch.object(MigrationOrchestrator, "validate_prerequisites", validate),
            patch.object(MigrationOrchestrator, "run_migration", run),
        )

    @pytest.mark.parametrize(
        ("behaviour", "exit_code"),
        [
            ({"run": KeyboardInterrupt()}, 130),
            ({"run": RuntimeError("boom")}, 1),
            ({"validate": PermissionCheckError("missing scope")}, 1),
            ({"code": 1}, 1),  # cancelled at a prompt
        ],
        ids=["ctrl-c", "exception", "permission-check", "cancel"],
    )
    def test_no_completion(
        self, mock_run, mock_logger, mock_warn, tmp_path, behaviour, exit_code
    ) -> None:
        p1, p2 = self._patch(**behaviour)
        with p1, p2:
            result = _live_cli(tmp_path)
        assert result.exit_code == exit_code
        mock_run.assert_not_called()


class TestConfigForGating:
    """Sanity: the orchestrator hands its loaded config to the migrator."""

    def test_load_config_once(self, tmp_path: Any) -> None:
        orch = _orchestrator()
        orch.args.config = str(tmp_path / "missing.yaml")
        cfg = orch.load_config()
        assert isinstance(cfg, MigrationConfig)
        assert orch.load_config() is cfg
