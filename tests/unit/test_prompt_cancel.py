"""Ctrl-C or end of input at a real click prompt exits 130 in every command."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner, Result

from slack_chat_migrator.cli.commands import cli
from slack_chat_migrator.cli.common import EXIT_INTERRUPTED


class _CtrlCInput(io.BytesIO):
    """stdin on which the user presses Ctrl-C at the first prompt."""

    def readline(self, *args: Any) -> bytes:
        raise KeyboardInterrupt


def _setup(stdin: Any) -> Result:
    """Run the real setup command up to its first prompt (step 1)."""
    with (
        patch("slack_chat_migrator.cli.setup_cmd._check_setup_deps", return_value=True),
        patch(
            "slack_chat_migrator.services.setup.setup_service.load_state",
            return_value=None,
        ),
        patch(
            "slack_chat_migrator.services.setup.setup_service.save_state"
        ) as save_state,
        patch(
            "slack_chat_migrator.cli.setup_cmd._get_and_validate_credentials",
            return_value=MagicMock(),
        ),
    ):
        result = CliRunner().invoke(cli, ["setup"], input=stdin)
    save_state.assert_not_called()
    return result


def _init(tmp_path: Path, stdin: Any, minimal_export_dir: Path) -> Result:
    return CliRunner().invoke(
        cli,
        [
            "init",
            "--export_path",
            str(minimal_export_dir),
            "--output",
            str(tmp_path / "out.yaml"),
        ],
        input=stdin,
    )


class TestCtrlCAtAPrompt:
    @pytest.mark.parametrize("stdin", ["", _CtrlCInput()], ids=["eof", "ctrl-c"])
    def test_setup_prompt(self, stdin: Any) -> None:
        result = _setup(stdin)
        assert result.exit_code == EXIT_INTERRUPTED, result.output
        assert "Use an existing project?" in result.output
        assert "Cancelled." in result.output
        assert "Aborted!" not in result.output

    @pytest.mark.parametrize("stdin", ["", _CtrlCInput()], ids=["eof", "ctrl-c"])
    def test_init_prompt(
        self, tmp_path: Path, stdin: Any, minimal_export_dir: Path
    ) -> None:
        result = _init(tmp_path, stdin, minimal_export_dir)
        assert result.exit_code == EXIT_INTERRUPTED, result.output
        assert "Cancelled." in result.output
        assert "Aborted!" not in result.output
        assert not (tmp_path / "out.yaml").exists()

    @pytest.mark.parametrize("stdin", ["", _CtrlCInput()], ids=["eof", "ctrl-c"])
    def test_validate_init_offer(self, tmp_path: Path, stdin: Any) -> None:
        """validate's "Run init now?" (missing config) goes through the group."""
        with (
            patch(
                "slack_chat_migrator.cli.validate_cmd.stdin_is_interactive",
                return_value=True,
            ),
            patch("slack_chat_migrator.cli.common.log_with_context") as log,
        ):
            result = CliRunner().invoke(
                cli,
                [
                    "validate",
                    "--export_path",
                    str(tmp_path),
                    "--config",
                    str(tmp_path / "missing.yaml"),
                ],
                input=stdin,
            )
        assert result.exit_code == EXIT_INTERRUPTED, result.output
        assert "Run init now?" in result.output
        assert "\nCancelled.\n" in result.output
        log.assert_called_once_with(
            logging.WARNING,
            "Cancelled at a prompt (Ctrl-C or end of input).",
            console=False,
        )

    def test_ctrl_c_outside_a_prompt(self) -> None:
        with patch(
            "slack_chat_migrator.cli.setup_cmd._run_setup",
            side_effect=KeyboardInterrupt,
        ):
            result = CliRunner().invoke(cli, ["setup"])
        assert result.exit_code == EXIT_INTERRUPTED
        assert "Interrupted." in result.output
        assert "Aborted!" not in result.output

    def test_deliberate_abort_still_exits_1(self) -> None:
        """An Abort raised on purpose (not Ctrl-C/EOF) keeps click's handling."""
        with patch(
            "slack_chat_migrator.cli.setup_cmd._run_setup", side_effect=click.Abort
        ):
            result = CliRunner().invoke(cli, ["setup"])
        assert result.exit_code == 1
        assert "Aborted!" in result.output
