"""Tests for the deprecated ``cleanup`` command's exit codes."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from click.testing import CliRunner, Result

from slack_chat_migrator.cli.commands import cli
from slack_chat_migrator.cli.common import EXIT_FAILURE, EXIT_INTERRUPTED

CLEANUP_ARGS = [
    "cleanup",
    "--creds_path",
    "fake.json",
    "--workspace_admin",
    "admin@example.com",
    "--yes",
]

IMPORT_SPACE = {
    "name": "spaces/run",
    "displayName": "Slack #general",
    "importMode": True,
}


def _run(chat: Any) -> Result:
    with (
        patch(
            "slack_chat_migrator.cli.cleanup_cmd.get_gcp_service",
            return_value=object(),
        ),
        patch("slack_chat_migrator.cli.cleanup_cmd.ChatAdapter", lambda raw: chat),
        patch(
            "slack_chat_migrator.cli.common.stdin_is_interactive", return_value=False
        ),
    ):
        return CliRunner().invoke(cli, CLEANUP_ARGS)


class TestCleanupExitCodes:
    def test_ctrl_c_while_listing_exits_130(self) -> None:
        chat = MagicMock()
        chat.list_spaces.side_effect = KeyboardInterrupt
        result = _run(chat)
        assert result.exit_code == EXIT_INTERRUPTED, result.output
        assert "Aborted!" not in result.output
        chat.complete_import.assert_not_called()

    def test_ctrl_c_while_completing_exits_130(self) -> None:
        chat = MagicMock()
        chat.list_spaces.return_value = {"spaces": [IMPORT_SPACE]}
        chat.get_space.return_value = IMPORT_SPACE
        chat.complete_import.side_effect = KeyboardInterrupt
        result = _run(chat)
        assert result.exit_code == EXIT_INTERRUPTED, result.output

    def test_listing_error_exits_1(self) -> None:
        chat = MagicMock()
        chat.list_spaces.side_effect = ConnectionResetError("reset")
        result = _run(chat)
        assert result.exit_code == EXIT_FAILURE, result.output
