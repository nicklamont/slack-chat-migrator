"""End-to-end: no Slack token reaches any log file (issue #112)."""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from slack_chat_migrator.cli.commands import cli
from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_message_with_files,
    make_messages,
)

pytestmark = pytest.mark.integration

SLACK_FILE_CREDENTIAL_SAMPLE = "xoxe-1234567890-abcdefSECRET"


def _export_with_tokenised_files(tmp_path: Path) -> Path:
    export = tmp_path / "export"
    export.mkdir()
    message = make_message_with_files(ts="1609459300.000000", file_count=2)
    for f in message["files"]:
        f["url_private"] = (
            f"https://files.slack.com/files-pri/T1-{f['id']}/a.txt?t={SLACK_FILE_CREDENTIAL_SAMPLE}"
        )
        f["url_private_download"] = f["url_private"] + "&dl=1"
    message["text"] = f"see {message['files'][0]['url_private']} and xoxp-9-SECRET"
    return build_export(
        export,
        users=USERS,
        channels=[GENERAL_CHANNEL],
        messages_by_channel={"general": [*make_messages(2), message]},
    )


def test_dry_run_logs_contain_no_tokens(tmp_path: Path) -> None:
    from slack_chat_migrator.core.migrator import SlackToChatMigrator

    class Failing(SlackToChatMigrator):
        def __init__(self, *a: Any, **kw: Any) -> None:
            kw.setdefault("message_error_schedule", {3: 400})
            super().__init__(*a, **kw)

    export = _export_with_tokenised_files(tmp_path)
    config = tmp_path / "config.yaml"
    config.write_text("exclude_channels: []\n")
    old_umask = os.umask(0o022)
    try:
        with patch("slack_chat_migrator.cli.migrate_cmd.SlackToChatMigrator", Failing):
            result = CliRunner().invoke(
                cli,
                [
                    "migrate",
                    "--dry_run",
                    "--debug_api",
                    "--verbose",
                    "--export_path",
                    str(export),
                    "--config",
                    str(config),
                ],
            )
    finally:
        os.umask(old_umask)

    assert SLACK_FILE_CREDENTIAL_SAMPLE not in result.output
    assert "xoxp-9-SECRET" not in result.output

    run_dirs = list(Path("migration_logs").glob("run_*"))
    assert len(run_dirs) == 1
    files = [p for p in run_dirs[0].rglob("*") if p.is_file()]
    channel_log = next(p for p in files if p.name == "general_migration.log")
    # The injected failure was written (redacted) to the failure details
    assert "FAILED MESSAGES DETAILS" in channel_log.read_text()
    for path in files:
        text = path.read_text(errors="replace")
        assert SLACK_FILE_CREDENTIAL_SAMPLE not in text, path
        assert "SECRET" not in text, path
        if os.name != "nt":
            assert stat.S_IMODE(path.stat().st_mode) == 0o600, path
    if os.name != "nt":
        assert stat.S_IMODE(run_dirs[0].stat().st_mode) == 0o700
