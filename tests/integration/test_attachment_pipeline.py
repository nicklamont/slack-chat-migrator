"""Dry-run checks of attachment handling through the full migration pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_message_with_files,
    make_migrator,
)

pytestmark = pytest.mark.integration


def test_forwarded_files_are_counted(tmp_path: Path) -> None:
    """Files inside a shared/forwarded message count towards files_created."""
    message = make_message_with_files(user="U001", ts="1609459200.000000")
    message["attachments"] = [
        {
            "is_share": True,
            "text": "forwarded",
            "files": [
                {
                    "id": "FFWD1",
                    "name": "forwarded.pdf",
                    "mimetype": "application/pdf",
                    "url_private_download": "https://files.slack.com/forwarded.pdf",
                    "size": 10,
                }
            ],
        }
    ]
    export = build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL],
        messages_by_channel={"general": [message]},
    )
    m = make_migrator(export)
    m.migrate()

    summary = m.state.progress.migration_summary
    assert summary["messages_created"] == 1
    assert summary["files_created"] == 2
