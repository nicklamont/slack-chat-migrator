"""The migrator tells the file handler which channels are public."""

from __future__ import annotations

from pathlib import Path

from tests.integration.conftest import (
    GENERAL_CHANNEL,
    USERS,
    build_export,
    make_migrator,
)


def test_public_channel_from_channels_json_is_not_restricted(tmp_path: Path) -> None:
    build_export(
        tmp_path,
        users=USERS,
        channels=[GENERAL_CHANNEL],
        messages_by_channel={"general": []},
    )
    m = make_migrator(tmp_path)
    m._initialize_api_services()

    assert m.file_handler.is_public_conversation("general")
    # A directory with no channels.json entry is treated as private.
    assert not m.file_handler.is_public_conversation("secret-project")
