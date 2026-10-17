"""End-to-end attachment pipeline tests: real FileHandler and Drive helpers
over an in-memory Drive fake (tests/unit/drive_fakes.py)."""

from __future__ import annotations

import hashlib
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from slack_chat_migrator.core.config import MigrationConfig, SharedDriveConfig
from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.services.drive_adapter import DriveAdapter
from slack_chat_migrator.services.files import file as file_mod
from slack_chat_migrator.services.files.file_download import (
    DownloadResult,
    DownloadStatus,
)
from tests.unit.drive_fakes import FakeDrive

USERS = {
    "UA": "alice@example.com",
    "UB": "bob@example.com",
    "UX": "ext@other.org",
}
DRIVE_NAME = "Imported Slack Attachments"


class Downloads:
    """Stands in for file_download.download_file; serves bytes per file ID."""

    def __init__(self):
        self.content: dict[str, bytes] = {}
        self.calls: list[str] = []

    def __call__(self, file_obj, dest_dir, channel=None, **kwargs):
        self.calls.append(file_obj["id"])
        body = self.content[file_obj["id"]]
        fd, path = tempfile.mkstemp(dir=dest_dir or None, prefix="dl_")
        with os.fdopen(fd, "wb") as fh:
            fh.write(body)
        return DownloadResult(
            DownloadStatus.OK,
            path=path,
            md5=hashlib.md5(body).hexdigest(),  # noqa: S324 — fingerprint
            size=len(body),
        )


@pytest.fixture
def downloads():
    fake = Downloads()
    with patch.object(file_mod, "download_file", fake):
        yield fake


def make_handler(fake: FakeDrive, tmp_path, *, shared=True, config=None):
    """Build a real FileHandler over *fake*, initialised like a live run."""
    state = MigrationState()
    state.context.output_dir = str(tmp_path)
    resolver = MagicMock()
    resolver.is_external_user = lambda email: (
        bool(email) and not email.endswith("@example.com")
    )
    handler = file_mod.FileHandler(
        DriveAdapter(fake),
        MagicMock(),
        None,
        config or MigrationConfig(shared_drive=SharedDriveConfig(name=DRIVE_NAME)),
        "example.com",
        dict(USERS),
        resolver,
        state,
    )
    if shared:
        if not any(d["name"] == DRIVE_NAME for d in fake.drives_db):
            fake.drives_db.append({"id": "SD1", "name": DRIVE_NAME})
        handler.ensure_drive_initialized()
    else:
        fake.add_folder("ROOT", "Imported Slack Attachments", "root")
        handler._shared_drive_id = None
        handler._root_folder_id = "ROOT"
        handler._drive_initialized = True
    return handler


def upload(
    handler,
    downloads,
    channel,
    file_id,
    user,
    content,
    *,
    name="a.pdf",
    mime="application/pdf",
    space="spaces/S",
    sender=None,
):
    """Post one Slack file from *user* in *channel*."""
    downloads.content[file_id] = content
    handler.state.context.current_channel = channel
    file_obj = {
        "id": file_id,
        "name": name,
        "mimetype": mime,
        "user": user,
        "size": len(content),
        "url_private": f"https://files.slack.com/files-pri/T-{file_id}/{name}",
    }
    sender_email = sender if sender is not None else USERS.get(user)
    if sender_email and not sender_email.endswith("@example.com"):
        sender_email = None  # message_builder passes None for externals
    return handler.upload_attachment(file_obj, channel, space, None, sender_email)


# ---------------------------------------------------------------------------
# Permissions (I12)
# ---------------------------------------------------------------------------


class TestGrants:
    def test_no_request_uses_editor_role(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=False)

        result = upload(
            handler, downloads, "c", "F1", "UA", b"one", sender="bob@example.com"
        )

        assert result.success
        roles = [kw["body"]["role"] for kw in fake.permission_calls()]
        assert "editor" not in roles
        assert roles == ["writer", "owner"]

    def test_failed_transfer_is_counted(self, tmp_path, downloads):
        fake = FakeDrive(fail_transfer=True)
        handler = make_handler(fake, tmp_path, shared=False)

        result = upload(handler, downloads, "c", "F1", "UA", b"two")

        assert result.success  # the upload itself worked
        stats = handler.get_file_statistics()
        assert stats["ownership_transferred"] == 0
        assert stats["ownership_transfer_failed"] == 1
        # The poster keeps edit access although the transfer failed.
        grants = [
            (kw["body"]["emailAddress"], kw["body"]["role"])
            for kw in fake.permission_calls()
        ]
        assert ("alice@example.com", "writer") in grants

    def test_reused_file_is_not_transferred_again(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=False)

        first = upload(handler, downloads, "c", "F1", "UA", b"same")
        second = upload(handler, downloads, "c", "F2", "UB", b"same")

        assert first.drive_id == second.drive_id
        transfers = [
            kw for kw in fake.permission_calls() if kw.get("transferOwnership")
        ]
        assert len(transfers) == 1
        assert transfers[0]["body"]["emailAddress"] == "alice@example.com"
        assert handler.file_stats["ownership_transferred"] == 1
        # Bob still gets writer on the shared file.
        bob = [
            kw
            for kw in fake.permission_calls()
            if kw["body"]["emailAddress"] == "bob@example.com"
        ]
        assert [kw["body"]["role"] for kw in bob] == ["writer"]

    @pytest.mark.parametrize("shared", [True, False])
    def test_external_poster_gets_no_grant(self, tmp_path, downloads, shared):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=shared)

        result = upload(handler, downloads, "c", "FX", "UX", b"ext content")

        assert result.success
        emails = [kw["body"]["emailAddress"] for kw in fake.permission_calls()]
        assert "ext@other.org" not in emails
        assert emails == []

    def test_external_sender_passed_explicitly_gets_no_grant(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        downloads.content["F1"] = b"x"
        handler.upload_attachment(
            {
                "id": "F1",
                "name": "a.pdf",
                "user": "UA",
                "url_private": "https://files.slack.com/x",
            },
            "c",
            "spaces/S",
            None,
            "ext@other.org",
        )
        assert fake.permission_calls() == []

    def test_poster_who_becomes_owner_needs_no_writer_grant(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=False)

        upload(handler, downloads, "c", "F1", "UA", b"mine")

        calls = fake.permission_calls()
        assert len(calls) == 1
        assert calls[0]["body"] == {
            "type": "user",
            "role": "owner",
            "emailAddress": "alice@example.com",
        }
        assert calls[0]["transferOwnership"] is True

    def test_shared_drive_grants_writer_to_poster_only(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)

        upload(handler, downloads, "c", "F1", "UA", b"doc")

        calls = fake.permission_calls()
        assert [(kw["body"]["emailAddress"], kw["body"]["role"]) for kw in calls] == [
            ("alice@example.com", "writer")
        ]
        assert not any(kw.get("transferOwnership") for kw in calls)
