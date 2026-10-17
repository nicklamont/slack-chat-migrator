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


# ---------------------------------------------------------------------------
# Content type and resumable uploads (I28, I37)
# ---------------------------------------------------------------------------


class TestContentType:
    def test_slack_mime_type_reaches_drive_resumably(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)

        result = upload(
            handler,
            downloads,
            "c",
            "F1",
            "UA",
            b"\x89PNG",
            name="Screenshot",
            mime="image/png",
            space=None,
        )

        record = fake.files_db[result.drive_id]
        assert record["mimeType"] == "image/png"
        assert record["resumable"] is True
        assert record["chunksize"] == 16 * 1024 * 1024
        assert result.mime_type == "image/png"

    def test_slack_mime_type_enables_direct_upload(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        handler.chat_service.upload_media.return_value = {
            "attachmentDataRef": {"resourceName": "ref"}
        }

        result = upload(
            handler,
            downloads,
            "c",
            "F1",
            "UA",
            b"\x89PNG",
            name="Screenshot",
            mime="image/png",
        )

        assert result.upload_type == "direct"
        media = handler.chat_service.upload_media.call_args.kwargs["media_body"]
        assert media.mimetype() == "image/png"
        assert fake.count("files.create") == 0


# ---------------------------------------------------------------------------
# Dedup scoping (I11) and re-runs (P3)
# ---------------------------------------------------------------------------


def folder_of(handler, channel):
    """Drive folder ID FileHandler uses for *channel*."""
    return handler._resolve_upload_folder(channel, "lookup")


class TestDedupScope:
    def test_identical_bytes_get_one_file_per_channel_folder(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)

        f1 = upload(
            handler,
            downloads,
            "private-hr",
            "F1",
            "UA",
            b"salary bands",
            name="bands.pdf",
        )
        f2 = upload(
            handler,
            downloads,
            "private-hr",
            "F2",
            "UA",
            b"salary bands",
            name="bands-v2.pdf",
        )
        f3 = upload(
            handler,
            downloads,
            "general",
            "F3",
            "UB",
            b"salary bands",
            name="handbook.pdf",
        )

        assert f1.drive_id == f2.drive_id  # same-folder dedup still works
        assert f3.drive_id != f1.drive_id
        assert fake.files_db[f3.drive_id]["parents"] == [folder_of(handler, "general")]
        assert fake.files_db[f3.drive_id]["name"] == "handbook.pdf"

    def test_same_slack_file_in_two_channels(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)

        a = upload(handler, downloads, "chan-a", "F1", "UA", b"shared")
        b = upload(handler, downloads, "chan-b", "F1", "UB", b"shared")

        assert b.cached is False
        assert a.drive_id != b.drive_id
        assert fake.files_db[b.drive_id]["parents"] == [folder_of(handler, "chan-b")]
        again = upload(handler, downloads, "chan-b", "F1", "UB", b"shared")
        assert again.cached is True
        assert again.drive_id == b.drive_id

    def test_direct_upload_ref_is_not_reused_across_spaces(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        handler.chat_service.upload_media.side_effect = [
            {"attachmentDataRef": {"resourceName": "AAA-ref"}},
            {"attachmentDataRef": {"resourceName": "BBB-ref"}},
        ]

        a = upload(
            handler,
            downloads,
            "c",
            "F1",
            "UA",
            b"\x89PNG",
            name="p.png",
            mime="image/png",
            space="spaces/AAA",
        )
        b = upload(
            handler,
            downloads,
            "c",
            "F1",
            "UA",
            b"\x89PNG",
            name="p.png",
            mime="image/png",
            space="spaces/BBB",
        )

        assert handler.chat_service.upload_media.call_count == 2
        assert a.attachment_ref["attachmentDataRef"]["resourceName"] == "AAA-ref"
        assert b.attachment_ref["attachmentDataRef"]["resourceName"] == "BBB-ref"

    def test_rerun_never_links_across_folders(self, tmp_path, downloads):
        fake = FakeDrive()
        run1 = make_handler(fake, tmp_path)
        offer = upload(
            run1, downloads, "private", "F1", "UA", b"offer letter", name="offer.pdf"
        )

        run2 = make_handler(fake, tmp_path)
        handbook = upload(
            run2, downloads, "general", "F2", "UB", b"offer letter", name="handbook.pdf"
        )

        assert handbook.drive_id != offer.drive_id
        assert fake.files_db[handbook.drive_id]["parents"] == [
            folder_of(run2, "general")
        ]

    def test_new_uploads_feed_the_hash_cache(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        upload(handler, downloads, "c", "F6", "UA", b"bytes")
        fake.calls.clear()

        dup = upload(handler, downloads, "c", "F7", "UA", b"bytes")

        assert dup.metadata["reused"] is True
        md5_searches = [
            kw
            for name, kw in fake.calls
            if name == "files.list" and "md5Checksum" in kw.get("q", "")
        ]
        assert md5_searches == []
        assert fake.count("files.create") == 0


class TestRerun:
    def test_unchanged_export_needs_no_downloads(self, tmp_path, downloads):
        fake = FakeDrive()
        run1 = make_handler(fake, tmp_path)
        first = upload(run1, downloads, "general", "F1", "UA", b"report body")
        assert downloads.calls == ["F1"]
        record = fake.files_db[first.drive_id]
        assert record["appProperties"] == {
            "slackFileId": "F1",
            "slackChannel": "general",
        }

        run2 = make_handler(fake, tmp_path)
        fake.calls.clear()
        again = upload(run2, downloads, "general", "F1", "UA", b"report body")

        assert downloads.calls == ["F1"]  # nothing downloaded in run 2
        assert again.drive_id == first.drive_id
        assert again.metadata["source"] == "slack_id"
        assert fake.count("files.create") == 0
        assert fake.count("permissions.create") == 0


# ---------------------------------------------------------------------------
# Shared drive and folder listings are paged (I13)
# ---------------------------------------------------------------------------


class TestListingsArePaged:
    def test_existing_drive_on_page_three_is_reused(self, tmp_path):
        drives = [{"id": f"D{i}", "name": f"Team {i}"} for i in range(24)]
        drives.append({"id": "SD1", "name": DRIVE_NAME})
        fake = FakeDrive(drives=drives, drive_page_cap=10, honor_drive_query=False)

        handler = make_handler(fake, tmp_path)

        assert handler.shared_drive_id == "SD1"
        assert fake.count("drives.create") == 0

    def test_every_page_of_channel_folders_is_read(self, tmp_path):
        fake = FakeDrive(file_page_cap=1000)
        fake.drives_db.append({"id": "SD1", "name": DRIVE_NAME})
        for i in range(1500):
            fake.add_folder(f"fold{i}", f"chan{i}", "SD1")

        handler = make_handler(fake, tmp_path)

        folder_lists = [
            kw
            for name, kw in fake.calls
            if name == "files.list" and "vnd.google-apps.folder" in kw.get("q", "")
        ]
        assert len(folder_lists) == 2
        assert "nextPageToken" in folder_lists[0]["fields"]
        assert len(handler.folder_manager.folder_cache) == 1500
        fake.calls.clear()
        # A known channel folder is found without a search.
        assert folder_of(handler, "chan1499") == "fold1499"
        assert not any(
            name == "files.list" and "name =" in kw.get("q", "")
            for name, kw in fake.calls
        )

    def test_inaccessible_configured_drive_stops_initialisation(self, tmp_path):
        from slack_chat_migrator.exceptions import ConfigError

        fake = FakeDrive(drives=[{"id": "SD1", "name": DRIVE_NAME}])
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name=DRIVE_NAME, id="0AMissing")
        )
        with pytest.raises(ConfigError, match="0AMissing"):
            make_handler(fake, tmp_path, config=config)

        assert fake.count("drives.create") == 0
        assert fake.count("files.create") == 0  # no fallback folder either


# ---------------------------------------------------------------------------
# Drive call budget (P3)
# ---------------------------------------------------------------------------


class TestDriveCallBudget:
    @pytest.mark.parametrize(
        ("shared", "poster"),
        [(True, None), (False, None), (False, "bob@example.com")],
        ids=["shared-drive", "regular-poster-is-owner", "regular-other-poster"],
    )
    def test_new_file_costs_at_most_two_calls(
        self, tmp_path, downloads, shared, poster
    ):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=shared)
        upload(handler, downloads, "c", "F0", "UA", b"warm-up")  # folder + listing
        fake.calls.clear()
        fake.batch_sizes.clear()

        for i in range(1, 6):
            upload(
                handler,
                downloads,
                "c",
                f"F{i}",
                "UA",
                f"unique {i}".encode(),
                sender=poster,
            )

        # Requests sent inside a batch share the batch's round trip.
        sub_requests = sum(fake.batch_sizes)
        round_trips = len(fake.calls) - sub_requests
        assert round_trips <= 2 * 5
        assert fake.count("files.get") == 0
        assert fake.count("files.list") == 0
