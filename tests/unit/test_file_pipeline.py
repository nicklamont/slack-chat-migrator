"""End-to-end attachment pipeline tests: real FileHandler and Drive helpers
over an in-memory Drive fake (tests/unit/drive_fakes.py)."""

from __future__ import annotations

import hashlib
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from google.auth.exceptions import RefreshError

from slack_chat_migrator.core.config import MigrationConfig, SharedDriveConfig
from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.exceptions import APIError
from slack_chat_migrator.services.drive.folder_manager import FolderLookup
from slack_chat_migrator.services.drive_adapter import DriveAdapter
from slack_chat_migrator.services.files import file as file_mod
from slack_chat_migrator.services.files.file_download import (
    DownloadResult,
    DownloadStatus,
)
from slack_chat_migrator.services.user_resolver import UserResolver
from tests.unit.drive_fakes import FakeDrive, http_error

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


def make_handler(
    fake: FakeDrive, tmp_path, *, shared=True, config=None, transfer=False
):
    """Build a real FileHandler over *fake*, initialised like a live run.

    *transfer* turns on ``transfer_file_ownership`` (when no *config* is given).
    """
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
        config
        or MigrationConfig(
            shared_drive=SharedDriveConfig(name=DRIVE_NAME),
            transfer_file_ownership=transfer,
        ),
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
        handler.folder_id = "ROOT"  # a regular Drive folder, no shared drive
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
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

        result = upload(
            handler, downloads, "c", "F1", "UA", b"one", sender="bob@example.com"
        )

        assert result.success
        roles = [kw["body"]["role"] for kw in fake.permission_calls()]
        assert "editor" not in roles
        assert roles == ["writer", "owner"]

    def test_failed_transfer_is_counted(self, tmp_path, downloads):
        fake = FakeDrive(fail_transfer=True)
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

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
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

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
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

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


class TestDriveSetup:
    """Drive setup stops the run on transient errors (#118)."""

    @pytest.mark.parametrize(
        "error",
        [ConnectionResetError("connection reset"), RefreshError("unauthorized_client")],
        ids=["network", "refresh"],
    )
    def test_existing_shared_drive_is_never_replaced_by_my_drive(self, tmp_path, error):
        fake = FakeDrive(drives=[{"id": "SD1", "name": DRIVE_NAME}])
        fake.failing["drives.list"] = error
        handler = make_handler(fake, tmp_path, shared=False)
        handler._drive_status = file_mod.DriveStatus("not_initialized")

        with pytest.raises(APIError, match=DRIVE_NAME):
            handler.ensure_drive_initialized()

        assert handler.drive_status.mode == "none"
        assert fake.count("files.create") == 0  # no My Drive folder made

    def test_account_without_shared_drives_uses_my_drive(self, tmp_path):
        fake = FakeDrive()
        fake.failing["drives.create"] = http_error(403, "insufficient permissions")
        handler = make_handler(fake, tmp_path, shared=False)
        handler._drive_status = file_mod.DriveStatus("not_initialized")

        handler.ensure_drive_initialized()

        assert handler.drive_status.mode == "my_drive"
        assert "insufficient permissions" in handler.drive_status.detail


class TestWorkspaceDomains:
    """Users on any internal domain count as internal (Refs #89)."""

    CORP = "carol@corp.example.net"

    def _handler(self, fake, tmp_path, *, resolver=None, domains=()):
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name=DRIVE_NAME),
            workspace_domains=list(domains),
        )
        handler = make_handler(fake, tmp_path, config=config)
        handler.user_resolver = resolver
        return handler

    @staticmethod
    def _post(handler, downloads, sender):
        downloads.content["F1"] = b"x"
        handler.state.context.current_channel = "c"
        return handler.upload_attachment(
            {
                "id": "F1",
                "name": "a.pdf",
                "user": "UC",
                "url_private": "https://files.slack.com/files-pri/T-F1/a.pdf",
            },
            "c",
            "spaces/S",
            None,
            sender,
        )

    @staticmethod
    def _grants(fake):
        return [
            (kw["body"]["emailAddress"], kw["body"]["role"])
            for kw in fake.permission_calls()
        ]

    def test_secondary_domain_poster_gets_writer_grant(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = self._handler(fake, tmp_path, domains=[" Corp.Example.NET "])

        result = self._post(handler, downloads, self.CORP)

        assert result.success
        assert self._grants(fake) == [(self.CORP, "writer")]

    def test_unlisted_domain_poster_gets_no_grant(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = self._handler(fake, tmp_path, domains=["corp.example.net"])

        assert self._post(handler, downloads, "ext@other.org").success

        assert self._grants(fake) == []

    def test_production_wiring_honours_workspace_domains(self, tmp_path, downloads):
        """A real UserResolver, as the migrator passes it (#89)."""
        fake = FakeDrive()
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name=DRIVE_NAME),
            workspace_domains=["corp.example.net"],
        )
        users = {**USERS, "UC": self.CORP}
        resolver = UserResolver(
            config=config,
            state=MigrationState(),
            chat=MagicMock(),
            creds_path=None,
            user_map=users,
            unmapped_user_tracker=MagicMock(),
            export_root=str(tmp_path),
            workspace_admin="admin@example.com",
            workspace_domain="example.com",
        )
        handler = make_handler(fake, tmp_path, config=config)
        handler.user_resolver = resolver
        handler.user_map = users

        result = self._post(handler, downloads, self.CORP)

        assert result.success
        assert self._grants(fake) == [(self.CORP, "writer")]
        assert handler.get_file_statistics()["external_user_files"] == 0

    def test_the_resolver_decides_when_there_is_one(self, tmp_path, downloads):
        """The resolver's answer wins, so the whole tool agrees on who is internal."""
        fake = FakeDrive()
        resolver = MagicMock()
        resolver.workspace_admin = "admin@example.com"
        resolver.is_external_user = lambda email: (
            not email.endswith(("@example.com", "@corp.example.net"))
        )
        handler = self._handler(fake, tmp_path, resolver=resolver)  # no domains

        assert self._post(handler, downloads, self.CORP).success

        assert self._grants(fake) == [(self.CORP, "writer")]


class TestOwnershipTransfer:
    """Transfers are opt-in and never disable Drive's notification (CR1)."""

    def test_off_by_default_poster_gets_writer(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=False)

        upload(handler, downloads, "c", "F1", "UA", b"mine")

        calls = fake.permission_calls()
        assert [(kw["body"]["emailAddress"], kw["body"]["role"]) for kw in calls] == [
            ("alice@example.com", "writer")
        ]
        assert not any(kw.get("transferOwnership") for kw in calls)
        stats = handler.get_file_statistics()
        assert stats["ownership_transferred"] == 0
        assert stats["ownership_transfer_failed"] == 0

    @pytest.mark.parametrize(
        "sender", [None, "bob@example.com"], ids=["single", "batched"]
    )
    def test_transfer_requests_keep_the_notification(self, tmp_path, downloads, sender):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

        upload(handler, downloads, "c", "F1", "UA", b"mine", sender=sender)

        transfers = [
            kw for kw in fake.permission_calls() if kw.get("transferOwnership")
        ]
        assert len(transfers) == 1
        assert "sendNotificationEmail" not in transfers[0]
        assert handler.get_file_statistics()["ownership_transferred"] == 1

    def test_failed_batched_transfer_is_counted_and_logged(
        self, tmp_path, downloads, caplog
    ):
        """Poster != owner: writer grant and transfer share a batch (TA3.3)."""
        fake = FakeDrive(fail_transfer=True)
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

        result = upload(
            handler, downloads, "c", "F1", "UA", b"shared", sender="bob@example.com"
        )

        assert result.success
        assert fake.batch_sizes == [2]
        stats = handler.get_file_statistics()
        assert stats["ownership_transfer_failed"] == 1
        assert stats["ownership_transferred"] == 0
        bob = [
            kw
            for kw in fake.permission_calls()
            if kw["body"]["emailAddress"] == "bob@example.com"
        ]
        assert [kw["body"]["role"] for kw in bob] == ["writer"]
        assert (
            fake.permissions_db[result.drive_id][0]["emailAddress"] == "bob@example.com"
        )
        assert any(
            "alice@example.com" in r.getMessage() and "forbidden" in r.getMessage()
            for r in caplog.records
            if r.levelname == "WARNING"
        )

    def test_failed_batched_writer_grant_is_not_marked_granted(
        self, tmp_path, downloads
    ):
        fake = FakeDrive(reject_grants_to={"bob@example.com"})
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

        result = upload(
            handler, downloads, "c", "F1", "UA", b"shared", sender="bob@example.com"
        )

        assert result.success
        assert not handler._already_granted(result.drive_id, "bob@example.com")
        assert handler.get_file_statistics()["ownership_transferred"] == 1


class TestUploadErrorsAreCounted:
    """Every upload error comes back as a counted failure (#118)."""

    def test_refresh_error_during_upload_is_counted(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        assert upload(handler, downloads, "c", "F1", "UA", b"one").success
        fake.failing["files.create"] = RefreshError("invalid_grant: not authorized")

        result = upload(handler, downloads, "c", "F2", "UA", b"two")

        assert not result.success
        assert "RefreshError" in (result.error or "")
        stats = handler.get_file_statistics()
        assert stats["failed_uploads"] == 1
        assert stats["success_rate"] == 50.0


class TestGrantFailures:
    """A failed grant never fails the upload, and is retried (#98)."""

    @staticmethod
    def _writer_grants(fake, email="alice@example.com"):
        return [
            kw
            for kw in fake.permission_calls()
            if kw["body"]["emailAddress"] == email and kw["body"]["role"] == "writer"
        ]

    def test_network_error_on_grant_keeps_the_upload(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = ConnectionResetError("reset")

        result = upload(handler, downloads, "c", "F1", "UA", b"body")

        assert result.success
        stats = handler.get_file_statistics()
        assert stats["poster_grant_failures"] == 1
        assert stats["failed_uploads"] == 0
        assert fake.files_db[result.drive_id]["appProperties"]["slackGrantPending"]
        assert not handler._already_granted(result.drive_id, "alice@example.com")

    def test_failed_grant_on_a_canvas_makes_no_second_copy(
        self, tmp_path, doc_downloads
    ):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = TimeoutError("timed out")

        result = post_document(handler, CANVAS)

        assert result.success
        uploads = [
            kw
            for name, kw in fake.calls
            if name == "files.create" and kw.get("media_body") is not None
        ]
        assert len(uploads) == 1

    def test_grant_is_retried_when_the_file_is_linked_again(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = ConnectionResetError("reset")
        first = upload(handler, downloads, "c", "F1", "UA", b"body")
        del fake.failing["permissions.create"]

        # The same Slack file again, for another space: linked by Slack ID.
        again = upload(handler, downloads, "c", "F1", "UA", b"body", space="spaces/T")

        assert again.drive_id == first.drive_id
        assert again.metadata["source"] == "slack_id"
        assert len(self._writer_grants(fake)) == 2  # the failed one and the retry
        assert handler._already_granted(first.drive_id, "alice@example.com")
        assert "slackGrantPending" not in fake.files_db[first.drive_id]["appProperties"]

    def test_rerun_retries_a_flagged_grant_only(self, tmp_path, downloads):
        fake = FakeDrive()
        run1 = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = ConnectionResetError("reset")
        failed = upload(run1, downloads, "c", "F1", "UA", b"one")
        del fake.failing["permissions.create"]
        granted = upload(run1, downloads, "c", "F2", "UA", b"two")

        run2 = make_handler(fake, tmp_path)
        fake.calls.clear()
        downloads.calls.clear()
        upload(run2, downloads, "c", "F1", "UA", b"one")
        upload(run2, downloads, "c", "F2", "UA", b"two")

        assert downloads.calls == []
        grants = [kw["fileId"] for kw in fake.permission_calls()]
        assert grants == [failed.drive_id]
        assert granted.drive_id not in grants
        assert run2.get_file_statistics()["poster_grant_failures"] == 0
        assert (
            "slackGrantPending" not in fake.files_db[failed.drive_id]["appProperties"]
        )

    def test_rerun_retries_a_failed_transfer(self, tmp_path, downloads):
        fake = FakeDrive(fail_transfer=True)
        run1 = make_handler(fake, tmp_path, shared=False, transfer=True)
        first = upload(run1, downloads, "c", "F1", "UA", b"mine")
        assert run1.get_file_statistics()["ownership_transfer_failed"] == 1
        assert fake.files_db[first.drive_id]["appProperties"]["slackOwnerPending"]

        fake.fail_transfer = False
        run2 = make_handler(fake, tmp_path, shared=False, transfer=True)
        fake.calls.clear()
        upload(run2, downloads, "c", "F1", "UA", b"mine")

        transfers = [
            kw for kw in fake.permission_calls() if kw.get("transferOwnership")
        ]
        assert [kw["fileId"] for kw in transfers] == [first.drive_id]
        assert run2.get_file_statistics()["ownership_transferred"] == 1
        assert "slackOwnerPending" not in fake.files_db[first.drive_id]["appProperties"]

    def test_failed_batched_writer_grant_is_counted_and_flagged(
        self, tmp_path, downloads
    ):
        fake = FakeDrive(reject_grants_to={"bob@example.com"})
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)

        result = upload(
            handler, downloads, "c", "F1", "UA", b"shared", sender="bob@example.com"
        )

        assert fake.batch_sizes == [2]
        assert handler.get_file_statistics()["poster_grant_failures"] == 1
        assert handler.drive_uploader.pending.has_grant(
            result.drive_id, "bob@example.com"
        )
        assert fake.files_db[result.drive_id]["appProperties"]["slackGrantPending"]

    def test_second_poster_in_the_same_run_gets_a_grant(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        upload(handler, downloads, "c", "F1", "UA", b"body")

        upload(
            handler,
            downloads,
            "c",
            "F1",
            "UA",
            b"body",
            space="spaces/T",
            sender="bob@example.com",
        )

        assert self._writer_grants(fake, "bob@example.com")

    def test_network_error_storing_the_flag_keeps_the_upload(
        self, tmp_path, downloads, caplog
    ):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = ConnectionResetError("reset")
        fake.failing["files.update"] = ConnectionResetError("reset")

        result = upload(handler, downloads, "c", "F1", "UA", b"body")

        assert result.success
        stats = handler.get_file_statistics()
        assert stats["failed_uploads"] == 0
        assert stats["poster_grant_failures"] == 1
        assert stats["pending_flag_failures"] == 1
        assert any(
            r.levelname == "WARNING" and result.drive_id in r.getMessage()
            for r in caplog.records
        )

    def test_new_owner_gets_no_writer_grant_on_relink(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)
        upload(handler, downloads, "c", "F1", "UA", b"mine")

        upload(handler, downloads, "c", "F1", "UA", b"mine", space="spaces/T")

        assert self._writer_grants(fake, "alice@example.com") == []

    def test_transfer_is_not_retried_on_relink_in_the_same_run(
        self, tmp_path, downloads
    ):
        """In-run retries of transfers are left to the end-of-run sweep."""
        fake = FakeDrive(fail_transfer=True)
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)
        upload(handler, downloads, "c", "F1", "UA", b"mine")
        fake.fail_transfer = False

        upload(handler, downloads, "c", "F1", "UA", b"mine", space="spaces/T")

        transfers = [
            kw for kw in fake.permission_calls() if kw.get("transferOwnership")
        ]
        assert len(transfers) == 1

    def test_rerun_after_a_transfer_sends_nothing(self, tmp_path, downloads):
        fake = FakeDrive()
        run1 = make_handler(fake, tmp_path, shared=False, transfer=True)
        upload(run1, downloads, "c", "F1", "UA", b"mine")

        run2 = make_handler(fake, tmp_path, shared=False, transfer=True)
        fake.calls.clear()
        upload(run2, downloads, "c", "F1", "UA", b"mine")

        assert fake.permission_calls() == []
        assert fake.count("files.update") == 0


class TestPendingSweep:
    """The end-of-run sweep retries every flagged grant and transfer (#98)."""

    @staticmethod
    def _writer_grants(fake, email="alice@example.com"):
        return TestGrantFailures._writer_grants(fake, email)

    def test_sweep_retries_a_grant_from_this_run(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = ConnectionResetError("reset")
        result = upload(handler, downloads, "c", "F1", "UA", b"body")
        # The same file in a later message: answered from the in-run cache.
        upload(handler, downloads, "c", "F1", "UA", b"body")
        del fake.failing["permissions.create"]

        handler.retry_pending_permissions()

        assert len(self._writer_grants(fake)) == 2  # the failure and the retry
        stats = handler.get_file_statistics()
        assert stats["pending_retried"] == 1
        assert stats["pending_retry_failed"] == 0
        assert (
            "slackGrantPending" not in fake.files_db[result.drive_id]["appProperties"]
        )

    @pytest.mark.parametrize("shared", [True, False], ids=["shared", "my_drive"])
    def test_resume_retries_files_it_never_links(self, tmp_path, downloads, shared):
        """--resume skips sent messages, so the sweep finds the flagged file."""
        fake = FakeDrive()
        run1 = make_handler(fake, tmp_path, shared=shared)
        fake.failing["permissions.create"] = ConnectionResetError("reset")
        result = upload(run1, downloads, "c", "F1", "UA", b"body")
        del fake.failing["permissions.create"]

        run2 = make_handler(fake, tmp_path, shared=shared)  # links nothing
        run2.retry_pending_permissions()

        assert len(self._writer_grants(fake)) == 2
        assert run2.get_file_statistics()["pending_retried"] == 1
        props = fake.files_db[result.drive_id]["appProperties"]
        assert not any(key.startswith("slackGrant") for key in props)

    def test_one_posters_success_keeps_anothers_flag(self, tmp_path, downloads):
        fake = FakeDrive(reject_grants_to={"alice@example.com"})
        run1 = make_handler(fake, tmp_path)
        first = upload(run1, downloads, "c", "F1", "UA", b"body")
        upload(
            run1,
            downloads,
            "c",
            "F1",
            "UA",
            b"body",
            space="spaces/T",
            sender="bob@example.com",
        )  # Bob's grant works

        fake.reject_grants_to = set()
        run2 = make_handler(fake, tmp_path)
        run2.retry_pending_permissions()

        assert len(self._writer_grants(fake, "alice@example.com")) == 2
        assert run2.get_file_statistics()["pending_retried"] == 1
        props = fake.files_db[first.drive_id]["appProperties"]
        assert "slackGrantPending" not in props

    def test_sweep_retries_a_failed_transfer(self, tmp_path, downloads):
        fake = FakeDrive(fail_transfer=True)
        handler = make_handler(fake, tmp_path, shared=False, transfer=True)
        result = upload(handler, downloads, "c", "F1", "UA", b"mine")
        fake.fail_transfer = False

        handler.retry_pending_permissions()

        transfers = [
            kw for kw in fake.permission_calls() if kw.get("transferOwnership")
        ]
        assert len(transfers) == 2
        assert handler.get_file_statistics()["pending_retried"] == 1
        assert (
            "slackOwnerPending" not in fake.files_db[result.drive_id]["appProperties"]
        )

    def test_transfer_is_not_retried_once_the_option_is_off(self, tmp_path, downloads):
        fake = FakeDrive(fail_transfer=True)
        run1 = make_handler(fake, tmp_path, shared=False, transfer=True)
        upload(run1, downloads, "c", "F1", "UA", b"mine")
        fake.fail_transfer = False

        run2 = make_handler(fake, tmp_path, shared=False)  # transfers now off
        run2.retry_pending_permissions()

        transfers = [
            kw for kw in fake.permission_calls() if kw.get("transferOwnership")
        ]
        assert len(transfers) == 1
        assert run2.get_file_statistics()["pending_retried"] == 0

    def test_still_failing_retry_is_counted_and_kept(self, tmp_path, downloads):
        fake = FakeDrive(reject_grants_to={"alice@example.com"})
        handler = make_handler(fake, tmp_path)
        result = upload(handler, downloads, "c", "F1", "UA", b"body")

        handler.retry_pending_permissions()

        stats = handler.get_file_statistics()
        assert stats["pending_retried"] == 0
        assert stats["pending_retry_failed"] == 1
        assert handler.drive_uploader.pending.has_grant(
            result.drive_id, "alice@example.com"
        )

    def test_unknown_recipient_is_skipped_with_a_warning(
        self, tmp_path, downloads, caplog
    ):
        fake = FakeDrive()
        run1 = make_handler(fake, tmp_path)
        result = upload(run1, downloads, "c", "F1", "UA", b"body")
        record = fake.files_db[result.drive_id]
        record["appProperties"].update(
            {"slackGrantPending": "true", "slackGrant.0123abcd": "-"}
        )

        run2 = make_handler(fake, tmp_path)
        fake.calls.clear()
        run2.retry_pending_permissions()

        assert fake.permission_calls() == []
        assert any("too long" in r.getMessage() for r in caplog.records)

    def test_failed_search_still_retries_this_runs_flags(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.failing["permissions.create"] = ConnectionResetError("reset")
        upload(handler, downloads, "c", "F1", "UA", b"body")
        del fake.failing["permissions.create"]
        fake.fail_query = lambda q: http_error(500) if "Pending" in q else None

        handler.retry_pending_permissions()

        assert handler.get_file_statistics()["pending_retried"] == 1

    def test_nothing_to_do_without_drive(self, tmp_path):
        handler = make_handler(FakeDrive(), tmp_path)
        handler.folder_id = None

        handler.retry_pending_permissions()  # no error, nothing counted

        assert handler.get_file_statistics()["pending_retried"] == 0


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
    return handler._choose_upload_folder(channel, "lookup").folder_id


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


# ---------------------------------------------------------------------------
# Limited-access folders for private conversations (F10)
# ---------------------------------------------------------------------------

CHANNELS_META = {
    "general": {"name": "general", "is_private": False},
    "hr": {"name": "hr", "is_private": True},
}


def make_meta_handler(
    fake, tmp_path, *, shared=True, restrict=True, meta=CHANNELS_META
):
    config = MigrationConfig(
        shared_drive=SharedDriveConfig(name=DRIVE_NAME),
        restrict_private_channel_folders=restrict,
    )
    handler = make_handler(fake, tmp_path, shared=shared, config=config)
    handler.channels_meta = meta
    return handler


def created_folder(fake, name):
    return next(
        kw["body"]
        for method, kw in fake.calls
        if method == "files.create" and kw["body"].get("name") == name
    )


class TestLimitedAccessFolders:
    @pytest.mark.parametrize(
        ("channel", "restricted"),
        [("hr", True), ("general", False), ("D0123-dm", True)],
        ids=["private", "public", "not-in-channels-json"],
    )
    def test_folder_body(self, tmp_path, downloads, channel, restricted):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path)

        result = upload(handler, downloads, channel, "F1", "UA", b"x", space=None)

        body = created_folder(fake, channel)
        assert body.get("inheritedPermissionsDisabled", False) is restricted
        folder_id = fake.files_db[result.drive_id]["parents"][0]
        assert (
            fake.files_db[folder_id].get("inheritedPermissionsDisabled", False)
            is restricted
        )

    def test_restriction_can_be_turned_off(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path, restrict=False)
        upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)
        assert "inheritedPermissionsDisabled" not in created_folder(fake, "hr")

    def test_regular_drive_folders_are_not_restricted(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path, shared=False)
        upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)
        assert "inheritedPermissionsDisabled" not in created_folder(fake, "hr")

    def test_explicit_conversation_type_wins(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path)
        handler.set_conversation_type("general", "private")
        handler.set_conversation_type("hr", "public")

        upload(handler, downloads, "general", "F1", "UA", b"x", space=None)
        upload(handler, downloads, "hr", "F2", "UA", b"y", space=None)

        assert created_folder(fake, "general")["inheritedPermissionsDisabled"] is True
        assert "inheritedPermissionsDisabled" not in created_folder(fake, "hr")

    def test_existing_open_private_folder_is_restricted_once(self, tmp_path, downloads):
        fake = FakeDrive()
        fake.drives_db.append({"id": "SD1", "name": DRIVE_NAME})
        fake.add_folder("OLD", "hr", "SD1")  # from a run before this feature
        handler = make_meta_handler(fake, tmp_path)

        upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)
        upload(handler, downloads, "hr", "F2", "UA", b"y", space=None)

        updates = [kw for method, kw in fake.calls if method == "files.update"]
        assert len(updates) == 1
        assert updates[0]["fileId"] == "OLD"
        assert updates[0]["body"] == {"inheritedPermissionsDisabled": True}
        assert fake.files_db["OLD"]["inheritedPermissionsDisabled"] is True

    def test_restricted_folder_from_listing_needs_no_update(self, tmp_path, downloads):
        fake = FakeDrive()
        fake.drives_db.append({"id": "SD1", "name": DRIVE_NAME})
        fake.add_folder("OLD", "hr", "SD1", inheritedPermissionsDisabled=True)
        handler = make_meta_handler(fake, tmp_path)

        upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)

        assert fake.count("files.update") == 0

    def test_create_rejecting_the_flag_falls_back_to_update(self, tmp_path, downloads):
        fake = FakeDrive(reject_inherited_flag_on_create=True)
        handler = make_meta_handler(fake, tmp_path)

        result = upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)

        assert result.success
        folder_id = fake.files_db[result.drive_id]["parents"][0]
        assert fake.files_db[folder_id]["inheritedPermissionsDisabled"] is True

    def test_private_folder_is_shared_with_members_and_admin_only(
        self, tmp_path, downloads
    ):
        """Members (external ones included) and the admin; nobody else."""
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path)
        handler.user_resolver.workspace_admin = "admin@example.com"
        result = upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)
        folder_id = fake.files_db[result.drive_id]["parents"][0]
        fake.permissions_db[folder_id] = [
            {
                "id": "a",
                "type": "user",
                "role": "writer",
                "emailAddress": "admin@example.com",
            },
            {
                "id": "s",
                "type": "user",
                "role": "reader",
                "emailAddress": "stale@example.com",
            },
        ]

        handler.folder_manager.set_channel_folder_permissions(
            folder_id,
            "hr",
            ["alice@example.com", "ext@other.org"],
            handler.shared_drive_id,
        )

        members = sorted(p["emailAddress"] for p in fake.permissions_db[folder_id])
        assert members == ["admin@example.com", "alice@example.com", "ext@other.org"]
        assert fake.files_db[folder_id]["inheritedPermissionsDisabled"] is True

    def test_failed_external_folder_grants_are_counted(self, tmp_path, downloads):
        fake = FakeDrive(reject_grants_to={"ext@other.org"})
        handler = make_meta_handler(fake, tmp_path)
        result = upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)
        folder_id = fake.files_db[result.drive_id]["parents"][0]

        sync = handler.folder_manager.set_channel_folder_permissions(
            folder_id,
            "hr",
            ["alice@example.com", "ext@other.org"],
            handler.shared_drive_id,
        )

        assert sync.ok  # external failures don't fail the sync
        stats = handler.get_file_statistics()
        assert stats["external_folder_grant_failures"] == 1
        assert stats["folder_sync_failures"] == 0
        assert stats["failed_uploads"] == 0

    def test_failed_internal_folder_grants_are_counted(self, tmp_path, downloads):
        fake = FakeDrive(reject_grants_to={"bob@example.com"})
        handler = make_meta_handler(fake, tmp_path)
        upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)

        result = handler.sync_channel_folder_permissions(
            "hr", ["alice@example.com", "bob@example.com"]
        )

        assert not result.ok
        assert result.failed_grants == ("bob@example.com",)
        stats = handler.get_file_statistics()
        assert stats["folder_sync_failures"] == 1
        assert stats["folder_grant_failures"] == 1
        assert stats["external_folder_grant_failures"] == 0

    def test_failed_folder_lookup_is_counted(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path)
        fake.failing["files.list"] = http_error(403)

        result = handler.sync_channel_folder_permissions("hr", ["alice@example.com"])

        assert result.lookup_error
        assert handler.get_file_statistics()["folder_sync_failures"] == 1

    def test_failed_removals_are_counted(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        upload(handler, downloads, "c", "F1", "UA", b"x")
        assert handler.sync_channel_folder_permissions("c", ["alice@example.com"]).ok
        fake.failing["permissions.delete"] = http_error(403)

        result = handler.sync_channel_folder_permissions("c", [])

        assert result.failed_removals == ("alice@example.com",)
        stats = handler.get_file_statistics()
        assert stats["folder_removal_failures"] == 1
        assert stats["folder_sync_failures"] == 1

    def test_sync_without_drive_is_a_counted_failure(self, tmp_path):
        handler = make_handler(FakeDrive(), tmp_path)
        handler.folder_id = None  # Drive could not be set up

        result = handler.sync_channel_folder_permissions("c", ["alice@example.com"])

        assert not result.ok
        assert result.lookup_error
        assert handler.get_file_statistics()["folder_sync_failures"] == 1

    @pytest.mark.parametrize(
        "error",
        [http_error(500), ConnectionResetError("reset"), RefreshError("revoked")],
        ids=["http", "network", "refresh"],
    )
    def test_production_caller_counts_a_failed_lookup(self, tmp_path, downloads, error):
        """regular_membership goes through the counting path (#118)."""
        from slack_chat_migrator.services.spaces.regular_membership import (
            _update_folder_permissions,
        )

        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        upload(handler, downloads, "c", "F1", "UA", b"x")
        handler.folder_manager.folder_cache.clear()
        fake.failing["files.list"] = error

        _update_folder_permissions(handler, "c", ["alice@example.com"])  # no raise

        assert handler.get_file_statistics()["folder_sync_failures"] == 1

    def test_channel_without_folder_needs_no_sync(self, tmp_path):
        handler = make_meta_handler(FakeDrive(), tmp_path)

        result = handler.sync_channel_folder_permissions("hr", ["alice@example.com"])

        assert result.ok
        assert result.folder_id is None
        assert handler.get_file_statistics()["folder_sync_failures"] == 0

    @pytest.mark.parametrize(
        "error",
        [http_error(503), http_error(403), ConnectionResetError("reset")],
        ids=["503", "403", "network"],
    )
    @pytest.mark.parametrize("shared", [True, False], ids=["shared", "my_drive"])
    def test_public_files_never_fall_back_to_the_root(
        self, tmp_path, downloads, error, shared
    ):
        """The root's permissions don't include the channel's members (#118)."""
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path, shared=shared)
        handler.folder_manager.folder_cache.clear()
        fake.fail_query = lambda q: error if "mimeType" in q else None

        result = upload(handler, downloads, "general", "F1", "UA", b"x", space=None)

        assert not result.success
        assert result.error and "folder_unavailable" in result.error
        assert not [
            kw for name, kw in fake.calls if name == "files.create"
        ]  # nothing stored, in the root or anywhere
        stats = handler.get_file_statistics()
        assert stats["failed_uploads"] == 1
        assert stats["channel_folder_failures"] == 1
        assert downloads.calls == []  # failed before downloading

    def test_file_without_a_channel_goes_into_the_root(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        downloads.content["F1"] = b"x"

        result = handler.upload_attachment(
            {
                "id": "F1",
                "name": "a.pdf",
                "url_private": "https://files.slack.com/files-pri/T-F1/a.pdf",
            }
        )

        assert result.success
        assert fake.files_db[result.drive_id]["parents"] == ["SD1"]

    def test_private_files_never_fall_back_to_the_root(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path)
        handler.folder_manager.find_or_create_channel_folder = MagicMock(
            return_value=FolderLookup(None, "boom")
        )

        result = upload(handler, downloads, "hr", "F1", "UA", b"x", space=None)

        assert not result.success
        assert fake.count("files.create") == 0


# ---------------------------------------------------------------------------
# Canvases -> Google Docs, Lists -> Google Sheets (F9)
# ---------------------------------------------------------------------------

# Shape from Slack's "How to read Slack data exports" (canvases.json).
CANVAS = {
    "id": "F05UA7XQXHS",
    "name": "This_canvas_is_shared_in_General",
    "title": "This canvas is shared in General",
    "mimetype": "application/vnd.slack-docs",
    "filetype": "quip",
    "pretty_type": "Canvas",
    "user": "UA",
    "editable": True,
    "size": 48,  # not the size of the HTML rendering
    "mode": "quip",
    "is_tombstoned": False,
    "url_private_download": "https://files.slack.com/files-pri/T019B299WNQ-F05UA7XQXHS/download/canvas",
}
SLACK_LIST = {
    "id": "F0LIST",
    "name": "Roadmap",
    "title": "Roadmap",
    "filetype": "list",
    "mimetype": "application/vnd.slack-list",
    "user": "UA",
    "size": 9,
    "list_csv_download_url": "https://files.slack.com/files-pri/T1-F0LIST/download/list.csv",
}
CANVAS_HTML = b"<html><body><h1>Plan</h1><p>Ship it.</p></body></html>"
LIST_CSV = b"Task,Owner\nShip,Alice\n"


class DocumentDownloads(Downloads):
    """Records the URL and validation options each download used."""

    def __init__(self):
        super().__init__()
        self.kwargs: list[dict] = []

    def __call__(self, file_obj, dest_dir, channel=None, **kwargs):
        self.kwargs.append(kwargs)
        return super().__call__(file_obj, dest_dir, channel, **kwargs)


@pytest.fixture
def doc_downloads():
    fake = DocumentDownloads()
    fake.content.update({CANVAS["id"]: CANVAS_HTML, SLACK_LIST["id"]: LIST_CSV})
    with patch.object(file_mod, "download_file", fake):
        yield fake


def post_document(handler, file_obj, channel="general"):
    handler.state.context.current_channel = channel
    return handler.upload_attachment(
        dict(file_obj), channel, "spaces/S", None, "alice@example.com"
    )


class TestCanvasConversion:
    def test_canvas_becomes_a_google_doc(self, tmp_path, doc_downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)

        result = post_document(handler, CANVAS)

        assert result.success
        assert result.upload_type == "drive"
        record = fake.files_db[result.drive_id]
        assert record["name"] == "This canvas is shared in General"
        assert record["mimeType"] == "application/vnd.google-apps.document"
        assert record["media_mimetype"] == "text/html"
        assert record["appProperties"]["slackFileId"] == CANVAS["id"]
        assert result.metadata["converted_to"] == "application/vnd.google-apps.document"
        # HTML is the expected body, and the recorded size isn't checked.
        assert doc_downloads.kwargs[-1]["allow_html"] is True
        assert doc_downloads.kwargs[-1]["check_size"] is False

    def test_list_becomes_a_google_sheet_from_its_csv(self, tmp_path, doc_downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)

        result = post_document(handler, SLACK_LIST)

        record = fake.files_db[result.drive_id]
        assert record["name"] == "Roadmap"
        assert record["mimeType"] == "application/vnd.google-apps.spreadsheet"
        assert record["media_mimetype"] == "text/csv"
        assert doc_downloads.kwargs[-1]["url"] == SLACK_LIST["list_csv_download_url"]

    def test_failed_conversion_falls_back_to_plain_upload(
        self, tmp_path, doc_downloads
    ):
        fake = FakeDrive(fail_conversion=True)
        handler = make_handler(fake, tmp_path)

        canvas = post_document(handler, CANVAS)
        sheet = post_document(handler, SLACK_LIST)

        assert canvas.success and sheet.success
        assert (
            fake.files_db[canvas.drive_id]["name"]
            == "This canvas is shared in General.html"
        )
        assert fake.files_db[canvas.drive_id]["mimeType"] == "text/html"
        assert fake.files_db[sheet.drive_id]["name"] == "Roadmap.csv"
        assert fake.files_db[sheet.drive_id]["mimeType"] == "text/csv"
        assert "converted_to" not in canvas.metadata

    def test_other_upload_failures_are_not_blamed_on_conversion(
        self, tmp_path, doc_downloads, caplog
    ):
        """SF14: one attempt, the real cause, no "could not convert" warning."""
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        fake.add_folder("CHF", "general", "SD1")  # only the upload fails
        fake.failing["files.create"] = ConnectionResetError("connection reset")

        result = post_document(handler, CANVAS)

        assert not result.success
        assert result.error == (
            "Drive upload failed: ConnectionResetError: connection reset"
        )
        uploads = [
            kw
            for name, kw in fake.calls
            if name == "files.create" and kw.get("media_body") is not None
        ]
        assert len(uploads) == 1
        assert not any("Could not convert" in r.getMessage() for r in caplog.records)
        assert handler.get_file_statistics()["failed_uploads"] == 1

    def test_network_error_creating_the_folder_fails_the_file(
        self, tmp_path, doc_downloads
    ):
        fake = FakeDrive()
        handler = make_meta_handler(fake, tmp_path)  # "general" is public
        fake.failing["files.create"] = ConnectionResetError("connection reset")

        result = post_document(handler, CANVAS)

        assert not result.success
        assert result.error and result.error.startswith(
            "No Drive folder (folder_unavailable):"
        )
        assert "connection reset" in result.error
        assert handler.get_file_statistics()["channel_folder_failures"] == 1

    def test_conversion_can_be_turned_off(self, tmp_path, doc_downloads):
        fake = FakeDrive()
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name=DRIVE_NAME), convert_canvases=False
        )
        handler = make_handler(fake, tmp_path, config=config)

        result = post_document(handler, CANVAS)

        create = next(
            kw
            for name, kw in fake.calls
            if name == "files.create" and kw.get("media_body") is not None
        )
        assert "mimeType" not in create["body"]
        assert (
            fake.files_db[result.drive_id]["name"]
            == "This canvas is shared in General.html"
        )
        assert fake.files_db[result.drive_id]["mimeType"] == "text/html"

    def test_converted_canvas_is_found_again_on_rerun(self, tmp_path, doc_downloads):
        fake = FakeDrive()
        first = post_document(make_handler(fake, tmp_path), CANVAS)

        rerun = make_handler(fake, tmp_path)
        doc_downloads.calls.clear()
        again = post_document(rerun, CANVAS)

        assert again.drive_id == first.drive_id
        assert doc_downloads.calls == []

    def test_rerun_finds_the_canvas_when_its_folder_cannot_be_listed(
        self, tmp_path, doc_downloads
    ):
        """SF9: a failed listing is counted and retried; dedup still works."""
        fake = FakeDrive()
        first = post_document(make_handler(fake, tmp_path), CANVAS)
        folder = fake.files_db[first.drive_id]["parents"][0]
        listing = f"'{folder}' in parents and trashed=false"
        fake.fail_query = lambda q: http_error(500) if q == listing else None

        fake.calls.clear()
        rerun = make_handler(fake, tmp_path)
        doc_downloads.calls.clear()
        again = post_document(rerun, CANVAS)
        post_document(rerun, SLACK_LIST)

        assert again.drive_id == first.drive_id  # no second Google Doc
        assert doc_downloads.calls == [SLACK_LIST["id"]]
        listings = [kw for name, kw in fake.calls if kw.get("q") == listing]
        assert len(listings) == 2  # tried again with the next upload
        assert rerun.get_file_statistics()["folder_listing_failures"] == 2

    def test_failed_search_makes_no_second_canvas(self, tmp_path, doc_downloads):
        """Listing and per-file search both fail: fail the file, no new copy."""
        fake = FakeDrive()
        first = post_document(make_handler(fake, tmp_path), CANVAS)
        docs_before = len(fake.files_db)
        folder = fake.files_db[first.drive_id]["parents"][0]
        listing = f"'{folder}' in parents and trashed=false"

        def fail_file_lookups(q):
            looks_up_files = q == listing or q.startswith("appProperties")
            return http_error(500) if looks_up_files else None

        fake.fail_query = fail_file_lookups

        rerun = make_handler(fake, tmp_path)
        result = post_document(rerun, CANVAS)

        assert not result.success
        assert "earlier upload" in (result.error or "")
        assert len(fake.files_db) == docs_before
        assert rerun.get_file_statistics()["failed_uploads"] == 1

    def test_failed_listing_means_one_search_per_file(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        upload(handler, downloads, "c", "F0", "UA", b"zero")  # makes the folder
        folder = next(iter(handler.drive_uploader.folders_pre_cached - {"SD1"}))
        handler.drive_uploader.folders_pre_cached.discard(folder)
        listing = f"'{folder}' in parents and trashed=false"
        fake.fail_query = lambda q: http_error(500) if q == listing else None
        fake.calls.clear()

        result = upload(handler, downloads, "c", "F1", "UA", b"one")

        assert result.success
        searches = [
            kw for name, kw in fake.calls if kw.get("q", "").startswith("appProperties")
        ]
        assert len(searches) == 1

    def test_plain_csv_attachment_is_not_converted(self, tmp_path, downloads):
        fake = FakeDrive()
        handler = make_handler(fake, tmp_path)
        result = upload(
            handler,
            downloads,
            "c",
            "F1",
            "UA",
            b"a,b\n",
            name="data.csv",
            mime="text/csv",
            space=None,
        )
        assert fake.files_db[result.drive_id]["mimeType"] == "text/csv"
        assert fake.files_db[result.drive_id]["name"] == "data.csv"


class TestTransferConfig:
    def test_default_is_off(self):
        assert MigrationConfig().transfer_file_ownership is False
        assert MigrationConfig.from_dict({}).transfer_file_ownership is False

    def test_from_dict(self):
        config = MigrationConfig.from_dict({"transfer_file_ownership": True})
        assert config.transfer_file_ownership is True

    def test_rejects_non_booleans(self):
        from slack_chat_migrator.exceptions import ConfigError

        with pytest.raises(ConfigError, match="transfer_file_ownership"):
            MigrationConfig.from_dict({"transfer_file_ownership": "yes"})


# ---------------------------------------------------------------------------
# Private folders fail closed (SF2)
# ---------------------------------------------------------------------------


class TestPrivateFoldersFailClosed:
    def _handler(self, fake, tmp_path):
        return make_meta_handler(fake, tmp_path)

    def test_no_file_goes_into_an_open_private_folder(
        self, tmp_path, downloads, caplog
    ):
        fake = FakeDrive(reject_inherited_flag_on_create=True, fail_restriction=True)
        handler = self._handler(fake, tmp_path)

        result = upload(handler, downloads, "hr", "F1", "UA", b"secret", space=None)

        assert not result.success
        assert "limited-access" in result.error
        uploads = [
            kw
            for name, kw in fake.calls
            if name == "files.create" and kw.get("media_body")
        ]
        assert uploads == []  # nothing was uploaded
        assert downloads.calls == []  # nor downloaded
        stats = handler.get_file_statistics()
        assert stats["failed_uploads"] == 1
        assert stats["restricted_folder_failures"] == 1
        assert any(
            r.levelname == "ERROR" and "hr" in r.getMessage() for r in caplog.records
        )

    def test_restriction_is_retried_with_the_next_file(self, tmp_path, downloads):
        fake = FakeDrive(reject_inherited_flag_on_create=True, fail_restriction=True)
        handler = self._handler(fake, tmp_path)

        first = upload(handler, downloads, "hr", "F1", "UA", b"one", space=None)
        fake.fail_restriction = False
        second = upload(handler, downloads, "hr", "F2", "UA", b"two", space=None)

        assert not first.success
        assert second.success
        folder_id = fake.files_db[second.drive_id]["parents"][0]
        assert fake.files_db[folder_id]["inheritedPermissionsDisabled"] is True

    def test_public_channel_is_unaffected(self, tmp_path, downloads):
        fake = FakeDrive(reject_inherited_flag_on_create=True, fail_restriction=True)
        handler = self._handler(fake, tmp_path)
        result = upload(handler, downloads, "general", "F1", "UA", b"x", space=None)
        assert result.success
