"""FakeDrive answers shared-drive calls the way Drive does (TA S19/S5b)."""

import pytest
from googleapiclient.errors import HttpError

from tests.unit.drive_fakes import FakeDrive


def _fake():
    fake = FakeDrive(drives=[{"id": "SD", "name": "Attachments"}])
    fake.add_folder("CH", "general", "SD")
    fake.add_file("in_drive", parents=["CH"])
    fake.add_file("in_my_drive", parents=["ROOT"])
    return fake


def _status(call):
    with pytest.raises(HttpError) as excinfo:
        call.execute()
    return excinfo.value.resp.status


class TestSupportsAllDrives:
    def test_shared_drive_file_is_not_found_without_the_flag(self):
        fake = _fake()
        body = {"role": "writer", "type": "user", "emailAddress": "a@example.com"}

        assert _status(fake.files().get(fileId="in_drive")) == 404
        assert _status(fake.files().update(fileId="in_drive", body={})) == 404
        assert _status(fake.permissions().create(fileId="in_drive", body=body)) == 404
        assert _status(fake.permissions().list(fileId="in_drive")) == 404
        assert (
            _status(fake.files().create(body={"name": "x", "parents": ["CH"]})) == 404
        )

    def test_with_the_flag_the_file_is_found(self):
        fake = _fake()

        got = fake.files().get(fileId="in_drive", supportsAllDrives=True).execute()

        assert got["id"] == "in_drive"

    def test_my_drive_file_needs_no_flag(self):
        assert _fake().files().get(fileId="in_my_drive").execute()["id"]


class TestListScope:
    def test_shared_drive_items_need_include_items_from_all_drives(self):
        fake = _fake()

        listed = fake.files().list(q="trashed=false").execute()["files"]

        assert {f["id"] for f in listed} == {"in_my_drive"}

    @pytest.mark.parametrize(
        "missing", ["driveId", "includeItemsFromAllDrives", "supportsAllDrives"]
    )
    def test_corpora_drive_needs_the_drive_and_both_flags(self, missing):
        kwargs = {
            "corpora": "drive",
            "driveId": "SD",
            "includeItemsFromAllDrives": True,
            "supportsAllDrives": True,
        }
        del kwargs[missing]

        assert _status(_fake().files().list(q="trashed=false", **kwargs)) == 400

    def test_corpora_drive_lists_only_that_drive(self):
        listed = (
            _fake()
            .files()
            .list(
                q="trashed=false",
                corpora="drive",
                driveId="SD",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()["files"]
        )

        assert {f["id"] for f in listed} == {"CH", "in_drive"}


class TestOwnership:
    """Only the owner can transfer, and the old owner becomes a writer."""

    def _transfer(self, fake, file_id, email):
        body = {"type": "user", "role": "owner", "emailAddress": email}
        return fake.permissions().create(
            fileId=file_id, body=body, transferOwnership=True
        )

    def test_transfer_moves_ownership_and_downgrades_the_admin(self):
        fake = FakeDrive()
        fake.add_file("F", parents=["ROOT"])

        self._transfer(fake, "F", "alice@example.com").execute()

        got = fake.files().get(fileId="F").execute()
        assert got["owners"] == [{"emailAddress": "alice@example.com"}]
        assert got["ownedByMe"] is False
        roles = {p["emailAddress"]: p["role"] for p in fake.permissions_db["F"]}
        assert roles == {"admin@example.com": "writer", "alice@example.com": "owner"}

    def test_a_non_owner_cannot_transfer(self):
        fake = FakeDrive()
        fake.add_file("F", parents=["ROOT"])
        self._transfer(fake, "F", "alice@example.com").execute()

        assert _status(self._transfer(fake, "F", "bob@example.com")) == 403

    def test_shared_drive_files_have_no_owners(self):
        got = _fake().files().get(fileId="in_drive", supportsAllDrives=True).execute()

        assert "owners" not in got


def test_a_pagination_loop_fails_fast():
    fake = FakeDrive(file_page_cap=1)
    fake.add_file("A", parents=["ROOT"])
    fake.add_file("B", parents=["ROOT"])

    with pytest.raises(AssertionError, match="pagination loop"):
        for _ in range(1000):  # never passes the nextPageToken it gets
            fake.files().list(q="trashed=false").execute()
