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
