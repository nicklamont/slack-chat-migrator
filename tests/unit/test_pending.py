"""Unit tests for PendingPermissions (failed grants and transfers, #98)."""

from unittest.mock import MagicMock

import pytest

from slack_chat_migrator.services.drive.pending import (
    UNKNOWN_TARGET,
    PendingPermissions,
    grant_key,
)
from slack_chat_migrator.services.drive_adapter import DriveAdapter
from tests.unit.drive_fakes import FakeDrive, http_error


def _pending():
    return PendingPermissions(MagicMock())


def _props(update_call):
    return update_call.args[1]["appProperties"]


class TestGrantFlags:
    def test_flag_is_stored_once_and_cleared(self):
        pending = _pending()
        key = grant_key("Bob@Example.com ")

        pending.set_grant("f1", "bob@example.com", True, "SD")
        pending.set_grant("f1", "bob@example.com", True, "SD")
        pending.set_grant("f1", "bob@example.com", False, "SD")
        pending.set_grant("f1", "bob@example.com", False, "SD")

        calls = pending.drive_service.update_file.call_args_list
        assert [_props(c) for c in calls] == [
            {"slackGrantPending": "true", key: "bob@example.com"},
            {key: None, "slackGrantPending": None},
        ]
        assert all(c.kwargs["supports_all_drives"] is True for c in calls)
        assert pending.grants == {}

    def test_flags_are_per_poster(self):
        """Y's success never clears X's pending grant."""
        pending = _pending()

        pending.set_grant("f1", "x@example.com", True)
        pending.set_grant("f1", "y@example.com", True)
        pending.set_grant("f1", "y@example.com", False)

        assert pending.has_grant("f1", "x@example.com")
        assert not pending.has_grant("f1", "y@example.com")
        last = _props(pending.drive_service.update_file.call_args)
        assert last == {grant_key("y@example.com"): None}  # marker kept for X

    def test_key_and_value_fit_in_an_app_property(self):
        pending = _pending()
        long_email = "a" * 120 + "@example.com"

        pending.set_grant("f1", long_email, True)

        props = _props(pending.drive_service.update_file.call_args)
        key = grant_key(long_email)
        assert props[key] == UNKNOWN_TARGET
        assert len(key) <= 124
        assert pending.has_grant("f1", long_email)  # this run still knows it
        assert pending.grants["f1"][key] == long_email

    @pytest.mark.parametrize(
        "error", [http_error(403), ConnectionResetError("reset")], ids=["http", "net"]
    )
    def test_failure_to_store_the_flag_warns_and_is_reported(self, error, caplog):
        failures = []
        pending = PendingPermissions(
            MagicMock(), on_write_failure=lambda f, e: failures.append(f)
        )
        pending.drive_service.update_file.side_effect = error

        pending.set_owner("f1", "alice@example.com", True)

        assert pending.owners == {"f1": "alice@example.com"}
        assert failures == ["f1"]
        assert any(
            r.levelname == "WARNING" and "f1" in r.getMessage() for r in caplog.records
        )


class TestRead:
    def test_known_email_is_not_replaced_by_the_stored_dash(self):
        pending = _pending()
        email = "a" * 120 + "@example.com"
        pending.set_grant("f1", email, True)

        pending.read(
            "f1", {"slackGrantPending": "true", grant_key(email): UNKNOWN_TARGET}
        )

        assert pending.grants["f1"][grant_key(email)] == email

    def test_targets_without_the_marker_are_ignored(self):
        pending = _pending()

        pending.read("f1", {grant_key("x@example.com"): "x@example.com"})

        assert pending.grants == {}


class TestFindFlagged:
    def _fake(self):
        fake = FakeDrive()
        flags = {"slackGrantPending": "true", grant_key("x@e.com"): "x@e.com"}
        fake.add_file("in_root", parents=["ROOT"], appProperties=flags)
        fake.add_file("in_channel", parents=["CH"], appProperties=dict(flags))
        fake.add_file("elsewhere", parents=["OTHER"], appProperties=dict(flags))
        fake.add_file(
            "owner",
            parents=["CH"],
            appProperties={"slackOwnerPending": "true", "slackOwner": "a@e.com"},
        )
        return fake

    def test_regular_drive_keeps_only_the_migration_folders(self):
        pending = PendingPermissions(DriveAdapter(self._fake()))

        found = pending.find_flagged(None, {"ROOT", "CH"})

        assert found == 3
        assert set(pending.grants) == {"in_root", "in_channel"}
        assert pending.owners == {"owner": "a@e.com"}

    def test_shared_drive_is_searched_as_a_whole(self):
        fake = self._fake()
        pending = PendingPermissions(DriveAdapter(fake))

        pending.find_flagged("SD", set())

        queries = [kw for name, kw in fake.calls if name == "files.list"]
        assert {kw["driveId"] for kw in queries} == {"SD"}
        assert all(kw["pageSize"] == 1000 for kw in queries)
        assert {kw["q"] for kw in queries} == {
            "appProperties has { key='slackGrantPending' and value='true' }"
            " and trashed=false",
            "appProperties has { key='slackOwnerPending' and value='true' }"
            " and trashed=false",
        }
