"""Unit tests for PendingPermissions (failed grants and transfers, #98)."""

from unittest.mock import MagicMock

import pytest

from slack_chat_migrator.services.drive.pending import (
    MAX_APP_PROPERTIES,
    MAX_PENDING_GRANTS,
    UNKNOWN_TARGET,
    PendingPermissions,
    Target,
    decode_target,
    encode_target,
    grant_key,
)
from slack_chat_migrator.services.drive_adapter import DriveAdapter
from tests.unit.drive_fakes import FakeDrive, http_error


def _pending(failures=None):
    return PendingPermissions(
        MagicMock(),
        on_write_failure=(
            None
            if failures is None
            else lambda f, clearing: failures.append((f, clearing))
        ),
    )


def _props(update_call):
    return update_call.args[1]["appProperties"]


def _last_props(pending):
    return _props(pending.drive_service.update_file.call_args)


def _email_of_bytes(total, key):
    """An address that makes key + value exactly *total* bytes."""
    domain = "@example.com"
    return "a" * (total - len(key.encode()) - len(domain)) + domain


class TestGrantFlags:
    def test_flag_is_stored_once_and_cleared(self):
        pending = _pending()
        key = grant_key("Bob@Example.com ")

        pending.set_grant("f1", "bob@example.com", True, "SD", user_id="UB")
        pending.set_grant("f1", "bob@example.com", True, "SD", user_id="UB")
        pending.set_grant("f1", "bob@example.com", False, "SD")
        pending.set_grant("f1", "bob@example.com", False, "SD")

        calls = pending.drive_service.update_file.call_args_list
        assert [_props(c) for c in calls] == [
            {"slackGrantPending": "true", key: "UB bob@example.com"},
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
        assert _last_props(pending) == {grant_key("y@example.com"): None}

    def test_a_set_flag_learns_what_it_lacked_without_a_write(self):
        """A flag read back as "-" keeps the email this run knows."""
        pending = _pending()
        key = grant_key("bob@example.com")
        pending.read("f1", {"slackGrantPending": "true", key: UNKNOWN_TARGET})

        pending.set_grant("f1", "bob@example.com", True, user_id="UB")

        pending.drive_service.update_file.assert_not_called()
        assert pending.grants["f1"][key] == Target("UB", "bob@example.com")

    def test_clear_by_key_clears_a_grant_stored_for_an_old_email(self):
        pending = _pending()
        key = grant_key("old@example.com")
        pending.read("f1", {"slackGrantPending": "true", key: "UB old@example.com"})

        pending.clear_grant("f1", key, "SD")

        assert _last_props(pending) == {key: None, "slackGrantPending": None}
        assert pending.grants == {}

    @pytest.mark.parametrize(
        "error", [http_error(403), ConnectionResetError("reset")], ids=["http", "net"]
    )
    def test_failure_to_store_the_flag_warns_and_is_reported(self, error, caplog):
        failures = []
        pending = _pending(failures)
        pending.drive_service.update_file.side_effect = error

        pending.set_owner("f1", "alice@example.com", True, user_id="UA")

        assert pending.owners == {"f1": Target("UA", "alice@example.com")}
        assert failures == [("f1", False)]
        (warning,) = [r.getMessage() for r in caplog.records]
        assert "Could not record" in warning and "f1" in warning
        assert "later runs won't know about it" in warning

    def test_failure_to_clear_is_reported_as_a_clear(self, caplog):
        """A flag left after a success means a harmless second grant."""
        failures = []
        pending = _pending(failures)
        pending.set_grant("f1", "bob@example.com", True)
        pending.drive_service.update_file.side_effect = ConnectionResetError("x")

        pending.set_grant("f1", "bob@example.com", False)

        assert failures == [("f1", True)]
        assert pending.grants == {}
        (warning,) = [r.getMessage() for r in caplog.records]
        assert "Could not clear the pending flag on Drive file f1" in warning
        assert "the next run's sweep clears it" in warning


class TestEncoding:
    """Drive limits an entry to 124 bytes of key plus value."""

    def test_entry_of_exactly_124_bytes_is_stored_and_125_is_not(self):
        key = grant_key("probe@example.com")  # every grant key is 19 bytes
        fits = _email_of_bytes(124, key)
        too_long = "b" + fits
        assert len(grant_key(fits)) == len(key) == len(grant_key(too_long))

        pending = _pending()
        pending.set_grant("f1", fits, True)
        assert _last_props(pending)[grant_key(fits)] == fits
        pending.set_grant("f2", too_long, True)
        assert _last_props(pending)[grant_key(too_long)] == UNKNOWN_TARGET

    def test_entry_limit_counts_the_key(self):
        email = _email_of_bytes(124, "")  # 124 bytes on its own
        pending = _pending()

        pending.set_grant("f1", email, True)

        assert _last_props(pending)[grant_key(email)] == UNKNOWN_TARGET

    def test_entry_limit_counts_utf8_bytes_not_characters(self):
        email = "é" * 50 + "@example.com"  # 62 characters, 112 bytes
        key = grant_key(email)
        assert len(key) + len(email) <= 124 < len(key) + len(email.encode())

        assert encode_target(key, Target(email=email)) == UNKNOWN_TARGET

    @pytest.mark.parametrize(
        ("target", "stored"),
        [
            (Target("U1", "bob@example.com"), "U1 bob@example.com"),
            (Target("U1", "a" * 100 + "@example.com"), "U1"),
            (Target(None, "bob@example.com"), "bob@example.com"),
            (Target("U1", None), "U1"),
            (Target.unknown(), UNKNOWN_TARGET),
        ],
        ids=["both", "id-when-both-dont-fit", "email-only", "id-only", "nothing"],
    )
    def test_user_id_and_email_are_stored_while_they_fit(self, target, stored):
        assert encode_target(grant_key("x@example.com"), target) == stored

    @pytest.mark.parametrize(
        ("value", "target"),
        [
            ("U1 bob@example.com", Target("U1", "bob@example.com")),
            ("U1", Target("U1", None)),
            ("bob@example.com", Target(None, "bob@example.com")),  # older runs
            (UNKNOWN_TARGET, Target.unknown()),
            (None, Target.unknown()),
        ],
    )
    def test_stored_values_are_read_back(self, value, target):
        assert decode_target(value) == target

    def test_long_owner_email_is_stored_as_dash_but_remembered(self):
        email = "a" * 120 + "@example.com"
        pending = _pending()

        pending.set_owner("f1", email, True)

        assert _last_props(pending) == {
            "slackOwnerPending": "true",
            "slackOwner": UNKNOWN_TARGET,
        }
        pending.read("f1", {"slackOwnerPending": "true", "slackOwner": UNKNOWN_TARGET})
        assert pending.owner("f1") == Target(email=email)

    def test_long_owner_email_keeps_the_user_id(self):
        pending = _pending()

        pending.set_owner("f1", "a" * 120 + "@example.com", True, user_id="UA")

        assert _last_props(pending)["slackOwner"] == "UA"

    def test_clearing_a_transfer_removes_the_stored_owner(self):
        pending = _pending()
        pending.set_owner("f1", "alice@example.com", True)

        pending.set_owner("f1", "alice@example.com", False)

        assert _last_props(pending) == {"slackOwnerPending": None, "slackOwner": None}

    def test_a_transfer_to_someone_else_replaces_the_stored_owner(self):
        pending = _pending()
        pending.set_owner("f1", "alice@example.com", True, user_id="UA")

        pending.set_owner("f1", "carol@example.com", True, user_id="UC")

        assert _last_props(pending)["slackOwner"] == "UC carol@example.com"
        assert pending.owner("f1") == Target("UC", "carol@example.com")


class TestTarget:
    def test_email_is_normalized_when_built(self):
        assert Target(" U1 ", " Bob@Example.COM ") == Target("U1", "bob@example.com")

    def test_empty_values_are_unknown(self):
        target = Target("", "  ")

        assert target == Target.unknown()
        assert not target.is_known
        assert Target(email="a@example.com").is_known


class TestUserIdFirst:
    @pytest.mark.parametrize(
        ("key", "email_bytes"),
        [(grant_key("x@example.com"), 100), ("slackOwner", 110)],
        ids=["grant", "owner"],
    )
    def test_user_id_alone_when_both_dont_fit(self, key, email_bytes):
        """The email would fit alone, but the user ID is what a later run needs."""
        email = "a" * (email_bytes - len("@example.com")) + "@example.com"
        uid = "U0123456789"
        assert len(key) + len(email) <= 124 < len(key) + len(uid) + 1 + len(email)

        assert encode_target(key, Target(uid, email)) == uid

    def test_same_slack_user_matches_across_an_email_change(self):
        assert Target("UA", "old@example.com").is_for("UA", "new@example.com")
        assert not Target("UA", "a@example.com").is_for("UB", "a@example.com")
        assert Target(email="A@Example.com").is_for(None, "a@example.com")


class TestPropertyCap:
    """Drive allows 30 private properties per file and app."""

    def test_budget_leaves_room_for_the_fixed_keys(self):
        assert MAX_PENDING_GRANTS == MAX_APP_PROPERTIES - 6 == 24

    def test_grants_beyond_the_cap_are_kept_in_memory_and_counted(self, caplog):
        failures = []
        pending = _pending(failures)
        emails = [f"p{i}@example.com" for i in range(MAX_PENDING_GRANTS + 1)]

        for email in emails:
            pending.set_grant("f1", email, True)

        assert pending.drive_service.update_file.call_count == MAX_PENDING_GRANTS
        assert failures == [("f1", False)]
        assert pending.has_grant("f1", emails[-1])  # this run's sweep retries it
        assert any("property limit" in r.getMessage() for r in caplog.records)

        pending.drive_service.update_file.reset_mock()
        pending.set_grant("f1", emails[-1], False)  # never stored: nothing sent
        pending.drive_service.update_file.assert_not_called()
        for email in emails[:-1]:
            pending.set_grant("f1", email, False)
        assert _last_props(pending) == {
            grant_key(emails[-2]): None,
            "slackGrantPending": None,
        }


class TestRead:
    def test_known_email_is_not_replaced_by_the_stored_dash(self):
        pending = _pending()
        email = "a" * 120 + "@example.com"
        pending.set_grant("f1", email, True)

        pending.read(
            "f1", {"slackGrantPending": "true", grant_key(email): UNKNOWN_TARGET}
        )

        assert pending.grants["f1"][grant_key(email)] == Target(email=email)

    def test_stored_user_id_is_added_to_what_this_run_knows(self):
        pending = _pending()
        email = "a" * 120 + "@example.com"
        pending.set_grant("f1", email, True)

        pending.read("f1", {"slackGrantPending": "true", grant_key(email): "UA"})

        assert pending.grants["f1"][grant_key(email)] == Target("UA", email)

    def test_targets_without_the_marker_are_ignored(self):
        pending = _pending()

        pending.read("f1", {grant_key("x@example.com"): "x@example.com"})

        assert pending.grants == {}

    def test_views_are_read_only(self):
        pending = _pending()
        pending.set_owner("f1", "alice@example.com", True)

        with pytest.raises(TypeError):
            pending.owners["f2"] = Target.unknown()  # type: ignore[index]
        with pytest.raises(TypeError):
            pending.grants["f2"] = {}  # type: ignore[index]


class TestFindFlagged:
    def _fake(self):
        fake = FakeDrive()
        flags = {"slackGrantPending": "true", grant_key("x@e.com"): "UX x@e.com"}
        fake.add_file("in_root", parents=["ROOT"], appProperties=flags)
        fake.add_file("in_channel", parents=["CH"], appProperties=dict(flags))
        fake.add_file("elsewhere", parents=["OTHER"], appProperties=dict(flags))
        fake.add_file(
            "owner",
            parents=["CH"],
            appProperties={"slackOwnerPending": "true", "slackOwner": "a@e.com"},
        )
        fake.add_file(
            "both",
            parents=["CH"],
            appProperties={
                **flags,
                "slackOwnerPending": "true",
                "slackOwner": "UA a@e.com",
            },
        )
        return fake

    def test_regular_drive_keeps_only_the_migration_folders(self):
        pending = PendingPermissions(DriveAdapter(self._fake()))

        found = pending.find_flagged(None, {"ROOT", "CH"})

        assert found == 4  # "both" counts once
        assert set(pending.grants) == {"in_root", "in_channel", "both"}
        assert pending.grants["in_root"] == {
            grant_key("x@e.com"): Target("UX", "x@e.com")
        }
        assert pending.owners == {
            "owner": Target(email="a@e.com"),
            "both": Target("UA", "a@e.com"),
        }

    def test_shared_drive_is_searched_as_a_whole(self):
        fake = self._fake()
        pending = PendingPermissions(DriveAdapter(fake))

        pending.find_flagged("SD", set())

        queries = [kw for name, kw in fake.calls if name == "files.list"]
        assert {kw["driveId"] for kw in queries} == {"SD"}
        assert all(kw["pageSize"] == 1000 for kw in queries)
        # Drive requires both flags with corpora=drive (FakeDrive checks too).
        for kw in queries:
            assert kw["corpora"] == "drive"
            assert kw["includeItemsFromAllDrives"] is True
            assert kw["supportsAllDrives"] is True
        assert {kw["q"] for kw in queries} == {
            "appProperties has { key='slackGrantPending' and value='true' }"
            " and trashed=false",
            "appProperties has { key='slackOwnerPending' and value='true' }"
            " and trashed=false",
        }
