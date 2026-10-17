"""Unit tests for the FolderManager class."""

from unittest.mock import MagicMock

import pytest
from googleapiclient.errors import HttpError

from slack_chat_migrator.services.drive.folder_manager import (
    FolderLookup,
    FolderManager,
    FolderSyncResult,
)
from slack_chat_migrator.services.drive_adapter import DriveAdapter
from tests.unit.drive_fakes import FakeDrive


def _make_http_error(status, reason="error"):
    """Create a mock HttpError with the given status."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=b"{}")


def _make_drive_service():
    """Create a mock DriveAdapter."""
    service = MagicMock()
    return service


# -----------------------------------------------------------
# get_or_create_channel_folder
# -----------------------------------------------------------


class TestGetOrCreateChannelFolder:
    """Tests for get_or_create_channel_folder."""

    def test_dry_run_delegates_to_drive_service(self):
        """Dry run no longer short-circuits — DI stubs handle API calls."""
        svc = _make_drive_service()
        svc.list_files.return_value = {"files": []}
        svc.create_file.return_value = {"id": "stub-chan-id"}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "stub-chan-id"
        svc.list_files.assert_called_once()

    def test_creates_new_channel_folder(self):
        svc = _make_drive_service()

        # list returns no existing folders
        svc.list_files.return_value = {"files": []}
        # create returns new folder
        svc.create_file.return_value = {"id": "chan_folder_id"}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "chan_folder_id"
        assert fm.folder_cache[("parent123", "general")] == "chan_folder_id"

    def test_creates_channel_folder_in_shared_drive(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {"files": []}
        svc.create_file.return_value = {"id": "shared_chan_id"}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder(
            "general", "parent123", shared_drive_id="drive456"
        )

        assert result == "shared_chan_id"
        create_kwargs = svc.create_file.call_args[1]
        assert create_kwargs["supports_all_drives"] is True

    def test_finds_existing_channel_folder(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {
            "files": [{"id": "existing_chan", "name": "general"}]
        }

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "existing_chan"
        svc.create_file.assert_not_called()
        assert fm.folder_cache[("parent123", "general")] == "existing_chan"

    def test_returns_cached_folder_id(self):
        """The per-run cache is trusted: no files.get per upload (P3)."""
        svc = _make_drive_service()

        fm = FolderManager(svc)
        fm.folder_cache[("parent123", "general")] = "cached_id"

        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "cached_id"
        svc.get_file.assert_not_called()
        svc.list_files.assert_not_called()
        svc.create_file.assert_not_called()

    def test_http_error_returns_none(self):
        svc = _make_drive_service()
        svc.list_files.side_effect = _make_http_error(403)

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result is None

    def test_create_returning_no_id_returns_none(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {"files": []}
        svc.create_file.return_value = {}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result is None


# -----------------------------------------------------------
# create_regular_drive_folder
# -----------------------------------------------------------


class TestCreateRegularDriveFolder:
    """Tests for create_regular_drive_folder."""

    def test_dry_run_delegates_to_drive_service(self):
        """Dry run no longer short-circuits — DI stubs handle API calls."""
        svc = _make_drive_service()
        svc.list_files.return_value = {"files": []}
        svc.create_file.return_value = {"id": "stub-regular-id"}

        fm = FolderManager(svc)
        result = fm.create_regular_drive_folder("attachments")

        assert result == "stub-regular-id"
        svc.list_files.assert_called_once()

    def test_creates_folder_with_correct_metadata(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {"files": []}
        svc.create_file.return_value = {"id": "regular_id"}

        fm = FolderManager(svc)
        result = fm.create_regular_drive_folder("attachments")

        assert result == "regular_id"
        body = svc.create_file.call_args[1]["body"]
        assert body["name"] == "attachments"
        assert body["mimeType"] == ("application/vnd.google-apps.folder")
        assert "parents" not in body

    def test_finds_existing_regular_folder(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {
            "files": [{"id": "existing_reg", "name": "attachments"}]
        }

        fm = FolderManager(svc)
        result = fm.create_regular_drive_folder("attachments")

        assert result == "existing_reg"
        svc.create_file.assert_not_called()

    def test_http_error_returns_none(self):
        svc = _make_drive_service()
        svc.list_files.side_effect = _make_http_error(500)

        fm = FolderManager(svc)
        result = fm.create_regular_drive_folder("attachments")

        assert result is None


# -----------------------------------------------------------
# get_channel_folder_id
# -----------------------------------------------------------


class TestGetChannelFolderId:
    """Tests for get_channel_folder_id."""

    def test_returns_cached_folder_id(self):
        svc = _make_drive_service()
        fm = FolderManager(svc)
        fm.folder_cache[("parent123", "general")] = "cached_id"

        result = fm.get_channel_folder_id("general", "parent123")

        assert result == "cached_id"
        svc.list_files.assert_not_called()

    def test_finds_folder_by_name(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {"files": [{"id": "found_id", "name": "general"}]}

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id("general", "parent123")

        assert result == "found_id"
        assert fm.folder_cache[("parent123", "general")] == "found_id"

    def test_no_matching_folder_returns_none(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {"files": []}

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id("general", "parent123")

        assert result is None

    def test_http_error_is_raised_not_read_as_no_folder(self):
        svc = _make_drive_service()
        svc.list_files.side_effect = _make_http_error(403)

        fm = FolderManager(svc)
        with pytest.raises(HttpError):
            fm.get_channel_folder_id("general", "parent123")

    def test_find_channel_folder_returns_the_error(self):
        svc = _make_drive_service()
        svc.list_files.side_effect = _make_http_error(403)

        lookup = FolderManager(svc).find_channel_folder("general", "parent123")

        assert lookup.folder_id is None
        assert lookup.error and "could not look up the folder" in lookup.error

    def test_find_channel_folder_without_folder_is_no_error(self):
        svc = _make_drive_service()
        svc.list_files.return_value = {"files": []}

        lookup = FolderManager(svc).find_channel_folder("general", "parent123")

        assert lookup == FolderLookup(None)

    def test_shared_drive_params_passed(self):
        svc = _make_drive_service()

        svc.list_files.return_value = {"files": [{"id": "sd_id", "name": "general"}]}

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id(
            "general", "parent123", shared_drive_id="drive789"
        )

        assert result == "sd_id"
        list_kwargs = svc.list_files.call_args[1]
        assert list_kwargs["drive_id"] == "drive789"
        assert list_kwargs["supports_all_drives"] is True
        assert list_kwargs["include_items_from_all_drives"] is True


# -----------------------------------------------------------
# set_channel_folder_permissions
# -----------------------------------------------------------


class TestSetChannelFolderPermissions:
    """Folder access is synced to the member list (I37)."""

    ADMIN = "admin@example.com"

    def _fm(self, fake=None, **kwargs):
        fake = fake or FakeDrive()
        fake.add_folder("F", "general", "ROOT")
        kwargs.setdefault("protected_emails", lambda: [self.ADMIN])
        return FolderManager(DriveAdapter(fake), "example.com", **kwargs), fake

    @staticmethod
    def _members(fake, folder="F"):
        return sorted(
            (p["emailAddress"], p["role"]) for p in fake.permissions_db.get(folder, [])
        )

    def test_grants_reader_access_to_all_users(self):
        fm, fake = self._fm()

        result = fm.set_channel_folder_permissions(
            "F", "general", ["a@example.com", "b@example.com"]
        )

        assert result.ok
        assert result == FolderSyncResult(folder_id="F", granted=2)
        assert self._members(fake) == [
            ("a@example.com", "reader"),
            ("b@example.com", "reader"),
        ]
        assert fake.batch_sizes == [2]  # one batch request, not two calls

    def test_resync_removes_departed_members(self):
        fm, fake = self._fm()
        fm.set_channel_folder_permissions(
            "F", "general", ["a@example.com", "b@example.com"]
        )
        fake.calls.clear()

        result = fm.set_channel_folder_permissions("F", "general", ["b@example.com"])

        assert result == FolderSyncResult(folder_id="F", removed=1)
        assert self._members(fake) == [("b@example.com", "reader")]
        assert fake.count("permissions.create") == 0
        assert fake.count("permissions.delete") == 1

    def test_never_removes_admin_owners_organizers_or_inherited(self):
        fm, fake = self._fm()
        fake.permissions_db["F"] = [
            {"id": "p1", "type": "user", "role": "writer", "emailAddress": self.ADMIN},
            {
                "id": "p2",
                "type": "user",
                "role": "owner",
                "emailAddress": "o@example.com",
            },
            {
                "id": "p3",
                "type": "user",
                "role": "organizer",
                "emailAddress": "m@example.com",
            },
            {
                "id": "p4",
                "type": "user",
                "role": "reader",
                "emailAddress": "drive-member@example.com",
                "permissionDetails": [{"inherited": True}],
            },
            {"id": "p5", "type": "domain", "role": "reader", "domain": "example.com"},
            {
                "id": "p6",
                "type": "user",
                "role": "reader",
                "emailAddress": "gone@example.com",
            },
        ]

        fm.set_channel_folder_permissions("F", "general", ["a@example.com"])

        remaining = {p["id"] for p in fake.permissions_db["F"]}
        assert {"p1", "p2", "p3", "p4", "p5"} <= remaining
        assert "p6" not in remaining

    def test_external_members_get_reader_access(self):
        """Slack Connect members keep folder access, as before the audit fixes."""
        fm, fake = self._fm()
        fake.permissions_db["F"] = [
            {
                "id": "x",
                "type": "user",
                "role": "reader",
                "emailAddress": "ext@other.org",
            }
        ]

        result = fm.set_channel_folder_permissions(
            "F", "general", ["a@example.com", "ext@other.org", "new@partner.io"]
        )

        assert result.ok
        assert result.granted == 2
        assert self._members(fake) == [
            ("a@example.com", "reader"),
            ("ext@other.org", "reader"),
            ("new@partner.io", "reader"),
        ]
        assert fake.count("permissions.delete") == 0

    def test_departed_external_member_is_removed(self):
        fm, fake = self._fm()
        fake.permissions_db["F"] = [
            {
                "id": "x",
                "type": "user",
                "role": "reader",
                "emailAddress": "ext@other.org",
            }
        ]
        fm.set_channel_folder_permissions("F", "general", ["a@example.com"])
        assert self._members(fake) == [("a@example.com", "reader")]

    def test_failed_external_grant_is_a_warning_not_a_failure(self, caplog):
        reported = []
        fm, fake = self._fm(on_sync_result=lambda ch, r: reported.append((ch, r)))
        fake.reject_grants_to = {"ext@other.org"}

        result = fm.set_channel_folder_permissions(
            "F", "general", ["a@example.com", "ext@other.org"]
        )

        assert result  # still truthy, as the old bool return was
        assert result.external_failures == ("ext@other.org",)
        assert result.granted == 1
        assert reported == [("general", result)]
        assert self._members(fake) == [("a@example.com", "reader")]
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("ext@other.org" in r.getMessage() for r in warnings)

    def test_failed_internal_grant_fails_the_sync(self, caplog):
        reported = []
        fm, fake = self._fm(on_sync_result=lambda ch, r: reported.append(r))
        fake.reject_grants_to = {"a@example.com"}

        result = fm.set_channel_folder_permissions(
            "F", "general", ["a@example.com", "ext@other.org"]
        )

        assert not result
        assert result.failed_grants == ("a@example.com",)
        assert result.external_failures == ()
        assert result.granted == 1
        assert reported == [result]
        assert "1 member grant(s) failed" in result.describe()
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert any("not in sync" in r.getMessage() for r in errors)

    def test_failed_removal_fails_the_sync(self):
        svc = MagicMock()
        svc.list_permissions.return_value = [
            {"id": "p", "type": "user", "role": "reader", "emailAddress": "g@x.com"}
        ]
        svc.apply_permission_changes.return_value = [_make_http_error(403)]
        fm = FolderManager(svc, "example.com")

        result = fm.set_channel_folder_permissions("F", "general", [])

        assert not result
        assert result.failed_removals == ("g@x.com",)
        assert result.removed == 0

    def test_custom_external_predicate(self):
        fm, fake = self._fm(is_external=lambda email: email.startswith("x"))
        fake.reject_grants_to = {"x@example.com", "y@other.org"}

        result = fm.set_channel_folder_permissions(
            "F", "general", ["x@example.com", "y@other.org"]
        )

        assert not result  # y@other.org counts as internal here
        assert result.external_failures == ("x@example.com",)
        assert result.failed_grants == ("y@other.org",)

    def test_existing_member_is_not_granted_again(self):
        fm, fake = self._fm()
        fake.permissions_db["F"] = [
            {
                "id": "p",
                "type": "user",
                "role": "writer",
                "emailAddress": "A@example.com",
            }
        ]
        fm.set_channel_folder_permissions("F", "general", ["a@example.com"])
        assert fake.count("permissions.create") == 0

    def test_large_member_lists_are_batched_by_100(self):
        fm, fake = self._fm()
        emails = [f"user{i}@example.com" for i in range(150)]

        assert fm.set_channel_folder_permissions("F", "general", emails).ok

        assert fake.batch_sizes == [100, 50]
        assert len(fake.permissions_db["F"]) == 150

    def test_partial_failure_returns_false(self):
        svc = MagicMock()
        svc.list_permissions.return_value = []
        svc.apply_permission_changes.return_value = [None, _make_http_error(403)]
        fm = FolderManager(svc, "example.com")

        result = fm.set_channel_folder_permissions(
            "folder123", "general", ["a@example.com", "b@example.com"]
        )

        assert not result
        assert result.failed_grants == ("b@example.com",)

    @pytest.mark.parametrize(
        "error", [_make_http_error(500), ConnectionResetError("reset")]
    )
    def test_listing_failure_adds_members_but_fails_the_sync(self, error):
        svc = MagicMock()
        svc.list_permissions.side_effect = error
        svc.apply_permission_changes.return_value = [None]
        fm = FolderManager(svc, "example.com")

        result = fm.set_channel_folder_permissions("F", "general", ["a@example.com"])

        assert not result
        assert result.listing_error
        assert result.granted == 1
        assert "former members kept" in result.describe()
        (changes,), _ = svc.apply_permission_changes.call_args
        assert [c.body["emailAddress"] for c in changes] == ["a@example.com"]

    def test_shared_drive_passes_supports_all_drives(self):
        fm, fake = self._fm()

        result = fm.set_channel_folder_permissions(
            "F", "general", ["a@example.com"], shared_drive_id="drive789"
        )

        assert result.ok
        listing = next(kw for name, kw in fake.calls if name == "permissions.list")
        create = next(kw for name, kw in fake.calls if name == "permissions.create")
        assert listing["supportsAllDrives"] is True
        assert create["supportsAllDrives"] is True
        assert create["sendNotificationEmail"] is False


class TestDefaultExternalCheck:
    """Without is_external, the admin's domain and workspace_domains are internal."""

    def test_secondary_domain_grant_failure_fails_the_sync(self):
        fake = FakeDrive(reject_grants_to={"c@corp.example.net", "x@other.org"})
        fake.add_folder("F", "general", "ROOT")
        fm = FolderManager(
            DriveAdapter(fake), "Example.com", workspace_domains=["@CORP.example.net "]
        )

        result = fm.set_channel_folder_permissions(
            "F", "general", ["c@corp.example.net", "x@other.org"]
        )

        assert result.failed_grants == ("c@corp.example.net",)
        assert result.external_failures == ("x@other.org",)


class TestSyncChannelFolder:
    """Lookup and sync in one call, with failures reported (SF5)."""

    def _fm(self, fake):
        reported: list[tuple[str, FolderSyncResult]] = []
        fm = FolderManager(
            DriveAdapter(fake),
            "example.com",
            on_sync_result=lambda ch, r: reported.append((ch, r)),
        )
        return fm, reported

    def test_syncs_an_existing_folder(self):
        fake = FakeDrive()
        fake.add_folder("F", "general", "ROOT")
        fm, reported = self._fm(fake)

        result = fm.sync_channel_folder("general", "ROOT", ["a@example.com"])

        assert result == FolderSyncResult(folder_id="F", granted=1)
        assert reported == [("general", result)]

    def test_no_folder_needs_no_sync(self):
        fm, reported = self._fm(FakeDrive())

        result = fm.sync_channel_folder("general", "ROOT", ["a@example.com"])

        assert result.ok
        assert result.folder_id is None
        assert reported == []

    def test_failed_lookup_is_a_failed_sync(self, caplog):
        svc = MagicMock()
        svc.list_files.side_effect = _make_http_error(403)
        reported = []
        fm = FolderManager(svc, on_sync_result=lambda ch, r: reported.append(r))

        result = fm.sync_channel_folder("general", "ROOT", ["a@example.com"])

        assert not result
        assert result.lookup_error
        assert reported == [result]
        svc.apply_permission_changes.assert_not_called()
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert any("folder lookup failed" in r.getMessage() for r in errors)


class TestTrashedFolders:
    """Folder lookups ignore trashed folders and share one cache (I37)."""

    def test_permission_lookup_skips_trashed_folder(self):
        fake = FakeDrive()
        fake.add_folder("T", "general", "ROOT", trashed=True)
        fake.add_folder("L", "general", "ROOT")
        fm = FolderManager(DriveAdapter(fake))

        assert fm.get_channel_folder_id("general", "ROOT", "SD") == "L"
        query = next(kw["q"] for name, kw in fake.calls if name == "files.list")
        assert "trashed = false" in query

    def test_upload_and_permission_paths_share_the_cache(self):
        fake = FakeDrive()
        fake.add_folder("L", "general", "ROOT")
        fm = FolderManager(DriveAdapter(fake))

        assert fm.get_or_create_channel_folder("general", "ROOT", "SD") == "L"
        fake.calls.clear()
        assert fm.get_channel_folder_id("general", "ROOT", "SD") == "L"
        assert fake.calls == []


# -----------------------------------------------------------
# _sanitize_folder_name
# -----------------------------------------------------------


class TestSanitizeFolderName:
    """Tests for _sanitize_folder_name."""

    def test_strips_whitespace(self):
        svc = _make_drive_service()
        fm = FolderManager(svc)

        assert fm._sanitize_folder_name("  general  ") == "general"

    def test_preserves_normal_name(self):
        svc = _make_drive_service()
        fm = FolderManager(svc)

        assert fm._sanitize_folder_name("general") == "general"


class TestEnsureRestricted:
    """Limited-access folders (F10)."""

    def test_failed_update_is_retried_next_time(self, caplog):
        svc = MagicMock()
        svc.update_file.side_effect = _make_http_error(403)
        fm = FolderManager(svc)

        assert fm.ensure_restricted("F", "hr", "SD") is False
        assert fm.ensure_restricted("F", "hr", "SD") is False
        assert svc.update_file.call_count == 2
        assert fm.restriction_failures == {"F": 2}
        errors = [r for r in caplog.records if r.levelname == "ERROR"]
        assert len(errors) == 1  # said loudly once, then quietly

    def test_network_error_fails_closed(self):
        svc = MagicMock()
        svc.update_file.side_effect = TimeoutError("timed out")
        fm = FolderManager(svc)

        assert fm.ensure_restricted("F", "hr", "SD") is False
        assert fm.restriction_failures == {"F": 1}

    def test_restricted_lookup_fails_closed(self):
        svc = MagicMock()
        svc.list_files.return_value = {"files": [{"id": "OPEN", "name": "hr"}]}
        svc.update_file.side_effect = _make_http_error(403)
        fm = FolderManager(svc)

        lookup = fm.find_or_create_channel_folder("hr", "ROOT", "SD", restricted=True)

        assert lookup.folder_id is None
        assert lookup.restriction_failed is True
        assert "limited-access" in lookup.error
        assert (
            fm.get_or_create_channel_folder("hr", "ROOT", "SD", restricted=True) is None
        )
        # Unrestricted callers still get the folder.
        assert fm.get_or_create_channel_folder("hr", "ROOT", "SD") == "OPEN"

    def test_update_sets_flag_and_supports_all_drives(self):
        svc = MagicMock()
        svc.update_file.return_value = {"id": "F", "inheritedPermissionsDisabled": True}
        fm = FolderManager(svc)

        assert fm.ensure_restricted("F", "hr", "SD") is True
        assert fm.ensure_restricted("F", "hr", "SD") is True
        svc.update_file.assert_called_once_with(
            "F",
            {"inheritedPermissionsDisabled": True},
            fields="id, inheritedPermissionsDisabled",
            supports_all_drives=True,
        )

    def test_drive_ignoring_the_flag_is_reported(self):
        svc = MagicMock()
        svc.update_file.return_value = {
            "id": "F",
            "inheritedPermissionsDisabled": False,
        }
        fm = FolderManager(svc)
        assert fm.ensure_restricted("F", "hr") is False

    def test_unrestricted_request_leaves_folder_alone(self):
        fake = FakeDrive()
        fm = FolderManager(DriveAdapter(fake))
        fm.get_or_create_channel_folder("general", "ROOT", "SD")
        assert fake.count("files.update") == 0
        body = next(kw["body"] for name, kw in fake.calls if name == "files.create")
        assert "inheritedPermissionsDisabled" not in body
