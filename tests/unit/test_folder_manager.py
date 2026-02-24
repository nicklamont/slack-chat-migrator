"""Unit tests for the FolderManager class."""

from unittest.mock import MagicMock

from googleapiclient.errors import HttpError

from slack_migrator.services.drive.folder_manager import (
    FolderManager,
)


def _make_http_error(status, reason="error"):
    """Create a mock HttpError with the given status."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=b"{}")


def _make_drive_service():
    """Create a mock Google Drive API service."""
    service = MagicMock()
    return service


# -----------------------------------------------------------
# create_root_folder_in_shared_drive
# -----------------------------------------------------------


class TestCreateRootFolderInSharedDrive:
    """Tests for create_root_folder_in_shared_drive."""

    def test_dry_run_returns_fake_folder_id(self):
        svc = _make_drive_service()
        fm = FolderManager(svc, dry_run=True)

        result = fm.create_root_folder_in_shared_drive("attachments", "drive123")

        assert result == "DRY_ROOT_FOLDER_attachments"
        svc.files.assert_not_called()

    def test_creates_folder_in_shared_drive(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        # list returns no existing folders
        files_mock.list.return_value.execute.return_value = {"files": []}
        # create returns new folder id
        files_mock.create.return_value.execute.return_value = {"id": "new_folder_id"}

        fm = FolderManager(svc)
        result = fm.create_root_folder_in_shared_drive("attachments", "drive123")

        assert result == "new_folder_id"
        files_mock.create.assert_called_once()
        call_kwargs = files_mock.create.call_args
        body = call_kwargs[1]["body"]
        assert body["name"] == "attachments"
        assert body["parents"] == ["drive123"]
        assert call_kwargs[1]["supportsAllDrives"] is True

    def test_finds_existing_folder_returns_its_id(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {
            "files": [{"id": "existing_id", "name": "attachments"}]
        }

        fm = FolderManager(svc)
        result = fm.create_root_folder_in_shared_drive("attachments", "drive123")

        assert result == "existing_id"
        files_mock.create.assert_not_called()

    def test_http_error_returns_none(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value
        files_mock.list.return_value.execute.side_effect = _make_http_error(500)

        fm = FolderManager(svc)
        result = fm.create_root_folder_in_shared_drive("attachments", "drive123")

        assert result is None


# -----------------------------------------------------------
# get_or_create_channel_folder
# -----------------------------------------------------------


class TestGetOrCreateChannelFolder:
    """Tests for get_or_create_channel_folder."""

    def test_dry_run_returns_fake_folder_id(self):
        svc = _make_drive_service()
        fm = FolderManager(svc, dry_run=True)

        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "DRY_CHANNEL_FOLDER_general"
        svc.files.assert_not_called()

    def test_creates_new_channel_folder(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        # list returns no existing folders
        files_mock.list.return_value.execute.return_value = {"files": []}
        # create returns new folder
        files_mock.create.return_value.execute.return_value = {"id": "chan_folder_id"}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "chan_folder_id"
        assert fm.folder_cache["folder_general"] == "chan_folder_id"

    def test_creates_channel_folder_in_shared_drive(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {"files": []}
        files_mock.create.return_value.execute.return_value = {"id": "shared_chan_id"}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder(
            "general", "parent123", shared_drive_id="drive456"
        )

        assert result == "shared_chan_id"
        create_kwargs = files_mock.create.call_args[1]
        assert create_kwargs["supportsAllDrives"] is True

    def test_finds_existing_channel_folder(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {
            "files": [{"id": "existing_chan", "name": "general"}]
        }

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "existing_chan"
        files_mock.create.assert_not_called()
        assert fm.folder_cache["folder_general"] == "existing_chan"

    def test_returns_cached_folder_id(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        # Simulate successful cache verification
        files_mock.get.return_value.execute.return_value = {"id": "cached_id"}

        fm = FolderManager(svc)
        fm.folder_cache["folder_general"] = "cached_id"

        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "cached_id"
        files_mock.list.assert_not_called()
        files_mock.create.assert_not_called()

    def test_stale_cache_creates_new_folder(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        # Cache verification fails (folder deleted)
        files_mock.get.return_value.execute.side_effect = _make_http_error(404)
        # list returns no existing folders
        files_mock.list.return_value.execute.return_value = {"files": []}
        # create returns new folder
        files_mock.create.return_value.execute.return_value = {"id": "new_id"}

        fm = FolderManager(svc)
        fm.folder_cache["folder_general"] = "stale_id"

        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result == "new_id"
        assert fm.folder_cache["folder_general"] == "new_id"

    def test_http_error_returns_none(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value
        files_mock.list.return_value.execute.side_effect = _make_http_error(403)

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result is None

    def test_create_returning_no_id_returns_none(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {"files": []}
        files_mock.create.return_value.execute.return_value = {}

        fm = FolderManager(svc)
        result = fm.get_or_create_channel_folder("general", "parent123")

        assert result is None


# -----------------------------------------------------------
# create_regular_drive_folder
# -----------------------------------------------------------


class TestCreateRegularDriveFolder:
    """Tests for create_regular_drive_folder."""

    def test_dry_run_returns_fake_folder_id(self):
        svc = _make_drive_service()
        fm = FolderManager(svc, dry_run=True)

        result = fm.create_regular_drive_folder("attachments")

        assert result == "DRY_REGULAR_FOLDER_attachments"

    def test_creates_folder_with_correct_metadata(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {"files": []}
        files_mock.create.return_value.execute.return_value = {"id": "regular_id"}

        fm = FolderManager(svc)
        result = fm.create_regular_drive_folder("attachments")

        assert result == "regular_id"
        body = files_mock.create.call_args[1]["body"]
        assert body["name"] == "attachments"
        assert body["mimeType"] == ("application/vnd.google-apps.folder")
        assert "parents" not in body

    def test_finds_existing_regular_folder(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {
            "files": [{"id": "existing_reg", "name": "attachments"}]
        }

        fm = FolderManager(svc)
        result = fm.create_regular_drive_folder("attachments")

        assert result == "existing_reg"
        files_mock.create.assert_not_called()

    def test_http_error_returns_none(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value
        files_mock.list.return_value.execute.side_effect = _make_http_error(500)

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
        fm.folder_cache["general:parent123"] = "cached_id"

        result = fm.get_channel_folder_id("general", "parent123")

        assert result == "cached_id"
        svc.files.return_value.list.assert_not_called()

    def test_finds_folder_by_name(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {
            "files": [{"id": "found_id", "name": "general"}]
        }

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id("general", "parent123")

        assert result == "found_id"
        assert fm.folder_cache["general:parent123"] == "found_id"

    def test_no_matching_folder_returns_none(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {"files": []}

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id("general", "parent123")

        assert result is None

    def test_http_error_returns_none(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value
        files_mock.list.return_value.execute.side_effect = _make_http_error(403)

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id("general", "parent123")

        assert result is None

    def test_shared_drive_params_passed(self):
        svc = _make_drive_service()
        files_mock = svc.files.return_value

        files_mock.list.return_value.execute.return_value = {
            "files": [{"id": "sd_id", "name": "general"}]
        }

        fm = FolderManager(svc)
        result = fm.get_channel_folder_id(
            "general", "parent123", shared_drive_id="drive789"
        )

        assert result == "sd_id"
        list_kwargs = files_mock.list.call_args[1]
        assert list_kwargs["driveId"] == "drive789"
        assert list_kwargs["supportsAllDrives"] is True
        assert list_kwargs["includeItemsFromAllDrives"] is True


# -----------------------------------------------------------
# set_channel_folder_permissions
# -----------------------------------------------------------


class TestSetChannelFolderPermissions:
    """Tests for set_channel_folder_permissions."""

    def test_dry_run_returns_true(self):
        svc = _make_drive_service()
        fm = FolderManager(svc, dry_run=True)

        result = fm.set_channel_folder_permissions(
            "folder123",
            "general",
            ["a@example.com", "b@example.com"],
        )

        assert result is True
        svc.permissions.assert_not_called()

    def test_grants_reader_access_to_all_users(self):
        svc = _make_drive_service()
        fm = FolderManager(svc)
        emails = ["a@example.com", "b@example.com"]

        result = fm.set_channel_folder_permissions("folder123", "general", emails)

        assert result is True
        perms_mock = svc.permissions.return_value
        assert perms_mock.create.call_count == 2

    def test_partial_failure_returns_false(self):
        svc = _make_drive_service()
        perms_mock = svc.permissions.return_value

        # First call succeeds, second raises
        perms_mock.create.return_value.execute.side_effect = [
            {},
            _make_http_error(403),
        ]

        fm = FolderManager(svc)
        result = fm.set_channel_folder_permissions(
            "folder123",
            "general",
            ["a@example.com", "b@example.com"],
        )

        assert result is False

    def test_shared_drive_passes_supports_all_drives(self):
        svc = _make_drive_service()
        fm = FolderManager(svc)

        result = fm.set_channel_folder_permissions(
            "folder123",
            "general",
            ["a@example.com"],
            shared_drive_id="drive789",
        )

        assert result is True
        create_kwargs = svc.permissions.return_value.create.call_args[1]
        assert create_kwargs["supportsAllDrives"] is True


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
