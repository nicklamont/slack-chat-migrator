"""Unit tests for the shared drive manager module."""

from unittest.mock import MagicMock

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_chat_migrator.core.config import MigrationConfig, SharedDriveConfig
from slack_chat_migrator.exceptions import ConfigError
from slack_chat_migrator.services.drive.shared_drive_manager import (
    SharedDriveManager,
)


def _make_http_error(status=404, content=b"Not Found"):
    """Create an HttpError with the given status and content."""
    resp = Response({"status": str(status)})
    return HttpError(resp=resp, content=content)


def _make_manager(
    shared_drive_name="Imported Slack Attachments",
    shared_drive_id=None,
):
    """Create a SharedDriveManager with a mock drive service."""
    drive_service = MagicMock()
    config = MigrationConfig(
        shared_drive=SharedDriveConfig(
            name=shared_drive_name,
            id=shared_drive_id,
        ),
    )
    manager = SharedDriveManager(
        drive_service=drive_service,
        config=config,
    )
    return manager, drive_service


class TestGetOrCreateSharedDrive:
    """Tests for SharedDriveManager.get_or_create_shared_drive."""

    def test_dry_run_delegates_to_drive_service(self):
        """Dry run no longer short-circuits — DI stubs handle it."""
        manager, drive_service = _make_manager()
        drive_service.list_drives.return_value = {
            "drives": [{"id": "stub-id", "name": "Imported Slack Attachments"}]
        }

        result = manager.get_or_create_shared_drive()

        assert result == "stub-id"
        drive_service.list_drives.assert_called_once()

    def test_configured_drive_id_valid_returns_it(self):
        manager, drive_service = _make_manager(
            shared_drive_id="configured-id",
        )
        drive_service.get_drive.return_value = {
            "id": "configured-id",
            "name": "Existing Drive",
        }

        result = manager.get_or_create_shared_drive()

        assert result == "configured-id"

    def test_inaccessible_configured_drive_id_raises(self):
        """A bad shared_drive.id stops the run instead of creating a drive (I13)."""
        manager, drive_service = _make_manager(
            shared_drive_name="Fallback Drive",
            shared_drive_id="bad-id",
        )
        drive_service.get_drive.side_effect = _make_http_error(404, b"Not Found")
        drive_service.list_drives.return_value = {
            "drives": [{"id": "found-id", "name": "Fallback Drive"}]
        }

        with pytest.raises(ConfigError, match="bad-id"):
            manager.get_or_create_shared_drive()

        drive_service.list_drives.assert_not_called()
        drive_service.create_drive.assert_not_called()

    def test_no_drive_id_finds_existing_by_name(self):
        manager, drive_service = _make_manager(
            shared_drive_name="Slack Attachments",
            shared_drive_id=None,
        )
        drive_service.list_drives.return_value = {
            "drives": [
                {"id": "existing-id", "name": "Slack Attachments"},
            ]
        }

        result = manager.get_or_create_shared_drive()

        assert result == "existing-id"

    def test_no_drive_id_creates_new_drive(self):
        manager, drive_service = _make_manager(
            shared_drive_name="New Drive",
            shared_drive_id=None,
        )
        drive_service.list_drives.return_value = {"drives": []}
        drive_service.create_drive.return_value = {
            "id": "created-id",
            "name": "New Drive",
        }

        result = manager.get_or_create_shared_drive()

        assert result == "created-id"

    def test_no_config_uses_default_name(self):
        """When both name and id are empty, defaults to standard name."""
        drive_service = MagicMock()
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="", id=None),
        )
        manager = SharedDriveManager(
            drive_service=drive_service,
            config=config,
        )
        drive_service.list_drives.return_value = {
            "drives": [
                {
                    "id": "default-id",
                    "name": "Imported Slack Attachments",
                },
            ]
        }

        result = manager.get_or_create_shared_drive()

        assert result == "default-id"

    def test_http_error_during_get_returns_none(self):
        """HttpError at the top-level try block returns None."""
        manager, drive_service = _make_manager(
            shared_drive_id=None,
            shared_drive_name="Test Drive",
        )
        # list_drives raises HttpError
        drive_service.list_drives.side_effect = _make_http_error(
            500, b"Internal Server Error"
        )

        result = manager.get_or_create_shared_drive()

        assert result is None


class TestFindOrCreateSharedDrive:
    """Tests for SharedDriveManager._find_or_create_shared_drive."""

    def test_finds_existing_drive_by_name(self):
        manager, drive_service = _make_manager()
        drive_service.list_drives.return_value = {
            "drives": [
                {"id": "other-id", "name": "Other Drive"},
                {"id": "target-id", "name": "Target Drive"},
            ]
        }

        result = manager._find_or_create_shared_drive("Target Drive")

        assert result == "target-id"

    def test_creates_new_drive_when_not_found(self):
        manager, drive_service = _make_manager()
        drive_service.list_drives.return_value = {
            "drives": [
                {"id": "other-id", "name": "Other Drive"},
            ]
        }
        drive_service.create_drive.return_value = {
            "id": "new-id",
            "name": "My New Drive",
        }

        result = manager._find_or_create_shared_drive("My New Drive")

        assert result == "new-id"

    def test_http_error_returns_none(self):
        manager, drive_service = _make_manager()
        drive_service.list_drives.side_effect = _make_http_error(403, b"Forbidden")

        result = manager._find_or_create_shared_drive("Some Drive")

        assert result is None


class TestSharedDrivePaging:
    """drives.list is read page by page (I13)."""

    def _manager(self, fake, **config):
        from slack_chat_migrator.services.drive_adapter import DriveAdapter

        manager = SharedDriveManager(
            DriveAdapter(fake),
            MigrationConfig(shared_drive=SharedDriveConfig(**config)),
        )
        return manager

    def test_target_on_page_three_is_found(self):
        from tests.unit.drive_fakes import FakeDrive

        drives = [{"id": f"D{i}", "name": f"Team {i}"} for i in range(24)]
        drives.append({"id": "SD1", "name": "Imported Slack Attachments"})
        # The server returns 10 per page and ignores the name filter.
        fake = FakeDrive(drives=drives, drive_page_cap=10, honor_drive_query=False)

        result = self._manager(
            fake, name="Imported Slack Attachments"
        ).get_or_create_shared_drive()

        assert result == "SD1"
        lists = [kw for name, kw in fake.calls if name == "drives.list"]
        assert len(lists) == 3
        assert lists[0]["pageSize"] == 100
        assert lists[0]["q"] == "name = 'Imported Slack Attachments'"
        assert [kw.get("pageToken") for kw in lists] == [None, "10", "20"]
        assert fake.count("drives.create") == 0

    def test_name_query_is_escaped(self):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive(drives=[{"id": "X", "name": "Bob's files"}])
        result = self._manager(fake, name="Bob's files").get_or_create_shared_drive()
        assert result == "X"
        query = next(kw["q"] for name, kw in fake.calls if name == "drives.list")
        assert query == "name = 'Bob\\'s files'"

    def test_duplicate_names_pick_first_without_creating(self, caplog):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive(
            drives=[
                {"id": "A", "name": "Imported Slack Attachments"},
                {"id": "B", "name": "Imported Slack Attachments"},
            ]
        )
        result = self._manager(
            fake, name="Imported Slack Attachments"
        ).get_or_create_shared_drive()

        assert result == "A"
        assert fake.count("drives.create") == 0
        assert "shared_drive.id" in caplog.text

    def test_created_when_absent(self):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive(drives=[{"id": "D1", "name": "Other"}])
        result = self._manager(
            fake, name="Imported Slack Attachments"
        ).get_or_create_shared_drive()
        assert result.startswith("NEWDRIVE")
        assert fake.count("drives.create") == 1
