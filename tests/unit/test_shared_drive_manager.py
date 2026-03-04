"""Unit tests for the shared drive manager module."""

from unittest.mock import MagicMock

from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_chat_migrator.core.config import MigrationConfig, SharedDriveConfig
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


class TestValidateSharedDrive:
    """Tests for SharedDriveManager.validate_shared_drive."""

    def test_dry_run_delegates_to_drive_service(self):
        """Dry run no longer short-circuits — DI stubs handle it."""
        manager, drive_service = _make_manager()
        drive_service.get_drive.return_value = {"id": "some-drive-id"}

        result = manager.validate_shared_drive("some-drive-id")

        assert result is True
        drive_service.get_drive.assert_called_once()

    def test_valid_drive_returns_true(self):
        manager, drive_service = _make_manager()
        drive_service.get_drive.return_value = {
            "id": "drive-123",
            "name": "My Drive",
        }

        result = manager.validate_shared_drive("drive-123")

        assert result is True

    def test_http_error_returns_false(self):
        manager, drive_service = _make_manager()
        drive_service.get_drive.side_effect = _make_http_error(404, b"Not Found")

        result = manager.validate_shared_drive("bad-id")

        assert result is False


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

    def test_configured_drive_id_fails_falls_back_to_name(self):
        manager, drive_service = _make_manager(
            shared_drive_name="Fallback Drive",
            shared_drive_id="bad-id",
        )
        # First call: get by ID fails
        drive_service.get_drive.side_effect = _make_http_error(404, b"Not Found")
        # Second call: list drives finds existing by name
        drive_service.list_drives.return_value = {
            "drives": [
                {"id": "found-id", "name": "Fallback Drive"},
            ]
        }

        result = manager.get_or_create_shared_drive()

        assert result == "found-id"

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
