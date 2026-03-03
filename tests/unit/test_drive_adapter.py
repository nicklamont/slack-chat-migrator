"""Unit tests for the DriveAdapter."""

from unittest.mock import MagicMock

import pytest

from slack_migrator.services.drive_adapter import DriveAdapter


@pytest.fixture()
def mock_service():
    """Return a deeply-mocked Drive API service."""
    return MagicMock(name="drive_service")


@pytest.fixture()
def adapter(mock_service):
    """Return a DriveAdapter wrapping the mock service."""
    return DriveAdapter(mock_service)


# ---------------------------------------------------------------------------
# Files
# ---------------------------------------------------------------------------


class TestListFiles:
    def test_minimal_call(self, adapter, mock_service):
        adapter.list_files()
        kwargs = mock_service.files().list.call_args.kwargs
        assert kwargs["pageSize"] == 100
        assert kwargs["fields"] == "files(id, name)"
        assert kwargs["spaces"] == "drive"
        assert "q" not in kwargs

    def test_with_query(self, adapter, mock_service):
        adapter.list_files(q="name='test'")
        kwargs = mock_service.files().list.call_args.kwargs
        assert kwargs["q"] == "name='test'"

    def test_with_shared_drive_params(self, adapter, mock_service):
        adapter.list_files(
            q="name='x'",
            corpora="drive",
            drive_id="0ABC",
            include_items_from_all_drives=True,
            supports_all_drives=True,
        )
        kwargs = mock_service.files().list.call_args.kwargs
        assert kwargs["corpora"] == "drive"
        assert kwargs["driveId"] == "0ABC"
        assert kwargs["includeItemsFromAllDrives"] is True
        assert kwargs["supportsAllDrives"] is True

    def test_with_pagination(self, adapter, mock_service):
        adapter.list_files(page_token="tok", page_size=50)
        kwargs = mock_service.files().list.call_args.kwargs
        assert kwargs["pageToken"] == "tok"
        assert kwargs["pageSize"] == 50

    def test_with_order_by(self, adapter, mock_service):
        adapter.list_files(order_by="createdTime desc")
        kwargs = mock_service.files().list.call_args.kwargs
        assert kwargs["orderBy"] == "createdTime desc"

    def test_returns_api_response(self, adapter, mock_service):
        expected = {"files": [{"id": "f1"}], "nextPageToken": "abc"}
        mock_service.files().list().execute.return_value = expected
        result = adapter.list_files()
        assert result == expected


class TestCreateFile:
    def test_minimal_call(self, adapter, mock_service):
        body = {"name": "test.txt", "mimeType": "text/plain"}
        adapter.create_file(body)
        kwargs = mock_service.files().create.call_args.kwargs
        assert kwargs["body"] == body
        assert kwargs["fields"] == "id"
        assert "media_body" not in kwargs

    def test_with_media_body(self, adapter, mock_service):
        body = {"name": "test.txt"}
        media = MagicMock(name="media_upload")
        adapter.create_file(body, media_body=media, fields="id,webViewLink")
        kwargs = mock_service.files().create.call_args.kwargs
        assert kwargs["media_body"] is media
        assert kwargs["fields"] == "id,webViewLink"

    def test_with_shared_drive_support(self, adapter, mock_service):
        adapter.create_file({"name": "x"}, supports_all_drives=True)
        kwargs = mock_service.files().create.call_args.kwargs
        assert kwargs["supportsAllDrives"] is True


class TestGetFile:
    def test_minimal_call(self, adapter, mock_service):
        adapter.get_file("file123")
        kwargs = mock_service.files().get.call_args.kwargs
        assert kwargs["fileId"] == "file123"
        assert "fields" not in kwargs

    def test_with_fields_and_shared_drive(self, adapter, mock_service):
        adapter.get_file("file123", fields="id,name", supports_all_drives=True)
        kwargs = mock_service.files().get.call_args.kwargs
        assert kwargs["fields"] == "id,name"
        assert kwargs["supportsAllDrives"] is True


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------


class TestCreatePermission:
    def test_minimal_call(self, adapter, mock_service):
        body = {"role": "reader", "type": "user", "emailAddress": "a@b.com"}
        adapter.create_permission("file123", body)
        kwargs = mock_service.permissions().create.call_args.kwargs
        assert kwargs["fileId"] == "file123"
        assert kwargs["body"] == body
        assert kwargs["fields"] == "id"
        assert kwargs["sendNotificationEmail"] is False

    def test_with_shared_drive_and_transfer(self, adapter, mock_service):
        body = {"role": "owner", "type": "user", "emailAddress": "a@b.com"}
        adapter.create_permission(
            "file123",
            body,
            supports_all_drives=True,
            transfer_ownership=True,
        )
        kwargs = mock_service.permissions().create.call_args.kwargs
        assert kwargs["supportsAllDrives"] is True
        assert kwargs["transferOwnership"] is True

    def test_with_notification_email(self, adapter, mock_service):
        body = {"role": "writer", "type": "user", "emailAddress": "a@b.com"}
        adapter.create_permission("file123", body, send_notification_email=True)
        kwargs = mock_service.permissions().create.call_args.kwargs
        assert kwargs["sendNotificationEmail"] is True


class TestUpdatePermission:
    def test_minimal_call(self, adapter, mock_service):
        body = {"role": "writer"}
        adapter.update_permission("file123", "perm456", body)
        kwargs = mock_service.permissions().update.call_args.kwargs
        assert kwargs["fileId"] == "file123"
        assert kwargs["permissionId"] == "perm456"
        assert kwargs["body"] == body

    def test_with_shared_drive_and_transfer(self, adapter, mock_service):
        adapter.update_permission(
            "file123",
            "perm456",
            {"role": "owner"},
            supports_all_drives=True,
            transfer_ownership=True,
        )
        kwargs = mock_service.permissions().update.call_args.kwargs
        assert kwargs["supportsAllDrives"] is True
        assert kwargs["transferOwnership"] is True


# ---------------------------------------------------------------------------
# Shared Drives
# ---------------------------------------------------------------------------


class TestGetDrive:
    def test_calls_drives_get(self, adapter, mock_service):
        adapter.get_drive("0ABC")
        mock_service.drives().get.assert_called_once_with(driveId="0ABC")


class TestListDrives:
    def test_calls_drives_list(self, adapter, mock_service):
        adapter.list_drives()
        mock_service.drives().list.assert_called_once_with()


class TestCreateDrive:
    def test_calls_drives_create(self, adapter, mock_service):
        body = {"name": "My Drive"}
        adapter.create_drive(body, request_id="req-123")
        mock_service.drives().create.assert_called_once_with(
            body=body, requestId="req-123"
        )
