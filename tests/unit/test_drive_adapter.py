"""Unit tests for the DriveAdapter."""

from unittest.mock import MagicMock

import pytest

from slack_chat_migrator.services.drive_adapter import DriveAdapter


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
        mock_service.drives().list.assert_called_once_with(pageSize=100)

    def test_passes_paging_and_query(self, adapter, mock_service):
        adapter.list_drives(
            page_size=50, page_token="tok", q="name = 'x'", fields="drives(id)"
        )
        mock_service.drives().list.assert_called_once_with(
            pageSize=50, pageToken="tok", q="name = 'x'", fields="drives(id)"
        )


class TestCreateDrive:
    def test_calls_drives_create(self, adapter, mock_service):
        body = {"name": "My Drive"}
        adapter.create_drive(body, request_id="req-123")
        mock_service.drives().create.assert_called_once_with(
            body=body, requestId="req-123"
        )


# ---------------------------------------------------------------------------
# Permissions listing, deletion and batching
# ---------------------------------------------------------------------------


class TestPermissionHelpers:
    def test_list_permissions_reads_every_page(self):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        fake.permissions_db["F"] = [
            {"id": f"p{i}", "type": "user", "role": "reader", "emailAddress": f"u{i}@x"}
            for i in range(250)
        ]
        perms = DriveAdapter(fake).list_permissions("F", supports_all_drives=True)
        assert len(perms) == 250
        lists = [kw for name, kw in fake.calls if name == "permissions.list"]
        assert len(lists) == 3
        assert all(kw["supportsAllDrives"] for kw in lists)

    def test_delete_permission(self, adapter, mock_service):
        adapter.delete_permission("F", "p1", supports_all_drives=True)
        mock_service.permissions().delete.assert_called_once_with(
            fileId="F", permissionId="p1", supportsAllDrives=True
        )

    def test_update_file(self, adapter, mock_service):
        adapter.update_file("F", {"inheritedPermissionsDisabled": True})
        mock_service.files().update.assert_called_once_with(
            fileId="F", body={"inheritedPermissionsDisabled": True}, fields="id"
        )


class TestApplyPermissionChanges:
    @staticmethod
    def _create(file_id, email, **kw):
        from slack_chat_migrator.services.drive_adapter import PermissionChange

        return PermissionChange(
            file_id,
            body={"type": "user", "role": "reader", "emailAddress": email},
            **kw,
        )

    def test_single_change_is_sent_directly(self):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        errors = DriveAdapter(fake).apply_permission_changes([self._create("F", "a@x")])
        assert errors == [None]
        assert fake.batch_sizes == []
        assert fake.count("permissions.create") == 1

    def test_several_changes_share_one_batch(self):
        from slack_chat_migrator.services.drive_adapter import PermissionChange
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        fake.permissions_db["F"] = [{"id": "old", "type": "user", "role": "reader"}]
        changes = [
            self._create("F", "a@x"),
            PermissionChange("F", permission_id="old"),
            PermissionChange(
                "F",
                body={"type": "user", "role": "owner", "emailAddress": "o@x"},
                transfer_ownership=True,
            ),
        ]
        errors = DriveAdapter(fake).apply_permission_changes(changes)

        assert errors == [None, None, None]
        assert fake.batch_sizes == [3]
        transfer = [kw for name, kw in fake.calls if name == "permissions.create"][-1]
        assert transfer["transferOwnership"] is True

    def test_errors_are_reported_per_change(self):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        bad = self._create("F", "b@x")
        object.__setattr__(
            bad, "body", {"type": "user", "role": "editor", "emailAddress": "b@x"}
        )
        errors = DriveAdapter(fake).apply_permission_changes(
            [self._create("F", "a@x"), bad]
        )
        assert errors[0] is None
        assert errors[1] is not None and errors[1].resp.status == 400

    def test_rate_limited_batch_items_are_resent_alone(self):
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        attempts = {"n": 0}
        original_create = fake.permissions().create

        class Permissions:
            def create(self, **kw):
                request = original_create(**kw)
                inner = request._fn

                def flaky():
                    attempts["n"] += 1
                    if attempts["n"] == 1:
                        raise HttpError(Response({"status": "429"}), b"slow down")
                    return inner()

                request._fn = flaky
                return request

        fake.permissions = lambda: Permissions()
        errors = DriveAdapter(fake).apply_permission_changes(
            [self._create("F", "a@x"), self._create("F", "b@x")]
        )
        assert errors == [None, None]
        assert attempts["n"] == 3  # 2 in the batch, then 1 retry

    def test_without_batch_support_runs_sequentially(self):
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()

        class NoBatch:
            def permissions(self):
                return fake.permissions()

        errors = DriveAdapter(NoBatch()).apply_permission_changes(
            [self._create("F", "a@x"), self._create("F", "b@x")]
        )
        assert errors == [None, None]
        assert fake.count("permissions.create") == 2
        assert fake.batch_sizes == []

    def test_retry_wrapper_is_unwrapped_for_batches(self):
        from slack_chat_migrator.utils.api import RetryWrapper
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        errors = DriveAdapter(RetryWrapper(fake)).apply_permission_changes(
            [self._create("F", "a@x"), self._create("F", "b@x")]
        )
        assert errors == [None, None]
        assert fake.batch_sizes == [2]
