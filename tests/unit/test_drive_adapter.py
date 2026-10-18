"""Unit tests for the DriveAdapter."""

import errno
import os
import socket
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

    def test_passes_the_field_mask(self, adapter, mock_service):
        adapter.get_drive("0ABC", fields="id, capabilities(canAddChildren)")
        mock_service.drives().get.assert_called_once_with(
            driveId="0ABC", fields="id, capabilities(canAddChildren)"
        )


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

    def test_failed_batch_request_falls_back_to_individual_calls(self):
        """A transport error on the batch POST re-sends every call alone."""
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        original_batch = fake.new_batch_http_request

        def broken_batch(callback=None):
            batch = original_batch(callback)

            def execute():
                raise ConnectionResetError("connection reset by peer")

            batch.execute = execute
            return batch

        fake.new_batch_http_request = broken_batch
        errors = DriveAdapter(fake).apply_permission_changes(
            [self._create("F", "a@x"), self._create("F", "b@x")]
        )
        assert errors == [None, None]
        assert fake.count("permissions.create") == 2

    def test_network_errors_are_returned_not_raised(self):
        """A call that still fails with a network error is reported per change."""
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        original_create = fake.permissions().create

        class Permissions:
            def create(self, **kw):
                request = original_create(**kw)

                def timeout():
                    raise TimeoutError("read timed out")

                request._fn = timeout
                return request

        fake.permissions = lambda: Permissions()
        errors = DriveAdapter(fake).apply_permission_changes([self._create("F", "a@x")])
        assert len(errors) == 1 and isinstance(errors[0], TimeoutError)

    def test_non_http_error_in_batch_part_is_not_counted_as_success(self):
        """A batch part failing with a non-HttpError is retried individually."""
        from tests.unit.drive_fakes import FakeBatch, FakeDrive

        fake = FakeDrive()
        attempts = {"n": 0}

        class PartlyBrokenBatch(FakeBatch):
            def execute(self):
                for rid, _request, callback in self.requests:
                    callback(rid, None, OSError("socket closed"))

        fake.new_batch_http_request = lambda callback=None: PartlyBrokenBatch(
            fake, callback
        )
        original_create = fake.permissions().create

        class Permissions:
            def create(self, **kw):
                request = original_create(**kw)
                inner = request._fn

                def counted():
                    attempts["n"] += 1
                    return inner()

                request._fn = counted
                return request

        fake.permissions = lambda: Permissions()
        errors = DriveAdapter(fake).apply_permission_changes(
            [self._create("F", "a@x"), self._create("F", "b@x")]
        )
        assert errors == [None, None]
        assert attempts["n"] == 2  # both re-sent on their own


class TestOwnershipTransferRequests:
    """Drive rejects transfers with sendNotificationEmail=false (#98)."""

    def test_create_permission_omits_notification_flag_for_transfers(
        self, adapter, mock_service
    ):
        adapter.create_permission(
            "F",
            {"role": "owner"},
            transfer_ownership=True,
            send_notification_email=False,
        )
        kwargs = mock_service.permissions().create.call_args.kwargs
        assert kwargs["transferOwnership"] is True
        assert "sendNotificationEmail" not in kwargs

    def test_create_permission_keeps_flag_for_grants(self, adapter, mock_service):
        adapter.create_permission("F", {"role": "reader"})
        kwargs = mock_service.permissions().create.call_args.kwargs
        assert kwargs["sendNotificationEmail"] is False

    def test_batched_transfer_omits_notification_flag(self):
        from slack_chat_migrator.services.drive_adapter import PermissionChange
        from tests.unit.drive_fakes import FakeDrive

        fake = FakeDrive()
        errors = DriveAdapter(fake).apply_permission_changes(
            [
                PermissionChange(
                    "F", body={"type": "user", "role": "writer", "emailAddress": "b@x"}
                ),
                PermissionChange(
                    "F",
                    body={"type": "user", "role": "owner", "emailAddress": "o@x"},
                    transfer_ownership=True,
                ),
            ]
        )
        assert errors == [None, None]
        grant, transfer = [
            kw for name, kw in fake.calls if name == "permissions.create"
        ]
        assert grant["sendNotificationEmail"] is False
        assert "sendNotificationEmail" not in transfer


class TestPermissionChangeInvariants:
    """A change is exactly one of a grant or a removal (#98)."""

    def test_needs_a_body_or_a_permission_id(self):
        from slack_chat_migrator.services.drive_adapter import PermissionChange

        with pytest.raises(ValueError, match="exactly one"):
            PermissionChange("F")
        with pytest.raises(ValueError, match="exactly one"):
            PermissionChange("F", body={"role": "reader"}, permission_id="p")

    def test_transfer_needs_a_grant(self):
        from slack_chat_migrator.services.drive_adapter import PermissionChange

        with pytest.raises(ValueError, match="transfer_ownership"):
            PermissionChange("F", permission_id="p", transfer_ownership=True)
        PermissionChange("F", body={"role": "owner"}, transfer_ownership=True)


class TestResendRule:
    """Which calls that failed in a batch are sent again on their own."""

    @pytest.mark.parametrize(
        ("status", "resent"),
        [
            (400, False),
            (403, True),
            (404, False),
            (408, True),
            (429, True),
            (503, True),
        ],
    )
    def test_http_statuses(self, status, resent):
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        from slack_chat_migrator.services.drive_adapter import _retry_individually

        error = HttpError(Response({"status": str(status)}), b"x")
        assert _retry_individually(error) is resent

    def test_transport_errors_are_resent_but_refresh_errors_are_not(self):
        from google.auth.exceptions import RefreshError

        from slack_chat_migrator.services.drive_adapter import _retry_individually

        assert _retry_individually(ConnectionError("reset")) is True
        assert _retry_individually(OSError("socket closed")) is True
        assert _retry_individually(RefreshError("expired")) is False

    def test_refresh_error_is_returned_not_raised(self):
        from google.auth.exceptions import RefreshError

        from slack_chat_migrator.services.drive_adapter import PermissionChange

        class Failing:
            def permissions(self):
                return self

            def create(self, **kw):
                return self

            def execute(self):
                raise RefreshError("token expired")

        errors = DriveAdapter(Failing()).apply_permission_changes(
            [PermissionChange("F", body={"role": "reader"})]
        )
        assert isinstance(errors[0], RefreshError)


class TestErrorClassification:
    """Network-down OSErrors fail a call; local faults stop the run (#118)."""

    @pytest.mark.parametrize(
        "error",
        [
            OSError(errno.ENETUNREACH, "Network is unreachable"),
            OSError(errno.EHOSTUNREACH, "No route to host"),
            ConnectionResetError(errno.ECONNRESET, "reset"),
            TimeoutError("timed out"),
            socket.gaierror(-2, "Name or service not known"),
            OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            OSError(errno.ENOBUFS, "No buffer space available"),
        ],
        ids=[
            "enetunreach",
            "ehostunreach",
            "reset",
            "timeout",
            "dns",
            "enotconn",
            "enobufs",
        ],
    )
    def test_network_errors_are_drive_call_errors(self, error):
        from slack_chat_migrator.services.drive_adapter import (
            is_drive_call_error,
            is_fatal_local_error,
            is_network_error,
        )

        assert is_network_error(error)
        assert is_drive_call_error(error)
        assert not is_fatal_local_error(error)

    @pytest.mark.parametrize(
        "code",
        [
            errno.ENOSPC,
            errno.EROFS,
            errno.EMFILE,
            errno.ENFILE,
            *([errno.EDQUOT] if hasattr(errno, "EDQUOT") else []),
        ],
    )
    def test_local_faults_are_fatal_not_call_errors(self, code):
        from slack_chat_migrator.services.drive_adapter import (
            is_drive_call_error,
            is_fatal_local_error,
        )

        error = OSError(code, os.strerror(code))
        assert is_fatal_local_error(error)
        assert not is_drive_call_error(error)

    def test_network_oserror_in_a_grant_is_returned(self):
        from slack_chat_migrator.services.drive_adapter import PermissionChange

        failing = MagicMock()
        failing.permissions.return_value.create.return_value.execute.side_effect = (
            OSError(errno.ENETUNREACH, "Network is unreachable")
        )

        errors = DriveAdapter(failing).apply_permission_changes(
            [PermissionChange("F", body={"role": "reader"})]
        )

        assert isinstance(errors[0], OSError)

    def test_local_fault_in_a_grant_is_raised(self):
        from slack_chat_migrator.services.drive_adapter import PermissionChange

        failing = MagicMock()
        failing.permissions.return_value.create.return_value.execute.side_effect = (
            OSError(errno.EMFILE, "Too many open files")
        )

        with pytest.raises(OSError, match="Too many open files"):
            DriveAdapter(failing).apply_permission_changes(
                [PermissionChange("F", body={"role": "reader"})]
            )


class TestHttpStatus:
    def test_reads_the_status(self):
        from googleapiclient.errors import HttpError
        from httplib2 import Response

        from slack_chat_migrator.services.drive_adapter import http_status

        assert http_status(HttpError(Response({"status": "503"}), b"x")) == 503

    def test_none_without_a_response(self):
        from googleapiclient.errors import BatchError

        from slack_chat_migrator.services.drive_adapter import http_status

        assert http_status(BatchError("bad batch")) is None
        assert http_status(ValueError("x")) is None


def _http_error(status, reason=None):
    from googleapiclient.errors import HttpError
    from httplib2 import Response

    body = (
        b'{"error": {"errors": [{"reason": "%s"}]}}' % reason.encode()
        if reason
        else b"error"
    )
    return HttpError(Response({"status": str(status)}), body)


class TestIsTransientError:
    """is_transient_error decides UploadResult.retryable for Drive failures."""

    @pytest.mark.parametrize(
        ("error", "transient"),
        [
            (_http_error(408), True),
            (_http_error(429), True),
            (_http_error(500), True),
            (_http_error(503), True),
            (_http_error(403, "userRateLimitExceeded"), True),
            (_http_error(403, "rateLimitExceeded"), True),
            (_http_error(403, "insufficientFilePermissions"), False),
            (_http_error(403), False),
            (_http_error(404), False),
            (_http_error(400), False),
            ("refresh", True),
            (ConnectionResetError("reset"), True),
            (OSError(errno.ENETUNREACH, "Network is unreachable"), True),
            (PermissionError(errno.EACCES, "Permission denied"), False),
            (OSError(errno.ENOSPC, "No space left on device"), False),
        ],
        ids=[
            "408",
            "429",
            "500",
            "503",
            "403-user-rate",
            "403-rate",
            "403-refused",
            "403-plain",
            "404",
            "400",
            "refresh",
            "reset",
            "enetunreach",
            "eacces",
            "enospc",
        ],
    )
    def test_table(self, error, transient):
        from google.auth.exceptions import RefreshError

        from slack_chat_migrator.services.drive_adapter import is_transient_error

        if error == "refresh":
            error = RefreshError("invalid_grant")
        assert is_transient_error(error) is transient

    def test_batch_error_without_a_response_is_permanent(self):
        from googleapiclient.errors import BatchError

        from slack_chat_migrator.services.drive_adapter import is_transient_error

        assert is_transient_error(BatchError("too many calls")) is False


class TestQuotaErrors:
    """A used-up quota is raised as DriveQuotaError by every call (#118)."""

    @pytest.mark.parametrize(
        "reason",
        [
            "dailyLimitExceeded",
            "quotaExceeded",
            "storageQuotaExceeded",
            "teamDriveFileLimitExceeded",
        ],
    )
    def test_quota_403_is_transient(self, reason):
        from slack_chat_migrator.services.drive_adapter import is_transient_error

        assert is_transient_error(_http_error(403, reason)) is True

    @pytest.mark.parametrize(
        ("call", "resource", "method"),
        [
            (lambda a: a.create_file({"name": "x"}), "files", "create"),
            (lambda a: a.list_files("trashed=false"), "files", "list"),
            (lambda a: a.update_file("F", {}), "files", "update"),
            (lambda a: a.create_permission("F", {}), "permissions", "create"),
            (lambda a: a.list_permissions("F"), "permissions", "list"),
            (lambda a: a.list_drives(), "drives", "list"),
        ],
        ids=["create", "list", "update", "grant", "permissions", "drives"],
    )
    def test_every_call_raises_drive_quota_error(
        self, adapter, mock_service, call, resource, method
    ):
        from slack_chat_migrator.exceptions import DriveQuotaError

        error = _http_error(403, "storageQuotaExceeded")
        request = getattr(getattr(mock_service, resource)(), method).return_value
        request.execute.side_effect = error

        with pytest.raises(DriveQuotaError) as excinfo:
            call(adapter)

        assert excinfo.value.reason == "storageQuotaExceeded"
        assert excinfo.value.__cause__ is error
        assert "Free space or add storage" in str(excinfo.value)

    @pytest.mark.parametrize(
        "error",
        [
            _http_error(403, "userRateLimitExceeded"),
            _http_error(403, "insufficientFilePermissions"),
            _http_error(500),
        ],
        ids=["rate-limit", "refused", "500"],
    )
    def test_other_errors_are_raised_as_they_are(self, adapter, mock_service, error):
        from googleapiclient.errors import HttpError

        mock_service.files().create.return_value.execute.side_effect = error

        with pytest.raises(HttpError) as excinfo:
            adapter.create_file({"name": "x"})

        assert excinfo.value is error

    def test_permission_changes_raise_after_every_change_was_tried(self):
        from slack_chat_migrator.exceptions import DriveQuotaError
        from slack_chat_migrator.services.drive_adapter import PermissionChange

        service = MagicMock(spec=["permissions"])  # no batches: one by one
        service.permissions.return_value.create.return_value.execute.side_effect = [
            _http_error(403, "dailyLimitExceeded"),
            {"id": "p2"},
        ]
        changes = [
            PermissionChange("A", body={"role": "writer"}),
            PermissionChange("B", body={"role": "writer"}),
        ]

        with pytest.raises(DriveQuotaError, match="dailyLimitExceeded"):
            DriveAdapter(service).apply_permission_changes(changes)

        execute = service.permissions.return_value.create.return_value.execute
        assert execute.call_count == 2
