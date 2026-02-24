"""Unit tests for the DriveFileUploader class."""

import hashlib
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError

from slack_migrator.core.state import MigrationState
from slack_migrator.services.drive.drive_uploader import (
    DriveFileUploader,
)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _make_uploader(
    dry_run=False,
    workspace_domain="example.com",
    service_account_email=None,
):
    """Build a DriveFileUploader with a mocked drive service."""
    drive_service = MagicMock()
    return DriveFileUploader(
        drive_service=drive_service,
        workspace_domain=workspace_domain,
        dry_run=dry_run,
        service_account_email=service_account_email,
    )


def _http_error(status=404, reason="Not Found"):
    """Create an HttpError for testing."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=b"error")


# -------------------------------------------------------------------
# TestCalculateFileHash
# -------------------------------------------------------------------


class TestCalculateFileHash:
    """Tests for _calculate_file_hash."""

    def test_correct_md5_hash(self, tmp_path):
        """Calculates correct MD5 hash for a file."""
        test_file = tmp_path / "test.txt"
        content = b"hello world"
        test_file.write_bytes(content)

        expected = hashlib.md5(content).hexdigest()  # noqa: S324

        uploader = _make_uploader()
        result = uploader._calculate_file_hash(str(test_file))

        assert result == expected

    def test_empty_file_hash(self, tmp_path):
        """Calculates correct MD5 hash for an empty file."""
        test_file = tmp_path / "empty.txt"
        test_file.write_bytes(b"")

        expected = hashlib.md5(b"").hexdigest()  # noqa: S324

        uploader = _make_uploader()
        result = uploader._calculate_file_hash(str(test_file))

        assert result == expected

    def test_large_file_hash(self, tmp_path):
        """Handles files larger than the 4096-byte chunk."""
        test_file = tmp_path / "large.bin"
        content = b"x" * 10000
        test_file.write_bytes(content)

        expected = hashlib.md5(content).hexdigest()  # noqa: S324

        uploader = _make_uploader()
        result = uploader._calculate_file_hash(str(test_file))

        assert result == expected


# -------------------------------------------------------------------
# TestGetCurrentChannel
# -------------------------------------------------------------------


class TestGetCurrentChannel:
    """Tests for _get_current_channel."""

    def test_returns_channel_when_migrator_set(self):
        """Returns channel name when migrator is set."""
        uploader = _make_uploader()
        migrator = MagicMock()
        migrator.state = MigrationState()
        migrator.state.current_channel = "general"
        uploader.migrator = migrator

        assert uploader._get_current_channel() == "general"

    def test_returns_none_when_migrator_is_none(self):
        """Returns None when migrator is None."""
        uploader = _make_uploader()
        uploader.migrator = None

        assert uploader._get_current_channel() is None

    def test_returns_none_when_no_current_channel(self):
        """Returns None when migrator has no current_channel."""
        uploader = _make_uploader()
        migrator = MagicMock(spec=[])  # no attributes
        uploader.migrator = migrator

        assert uploader._get_current_channel() is None


# -------------------------------------------------------------------
# TestPreCacheFolderFileHashes
# -------------------------------------------------------------------


class TestPreCacheFolderFileHashes:
    """Tests for pre_cache_folder_file_hashes."""

    def test_caches_file_hashes_from_response(self):
        """Caches file hashes from API response."""
        uploader = _make_uploader()

        api_response = {
            "files": [
                {
                    "id": "file1",
                    "name": "a.txt",
                    "md5Checksum": "abc123",
                    "webViewLink": "https://link1",
                },
                {
                    "id": "file2",
                    "name": "b.txt",
                    "md5Checksum": "def456",
                    "webViewLink": "https://link2",
                },
            ]
        }
        mock_list = MagicMock()
        mock_list.execute.return_value = api_response
        uploader.drive_service.files().list.return_value = mock_list

        count = uploader.pre_cache_folder_file_hashes("folder1")

        assert count == 2
        assert uploader.file_hash_cache["abc123"] == (
            "file1",
            "https://link1",
        )
        assert uploader.file_hash_cache["def456"] == (
            "file2",
            "https://link2",
        )
        assert "folder1" in uploader.folders_pre_cached

    def test_handles_pagination(self):
        """Handles nextPageToken for multi-page results."""
        uploader = _make_uploader()

        page1 = {
            "files": [
                {
                    "id": "f1",
                    "name": "a.txt",
                    "md5Checksum": "hash1",
                    "webViewLink": "https://l1",
                }
            ],
            "nextPageToken": "token2",
        }
        page2 = {
            "files": [
                {
                    "id": "f2",
                    "name": "b.txt",
                    "md5Checksum": "hash2",
                    "webViewLink": "https://l2",
                }
            ],
        }

        mock_list = MagicMock()
        mock_list.execute.side_effect = [page1, page2]
        uploader.drive_service.files().list.return_value = mock_list

        count = uploader.pre_cache_folder_file_hashes("folder_x")

        assert count == 2
        assert mock_list.execute.call_count == 2

    def test_empty_folder_returns_zero(self):
        """Empty folder returns 0 cached files."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        count = uploader.pre_cache_folder_file_hashes("empty_folder")

        assert count == 0

    def test_http_error_returns_zero(self):
        """HttpError returns 0 and logs warning."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.side_effect = _http_error(403)
        uploader.drive_service.files().list.return_value = mock_list

        count = uploader.pre_cache_folder_file_hashes("bad_folder")

        assert count == 0

    def test_already_cached_folder_skipped(self):
        """Skips folder that was already pre-cached."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        count = uploader.pre_cache_folder_file_hashes("folder1")

        assert count == 0
        uploader.drive_service.files().list.assert_not_called()

    def test_dry_run_returns_zero(self):
        """Dry run mode returns 0 without API calls."""
        uploader = _make_uploader(dry_run=True)

        count = uploader.pre_cache_folder_file_hashes("folder1")

        assert count == 0
        uploader.drive_service.files().list.assert_not_called()


# -------------------------------------------------------------------
# TestFindFileByHash (check_file_exists_by_hash)
# -------------------------------------------------------------------


class TestFindFileByHash:
    """Tests for _find_file_by_hash."""

    def test_file_found_in_cache(self):
        """File found in cache returns (file_id, web_link)."""
        uploader = _make_uploader()
        uploader.file_hash_cache["abc123"] = (
            "cached_id",
            "https://cached_link",
        )

        # Mock the verification call
        mock_get = MagicMock()
        mock_get.execute.return_value = {
            "id": "cached_id",
            "webViewLink": "https://verified_link",
        }
        uploader.drive_service.files().get.return_value = mock_get

        file_id, link = uploader._find_file_by_hash("abc123", "test.txt", "folder1")

        assert file_id == "cached_id"
        assert link == "https://verified_link"

    def test_file_not_in_cache_or_drive(self):
        """File not in cache and not found returns (None, None)."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        file_id, link = uploader._find_file_by_hash(
            "unknown_hash", "test.txt", "folder1"
        )

        assert file_id is None
        assert link is None

    def test_file_found_via_api_search(self):
        """File not in cache but found via API search."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.return_value = {
            "files": [
                {
                    "id": "found_id",
                    "name": "test.txt",
                    "webViewLink": "https://found_link",
                }
            ]
        }
        uploader.drive_service.files().list.return_value = mock_list

        file_id, link = uploader._find_file_by_hash("new_hash", "test.txt", "folder1")

        assert file_id == "found_id"
        assert link == "https://found_link"
        # Verify it was cached
        assert uploader.file_hash_cache["new_hash"] == (
            "found_id",
            "https://found_link",
        )

    def test_http_error_returns_none(self):
        """HttpError during search returns (None, None)."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.side_effect = _http_error(500)
        uploader.drive_service.files().list.return_value = mock_list

        file_id, link = uploader._find_file_by_hash("hash_err", "test.txt", "folder1")

        assert file_id is None
        assert link is None


# -------------------------------------------------------------------
# TestUploadFileToDrive
# -------------------------------------------------------------------


class TestUploadFileToDrive:
    """Tests for upload_file_to_drive."""

    def test_dry_run_returns_mock_response(self):
        """Dry run returns mock file ID and URL."""
        uploader = _make_uploader(dry_run=True)

        file_id, url = uploader.upload_file_to_drive(
            "/tmp/test.txt", "test.txt", "folder1"
        )

        assert file_id == "DRY_FILE_test.txt"
        assert "dry-run" in url

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_successful_upload(self, mock_media_cls, tmp_path):
        """Successful upload returns file metadata."""
        uploader = _make_uploader()
        # Pre-cache to avoid the pre_cache call
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "upload.txt"
        test_file.write_bytes(b"content")

        # _find_file_by_hash returns not found
        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        # create returns uploaded file
        mock_create = MagicMock()
        mock_create.execute.return_value = {
            "id": "new_file_id",
            "webViewLink": "https://new_link",
        }
        uploader.drive_service.files().create.return_value = mock_create

        file_id, url = uploader.upload_file_to_drive(
            str(test_file), "upload.txt", "folder1"
        )

        assert file_id == "new_file_id"
        assert url == "https://new_link"

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_http_error_during_upload(self, mock_media_cls, tmp_path):
        """HttpError during upload returns (None, None)."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "fail.txt"
        test_file.write_bytes(b"data")

        # _find_file_by_hash returns not found
        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        # create raises HttpError
        mock_create = MagicMock()
        mock_create.execute.side_effect = _http_error(500)
        uploader.drive_service.files().create.return_value = mock_create

        file_id, url = uploader.upload_file_to_drive(
            str(test_file), "fail.txt", "folder1"
        )

        assert file_id is None
        assert url is None

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_reuses_existing_file_by_hash(self, mock_media_cls, tmp_path):
        """Reuses existing file when hash matches."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "dup.txt"
        content = b"duplicate content"
        test_file.write_bytes(content)
        file_hash = hashlib.md5(content).hexdigest()  # noqa: S324

        # Pre-populate cache with matching hash
        uploader.file_hash_cache[file_hash] = (
            "existing_id",
            "https://existing_link",
        )

        # Verification call for _find_file_by_hash
        mock_get = MagicMock()
        mock_get.execute.return_value = {
            "id": "existing_id",
            "webViewLink": "https://existing_link",
        }
        uploader.drive_service.files().get.return_value = mock_get

        file_id, url = uploader.upload_file_to_drive(
            str(test_file), "dup.txt", "folder1"
        )

        assert file_id == "existing_id"
        assert url == "https://existing_link"
        # create should NOT have been called
        uploader.drive_service.files().create.assert_not_called()

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_reused_file_sets_poster_permission(self, mock_media_cls, tmp_path):
        """Sets poster permission on reused file when message_poster_email provided."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "dup.txt"
        content = b"duplicate content"
        test_file.write_bytes(content)
        file_hash = hashlib.md5(content).hexdigest()  # noqa: S324

        uploader.file_hash_cache[file_hash] = (
            "existing_id",
            "https://existing_link",
        )

        mock_get = MagicMock()
        mock_get.execute.return_value = {
            "id": "existing_id",
            "webViewLink": "https://existing_link",
        }
        uploader.drive_service.files().get.return_value = mock_get

        mock_perm_create = MagicMock()
        mock_perm_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_perm_create

        file_id, _url = uploader.upload_file_to_drive(
            str(test_file),
            "dup.txt",
            "folder1",
            message_poster_email="poster@example.com",
        )

        assert file_id == "existing_id"
        uploader.drive_service.files().create.assert_not_called()
        uploader.drive_service.permissions().create.assert_called_once()
        call_kwargs = uploader.drive_service.permissions().create.call_args
        body = call_kwargs.kwargs.get("body", call_kwargs[1].get("body"))
        assert body["emailAddress"] == "poster@example.com"
        assert body["role"] == "editor"


# -------------------------------------------------------------------
# TestSetMessagePosterPermission (_share_file_with_domain)
# -------------------------------------------------------------------


class TestSetMessagePosterPermission:
    """Tests for _set_message_poster_permission."""

    def test_success_creates_permission(self):
        """Successfully creates editor permission."""
        uploader = _make_uploader()

        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader._set_message_poster_permission("file1", "user@example.com")

        assert result is True

    def test_http_error_returns_false(self):
        """HttpError during permission creation returns False."""
        uploader = _make_uploader()

        mock_create = MagicMock()
        mock_create.execute.side_effect = _http_error(403)
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader._set_message_poster_permission("file1", "user@example.com")

        assert result is False

    def test_shared_drive_uses_writer_role(self):
        """Shared drive uses 'writer' role instead of 'editor'."""
        uploader = _make_uploader()

        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm2"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader._set_message_poster_permission(
            "file1",
            "user@example.com",
            shared_drive_id="drive1",
        )

        assert result is True
        # Verify body had 'writer' role
        call_kwargs = uploader.drive_service.permissions().create.call_args
        body = call_kwargs.kwargs.get("body", call_kwargs[1].get("body"))
        assert body["role"] == "writer"


# -------------------------------------------------------------------
# TestInit
# -------------------------------------------------------------------


class TestInit:
    """Tests for __init__."""

    def test_default_state(self):
        """Initializes with expected default attributes."""
        uploader = _make_uploader(
            workspace_domain="test.com",
            service_account_email="sa@test.com",
        )

        assert uploader.workspace_domain == "test.com"
        assert uploader.dry_run is False
        assert uploader.service_account_email == "sa@test.com"
        assert uploader.file_hash_cache == {}
        assert uploader.folders_pre_cached == set()
        assert uploader.migrator is None


# -------------------------------------------------------------------
# TestSetFilePermissionsForUsers
# -------------------------------------------------------------------


class TestSetFilePermissionsForUsers:
    """Tests for set_file_permissions_for_users."""

    def test_single_user_gets_reader_role(self):
        """A user not matching message_poster gets reader role."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users(
            "file1", ["reader@example.com"]
        )

        assert result is True
        call_kwargs = uploader.drive_service.permissions().create.call_args
        body = call_kwargs.kwargs.get("body", call_kwargs[1].get("body"))
        assert body["role"] == "reader"

    def test_message_poster_in_list_gets_editor(self):
        """Message poster in user list gets editor role."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users(
            "file1",
            ["poster@example.com"],
            message_poster_email="poster@example.com",
        )

        assert result is True
        call_kwargs = uploader.drive_service.permissions().create.call_args
        body = call_kwargs.kwargs.get("body", call_kwargs[1].get("body"))
        assert body["role"] == "editor"

    def test_message_poster_not_in_list_added_separately(self):
        """Message poster not in user_emails gets added with editor role."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users(
            "file1",
            ["reader@example.com"],
            message_poster_email="poster@example.com",
        )

        assert result is True
        # Should have 2 calls: reader + poster
        assert uploader.drive_service.permissions().create.call_count == 2

    def test_service_account_gets_editor(self):
        """Service account always gets editor permission."""
        uploader = _make_uploader(service_account_email="sa@example.com")
        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users(
            "file1", ["reader@example.com"]
        )

        assert result is True
        # 1 reader + 1 service account
        assert uploader.drive_service.permissions().create.call_count == 2

    def test_partial_failure(self):
        """Some users fail but processing continues."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        # First succeeds, second fails
        mock_create.execute.side_effect = [
            {"id": "perm1"},
            _http_error(403, "Forbidden"),
        ]
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users(
            "file1", ["a@example.com", "b@example.com"]
        )

        assert result is False

    def test_http_error_logged_not_crash(self):
        """HttpError during permission creation doesn't crash."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        mock_create.execute.side_effect = _http_error(500, "Server Error")
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users("file1", ["user@example.com"])

        assert result is False

    def test_empty_user_emails_returns_false(self):
        """Empty user list returns False."""
        uploader = _make_uploader()

        result = uploader.set_file_permissions_for_users("file1", [])

        assert result is False

    def test_all_permissions_succeed_returns_true(self):
        """Returns True when all permissions are set successfully."""
        uploader = _make_uploader(service_account_email="sa@example.com")
        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.set_file_permissions_for_users(
            "file1",
            ["a@example.com", "b@example.com"],
            message_poster_email="a@example.com",
        )

        assert result is True
        # 2 users + 1 service account = 3
        assert uploader.drive_service.permissions().create.call_count == 3


# -------------------------------------------------------------------
# TestTransferOwnership
# -------------------------------------------------------------------


class TestTransferOwnership:
    """Tests for transfer_ownership."""

    def test_successful_transfer(self):
        """Successful ownership transfer returns True."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        mock_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.transfer_ownership("file1", "owner@example.com")

        assert result is True
        call_kwargs = uploader.drive_service.permissions().create.call_args
        assert (
            call_kwargs.kwargs.get(
                "transferOwnership",
                call_kwargs[1].get("transferOwnership"),
            )
            is True
        )

    def test_dry_run_does_not_call_api(self):
        """Dry run logs but doesn't call API."""
        uploader = _make_uploader(dry_run=True)

        result = uploader.transfer_ownership("file1", "owner@example.com")

        assert result is True
        uploader.drive_service.permissions().create.assert_not_called()

    def test_http_error_returns_false(self):
        """HttpError during transfer returns False."""
        uploader = _make_uploader()
        mock_create = MagicMock()
        mock_create.execute.side_effect = _http_error(403, "Forbidden")
        uploader.drive_service.permissions().create.return_value = mock_create

        result = uploader.transfer_ownership("file1", "owner@example.com")

        assert result is False


# -------------------------------------------------------------------
# Additional edge-case tests for pre_cache and find_file_by_hash
# -------------------------------------------------------------------


class TestPreCacheFolderFileHashesSharedDrive:
    """Tests for pre_cache_folder_file_hashes with shared_drive_id."""

    def test_shared_drive_params_included(self):
        """Shared drive params (corpora, driveId, etc.) are included."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        uploader.pre_cache_folder_file_hashes("folder1", shared_drive_id="sd1")

        call_kwargs = uploader.drive_service.files().list.call_args
        assert call_kwargs.kwargs.get("driveId", call_kwargs[1].get("driveId")) == "sd1"
        assert (
            call_kwargs.kwargs.get(
                "includeItemsFromAllDrives",
                call_kwargs[1].get("includeItemsFromAllDrives"),
            )
            is True
        )

    def test_skips_files_without_hash(self):
        """Files missing md5Checksum or id are not cached."""
        uploader = _make_uploader()

        api_response = {
            "files": [
                {"id": "f1", "name": "a.txt", "webViewLink": "https://l1"},
                {
                    "id": "f2",
                    "name": "b.txt",
                    "md5Checksum": "hash2",
                    "webViewLink": "https://l2",
                },
            ]
        }
        mock_list = MagicMock()
        mock_list.execute.return_value = api_response
        uploader.drive_service.files().list.return_value = mock_list

        count = uploader.pre_cache_folder_file_hashes("folder1")

        assert count == 1
        assert "hash2" in uploader.file_hash_cache


class TestFindFileByHashCacheMiss:
    """Tests for _find_file_by_hash when cached file no longer exists."""

    def test_cached_file_deleted_falls_back_to_search(self):
        """When cached file verification fails, falls back to API search."""
        uploader = _make_uploader()
        uploader.file_hash_cache["hash1"] = ("old_id", "https://old_link")

        # Verification fails (file deleted)
        mock_get = MagicMock()
        mock_get.execute.side_effect = Exception("File not found")
        uploader.drive_service.files().get.return_value = mock_get

        # API search finds the file with new ID
        mock_list = MagicMock()
        mock_list.execute.return_value = {
            "files": [
                {
                    "id": "new_id",
                    "name": "test.txt",
                    "webViewLink": "https://new_link",
                }
            ]
        }
        uploader.drive_service.files().list.return_value = mock_list

        file_id, link = uploader._find_file_by_hash("hash1", "test.txt", "folder1")

        assert file_id == "new_id"
        assert link == "https://new_link"
        # Old cache entry replaced
        assert uploader.file_hash_cache["hash1"] == ("new_id", "https://new_link")

    def test_shared_drive_search_params(self):
        """Shared drive params passed to file search."""
        uploader = _make_uploader()

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        uploader._find_file_by_hash(
            "hash1", "test.txt", "folder1", shared_drive_id="sd1"
        )

        call_kwargs = uploader.drive_service.files().list.call_args
        assert call_kwargs.kwargs.get("driveId", call_kwargs[1].get("driveId")) == "sd1"


class TestUploadFileToDriveAdditional:
    """Additional tests for upload_file_to_drive."""

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_default_mime_type_fallback(self, mock_media_cls, tmp_path):
        """Unknown file extension falls back to application/octet-stream."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "data.xyz123"
        test_file.write_bytes(b"binary data")

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        mock_create = MagicMock()
        mock_create.execute.return_value = {
            "id": "file_id",
            "webViewLink": "https://link",
        }
        uploader.drive_service.files().create.return_value = mock_create

        file_id, _url = uploader.upload_file_to_drive(
            str(test_file), "data.xyz123", "folder1"
        )

        assert file_id == "file_id"
        # Verify MediaFileUpload was called with octet-stream
        mock_media_cls.assert_called_once_with(
            str(test_file), mimetype="application/octet-stream"
        )

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_upload_with_shared_drive(self, mock_media_cls, tmp_path):
        """Upload to shared drive includes supportsAllDrives."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "upload.txt"
        test_file.write_bytes(b"content")

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        mock_create = MagicMock()
        mock_create.execute.return_value = {
            "id": "new_id",
            "webViewLink": "https://new_link",
        }
        uploader.drive_service.files().create.return_value = mock_create

        file_id, _url = uploader.upload_file_to_drive(
            str(test_file), "upload.txt", "folder1", shared_drive_id="sd1"
        )

        assert file_id == "new_id"
        call_kwargs = uploader.drive_service.files().create.call_args
        assert (
            call_kwargs.kwargs.get(
                "supportsAllDrives",
                call_kwargs[1].get("supportsAllDrives"),
            )
            is True
        )

    @patch("slack_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_upload_sets_poster_permission_on_new_file(self, mock_media_cls, tmp_path):
        """Message poster email triggers permission call on new upload."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "upload.txt"
        test_file.write_bytes(b"content")

        mock_list = MagicMock()
        mock_list.execute.return_value = {"files": []}
        uploader.drive_service.files().list.return_value = mock_list

        mock_create = MagicMock()
        mock_create.execute.return_value = {
            "id": "new_id",
            "webViewLink": "https://link",
        }
        uploader.drive_service.files().create.return_value = mock_create

        mock_perm_create = MagicMock()
        mock_perm_create.execute.return_value = {"id": "perm1"}
        uploader.drive_service.permissions().create.return_value = mock_perm_create

        file_id, _url = uploader.upload_file_to_drive(
            str(test_file),
            "upload.txt",
            "folder1",
            message_poster_email="poster@example.com",
        )

        assert file_id == "new_id"
        uploader.drive_service.permissions().create.assert_called_once()
        call_kwargs = uploader.drive_service.permissions().create.call_args
        body = call_kwargs.kwargs.get("body", call_kwargs[1].get("body"))
        assert body["emailAddress"] == "poster@example.com"
        assert body["role"] == "editor"
