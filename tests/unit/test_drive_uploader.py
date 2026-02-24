"""Unit tests for the DriveFileUploader class."""

import hashlib
from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError

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
        migrator.current_channel = "general"
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
