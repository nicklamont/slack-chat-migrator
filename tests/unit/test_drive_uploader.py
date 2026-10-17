"""Unit tests for the DriveFileUploader class."""

import hashlib
from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_chat_migrator.constants import DRIVE_UPLOAD_CHUNK_BYTES
from slack_chat_migrator.services.drive.drive_uploader import (
    DriveFileUploader,
    DriveUpload,
)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _make_uploader(
    workspace_domain="example.com",
):
    """Build a DriveFileUploader with a mocked drive service."""
    drive_service = MagicMock()
    return DriveFileUploader(
        drive_service=drive_service,
        workspace_domain=workspace_domain,
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

    def test_returns_channel_when_set(self):
        """Returns channel name when current_channel is set."""
        uploader = _make_uploader()
        uploader.current_channel = "general"

        assert uploader._get_current_channel() == "general"

    def test_returns_none_when_not_set(self):
        """Returns None when current_channel is None (the default)."""
        uploader = _make_uploader()
        assert uploader.current_channel is None

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
        uploader.drive_service.list_files.return_value = api_response

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

        uploader.drive_service.list_files.side_effect = [page1, page2]

        count = uploader.pre_cache_folder_file_hashes("folder_x")

        assert count == 2
        assert uploader.drive_service.list_files.call_count == 2

    def test_empty_folder_returns_zero(self):
        """Empty folder returns 0 cached files."""
        uploader = _make_uploader()

        uploader.drive_service.list_files.return_value = {"files": []}

        count = uploader.pre_cache_folder_file_hashes("empty_folder")

        assert count == 0

    def test_http_error_returns_zero(self):
        """HttpError returns 0 and logs warning."""
        uploader = _make_uploader()

        uploader.drive_service.list_files.side_effect = _http_error(403)

        count = uploader.pre_cache_folder_file_hashes("bad_folder")

        assert count == 0

    def test_already_cached_folder_skipped(self):
        """Skips folder that was already pre-cached."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        count = uploader.pre_cache_folder_file_hashes("folder1")

        assert count == 0
        uploader.drive_service.list_files.assert_not_called()

    def test_dry_run_delegates_to_service(self):
        """Dry run mode delegates to the (injected) drive service."""
        uploader = _make_uploader()
        uploader.drive_service.list_files.return_value = {"files": []}

        count = uploader.pre_cache_folder_file_hashes("folder1")

        assert count == 0
        uploader.drive_service.list_files.assert_called_once()


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
        uploader.drive_service.get_file.return_value = {
            "id": "cached_id",
            "webViewLink": "https://verified_link",
        }

        file_id, link = uploader._find_file_by_hash("abc123", "test.txt", "folder1")

        assert file_id == "cached_id"
        assert link == "https://verified_link"

    def test_file_not_in_cache_or_drive(self):
        """File not in cache and not found returns (None, None)."""
        uploader = _make_uploader()

        uploader.drive_service.list_files.return_value = {"files": []}

        file_id, link = uploader._find_file_by_hash(
            "unknown_hash", "test.txt", "folder1"
        )

        assert file_id is None
        assert link is None

    def test_file_found_via_api_search(self):
        """File not in cache but found via API search."""
        uploader = _make_uploader()

        uploader.drive_service.list_files.return_value = {
            "files": [
                {
                    "id": "found_id",
                    "name": "test.txt",
                    "webViewLink": "https://found_link",
                }
            ]
        }

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

        uploader.drive_service.list_files.side_effect = _http_error(500)

        file_id, link = uploader._find_file_by_hash("hash_err", "test.txt", "folder1")

        assert file_id is None
        assert link is None


# -------------------------------------------------------------------
# TestUploadFileToDrive
# -------------------------------------------------------------------


class TestUploadFileToDrive:
    """Tests for upload_file_to_drive."""

    @patch("slack_chat_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_successful_upload(self, mock_media_cls, tmp_path):
        """Successful upload returns file metadata."""
        uploader = _make_uploader()
        # Pre-cache to avoid the pre_cache call
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "upload.txt"
        test_file.write_bytes(b"content")

        # _find_file_by_hash returns not found
        uploader.drive_service.list_files.return_value = {"files": []}

        # create returns uploaded file
        uploader.drive_service.create_file.return_value = {
            "id": "new_file_id",
            "webViewLink": "https://new_link",
        }

        upload = uploader.upload_file_to_drive(str(test_file), "upload.txt", "folder1")

        assert upload == DriveUpload("new_file_id", "https://new_link", reused=False)
        uploader.drive_service.create_permission.assert_not_called()

    @patch("slack_chat_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_http_error_during_upload(self, mock_media_cls, tmp_path):
        """HttpError during upload returns None."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "fail.txt"
        test_file.write_bytes(b"data")

        # _find_file_by_hash returns not found
        uploader.drive_service.list_files.return_value = {"files": []}

        # create raises HttpError
        uploader.drive_service.create_file.side_effect = _http_error(500)

        assert (
            uploader.upload_file_to_drive(str(test_file), "fail.txt", "folder1") is None
        )

    @patch("slack_chat_migrator.services.drive.drive_uploader.MediaFileUpload")
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
        uploader.drive_service.get_file.return_value = {
            "id": "existing_id",
            "webViewLink": "https://existing_link",
        }

        upload = uploader.upload_file_to_drive(str(test_file), "dup.txt", "folder1")

        assert upload == DriveUpload(
            "existing_id", "https://existing_link", reused=True
        )
        # create should NOT have been called
        uploader.drive_service.create_file.assert_not_called()


# -------------------------------------------------------------------
# TestSetMessagePosterPermission (_share_file_with_domain)
# -------------------------------------------------------------------


class TestSetMessagePosterPermission:
    """Tests for set_message_poster_permission."""

    def test_success_creates_permission(self):
        """Successfully creates a writer permission."""
        uploader = _make_uploader()

        uploader.drive_service.create_permission.return_value = {"id": "perm1"}

        result = uploader.set_message_poster_permission("file1", "user@example.com")

        assert result is True

    def test_http_error_returns_false(self):
        """HttpError during permission creation returns False."""
        uploader = _make_uploader()

        uploader.drive_service.create_permission.side_effect = _http_error(403)

        result = uploader.set_message_poster_permission("file1", "user@example.com")

        assert result is False

    @pytest.mark.parametrize("shared_drive_id", [None, "drive1"])
    def test_role_is_writer_in_both_modes(self, shared_drive_id):
        """Drive v3 has no "editor" role; posters always get "writer" (I12)."""
        uploader = _make_uploader()

        uploader.drive_service.create_permission.return_value = {"id": "perm2"}

        result = uploader.set_message_poster_permission(
            "file1",
            "user@example.com",
            shared_drive_id=shared_drive_id,
        )

        assert result is True
        call_kwargs = uploader.drive_service.create_permission.call_args
        body = call_kwargs.kwargs["body"]
        assert body["role"] == "writer"
        assert call_kwargs.kwargs["supports_all_drives"] is bool(shared_drive_id)


# -------------------------------------------------------------------
# TestInit
# -------------------------------------------------------------------


class TestInit:
    """Tests for __init__."""

    def test_default_state(self):
        """Initializes with expected default attributes."""
        uploader = _make_uploader(workspace_domain="test.com")

        assert uploader.workspace_domain == "test.com"
        assert uploader.file_hash_cache == {}
        assert uploader.folders_pre_cached == set()
        assert uploader.current_channel is None


# -------------------------------------------------------------------
# Additional edge-case tests for pre_cache and find_file_by_hash
# -------------------------------------------------------------------


class TestPreCacheFolderFileHashesSharedDrive:
    """Tests for pre_cache_folder_file_hashes with shared_drive_id."""

    def test_shared_drive_params_included(self):
        """Shared drive params (corpora, drive_id, etc.) are included."""
        uploader = _make_uploader()

        uploader.drive_service.list_files.return_value = {"files": []}

        uploader.pre_cache_folder_file_hashes("folder1", shared_drive_id="sd1")

        call_kwargs = uploader.drive_service.list_files.call_args
        assert call_kwargs.kwargs.get("drive_id") == "sd1"
        assert call_kwargs.kwargs.get("include_items_from_all_drives") is True

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
        uploader.drive_service.list_files.return_value = api_response

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
        uploader.drive_service.get_file.side_effect = HttpError(
            Response({"status": "404"}), b"File not found"
        )

        # API search finds the file with new ID
        uploader.drive_service.list_files.return_value = {
            "files": [
                {
                    "id": "new_id",
                    "name": "test.txt",
                    "webViewLink": "https://new_link",
                }
            ]
        }

        file_id, link = uploader._find_file_by_hash("hash1", "test.txt", "folder1")

        assert file_id == "new_id"
        assert link == "https://new_link"
        # Old cache entry replaced
        assert uploader.file_hash_cache["hash1"] == ("new_id", "https://new_link")

    def test_shared_drive_search_params(self):
        """Shared drive params passed to file search."""
        uploader = _make_uploader()

        uploader.drive_service.list_files.return_value = {"files": []}

        uploader._find_file_by_hash(
            "hash1", "test.txt", "folder1", shared_drive_id="sd1"
        )

        call_kwargs = uploader.drive_service.list_files.call_args
        assert call_kwargs.kwargs.get("drive_id") == "sd1"


class TestUploadFileToDriveAdditional:
    """Additional tests for upload_file_to_drive."""

    @patch("slack_chat_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_default_mime_type_fallback(self, mock_media_cls, tmp_path):
        """Unknown file extension falls back to application/octet-stream."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "data.xyz123"
        test_file.write_bytes(b"binary data")

        uploader.drive_service.list_files.return_value = {"files": []}

        uploader.drive_service.create_file.return_value = {
            "id": "file_id",
            "webViewLink": "https://link",
        }

        upload = uploader.upload_file_to_drive(str(test_file), "data.xyz123", "folder1")

        assert upload.file_id == "file_id"
        # Verify MediaFileUpload was called with octet-stream
        mock_media_cls.assert_called_once_with(
            str(test_file),
            mimetype="application/octet-stream",
            resumable=True,
            chunksize=DRIVE_UPLOAD_CHUNK_BYTES,
        )

    @patch("slack_chat_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_upload_with_shared_drive(self, mock_media_cls, tmp_path):
        """Upload to shared drive includes supports_all_drives."""
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")

        test_file = tmp_path / "upload.txt"
        test_file.write_bytes(b"content")

        uploader.drive_service.list_files.return_value = {"files": []}

        uploader.drive_service.create_file.return_value = {
            "id": "new_id",
            "webViewLink": "https://new_link",
        }

        upload = uploader.upload_file_to_drive(
            str(test_file), "upload.txt", "folder1", shared_drive_id="sd1"
        )

        assert upload.file_id == "new_id"
        call_kwargs = uploader.drive_service.create_file.call_args
        assert call_kwargs.kwargs.get("supports_all_drives") is True


class TestResumableUpload:
    """Drive uploads stream in resumable chunks with Slack's type (I28)."""

    @patch("slack_chat_migrator.services.drive.drive_uploader.MediaFileUpload")
    def test_uses_given_mime_type_and_hash(self, mock_media_cls, tmp_path):
        uploader = _make_uploader()
        uploader.folders_pre_cached.add("folder1")
        test_file = tmp_path / "Screenshot"
        test_file.write_bytes(b"\x89PNG")
        uploader.drive_service.list_files.return_value = {"files": []}
        uploader.drive_service.create_file.return_value = {
            "id": "f",
            "webViewLink": "l",
        }
        uploader._calculate_file_hash = MagicMock()

        uploader.upload_file_to_drive(
            str(test_file),
            "Screenshot",
            "folder1",
            mime_type="image/png",
            file_hash="abc",
        )

        uploader._calculate_file_hash.assert_not_called()  # hashed once, upstream
        mock_media_cls.assert_called_once_with(
            str(test_file),
            mimetype="image/png",
            resumable=True,
            chunksize=DRIVE_UPLOAD_CHUNK_BYTES,
        )

    def test_large_upload_memory_is_bounded(self, tmp_path):
        """A 512 MiB upload through the real client stays well under 64 MiB."""
        import json
        import tracemalloc

        from googleapiclient.discovery import build
        from googleapiclient.http import HttpMockSequence

        from slack_chat_migrator.services.drive_adapter import DriveAdapter

        size = 512 * 1024 * 1024
        big = tmp_path / "big.mov"
        with open(big, "wb") as fh:
            fh.truncate(size)  # sparse: no disk or memory cost

        chunk = DRIVE_UPLOAD_CHUNK_BYTES
        responses = [
            ({"status": "200"}, b'{"files": []}'),  # md5 duplicate search
            ({"status": "200", "location": "https://upload.example/s"}, b""),
        ]
        for end in range(chunk, size, chunk):
            responses.append(({"status": "308", "range": f"bytes=0-{end - 1}"}, b""))
        responses.append(
            ({"status": "200"}, json.dumps({"id": "BIG", "webViewLink": "l"}).encode())
        )
        http = HttpMockSequence(responses)
        service = build("drive", "v3", http=http, static_discovery=True)
        uploader = DriveFileUploader(DriveAdapter(service))
        uploader.folders_pre_cached.add("folder1")

        tracemalloc.start()
        try:
            upload = uploader.upload_file_to_drive(
                str(big),
                "big.mov",
                "folder1",
                mime_type="video/quicktime",
                file_hash="0" * 32,
            )
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert upload is not None
        assert upload.file_id == "BIG"
        assert http._iterable == []  # every chunk was sent
        assert peak < 64 * 1024 * 1024
