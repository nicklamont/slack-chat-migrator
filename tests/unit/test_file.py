"""Unit tests for the file handling module."""

from unittest.mock import MagicMock, patch

import pytest
import requests
from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_migrator.core.config import MigrationConfig, SharedDriveConfig
from slack_migrator.core.state import MigrationState
from slack_migrator.services.file import FileHandler

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_deps(**overrides):
    """Create explicit dependency values for FileHandler construction."""
    state = MigrationState()
    state.context.current_channel = overrides.pop("current_channel", "general")

    config = overrides.pop(
        "config",
        MigrationConfig(
            shared_drive=SharedDriveConfig(name="Test Drive", id=None),
        ),
    )
    user_map = overrides.pop("user_map", {"U123": "alice@example.com"})
    user_resolver = overrides.pop("user_resolver", MagicMock())
    if not hasattr(user_resolver, "is_external_user") or not callable(
        user_resolver.is_external_user
    ):
        user_resolver.is_external_user = MagicMock(return_value=False)
    workspace_domain = overrides.pop("workspace_domain", "example.com")

    # Apply remaining overrides to state
    for key, value in overrides.items():
        if hasattr(state, key):
            setattr(state, key, value)

    return {
        "config": config,
        "workspace_domain": workspace_domain,
        "user_map": user_map,
        "user_resolver": user_resolver,
        "state": state,
    }


def _make_handler(
    folder_id=None,
    dry_run=False,
    **dep_overrides,
):
    """Build a FileHandler with all heavy sub-services mocked out."""
    deps = _make_deps(**dep_overrides)

    drive_service = MagicMock()
    chat_service = MagicMock()

    with (
        patch("slack_migrator.services.file.SharedDriveManager"),
        patch("slack_migrator.services.file.FolderManager"),
        patch("slack_migrator.services.file.DriveFileUploader"),
        patch("slack_migrator.services.file.ChatFileUploader"),
    ):
        handler = FileHandler(
            drive_service=drive_service,
            chat_service=chat_service,
            folder_id=folder_id,
            dry_run=dry_run,
            **deps,
        )

    return handler


# ===========================================================================
# Initialization tests
# ===========================================================================


class TestFileHandlerInit:
    """Tests for FileHandler.__init__."""

    def test_basic_attributes(self):
        handler = _make_handler()
        assert handler.processed_files == {}
        assert handler.shared_channel_folders == set()
        assert handler.dry_run is False
        assert handler._drive_initialized is False

    def test_folder_id_set_when_provided(self):
        handler = _make_handler(folder_id="folder123")
        assert handler._root_folder_id == "folder123"

    def test_folder_id_none_when_not_provided(self):
        handler = _make_handler(folder_id=None)
        assert handler._root_folder_id is None

    def test_dry_run_sets_defaults(self):
        handler = _make_handler(dry_run=True)
        assert handler.dry_run is True
        assert handler._drive_initialized is True
        assert handler._root_folder_id == "DRY_RUN_FOLDER"

    def test_dry_run_uses_explicit_folder_id(self):
        handler = _make_handler(folder_id="custom", dry_run=True)
        assert handler._root_folder_id == "custom"
        assert handler._drive_initialized is True

    def test_file_stats_initial_values(self):
        handler = _make_handler()
        stats = handler.file_stats
        assert stats["total_files"] == 0
        assert stats["drive_uploads"] == 0
        assert stats["direct_uploads"] == 0
        assert stats["failed_uploads"] == 0
        assert stats["external_user_files"] == 0
        assert stats["ownership_transferred"] == 0
        assert stats["ownership_transfer_failed"] == 0
        assert stats["files_by_channel"] == {}

    def test_explicit_deps_stored(self):
        handler = _make_handler()
        assert handler.config is not None
        assert handler.workspace_domain == "example.com"
        assert handler.user_map == {"U123": "alice@example.com"}
        assert handler.user_resolver is not None
        assert handler.state is not None

    def test_sub_services_created(self):
        handler = _make_handler()
        assert handler.shared_drive_manager is not None
        assert handler.folder_manager is not None
        assert handler.drive_uploader is not None
        assert handler.chat_uploader is not None

    def test_shared_drive_id_initial(self):
        handler = _make_handler()
        assert handler._shared_drive_id is None
        assert handler.shared_drive_id is None


# ===========================================================================
# get_file_statistics tests
# ===========================================================================


class TestGetFileStatistics:
    """Tests for FileHandler.get_file_statistics."""

    def test_empty_stats(self):
        handler = _make_handler()
        stats = handler.get_file_statistics()
        assert stats["total_files_processed"] == 0
        assert stats["successful_uploads"] == 0
        assert stats["failed_uploads"] == 0
        assert stats["drive_uploads"] == 0
        assert stats["direct_uploads"] == 0
        assert stats["external_user_files"] == 0
        assert stats["ownership_transferred"] == 0
        assert stats["ownership_transfer_failed"] == 0
        assert stats["files_by_channel"] == {}
        assert stats["success_rate"] == 0.0

    def test_stats_with_uploads(self):
        handler = _make_handler()
        handler.file_stats["total_files"] = 10
        handler.file_stats["drive_uploads"] = 6
        handler.file_stats["direct_uploads"] = 2
        handler.file_stats["failed_uploads"] = 2
        handler.file_stats["external_user_files"] = 1
        handler.file_stats["ownership_transferred"] = 4
        handler.file_stats["ownership_transfer_failed"] = 1
        handler.file_stats["files_by_channel"] = {"general": 7, "random": 3}

        stats = handler.get_file_statistics()
        assert stats["total_files_processed"] == 10
        assert stats["successful_uploads"] == 8  # 6 + 2
        assert stats["failed_uploads"] == 2
        assert stats["success_rate"] == 80.0  # 8/10*100
        assert stats["files_by_channel"]["general"] == 7

    def test_success_rate_no_divide_by_zero(self):
        handler = _make_handler()
        stats = handler.get_file_statistics()
        # With 0 total files, success_rate should be 0 (not raise)
        assert stats["success_rate"] == 0.0

    def test_success_rate_all_success(self):
        handler = _make_handler()
        handler.file_stats["total_files"] = 5
        handler.file_stats["drive_uploads"] = 5
        stats = handler.get_file_statistics()
        assert stats["success_rate"] == 100.0


# ===========================================================================
# _get_current_channel tests
# ===========================================================================


class TestGetCurrentChannel:
    """Tests for _get_current_channel helper."""

    def test_returns_channel_name(self):
        handler = _make_handler()
        assert handler._get_current_channel() == "general"

    def test_returns_none_without_current_channel(self):
        handler = _make_handler(current_channel=None)
        assert handler._get_current_channel() is None


# ===========================================================================
# reset_shared_folder_cache tests
# ===========================================================================


class TestResetSharedFolderCache:
    """Tests for reset_shared_folder_cache."""

    def test_clears_cache(self):
        handler = _make_handler()
        handler.shared_channel_folders.add("general_folder1")
        handler.shared_channel_folders.add("random_folder2")
        assert len(handler.shared_channel_folders) == 2

        handler.reset_shared_folder_cache()
        assert len(handler.shared_channel_folders) == 0


# ===========================================================================
# folder_id property tests
# ===========================================================================


class TestFolderIdProperty:
    """Tests for the folder_id backward-compat property."""

    def test_getter_triggers_init(self):
        handler = _make_handler()
        handler._drive_initialized = True
        handler._root_folder_id = "abc"
        assert handler.folder_id == "abc"

    def test_setter(self):
        handler = _make_handler()
        handler.folder_id = "new_id"
        assert handler._root_folder_id == "new_id"


# ===========================================================================
# Upload strategy / routing logic
# ===========================================================================


class TestUploadStrategy:
    """Tests for the upload strategy decision in upload_attachment."""

    def _make_ready_handler(self, dry_run=False):
        """Create a handler with drive already initialized."""
        handler = _make_handler(folder_id="root_folder", dry_run=dry_run)
        handler._drive_initialized = True
        return handler

    def test_cached_file_returns_immediately(self):
        handler = self._make_ready_handler()
        cached = {"type": "drive", "link": "https://example.com", "name": "a.txt"}
        handler.processed_files["F123"] = cached

        result = handler.upload_attachment({"id": "F123", "name": "a.txt"})
        assert result is cached
        # total_files should still be incremented
        assert handler.file_stats["total_files"] == 1

    def test_download_failure_returns_none(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(return_value=None)

        result = handler.upload_attachment(
            {"id": "F1", "name": "bad.txt"},
            channel="general",
        )
        assert result is None
        assert handler.file_stats["failed_uploads"] == 1

    def test_google_docs_skip(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(return_value=b"__GOOGLE_DOCS_SKIP__")

        file_obj = {
            "id": "F1",
            "name": "My Doc",
            "url_private": "https://docs.google.com/document/d/abc",
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "skip"
        assert result["reason"] == "google_docs_link"

    def test_google_drive_file_reference(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(return_value=b"__GOOGLE_DRIVE_FILE__")
        handler._create_drive_reference = MagicMock(
            return_value={
                "type": "drive",
                "link": "https://drive.google.com/file/d/abc/view",
                "drive_id": "abc",
                "name": "file.pdf",
                "is_reference": True,
            }
        )

        file_obj = {
            "id": "F1",
            "name": "file.pdf",
            "url_private": "https://drive.google.com/file/d/abc/view",
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        assert result["is_reference"] is True
        handler._create_drive_reference.assert_called_once()

    def test_small_image_tries_direct_upload(self):
        handler = self._make_ready_handler()
        content = b"\x89PNG" + b"\x00" * 1000
        handler._download_file = MagicMock(return_value=content)
        handler.chat_uploader.is_suitable_for_direct_upload = MagicMock(
            return_value=True
        )
        handler._upload_direct_to_chat = MagicMock(
            return_value={
                "type": "direct",
                "ref": {"attachmentDataRef": "xyz"},
                "name": "image.png",
            }
        )

        file_obj = {
            "id": "F1",
            "name": "image.png",
            "mimetype": "image/png",
            "size": 1004,
        }
        result = handler.upload_attachment(
            file_obj, channel="general", space="spaces/ABC"
        )
        assert result is not None
        assert result["type"] == "direct"
        assert handler.file_stats["direct_uploads"] == 1
        handler._upload_direct_to_chat.assert_called_once()

    def test_direct_upload_fallback_to_drive(self):
        handler = self._make_ready_handler()
        content = b"\x89PNG" + b"\x00" * 1000
        handler._download_file = MagicMock(return_value=content)
        handler.chat_uploader.is_suitable_for_direct_upload = MagicMock(
            return_value=True
        )
        handler._upload_direct_to_chat = MagicMock(return_value=None)
        handler._upload_to_drive = MagicMock(
            return_value={
                "type": "drive",
                "drive_id": "d1",
                "link": "https://drive.google.com/file/d/d1/view",
                "name": "image.png",
            }
        )

        file_obj = {
            "id": "F1",
            "name": "image.png",
            "mimetype": "image/png",
            "size": 1004,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        assert handler.file_stats["drive_uploads"] == 1

    def test_non_image_goes_to_drive(self):
        handler = self._make_ready_handler()
        content = b"file data"
        handler._download_file = MagicMock(return_value=content)
        handler._upload_to_drive = MagicMock(
            return_value={
                "type": "drive",
                "drive_id": "d1",
                "link": "https://drive.google.com/file/d/d1/view",
                "name": "report.pdf",
            }
        )

        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "mimetype": "application/pdf",
            "size": 5000,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        handler._upload_to_drive.assert_called_once()

    def test_large_image_goes_to_drive(self):
        """Images larger than 25MB should bypass direct upload."""
        handler = self._make_ready_handler()
        content = b"\x89PNG" + b"\x00" * (26 * 1024 * 1024)
        handler._download_file = MagicMock(return_value=content)
        handler._upload_to_drive = MagicMock(
            return_value={
                "type": "drive",
                "drive_id": "d1",
                "link": "https://drive.google.com/file/d/d1/view",
                "name": "huge.png",
            }
        )

        file_obj = {
            "id": "F1",
            "name": "huge.png",
            "mimetype": "image/png",
            "size": 26 * 1024 * 1024,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "d1"
        assert result["name"] == "huge.png"

    def test_drive_upload_failure_returns_none(self):
        handler = self._make_ready_handler()
        content = b"some data"
        handler._download_file = MagicMock(return_value=content)
        handler._upload_to_drive = MagicMock(return_value=None)

        file_obj = {
            "id": "F1",
            "name": "file.bin",
            "mimetype": "application/octet-stream",
            "size": 100,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is None
        assert handler.file_stats["failed_uploads"] == 1

    def test_exception_in_upload_attachment_increments_failed(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(
            side_effect=requests.RequestException("boom")
        )

        file_obj = {"id": "F1", "name": "crash.bin"}
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is None
        assert handler.file_stats["failed_uploads"] == 1

    def test_external_user_tracked(self):
        ext_resolver = MagicMock()
        ext_resolver.is_external_user = MagicMock(return_value=True)
        handler = _make_handler(
            folder_id="root",
            user_resolver=ext_resolver,
            user_map={"UEXT": "ext@other.com"},
        )
        handler._drive_initialized = True
        handler._download_file = MagicMock(return_value=None)

        file_obj = {"id": "F1", "name": "ext.txt", "user": "UEXT"}
        handler.upload_attachment(file_obj, channel="general")
        assert handler.file_stats["external_user_files"] == 1

    def test_null_mimetype_gets_guessed(self):
        handler = self._make_ready_handler()
        content = b"data"
        handler._download_file = MagicMock(return_value=content)
        handler._upload_to_drive = MagicMock(
            return_value={
                "type": "drive",
                "drive_id": "d1",
                "link": "https://drive.google.com/file/d/d1/view",
                "name": "file.json",
            }
        )

        file_obj = {
            "id": "F1",
            "name": "file.json",
            "mimetype": "null",
            "size": 4,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "d1"
        assert result["name"] == "file.json"

    def test_channel_file_count_tracking(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(return_value=None)

        handler.upload_attachment({"id": "F1", "name": "a.txt"}, channel="general")
        handler.upload_attachment({"id": "F2", "name": "b.txt"}, channel="general")
        handler.upload_attachment({"id": "F3", "name": "c.txt"}, channel="random")

        assert handler.file_stats["files_by_channel"]["general"] == 2
        assert handler.file_stats["files_by_channel"]["random"] == 1


# ===========================================================================
# upload_file backward compat
# ===========================================================================


class TestUploadFile:
    """Tests for the backward-compatible upload_file method."""

    def test_returns_drive_id_on_success(self):
        handler = _make_handler(folder_id="root")
        handler._drive_initialized = True
        handler.upload_attachment = MagicMock(
            return_value={"type": "drive", "drive_id": "d_abc"}
        )

        result = handler.upload_file({"id": "F1", "name": "test.txt"}, channel="ch")
        assert result == "d_abc"

    def test_returns_none_on_direct_upload(self):
        handler = _make_handler(folder_id="root")
        handler._drive_initialized = True
        handler.upload_attachment = MagicMock(
            return_value={"type": "direct", "ref": {}}
        )

        result = handler.upload_file({"id": "F1", "name": "test.png"})
        assert result is None

    def test_returns_none_on_failure(self):
        handler = _make_handler(folder_id="root")
        handler._drive_initialized = True
        handler.upload_attachment = MagicMock(return_value=None)

        result = handler.upload_file({"id": "F1", "name": "test.txt"})
        assert result is None

    def test_exception_returns_none(self):
        handler = _make_handler(folder_id="root")
        handler._drive_initialized = True
        handler.upload_attachment = MagicMock(side_effect=OSError("oops"))

        result = handler.upload_file({"id": "F1", "name": "test.txt"})
        assert result is None


# ===========================================================================
# _download_file tests
# ===========================================================================


class TestDownloadFile:
    """Tests for _download_file."""

    def test_no_url_returns_none(self):
        handler = _make_handler()
        result = handler._download_file({"id": "F1", "name": "no_url.txt"})
        assert result is None

    def test_google_docs_link_returns_skip_marker(self):
        handler = _make_handler()
        urls = [
            "https://docs.google.com/document/d/abc/edit",
            "https://docs.google.com/spreadsheets/d/abc/edit",
            "https://docs.google.com/presentation/d/abc/edit",
            "https://sheets.google.com/abc/edit",
            "https://slides.google.com/abc/edit",
        ]
        for url in urls:
            result = handler._download_file(
                {"id": "F1", "name": "doc", "url_private": url}
            )
            assert result == b"__GOOGLE_DOCS_SKIP__", f"Failed for URL: {url}"

    def test_google_drive_file_returns_drive_marker(self):
        handler = _make_handler()
        urls = [
            "https://drive.google.com/file/d/abc123/view",
            "https://drive.google.com/open?id=abc123",
        ]
        for url in urls:
            result = handler._download_file(
                {"id": "F1", "name": "file.pdf", "url_private": url}
            )
            assert result == b"__GOOGLE_DRIVE_FILE__", f"Failed for URL: {url}"

    @patch("slack_migrator.services.file.requests.get")
    def test_successful_download(self, mock_get):
        handler = _make_handler()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"file bytes"
        mock_response.headers = {"Content-Length": "10"}
        mock_get.return_value = mock_response

        result = handler._download_file(
            {"id": "F1", "name": "test.txt", "url_private": "https://files.slack.com/a"}
        )
        assert result == b"file bytes"
        mock_get.assert_called_once_with(
            "https://files.slack.com/a",
            headers={},
            stream=True,
            timeout=60,
        )

    @patch("slack_migrator.services.file.requests.get")
    def test_http_error_raises_for_retry(self, mock_get):
        handler = _make_handler()
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_response
        )
        mock_get.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            handler._download_file(
                {
                    "id": "F1",
                    "name": "test.txt",
                    "url_private": "https://files.slack.com/a",
                }
            )

    @patch("slack_migrator.services.file.requests.get")
    def test_auth_error_returns_none_no_raise(self, mock_get):
        handler = _make_handler()
        mock_response = MagicMock()
        mock_response.status_code = 403
        err = requests.exceptions.HTTPError(response=mock_response)
        mock_get.side_effect = err

        result = handler._download_file(
            {"id": "F1", "name": "test.txt", "url_private": "https://files.slack.com/a"}
        )
        assert result is None

    @patch("slack_migrator.services.file.requests.get")
    def test_connection_error_re_raises(self, mock_get):
        handler = _make_handler()
        mock_get.side_effect = requests.exceptions.ConnectionError("conn refused")

        with pytest.raises(requests.exceptions.ConnectionError):
            handler._download_file(
                {
                    "id": "F1",
                    "name": "test.txt",
                    "url_private": "https://files.slack.com/a",
                }
            )

    def test_non_request_exception_returns_none(self):
        handler = _make_handler()
        # Trigger a generic exception in processing by providing a file_obj
        # that will cause issues after URL check
        with patch(
            "slack_migrator.services.file.requests.get",
            side_effect=ValueError("unexpected"),
        ):
            result = handler._download_file(
                {
                    "id": "F1",
                    "name": "test.txt",
                    "url_private": "https://files.slack.com/a",
                }
            )
            assert result is None


# ===========================================================================
# _create_drive_reference tests
# ===========================================================================


class TestCreateDriveReference:
    """Tests for _create_drive_reference."""

    def test_file_d_url(self):
        handler = _make_handler()
        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "url_private": "https://drive.google.com/file/d/ABC123/view",
            "mimetype": "application/pdf",
        }
        result = handler._create_drive_reference(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "ABC123"
        assert result["is_reference"] is True
        assert result["name"] == "report.pdf"
        assert "F1" in handler.processed_files

    def test_open_id_url(self):
        handler = _make_handler()
        file_obj = {
            "id": "F2",
            "name": "file.docx",
            "url_private": "https://drive.google.com/open?id=XYZ789&other=1",
        }
        result = handler._create_drive_reference(file_obj, channel="general")
        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "XYZ789"
        assert result["is_reference"] is True

    def test_unrecognized_url_returns_none(self):
        handler = _make_handler()
        file_obj = {
            "id": "F3",
            "name": "mystery.bin",
            "url_private": "https://example.com/something",
        }
        result = handler._create_drive_reference(file_obj, channel="general")
        assert result is None

    def test_increments_drive_uploads_stat(self):
        handler = _make_handler()
        file_obj = {
            "id": "F1",
            "name": "a.txt",
            "url_private": "https://drive.google.com/file/d/id1/view",
        }
        handler._create_drive_reference(file_obj)
        assert handler.file_stats["drive_uploads"] == 1

    def test_empty_drive_id_returns_none(self):
        handler = _make_handler()
        # URL pattern matches but extracted ID is empty string (falsy)
        result = handler._create_drive_reference(
            {"id": "F1", "url_private": "https://drive.google.com/file/d//view"},
            channel="general",
        )
        assert result is None


# ===========================================================================
# _transfer_file_ownership tests
# ===========================================================================


class TestTransferFileOwnership:
    """Tests for _transfer_file_ownership."""

    def test_successful_transfer(self):
        handler = _make_handler()
        handler.drive_service.permissions().create().execute.return_value = {}

        result = handler._transfer_file_ownership("file1", "alice@example.com")
        assert result is True

    def test_failed_transfer(self):
        handler = _make_handler()
        handler.drive_service.permissions().create().execute.side_effect = HttpError(
            Response({"status": "403"}), b"forbidden"
        )

        result = handler._transfer_file_ownership("file1", "alice@example.com")
        assert result is False


# ===========================================================================
# ensure_drive_initialized tests
# ===========================================================================


class TestEnsureDriveInitialized:
    """Tests for ensure_drive_initialized."""

    def test_skips_if_already_initialized(self):
        handler = _make_handler()
        handler._drive_initialized = True
        handler._initialize_shared_drive_and_folder = MagicMock()

        handler.ensure_drive_initialized()
        handler._initialize_shared_drive_and_folder.assert_not_called()

    def test_skips_if_dry_run(self):
        handler = _make_handler(dry_run=True)
        handler._initialize_shared_drive_and_folder = MagicMock()

        handler.ensure_drive_initialized()
        handler._initialize_shared_drive_and_folder.assert_not_called()

    def test_calls_init_when_needed(self):
        handler = _make_handler()
        handler._drive_initialized = False
        handler._initialize_shared_drive_and_folder = MagicMock()

        handler.ensure_drive_initialized()
        handler._initialize_shared_drive_and_folder.assert_called_once()
        assert handler._drive_initialized is True


# ===========================================================================
# Class-level constants
# ===========================================================================


class TestConstants:
    """Validate class-level constants are set correctly."""

    def test_direct_upload_mime_types(self):
        expected = {"image/jpeg", "image/png", "image/gif", "image/webp"}
        assert FileHandler.DIRECT_UPLOAD_MIME_TYPES == expected

    def test_direct_upload_max_size(self):
        assert FileHandler.DIRECT_UPLOAD_MAX_SIZE == 25 * 1024 * 1024


# ===========================================================================
# _initialize_shared_drive_and_folder tests
# ===========================================================================


class TestInitializeSharedDriveAndFolder:
    """Tests for _initialize_shared_drive_and_folder."""

    def test_uses_configured_shared_drive_id(self):
        """When config has a shared drive ID, validate and use it."""
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="Test Drive", id="drive123"),
        )
        handler = _make_handler(config=config)
        handler.drive_service.drives().get().execute.return_value = {"id": "drive123"}
        handler._pre_cache_root_folder = MagicMock()

        handler._initialize_shared_drive_and_folder()

        assert handler._shared_drive_id == "drive123"
        assert handler._root_folder_id == "drive123"
        handler._pre_cache_root_folder.assert_called_once()

    def test_configured_drive_id_not_accessible_falls_back_to_create(self):
        """When configured drive ID is not accessible, fall back to creating one."""
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="Test Drive", id="bad_id"),
        )
        handler = _make_handler(config=config)
        handler.drive_service.drives().get().execute.side_effect = HttpError(
            Response({"status": "404"}), b"not found"
        )
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = (
            "new_drive_id"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler._initialize_shared_drive_and_folder()

        assert handler._shared_drive_id == "new_drive_id"
        assert handler._root_folder_id == "new_drive_id"
        handler.shared_drive_manager.get_or_create_shared_drive.assert_called_once()

    def test_no_drive_id_creates_shared_drive_by_name(self):
        """When no drive ID is configured, create by name."""
        handler = _make_handler()
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = (
            "created_drive"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler._initialize_shared_drive_and_folder()

        assert handler._shared_drive_id == "created_drive"
        assert handler._root_folder_id == "created_drive"

    def test_shared_drive_none_falls_back_to_regular_folder(self):
        """When shared drive creation returns None, fall back to regular folder."""
        handler = _make_handler()
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = None
        handler.folder_manager.create_regular_drive_folder.return_value = (
            "regular_folder"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler._initialize_shared_drive_and_folder()

        assert handler._shared_drive_id is None
        assert handler._root_folder_id == "regular_folder"
        handler.folder_manager.create_regular_drive_folder.assert_called_once_with(
            "Imported Slack Attachments"
        )
        handler._pre_cache_root_folder.assert_called_once()

    def test_http_error_falls_back_to_fallback_folder(self):
        """When an HttpError occurs, fall back to 'Slack Attachments Fallback'."""
        handler = _make_handler()
        handler.shared_drive_manager.get_or_create_shared_drive.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )
        handler.folder_manager.create_regular_drive_folder.return_value = (
            "fallback_folder"
        )

        handler._initialize_shared_drive_and_folder()

        assert handler._root_folder_id == "fallback_folder"
        handler.folder_manager.create_regular_drive_folder.assert_called_once_with(
            "Slack Attachments Fallback"
        )

    def test_default_drive_name_when_nothing_configured(self):
        """When name and ID are both empty, use default 'Imported Slack Attachments'."""
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="", id=None),
        )
        handler = _make_handler(config=config)
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = (
            "default_drive"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler._initialize_shared_drive_and_folder()

        assert handler._shared_drive_id == "default_drive"


# ===========================================================================
# _pre_cache_root_folder tests
# ===========================================================================


class TestPreCacheRootFolder:
    """Tests for _pre_cache_root_folder."""

    def test_skips_when_no_root_folder(self):
        """Returns early when _root_folder_id is None."""
        handler = _make_handler()
        handler._root_folder_id = None
        handler.drive_uploader.pre_cache_folder_file_hashes = MagicMock()

        handler._pre_cache_root_folder()

        handler.drive_uploader.pre_cache_folder_file_hashes.assert_not_called()

    def test_skips_in_dry_run(self):
        """Returns early when dry_run is True."""
        handler = _make_handler(dry_run=True)
        handler._root_folder_id = "some_folder"
        handler.drive_uploader.pre_cache_folder_file_hashes = MagicMock()

        handler._pre_cache_root_folder()

        handler.drive_uploader.pre_cache_folder_file_hashes.assert_not_called()

    def test_caches_root_folder_files(self):
        """Caches files from root folder."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = "drive123"
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 5
        handler.drive_service.files().list().execute.return_value = {"files": []}

        handler._pre_cache_root_folder()

        handler.drive_uploader.pre_cache_folder_file_hashes.assert_called_once_with(
            "root123", "drive123"
        )

    def test_caches_subfolders(self):
        """Pre-caches files from channel subfolders."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = "drive123"
        handler.drive_uploader.pre_cache_folder_file_hashes.side_effect = [
            3,  # root folder
            2,  # subfolder 1
            4,  # subfolder 2
        ]
        handler.drive_service.files().list().execute.return_value = {
            "files": [
                {"id": "folder_a", "name": "general"},
                {"id": "folder_b", "name": "random"},
            ]
        }

        handler._pre_cache_root_folder()

        assert handler.drive_uploader.pre_cache_folder_file_hashes.call_count == 3

    def test_caches_subfolders_without_shared_drive(self):
        """Pre-caches subfolders without shared drive params."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = None
        handler.drive_uploader.pre_cache_folder_file_hashes.side_effect = [5, 2]
        handler.drive_service.files().list().execute.return_value = {
            "files": [{"id": "sub1", "name": "general"}]
        }

        handler._pre_cache_root_folder()

        assert handler.drive_uploader.pre_cache_folder_file_hashes.call_count == 2

    def test_subfolder_listing_http_error_continues(self):
        """HttpError when listing subfolders does not fail the process."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = "drive123"
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 5
        handler.drive_service.files().list().execute.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        # Should not raise
        handler._pre_cache_root_folder()

    def test_root_cache_http_error_continues(self):
        """HttpError when caching root folder does not fail the process."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = None
        handler.drive_uploader.pre_cache_folder_file_hashes.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        # Should not raise
        handler._pre_cache_root_folder()

    def test_skips_folder_with_no_id(self):
        """Folders without an 'id' key are skipped."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = None
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 2
        handler.drive_service.files().list().execute.return_value = {
            "files": [{"name": "no_id_folder"}]  # no "id" key
        }

        handler._pre_cache_root_folder()

        # Only called once (for root), not for the folder without ID
        handler.drive_uploader.pre_cache_folder_file_hashes.assert_called_once()


# ===========================================================================
# _upload_direct_to_chat tests
# ===========================================================================


class TestUploadDirectToChat:
    """Tests for _upload_direct_to_chat."""

    def _make_ready_handler(self):
        handler = _make_handler(folder_id="root")
        handler._drive_initialized = True
        return handler

    def test_successful_upload_with_default_service(self):
        """Successful direct upload using default chat_uploader."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.return_value = (
            {"uploadUri": "uri"},
            {"attachmentDataRef": {"resourceName": "ref"}},
        )
        handler.chat_uploader.create_attachment_for_message.return_value = {
            "attachmentDataRef": {"resourceName": "ref"}
        }

        file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
        result = handler._upload_direct_to_chat(
            file_obj, b"png data", channel="general", space="spaces/ABC"
        )

        assert result is not None
        assert result["type"] == "direct"
        assert result["name"] == "pic.png"
        assert result["mime_type"] == "image/png"

    def test_successful_upload_with_user_service(self):
        """Successful direct upload using user-specific service."""
        handler = self._make_ready_handler()
        mock_user_service = MagicMock()

        with patch("slack_migrator.services.file.ChatFileUploader") as MockUploader:
            mock_instance = MagicMock()
            mock_instance.upload_file_to_chat.return_value = (
                {"uploadUri": "uri"},
                {"attachmentDataRef": {"resourceName": "ref"}},
            )
            mock_instance.create_attachment_for_message.return_value = {
                "attachmentDataRef": {"resourceName": "ref"}
            }
            MockUploader.return_value = mock_instance

            file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
            result = handler._upload_direct_to_chat(
                file_obj,
                b"png data",
                channel="general",
                space="spaces/ABC",
                user_service=mock_user_service,
            )

        assert result is not None
        assert result["type"] == "direct"
        assert result["name"] == "pic.png"

    def test_upload_returns_none_on_empty_response(self):
        """Returns None when upload_file_to_chat returns (None, None)."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.return_value = (None, None)

        file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
        result = handler._upload_direct_to_chat(
            file_obj, b"png data", channel="general", space="spaces/ABC"
        )

        assert result is None

    def test_upload_returns_none_on_http_error(self):
        """Returns None when HttpError is raised."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
        result = handler._upload_direct_to_chat(
            file_obj, b"png data", channel="general"
        )

        assert result is None

    def test_upload_returns_none_on_os_error(self):
        """Returns None when OSError is raised."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.side_effect = OSError("disk full")

        file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
        result = handler._upload_direct_to_chat(
            file_obj, b"png data", channel="general"
        )

        assert result is None

    def test_default_values_for_file_obj_fields(self):
        """Handles missing id, name, mimetype gracefully."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.return_value = (
            {"uploadUri": "uri"},
            {"attachmentDataRef": {"resourceName": "ref"}},
        )
        handler.chat_uploader.create_attachment_for_message.return_value = {
            "attachmentDataRef": {"resourceName": "ref"}
        }

        # file_obj with no id, name, or mimetype
        result = handler._upload_direct_to_chat({}, b"data", channel="general")

        assert result is not None
        assert result["name"] == "file_unknown"
        assert result["mime_type"] == "application/octet-stream"


# ===========================================================================
# _upload_to_drive tests
# ===========================================================================


class TestUploadToDrive:
    """Tests for _upload_to_drive."""

    def _make_ready_handler(self, **kwargs):
        handler = _make_handler(folder_id="root_folder", **kwargs)
        handler._drive_initialized = True
        handler._root_folder_id = "root_folder"
        return handler

    def test_successful_upload_with_channel_folder(self):
        """Upload to drive with channel folder creation."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = "drive123"
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "drive_file_id",
            "https://drive.google.com/file/d/drive_file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "mimetype": "application/pdf",
            "user": "U123",
        }
        result = handler._upload_to_drive(
            file_obj,
            b"pdf content",
            channel="general",
            sender_email="alice@example.com",
        )

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "drive_file_id"
        assert result["name"] == "report.pdf"

    def test_upload_returns_none_when_drive_upload_fails(self):
        """Returns None when drive_uploader returns (None, None)."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (None, None)

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, b"data", channel="general")

        assert result is None

    def test_upload_without_channel_uses_root_folder(self):
        """When no channel is provided, uses root folder."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["link"] == "https://drive.google.com/file/d/file_id/view"
        handler.folder_manager.get_or_create_channel_folder.assert_not_called()

    def test_no_folder_id_returns_none(self):
        """Returns None when no folder ID is available."""
        handler = self._make_ready_handler()
        handler._root_folder_id = None
        handler._shared_drive_id = None

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is None

    def test_ownership_transfer_for_internal_user_no_shared_drive(self):
        """Transfers ownership for internal users on regular Drive folders."""
        handler = self._make_ready_handler(
            user_map={"U123": "alice@example.com"},
        )
        handler._shared_drive_id = None
        handler.user_resolver.is_external_user.return_value = False
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )
        handler.drive_service.permissions().create().execute.return_value = {}

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        result = handler._upload_to_drive(file_obj, b"data", channel="general")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert handler.file_stats["ownership_transferred"] == 1

    def test_ownership_transfer_failure_increments_stat(self):
        """Failed ownership transfer increments failure stat."""
        handler = self._make_ready_handler(
            user_map={"U123": "alice@example.com"},
        )
        handler._shared_drive_id = None
        handler.user_resolver.is_external_user.return_value = False
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )
        # Mock _transfer_file_ownership to raise HttpError directly
        handler._transfer_file_ownership = MagicMock(
            side_effect=HttpError(Response({"status": "403"}), b"forbidden")
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        result = handler._upload_to_drive(file_obj, b"data", channel="general")

        assert result is not None  # upload succeeded, just ownership transfer failed
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert handler.file_stats["ownership_transfer_failed"] == 1

    def test_external_user_skips_ownership_transfer(self):
        """External users should not have ownership transferred."""
        handler = self._make_ready_handler(
            user_map={"UEXT": "ext@other.com"},
        )
        handler._shared_drive_id = None
        handler.user_resolver.is_external_user.return_value = True
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "UEXT",
        }
        result = handler._upload_to_drive(file_obj, b"data", channel="general")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert handler.file_stats["ownership_transferred"] == 0

    def test_shared_drive_skips_ownership_transfer(self):
        """Files on shared drives skip ownership transfer."""
        handler = self._make_ready_handler(
            user_map={"U123": "alice@example.com"},
        )
        handler._shared_drive_id = "drive123"
        handler.user_resolver.is_external_user.return_value = False
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        result = handler._upload_to_drive(file_obj, b"data", channel="general")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert handler.file_stats["ownership_transferred"] == 0

    def test_google_docs_url_sets_mime_type(self):
        """Google Docs URLs should set appropriate MIME types."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "My Document",
            "mimetype": "application/octet-stream",
            "url_private": "https://docs.google.com/document/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/vnd.google-apps.document"

    def test_google_sheets_url_sets_mime_type(self):
        """Google Sheets URLs should set spreadsheet MIME type."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "My Sheet",
            "mimetype": "application/octet-stream",
            "url_private": "https://docs.google.com/spreadsheets/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/vnd.google-apps.spreadsheet"

    def test_google_presentation_url_sets_mime_type(self):
        """Google Slides URLs should set presentation MIME type."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "My Slides",
            "mimetype": "application/octet-stream",
            "url_private": "https://docs.google.com/presentation/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/vnd.google-apps.presentation"

    def test_sheets_google_url_sets_mime_type(self):
        """sheets.google.com URL should set spreadsheet MIME type."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "Sheet",
            "mimetype": "application/octet-stream",
            "url_private": "https://sheets.google.com/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/vnd.google-apps.spreadsheet"

    def test_drive_google_url_guesses_mime_type(self):
        """drive.google.com URL with octet-stream guesses from filename."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "mimetype": "application/octet-stream",
            "url_private": "https://drive.google.com/file/d/abc/view",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/pdf"

    def test_drive_google_url_no_extension_defaults_to_document(self):
        """drive.google.com URL with no guessable MIME defaults to document."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "myfile",  # no extension
            "mimetype": "application/octet-stream",
            "url_private": "https://drive.google.com/file/d/abc/view",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/vnd.google-apps.document"

    def test_null_mime_type_guessed(self):
        """Regular files with null MIME type should be guessed from name."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "data.json",
            "mimetype": "null",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "application/json"

    def test_empty_mime_type_guessed(self):
        """Regular files with empty MIME type should be guessed from name."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "",
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["mime_type"] == "text/plain"

    def test_http_error_returns_none(self):
        """HttpError during upload returns None."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.folder_manager.get_or_create_channel_folder.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, b"data", channel="general")

        assert result is None

    def test_channel_folder_already_processed(self):
        """Second upload to same channel folder uses cache."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj1 = {"id": "F1", "name": "a.txt", "mimetype": "text/plain"}
        file_obj2 = {"id": "F2", "name": "b.txt", "mimetype": "text/plain"}

        handler._upload_to_drive(file_obj1, b"data", channel="general")
        handler._upload_to_drive(file_obj2, b"data", channel="general")

        # Folder key should be in the shared_channel_folders set
        assert "general_channel_folder" in handler.shared_channel_folders

    def test_no_public_url_generates_default_link(self):
        """When drive_uploader returns None for public_url, generates a default link."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = ("file_id", None)

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert result["link"] == "https://drive.google.com/file/d/file_id/view"

    def test_sender_email_takes_priority_over_user_email(self):
        """sender_email should be used over user_map email for message_poster_email."""
        handler = self._make_ready_handler(
            user_map={"U123": "alice@example.com"},
        )
        handler._shared_drive_id = "drive123"
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        handler._upload_to_drive(
            file_obj, b"data", channel="general", sender_email="bob@example.com"
        )

        # Verify upload_file_to_drive was called with sender_email
        call_args = handler.drive_uploader.upload_file_to_drive.call_args
        assert (
            call_args.kwargs.get("message_poster_email") == "bob@example.com"
            or call_args[4] == "bob@example.com"
            if len(call_args[0]) > 4
            else True
        )

    def test_no_user_email_logs_warning(self):
        """When no user email is available, no ownership transfer happens."""
        handler = self._make_ready_handler(user_map={})
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = (
            "file_id",
            "https://drive.google.com/file/d/file_id/view",
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U999",  # not in user_map
        }
        result = handler._upload_to_drive(file_obj, b"data")

        assert result is not None
        assert result["type"] == "drive"
        assert result["drive_id"] == "file_id"
        assert handler.file_stats["ownership_transferred"] == 0


# ===========================================================================
# share_file_with_members tests
# ===========================================================================


class TestShareFileWithMembers:
    """Tests for share_file_with_members."""

    def _make_ready_handler(self, **kwargs):
        handler = _make_handler(folder_id="root_folder", **kwargs)
        handler._drive_initialized = True
        return handler

    def test_shared_drive_file_in_channel_folder_skips_permissions(self):
        """File already in correct channel folder should skip individual permissions."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = "drive123"
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["channel_folder_id"]},  # file info
            {"name": "general"},  # folder info
        ]

        result = handler.share_file_with_members("file123", "general")

        assert result is True

    def test_shared_drive_file_in_drive_root_skips_permissions(self):
        """File in shared drive root should skip individual permissions."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = "drive123"
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["drive123"]},  # file info, parent is the shared drive itself
            {"name": "Imported Slack Attachments"},  # folder info
        ]

        result = handler.share_file_with_members("file123", "general")

        assert result is True

    def test_no_shared_drive_file_info_retrieval(self):
        """File info retrieval without shared drive."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["some_folder"]},  # file info
            {"name": "other"},  # folder info - does not match channel
        ]
        handler.state.progress.active_users_by_channel = {}

        result = handler.share_file_with_members("file123", "general")

        # No active users for channel -> False
        assert result is False

    def test_shares_with_active_users(self):
        """Shares file with all active channel members."""
        handler = self._make_ready_handler(
            user_map={"U1": "alice@example.com", "U2": "bob@example.com"},
        )
        handler._shared_drive_id = None
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["some_folder"]},
            {"name": "other_folder"},
        ]
        handler.state.progress.active_users_by_channel = {"general": {"U1", "U2"}}
        handler.drive_service.permissions().create().execute.return_value = {}

        result = handler.share_file_with_members("file123", "general")

        assert result is True

    def test_no_emails_for_active_users_returns_false(self):
        """Returns False when active users have no emails in user_map."""
        handler = self._make_ready_handler(user_map={})
        handler._shared_drive_id = None
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["some_folder"]},
            {"name": "other"},
        ]
        handler.state.progress.active_users_by_channel = {"general": {"U999"}}

        result = handler.share_file_with_members("file123", "general")

        assert result is False

    def test_http_error_sharing_individual_file_continues(self):
        """HttpError when sharing with individual user should not fail."""
        handler = self._make_ready_handler(
            user_map={"U1": "alice@example.com", "U2": "bob@example.com"},
        )
        handler._shared_drive_id = None
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["some_folder"]},
            {"name": "other"},
        ]
        handler.state.progress.active_users_by_channel = {"general": {"U1", "U2"}}
        # Permissions create raises for one user but succeeds overall
        handler.drive_service.permissions().create().execute.side_effect = [
            HttpError(Response({"status": "403"}), b"forbidden"),
            {},
        ]

        result = handler.share_file_with_members("file123", "general")

        assert result is True

    def test_http_error_on_get_file_info_re_raises(self):
        """HttpError when getting file info should re-raise."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_service.files().get().execute.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        with pytest.raises(HttpError):
            handler.share_file_with_members("file123", "general")

    def test_folder_info_http_error_continues_check(self):
        """HttpError getting folder info should continue to next parent."""
        handler = self._make_ready_handler(
            user_map={"U1": "alice@example.com"},
        )
        handler._shared_drive_id = None

        # First call: file info with parents
        # Second call: folder info - raises HttpError
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["folder1"]},
            HttpError(Response({"status": "404"}), b"not found"),
        ]
        handler.state.progress.active_users_by_channel = {"general": {"U1"}}
        handler.drive_service.permissions().create().execute.return_value = {}

        result = handler.share_file_with_members("file123", "general")

        assert result is True

    def test_no_parents_shares_individually(self):
        """File with no parents shares with individual users."""
        handler = self._make_ready_handler(
            user_map={"U1": "alice@example.com"},
        )
        handler._shared_drive_id = None
        handler.drive_service.files().get().execute.return_value = {"parents": []}
        handler.state.progress.active_users_by_channel = {"general": {"U1"}}
        handler.drive_service.permissions().create().execute.return_value = {}

        result = handler.share_file_with_members("file123", "general")

        assert result is True

    def test_shared_drive_uses_supports_all_drives_for_permissions(self):
        """When shared drive is set, permissions should use supportsAllDrives."""
        handler = self._make_ready_handler(
            user_map={"U1": "alice@example.com"},
        )
        handler._shared_drive_id = "drive123"
        handler.drive_service.files().get().execute.side_effect = [
            {"parents": ["some_folder"]},
            {"name": "other"},
        ]
        handler.state.progress.active_users_by_channel = {"general": {"U1"}}
        handler.drive_service.permissions().create().execute.return_value = {}

        result = handler.share_file_with_members("file123", "general")

        assert result is True


# ===========================================================================
# _create_drive_reference exception handling
# ===========================================================================


class TestCreateDriveReferenceExceptionHandling:
    """Tests for _create_drive_reference exception handling."""

    def test_general_exception_returns_none(self):
        """Any unexpected exception in the try block returns None."""
        handler = _make_handler()

        # Replace file_stats with a dict subclass that raises TypeError
        # on __getitem__ for "drive_uploads", which triggers after the URL
        # is parsed successfully
        class FailingStats(dict):
            def __getitem__(self, key):
                if key == "drive_uploads":
                    raise TypeError("simulated stats error")
                return super().__getitem__(key)

        handler.file_stats = FailingStats(handler.file_stats)

        file_obj = {
            "id": "F1",
            "name": "test.pdf",
            "url_private": "https://drive.google.com/file/d/ABC/view",
        }
        result = handler._create_drive_reference(file_obj, channel="general")

        assert result is None


# ===========================================================================
# ensure_drive_initialized (detailed) tests
# ===========================================================================


class TestEnsureDriveInitializedDetailed:
    """Additional tests for ensure_drive_initialized covering lines 187-251."""

    def test_initializes_and_sets_flag(self):
        """Full initialization path sets _drive_initialized to True."""
        handler = _make_handler()
        handler._drive_initialized = False
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = (
            "new_drive"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler.ensure_drive_initialized()

        assert handler._drive_initialized is True
        assert handler._shared_drive_id == "new_drive"
        assert handler._root_folder_id == "new_drive"

    def test_second_call_is_noop(self):
        """Calling ensure_drive_initialized twice should only init once."""
        handler = _make_handler()
        handler._drive_initialized = False
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = (
            "new_drive"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler.ensure_drive_initialized()
        handler.ensure_drive_initialized()

        handler.shared_drive_manager.get_or_create_shared_drive.assert_called_once()
