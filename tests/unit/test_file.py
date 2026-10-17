"""Unit tests for the file handling module."""

import hashlib
import os
import tempfile
from unittest.mock import MagicMock, patch

import pytest
import requests
from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_chat_migrator.core.config import MigrationConfig, SharedDriveConfig
from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.services.drive import DriveUpload
from slack_chat_migrator.services.files.file import FileHandler
from slack_chat_migrator.services.files.file_download import (
    DownloadResult,
    DownloadStatus,
)
from slack_chat_migrator.services.files.temp_files import safe_temp_suffix
from slack_chat_migrator.types import UploadResult

#: Path handed to mocked uploaders, which never open it.
FAKE_PATH = "/tmp/fake_upload.bin"

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
    user_resolver = overrides.pop("user_resolver", None)
    if user_resolver is None:
        user_resolver = MagicMock()
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


def _ok_download(content=b"data"):
    """Write *content* to a real temp file and wrap it as a successful download."""
    fd, path = tempfile.mkstemp(prefix="test_dl_")
    with os.fdopen(fd, "wb") as fh:
        fh.write(content)
    return DownloadResult(
        DownloadStatus.OK,
        path=path,
        md5=hashlib.md5(content).hexdigest(),  # noqa: S324 — fingerprint only
        size=len(content),
    )


def _failed_download(reason="network_error:ConnectionError"):
    return DownloadResult(DownloadStatus.FAILED, reason=reason)


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
        patch("slack_chat_migrator.services.files.file.SharedDriveManager"),
        patch("slack_chat_migrator.services.files.file.FolderManager"),
        patch("slack_chat_migrator.services.files.file.DriveFileUploader"),
        patch("slack_chat_migrator.services.files.file.ChatFileUploader"),
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
        assert handler.dry_run is False
        assert handler._drive_initialized is False

    def test_folder_id_set_when_provided(self):
        handler = _make_handler(folder_id="folder123")
        assert handler._root_folder_id == "folder123"

    def test_folder_id_none_when_not_provided(self):
        handler = _make_handler(folder_id=None)
        assert handler._root_folder_id is None

    def test_dry_run_defers_initialization(self):
        """Dry run no longer special-cases init — DI stubs handle API calls."""
        handler = _make_handler(dry_run=True)
        assert handler.dry_run is True
        assert handler._drive_initialized is False
        assert handler._root_folder_id is None

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
        cached = UploadResult(
            upload_type="drive", url="https://example.com", name="a.txt"
        )
        handler.processed_files[("F123", "", "")] = cached

        result = handler.upload_attachment({"id": "F123", "name": "a.txt"})
        assert result.cached is True
        assert result.url == cached.url
        assert cached.cached is False  # the stored result isn't mutated
        # total_files should still be incremented
        assert handler.file_stats["total_files"] == 1

    def test_download_failure_returns_error_result(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(return_value=_failed_download())

        result = handler.upload_attachment(
            {"id": "F1", "name": "bad.txt"},
            channel="general",
        )
        assert isinstance(result, UploadResult)
        assert not result.success
        assert not result.skipped
        assert result.error.startswith("Download failed")
        assert handler.file_stats["failed_uploads"] == 1

    def test_google_docs_skip(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(
            return_value=DownloadResult(
                DownloadStatus.GOOGLE_DOCS_LINK,
                url="https://docs.google.com/document/d/abc",
            )
        )

        file_obj = {
            "id": "F1",
            "name": "My Doc",
            "url_private": "https://docs.google.com/document/d/abc",
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert result.skipped
        assert result.skip_reason == "google_docs_link"

    def test_google_drive_file_reference(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(
            return_value=DownloadResult(DownloadStatus.GOOGLE_DRIVE_FILE)
        )
        handler._create_drive_reference = MagicMock(
            return_value=UploadResult(
                upload_type="drive",
                url="https://drive.google.com/file/d/abc/view",
                drive_id="abc",
                name="file.pdf",
                metadata={"is_reference": True},
            )
        )

        file_obj = {
            "id": "F1",
            "name": "file.pdf",
            "url_private": "https://drive.google.com/file/d/abc/view",
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        assert result.metadata.get("is_reference") is True
        handler._create_drive_reference.assert_called_once()

    def test_small_image_tries_direct_upload(self):
        handler = self._make_ready_handler()
        content = b"\x89PNG" + b"\x00" * 1000
        handler._download_file = MagicMock(return_value=_ok_download(content))
        handler.chat_uploader.is_suitable_for_direct_upload = MagicMock(
            return_value=True
        )
        handler._upload_direct_to_chat = MagicMock(
            return_value=UploadResult(
                upload_type="direct",
                attachment_ref={"attachmentDataRef": "xyz"},
                name="image.png",
            )
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
        assert isinstance(result, UploadResult)
        assert result.upload_type == "direct"
        assert handler.file_stats["direct_uploads"] == 1
        handler._upload_direct_to_chat.assert_called_once()

    def test_direct_upload_fallback_to_drive(self):
        handler = self._make_ready_handler()
        content = b"\x89PNG" + b"\x00" * 1000
        handler._download_file = MagicMock(return_value=_ok_download(content))
        handler.chat_uploader.is_suitable_for_direct_upload = MagicMock(
            return_value=True
        )
        handler._upload_direct_to_chat = MagicMock(return_value=None)
        handler._upload_to_drive = MagicMock(
            return_value=UploadResult(
                upload_type="drive",
                drive_id="d1",
                url="https://drive.google.com/file/d/d1/view",
                name="image.png",
            )
        )

        file_obj = {
            "id": "F1",
            "name": "image.png",
            "mimetype": "image/png",
            "size": 1004,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        assert handler.file_stats["drive_uploads"] == 1

    def test_non_image_goes_to_drive(self):
        handler = self._make_ready_handler()
        content = b"file data"
        handler._download_file = MagicMock(return_value=_ok_download(content))
        handler._upload_to_drive = MagicMock(
            return_value=UploadResult(
                upload_type="drive",
                drive_id="d1",
                url="https://drive.google.com/file/d/d1/view",
                name="report.pdf",
            )
        )

        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "mimetype": "application/pdf",
            "size": 5000,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        handler._upload_to_drive.assert_called_once()

    def test_large_image_goes_to_drive(self):
        """Images larger than 25MB should bypass direct upload."""
        handler = self._make_ready_handler()
        download = _ok_download(b"\x89PNG")
        download.size = 26 * 1024 * 1024  # as measured while streaming
        handler._download_file = MagicMock(return_value=download)
        handler._upload_to_drive = MagicMock(
            return_value=UploadResult(
                upload_type="drive",
                drive_id="d1",
                url="https://drive.google.com/file/d/d1/view",
                name="huge.png",
            )
        )

        file_obj = {
            "id": "F1",
            "name": "huge.png",
            "mimetype": "image/png",
            "size": 26 * 1024 * 1024,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        assert result.drive_id == "d1"
        assert result.name == "huge.png"

    def test_drive_upload_failure_returns_error_result(self):
        handler = self._make_ready_handler()
        content = b"some data"
        handler._download_file = MagicMock(return_value=_ok_download(content))
        handler._upload_to_drive = MagicMock(return_value=None)

        file_obj = {
            "id": "F1",
            "name": "file.bin",
            "mimetype": "application/octet-stream",
            "size": 100,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert not result.success
        assert result.error is not None
        assert handler.file_stats["failed_uploads"] == 1

    def test_exception_in_upload_attachment_increments_failed(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(
            side_effect=requests.RequestException("boom")
        )

        file_obj = {"id": "F1", "name": "crash.bin"}
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert not result.success
        assert result.error is not None
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
        handler._download_file = MagicMock(return_value=_failed_download())

        file_obj = {"id": "F1", "name": "ext.txt", "user": "UEXT"}
        handler.upload_attachment(file_obj, channel="general")
        assert handler.file_stats["external_user_files"] == 1

    def test_null_mimetype_gets_guessed(self):
        handler = self._make_ready_handler()
        content = b"data"
        handler._download_file = MagicMock(return_value=_ok_download(content))
        handler._upload_to_drive = MagicMock(
            return_value=UploadResult(
                upload_type="drive",
                drive_id="d1",
                url="https://drive.google.com/file/d/d1/view",
                name="file.json",
            )
        )

        file_obj = {
            "id": "F1",
            "name": "file.json",
            "mimetype": "null",
            "size": 4,
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        assert result.drive_id == "d1"
        assert result.name == "file.json"

    def test_channel_file_count_tracking(self):
        handler = self._make_ready_handler()
        handler._download_file = MagicMock(return_value=_failed_download())

        handler.upload_attachment({"id": "F1", "name": "a.txt"}, channel="general")
        handler.upload_attachment({"id": "F2", "name": "b.txt"}, channel="general")
        handler.upload_attachment({"id": "F3", "name": "c.txt"}, channel="random")

        assert handler.file_stats["files_by_channel"]["general"] == 2
        assert handler.file_stats["files_by_channel"]["random"] == 1


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
        assert result.upload_type == "drive"
        assert result.drive_id == "ABC123"
        assert result.metadata.get("is_reference") is True
        assert result.name == "report.pdf"

    def test_open_id_url(self):
        handler = _make_handler()
        file_obj = {
            "id": "F2",
            "name": "file.docx",
            "url_private": "https://drive.google.com/open?id=XYZ789&other=1",
        }
        result = handler._create_drive_reference(file_obj, channel="general")
        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "XYZ789"
        assert result.metadata.get("is_reference") is True

    def test_unrecognized_url_returns_none(self):
        handler = _make_handler()
        file_obj = {
            "id": "F3",
            "name": "mystery.bin",
            "url_private": "https://example.com/something",
        }
        result = handler._create_drive_reference(file_obj, channel="general")
        assert result is None

    def test_upload_attachment_counts_and_caches_reference(self):
        handler = _make_handler(folder_id="root")
        file_obj = {
            "id": "F1",
            "name": "a.txt",
            "url_private": "https://drive.google.com/file/d/id1/view",
        }
        result = handler.upload_attachment(file_obj, channel="general")
        assert result.drive_id == "id1"
        assert handler.file_stats["drive_uploads"] == 1
        assert ("F1", "general", "") in handler.processed_files

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
        handler.drive_service.create_permission.return_value = {}

        result = handler._transfer_file_ownership("file1", "alice@example.com")
        assert result is True

    def test_failed_transfer(self):
        handler = _make_handler()
        handler.drive_service.create_permission.side_effect = HttpError(
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

    def test_dry_run_delegates_to_init(self):
        """Dry run no longer short-circuits — DI stubs handle API calls."""
        handler = _make_handler(dry_run=True)
        handler._initialize_shared_drive_and_folder = MagicMock()

        handler.ensure_drive_initialized()
        handler._initialize_shared_drive_and_folder.assert_called_once()

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
        from slack_chat_migrator.constants import DIRECT_UPLOAD_MAX_BYTES

        assert DIRECT_UPLOAD_MAX_BYTES == 25 * 1024 * 1024


# ===========================================================================
# _initialize_shared_drive_and_folder tests
# ===========================================================================


class TestInitializeSharedDriveAndFolder:
    """Tests for _initialize_shared_drive_and_folder."""

    def test_uses_configured_shared_drive_id(self):
        """When SharedDriveManager returns a drive ID, use it as root folder."""
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="Test Drive", id="drive123"),
        )
        handler = _make_handler(config=config)
        handler.shared_drive_manager.get_or_create_shared_drive.return_value = (
            "drive123"
        )
        handler._pre_cache_root_folder = MagicMock()

        handler._initialize_shared_drive_and_folder()

        assert handler._shared_drive_id == "drive123"
        assert handler._root_folder_id == "drive123"
        handler._pre_cache_root_folder.assert_called_once()
        handler.shared_drive_manager.get_or_create_shared_drive.assert_called_once()

    def test_shared_drive_manager_returns_fallback_drive(self):
        """When SharedDriveManager falls back to a new drive, use its ID."""
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="Test Drive", id="bad_id"),
        )
        handler = _make_handler(config=config)
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

    def test_dry_run_delegates_to_pre_cache(self):
        """Dry run no longer short-circuits — DI stubs handle pre-caching."""
        handler = _make_handler(dry_run=True)
        handler._root_folder_id = "some_folder"
        handler.drive_uploader.pre_cache_folder_file_hashes = MagicMock(return_value=0)
        handler.drive_service.list_files.return_value = {"files": []}

        handler._pre_cache_root_folder()

        handler.drive_uploader.pre_cache_folder_file_hashes.assert_called_once()

    def test_caches_root_folder_files(self):
        """Caches files from root folder."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = "drive123"
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 5
        handler.drive_service.list_files.return_value = {"files": []}

        handler._pre_cache_root_folder()

        handler.drive_uploader.pre_cache_folder_file_hashes.assert_called_once_with(
            "root123", "drive123"
        )

    def test_seeds_folder_cache_from_every_subfolder(self):
        """Channel folders are learned up front; their files are cached lazily."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = "drive123"
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 3
        handler.folder_manager.list_child_folders.return_value = [
            {"id": "folder_a", "name": "general"},
            {"id": "folder_b", "name": "random"},
        ]

        handler._pre_cache_root_folder()

        handler.folder_manager.list_child_folders.assert_called_once_with(
            "root123", "drive123"
        )
        handler.folder_manager.remember_folder.assert_any_call(
            "root123", "general", "folder_a"
        )
        handler.folder_manager.remember_folder.assert_any_call(
            "root123", "random", "folder_b"
        )
        # Only the root folder's files are listed at start-up.
        handler.drive_uploader.pre_cache_folder_file_hashes.assert_called_once_with(
            "root123", "drive123"
        )

    def test_subfolder_listing_http_error_continues(self):
        """HttpError when listing subfolders does not fail the process."""
        handler = _make_handler()
        handler._root_folder_id = "root123"
        handler._shared_drive_id = "drive123"
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 5
        handler.folder_manager.list_child_folders.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        # Should not raise
        handler._pre_cache_root_folder()
        handler.folder_manager.remember_folder.assert_not_called()

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
            file_obj, FAKE_PATH, "image/png", channel="general", space="spaces/ABC"
        )

        assert result is not None
        assert isinstance(result, UploadResult)
        assert result.upload_type == "direct"
        assert result.name == "pic.png"
        assert result.mime_type == "image/png"

    def test_successful_upload_with_user_service(self):
        """Successful direct upload using user-specific service."""
        handler = self._make_ready_handler()
        mock_user_service = MagicMock()

        with patch(
            "slack_chat_migrator.services.files.file.ChatFileUploader"
        ) as MockUploader:
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
                FAKE_PATH,
                "image/png",
                channel="general",
                space="spaces/ABC",
                user_service=mock_user_service,
            )

        assert result is not None
        assert isinstance(result, UploadResult)
        assert result.upload_type == "direct"
        assert result.name == "pic.png"

    def test_upload_returns_none_on_empty_response(self):
        """Returns None when upload_file_to_chat returns (None, None)."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.return_value = (None, None)

        file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
        result = handler._upload_direct_to_chat(
            file_obj, FAKE_PATH, "image/png", channel="general", space="spaces/ABC"
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
            file_obj, FAKE_PATH, "image/png", channel="general"
        )

        assert result is None

    def test_upload_returns_none_on_os_error(self):
        """Returns None when OSError is raised."""
        handler = self._make_ready_handler()
        handler.chat_uploader.upload_file_to_chat.side_effect = OSError("disk full")

        file_obj = {"id": "F1", "name": "pic.png", "mimetype": "image/png"}
        result = handler._upload_direct_to_chat(
            file_obj, FAKE_PATH, "image/png", channel="general"
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
        result = handler._upload_direct_to_chat(
            {}, FAKE_PATH, "application/octet-stream", channel="general"
        )

        assert result is not None
        assert isinstance(result, UploadResult)
        assert result.name == "file_unknown"
        assert result.mime_type == "application/octet-stream"


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
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "drive_file_id", "https://drive.google.com/file/d/drive_file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "mimetype": "application/pdf",
            "user": "U123",
        }
        result = handler._upload_to_drive(
            file_obj,
            FAKE_PATH,
            channel="general",
            sender_email="alice@example.com",
        )

        assert result is not None
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        assert result.drive_id == "drive_file_id"
        assert result.name == "report.pdf"

    def test_upload_returns_none_when_drive_upload_fails(self):
        """Returns None when drive_uploader returns (None, None)."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = None

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, FAKE_PATH, channel="general")

        assert result is None

    def test_upload_without_channel_uses_root_folder(self):
        """When no channel is provided, uses root folder."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert isinstance(result, UploadResult)
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.url == "https://drive.google.com/file/d/file_id/view"
        handler.folder_manager.get_or_create_channel_folder.assert_not_called()

    def test_no_folder_id_returns_none(self):
        """Returns None when no folder ID is available."""
        handler = self._make_ready_handler()
        handler._root_folder_id = None
        handler._shared_drive_id = None

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

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
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )
        handler.drive_service.create_permission.return_value = {}

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH, channel="general")

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
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
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )
        # transfer_file_ownership() reports a 403 by returning False
        handler.drive_service.create_permission.side_effect = HttpError(
            Response({"status": "403"}), b"forbidden"
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH, channel="general")

        assert result is not None  # upload succeeded, just ownership transfer failed
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert handler.file_stats["ownership_transfer_failed"] == 1
        assert handler.file_stats["ownership_transferred"] == 0

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
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "UEXT",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH, channel="general")

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
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
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH, channel="general")

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert handler.file_stats["ownership_transferred"] == 0

    def test_google_docs_url_sets_mime_type(self):
        """Google Docs URLs should set appropriate MIME types."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "My Document",
            "mimetype": "application/octet-stream",
            "url_private": "https://docs.google.com/document/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/vnd.google-apps.document"

    def test_google_sheets_url_sets_mime_type(self):
        """Google Sheets URLs should set spreadsheet MIME type."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "My Sheet",
            "mimetype": "application/octet-stream",
            "url_private": "https://docs.google.com/spreadsheets/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/vnd.google-apps.spreadsheet"

    def test_google_presentation_url_sets_mime_type(self):
        """Google Slides URLs should set presentation MIME type."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "My Slides",
            "mimetype": "application/octet-stream",
            "url_private": "https://docs.google.com/presentation/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/vnd.google-apps.presentation"

    def test_sheets_google_url_sets_mime_type(self):
        """sheets.google.com URL should set spreadsheet MIME type."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "Sheet",
            "mimetype": "application/octet-stream",
            "url_private": "https://sheets.google.com/d/abc/edit",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/vnd.google-apps.spreadsheet"

    def test_drive_google_url_guesses_mime_type(self):
        """drive.google.com URL with octet-stream guesses from filename."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "report.pdf",
            "mimetype": "application/octet-stream",
            "url_private": "https://drive.google.com/file/d/abc/view",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/pdf"

    def test_drive_google_url_no_extension_defaults_to_document(self):
        """drive.google.com URL with no guessable MIME defaults to document."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "myfile",  # no extension
            "mimetype": "application/octet-stream",
            "url_private": "https://drive.google.com/file/d/abc/view",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/vnd.google-apps.document"

    def test_null_mime_type_guessed(self):
        """Regular files with null MIME type should be guessed from name."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "data.json",
            "mimetype": "null",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "application/json"

    def test_empty_mime_type_guessed(self):
        """Regular files with empty MIME type should be guessed from name."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "",
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.mime_type == "text/plain"

    def test_http_error_returns_none(self):
        """HttpError during upload returns None."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.folder_manager.get_or_create_channel_folder.side_effect = HttpError(
            Response({"status": "500"}), b"error"
        )

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, FAKE_PATH, channel="general")

        assert result is None

    def test_channel_folder_already_processed(self):
        """Second upload to same channel folder uses cache."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.pre_cache_folder_file_hashes.return_value = 0
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj1 = {"id": "F1", "name": "a.txt", "mimetype": "text/plain"}
        file_obj2 = {"id": "F2", "name": "b.txt", "mimetype": "text/plain"}

        handler._upload_to_drive(file_obj1, FAKE_PATH, channel="general")
        handler._upload_to_drive(file_obj2, FAKE_PATH, channel="general")

        assert handler.drive_uploader.upload_file_to_drive.call_count == 2

    def test_no_public_url_generates_default_link(self):
        """When drive_uploader returns None for public_url, generates a default link."""
        handler = self._make_ready_handler()
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id"
        )

        file_obj = {"id": "F1", "name": "file.txt", "mimetype": "text/plain"}
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert result.url == "https://drive.google.com/file/d/file_id/view"

    def test_writer_grant_goes_to_sender_not_file_owner(self):
        """Only the message sender gets writer; the file owner is not granted."""
        handler = self._make_ready_handler(
            user_map={"U123": "alice@example.com"},
        )
        handler._shared_drive_id = "drive123"
        handler.folder_manager.get_or_create_channel_folder.return_value = (
            "channel_folder"
        )
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U123",
        }
        handler._upload_to_drive(
            file_obj, FAKE_PATH, channel="general", sender_email="bob@example.com"
        )

        handler.drive_uploader.set_message_poster_permission.assert_called_once_with(
            "file_id", "bob@example.com", "drive123"
        )

    def test_no_user_email_logs_warning(self):
        """When no user email is available, no ownership transfer happens."""
        handler = self._make_ready_handler(user_map={})
        handler._shared_drive_id = None
        handler.drive_uploader.upload_file_to_drive.return_value = DriveUpload(
            "file_id", "https://drive.google.com/file/d/file_id/view"
        )

        file_obj = {
            "id": "F1",
            "name": "file.txt",
            "mimetype": "text/plain",
            "user": "U999",  # not in user_map
        }
        result = handler._upload_to_drive(file_obj, FAKE_PATH)

        assert result is not None
        assert result.upload_type == "drive"
        assert result.drive_id == "file_id"
        assert handler.file_stats["ownership_transferred"] == 0


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


# ---------------------------------------------------------------------------
# _safe_temp_suffix
# ---------------------------------------------------------------------------


class TestSafeTempSuffix:
    """Tests for safe_temp_suffix()."""

    def test_normal_filename(self):
        result = safe_temp_suffix("report.pdf")
        assert result == "_report.pdf"

    def test_path_separators_replaced(self):
        result = safe_temp_suffix("../../etc/passwd")
        assert "/" not in result
        assert "\\" not in result
        assert result.startswith("_")

    def test_null_bytes_replaced(self):
        result = safe_temp_suffix("file\x00.txt")
        assert "\x00" not in result
        assert result.startswith("_")

    def test_long_name_truncated(self):
        long_name = "a" * 200
        result = safe_temp_suffix(long_name)
        # underscore prefix + at most 64 chars from the name
        assert len(result) <= 65

    def test_preserves_safe_characters(self):
        result = safe_temp_suffix("my-file_v2.tar.gz")
        assert result == "_my-file_v2.tar.gz"

    def test_spaces_replaced(self):
        result = safe_temp_suffix("my file name.txt")
        assert " " not in result
        assert result.startswith("_")

    def test_empty_string(self):
        result = safe_temp_suffix("")
        assert result == "_"


# ===========================================================================
# Streaming pipeline: unavailable files, temp files, fallback reuse
# ===========================================================================


class TestUnavailableFiles:
    """Deleted, hidden and URL-less files are reported, not failed (I37)."""

    @pytest.mark.parametrize(
        ("file_obj", "reason"),
        [
            ({"id": "F3", "mode": "tombstone"}, "deleted"),
            ({"id": "F4", "mode": "hidden_by_limit"}, "hidden_by_limit"),
            ({"id": "F5", "name": "x.pdf", "url_private": None}, "no_url"),
        ],
    )
    def test_unavailable_is_skipped_not_failed(self, file_obj, reason):
        handler = _make_handler(folder_id="root")
        handler.temp_dir = MagicMock()

        result = handler.upload_attachment(dict(file_obj), "general", "spaces/S")

        assert result.skipped
        assert result.skip_reason == "unavailable"
        assert result.metadata["unavailable_reason"] == reason
        assert result.error is None
        assert handler.file_stats["unavailable_files"] == 1
        assert handler.file_stats["failed_uploads"] == 0
        handler.temp_dir.path.assert_not_called()  # nothing to download

    def test_external_file_keeps_its_link(self):
        handler = _make_handler(folder_id="root")
        file_obj = {
            "id": "F6",
            "name": "deck.pdf",
            "mode": "external",
            "url_private": "https://www.dropbox.com/s/abc/deck.pdf",
        }
        result = handler.upload_attachment(file_obj, "general")
        assert result.skip_reason == "unavailable"
        assert result.url == "https://www.dropbox.com/s/abc/deck.pdf"

    def test_unavailable_result_is_cached(self):
        handler = _make_handler(folder_id="root")
        handler._download_file = MagicMock(
            return_value=DownloadResult(DownloadStatus.UNAVAILABLE, reason="html_page")
        )
        handler.upload_attachment({"id": "F1", "name": "a.png"}, "general")
        second = handler.upload_attachment({"id": "F1", "name": "a.png"}, "general")

        assert second.cached is True
        handler._download_file.assert_called_once()
        assert handler.file_stats["unavailable_files"] == 1

    def test_failed_download_is_retried_next_time(self):
        handler = _make_handler(folder_id="root")
        handler._download_file = MagicMock(return_value=_failed_download())

        handler.upload_attachment({"id": "F1", "name": "a.png"}, "general")
        handler.upload_attachment({"id": "F1", "name": "a.png"}, "general")

        assert handler._download_file.call_count == 2
        assert handler.file_stats["failed_uploads"] == 2

    def test_success_rate_ignores_unavailable(self):
        handler = _make_handler()
        handler.file_stats["total_files"] = 4
        handler.file_stats["drive_uploads"] = 1
        handler.file_stats["unavailable_files"] = 3
        stats = handler.get_file_statistics()
        assert stats["success_rate"] == 100.0
        assert stats["unavailable_files"] == 3


class TestTempFileLifecycle:
    """Downloads are written once, reused for the fallback, then deleted."""

    def _handler(self):
        handler = _make_handler(folder_id="root")
        handler._drive_initialized = True
        return handler

    def test_temp_file_removed_after_drive_upload(self):
        handler = self._handler()
        download = _ok_download(b"pdf bytes")
        handler._download_file = MagicMock(return_value=download)
        seen = {}

        def fake_upload(file_obj, path, channel, sender_email, **kwargs):
            seen["exists"] = os.path.exists(path)
            return UploadResult(upload_type="drive", drive_id="d1", name="a.pdf")

        handler._upload_to_drive = MagicMock(side_effect=fake_upload)
        result = handler.upload_attachment(
            {"id": "F1", "name": "a.pdf", "mimetype": "application/pdf"}, "general"
        )

        assert result.success
        assert seen["exists"] is True
        assert not os.path.exists(download.path)

    def test_temp_file_removed_when_upload_raises(self):
        handler = self._handler()
        download = _ok_download(b"pdf bytes")
        handler._download_file = MagicMock(return_value=download)
        handler._upload_to_drive = MagicMock(side_effect=OSError("disk gone"))

        result = handler.upload_attachment({"id": "F1", "name": "a.pdf"}, "general")

        assert result.error == "disk gone"
        assert handler.file_stats["failed_uploads"] == 1
        assert not os.path.exists(download.path)

    def test_direct_failure_falls_back_to_drive_with_same_file(self):
        handler = self._handler()
        download = _ok_download(b"\x89PNG small")
        handler._download_file = MagicMock(return_value=download)
        handler.chat_uploader.is_suitable_for_direct_upload = MagicMock(
            return_value=True
        )
        handler._upload_direct_to_chat = MagicMock(return_value=None)
        handler._upload_to_drive = MagicMock(
            return_value=UploadResult(upload_type="drive", drive_id="d1")
        )

        handler.upload_attachment(
            {"id": "F1", "name": "a.png", "mimetype": "image/png"},
            "general",
            "spaces/S",
        )

        handler._download_file.assert_called_once()
        direct_path = handler._upload_direct_to_chat.call_args[0][1]
        drive_path = handler._upload_to_drive.call_args[0][1]
        assert direct_path == drive_path == download.path
        assert not os.path.exists(download.path)

    def test_download_uses_token_and_run_temp_dir(self, tmp_path):
        token = "tok"  # noqa: S105 — test value
        config = MigrationConfig(
            shared_drive=SharedDriveConfig(name="Test Drive"), slack_token=token
        )
        handler = _make_handler(folder_id="root", config=config)
        handler.state.context.output_dir = str(tmp_path)

        with patch(
            "slack_chat_migrator.services.files.file.download_file",
            return_value=_failed_download(),
        ) as mock_download:
            handler._download_file(
                {"id": "F1", "url_private": "https://files.slack.com/x"}
            )

        args, kwargs = mock_download.call_args
        assert kwargs["slack_token"] == token
        assert args[1].startswith(str(tmp_path / ".attachment_tmp"))
        handler.cleanup_temp_files()
        assert not (tmp_path / ".attachment_tmp").exists()

    def test_init_sweeps_stale_temp_dirs(self, tmp_path):
        with patch(
            "slack_chat_migrator.services.files.file.sweep_stale_temp_dirs"
        ) as sweep:
            deps = _make_deps()
            deps["state"].context.output_dir = str(tmp_path)
            FileHandler(MagicMock(), MagicMock(), None, **deps)
        sweep.assert_called_once_with(str(tmp_path))
