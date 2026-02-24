"""Unit tests for the ChatFileUploader class."""

import os
from unittest.mock import MagicMock, patch

import pytest

from slack_migrator.core.state import MigrationState
from slack_migrator.services.chat.chat_uploader import (
    ChatFileUploader,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_uploader(dry_run=False):
    """Create a ChatFileUploader with a mocked chat service."""
    chat_service = MagicMock()
    return ChatFileUploader(
        chat_service=chat_service,
        dry_run=dry_run,
    )


# ===========================================================================
# _get_current_channel tests
# ===========================================================================


class TestGetCurrentChannel:
    """Tests for ChatFileUploader._get_current_channel."""

    def test_returns_channel_when_migrator_is_set(self):
        uploader = _make_uploader()
        migrator = MagicMock()
        migrator.state = MigrationState()
        migrator.state.current_channel = "general"
        uploader.migrator = migrator

        assert uploader._get_current_channel() == "general"

    def test_returns_none_when_migrator_is_none(self):
        uploader = _make_uploader()
        uploader.migrator = None

        assert uploader._get_current_channel() is None

    def test_returns_none_when_migrator_has_no_current_channel(
        self,
    ):
        uploader = _make_uploader()
        migrator = MagicMock(spec=[])  # no attributes
        uploader.migrator = migrator

        assert uploader._get_current_channel() is None


# ===========================================================================
# is_suitable_for_direct_upload tests (can_upload_directly)
# ===========================================================================


class TestCanUploadDirectly:
    """Tests for ChatFileUploader.is_suitable_for_direct_upload."""

    def test_returns_true_for_supported_image_jpeg(self):
        uploader = _make_uploader()
        result = uploader.is_suitable_for_direct_upload("photo.jpeg", 1024)
        assert result is True

    def test_returns_true_for_supported_image_png(self):
        uploader = _make_uploader()
        result = uploader.is_suitable_for_direct_upload("icon.png", 2048)
        assert result is True

    def test_returns_true_for_supported_image_gif(self):
        uploader = _make_uploader()
        result = uploader.is_suitable_for_direct_upload("animation.gif", 5000)
        assert result is True

    def test_returns_true_for_supported_image_webp(self):
        uploader = _make_uploader()
        result = uploader.is_suitable_for_direct_upload("image.webp", 3000)
        assert result is True

    def test_returns_false_for_unsupported_mime_type(self):
        uploader = _make_uploader()
        # .xyz has no known MIME type
        result = uploader.is_suitable_for_direct_upload("file.xyz", 1024)
        assert result is False

    def test_returns_false_when_file_too_large(self):
        uploader = _make_uploader()
        over_limit = 26 * 1024 * 1024  # 26MB > 25MB limit
        result = uploader.is_suitable_for_direct_upload("big.png", over_limit)
        assert result is False

    def test_returns_true_for_file_under_size_limit(self):
        uploader = _make_uploader()
        under_limit = 10 * 1024 * 1024  # 10MB
        result = uploader.is_suitable_for_direct_upload("small.png", under_limit)
        assert result is True

    def test_returns_true_at_exact_size_limit(self):
        uploader = _make_uploader()
        exact_limit = 25 * 1024 * 1024  # 25MB exactly
        result = uploader.is_suitable_for_direct_upload("exact.png", exact_limit)
        assert result is True


# ===========================================================================
# upload_file_to_chat tests
# ===========================================================================


class TestUploadFileToChat:
    """Tests for ChatFileUploader.upload_file_to_chat."""

    def test_dry_run_returns_mock_result(self):
        uploader = _make_uploader(dry_run=True)
        token, metadata = uploader.upload_file_to_chat(
            "/tmp/test.png",
            "test.png",
            parent_space="spaces/ABC",
        )

        assert token == {"token": "DRY_CHAT_TOKEN_test.png"}
        assert metadata["name"] == "test.png"
        assert "DRY_CHAT_FILE_test.png" in (metadata["driveFile"]["name"])

    @patch("slack_migrator.services.chat.chat_uploader.MediaFileUpload")
    @patch("os.path.getsize", return_value=1024)
    def test_successful_upload(self, mock_getsize, mock_media_cls):
        uploader = _make_uploader(dry_run=False)
        mock_media = MagicMock()
        mock_media_cls.return_value = mock_media

        mock_response = {"attachmentDataRef": {"resourceName": "res/123"}}
        (uploader.chat_service.media().upload().execute.return_value) = mock_response
        # Reset to allow fresh call tracking
        uploader.chat_service.reset_mock()

        execute_mock = MagicMock(return_value=mock_response)
        upload_mock = MagicMock()
        upload_mock.execute = execute_mock
        media_mock = MagicMock()
        media_mock.upload.return_value = upload_mock
        uploader.chat_service.media.return_value = media_mock

        token, metadata = uploader.upload_file_to_chat(
            "/tmp/test.jpg",
            "test.jpg",
            parent_space="spaces/XYZ",
        )

        assert token is not None
        assert token == mock_response
        assert metadata is not None
        assert metadata["name"] == "test.jpg"
        assert metadata["mimeType"] == "image/jpeg"
        assert metadata["sizeBytes"] == "1024"
        media_mock.upload.assert_called_once()

    @patch("slack_migrator.services.chat.chat_uploader.MediaFileUpload")
    @patch("os.path.getsize", return_value=1024)
    def test_upload_failure_returns_none_none(self, mock_getsize, mock_media_cls):
        uploader = _make_uploader(dry_run=False)
        mock_media_cls.return_value = MagicMock()

        media_mock = MagicMock()
        upload_mock = MagicMock()
        upload_mock.execute.side_effect = Exception("API error")
        media_mock.upload.return_value = upload_mock
        uploader.chat_service.media.return_value = media_mock

        token, metadata = uploader.upload_file_to_chat(
            "/tmp/fail.png",
            "fail.png",
            parent_space="spaces/FAIL",
        )

        assert token is None
        assert metadata is None

    @patch("os.path.getsize")
    def test_oversized_file_returns_none_none(self, mock_getsize):
        uploader = _make_uploader(dry_run=False)
        # 201MB exceeds the 200MB Chat API limit
        mock_getsize.return_value = 201 * 1024 * 1024

        token, metadata = uploader.upload_file_to_chat(
            "/tmp/huge.bin",
            "huge.bin",
            parent_space="spaces/BIG",
        )

        assert token is None
        assert metadata is None


# ===========================================================================
# is_supported_file_type tests
# ===========================================================================


class TestIsSupportedFileType:
    """Tests for ChatFileUploader.is_supported_file_type."""

    def test_jpeg_is_supported(self):
        uploader = _make_uploader()
        assert uploader.is_supported_file_type("pic.jpg") is True

    def test_png_is_supported(self):
        uploader = _make_uploader()
        assert uploader.is_supported_file_type("img.png") is True

    def test_pdf_is_supported(self):
        uploader = _make_uploader()
        assert uploader.is_supported_file_type("doc.pdf") is True

    def test_unknown_extension_is_not_supported(self):
        uploader = _make_uploader()
        assert uploader.is_supported_file_type("data.xyz") is False

    def test_no_extension_is_not_supported(self):
        uploader = _make_uploader()
        assert uploader.is_supported_file_type("README") is False


# ===========================================================================
# get_supported_mime_types tests
# ===========================================================================


class TestGetSupportedMimeTypes:
    """Tests for ChatFileUploader.get_supported_mime_types."""

    def test_contains_common_image_types(self):
        uploader = _make_uploader()
        types = uploader.get_supported_mime_types()
        assert "image/jpeg" in types
        assert "image/png" in types
        assert "image/gif" in types
        assert "image/webp" in types

    def test_contains_document_types(self):
        uploader = _make_uploader()
        types = uploader.get_supported_mime_types()
        assert "application/pdf" in types
        assert "text/plain" in types

    def test_returns_list(self):
        uploader = _make_uploader()
        types = uploader.get_supported_mime_types()
        assert isinstance(types, list)
        assert len(types) > 0


# ===========================================================================
# File size handling in upload_file_to_chat
# ===========================================================================


class TestGetFileSize:
    """Tests for file size handling via os.path.getsize."""

    def test_returns_correct_file_size(self, tmp_path):
        test_file = tmp_path / "sized.txt"
        test_file.write_bytes(b"x" * 256)

        size = os.path.getsize(str(test_file))
        assert size == 256

    def test_returns_zero_for_empty_file(self, tmp_path):
        test_file = tmp_path / "empty.txt"
        test_file.write_bytes(b"")

        size = os.path.getsize(str(test_file))
        assert size == 0

    def test_missing_file_raises_os_error(self, tmp_path):
        missing = tmp_path / "nonexistent.txt"
        with pytest.raises(OSError):
            os.path.getsize(str(missing))


# ===========================================================================
# create_attachment_for_message tests
# ===========================================================================


class TestCreateAttachmentForMessage:
    """Tests for ChatFileUploader.create_attachment_for_message."""

    def test_direct_upload_response_returned_as_is(self):
        uploader = _make_uploader()
        response = {"attachmentDataRef": {"resourceName": "res/123"}}
        metadata = {"name": "file.png", "mimeType": "image/png"}

        result = uploader.create_attachment_for_message(response, metadata)
        assert result == response

    def test_drive_data_ref_already_formatted(self):
        uploader = _make_uploader()
        response = {"driveDataRef": {"driveFileId": "abc123"}}
        metadata = {"name": "doc.pdf"}

        result = uploader.create_attachment_for_message(response, metadata)
        assert result == response

    def test_drive_file_id_gets_reformatted(self):
        uploader = _make_uploader()
        response = {"driveFileId": "xyz789"}
        metadata = {"name": "sheet.xlsx"}

        result = uploader.create_attachment_for_message(response, metadata)
        assert "driveDataRef" in result
        assert result["driveDataRef"]["driveFileId"] == "xyz789"
