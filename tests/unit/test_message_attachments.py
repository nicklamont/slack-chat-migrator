"""Unit tests for the message attachment processing module."""

from unittest.mock import MagicMock

from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.services.messages.message_attachments import (
    AttachmentIssue,
    AttachmentList,
    MessageAttachmentProcessor,
    collect_message_files,
)
from slack_chat_migrator.types import UploadResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_file_handler(**overrides):
    """Create a mock file handler with reasonable defaults."""
    handler = MagicMock()
    handler.state = MigrationState()
    handler.state.context.current_channel = "general"
    for key, value in overrides.items():
        setattr(handler, key, value)
    return handler


def _make_processor(file_handler=None, dry_run=False):
    """Build a MessageAttachmentProcessor with mocked dependencies."""
    if file_handler is None:
        file_handler = _make_file_handler()
    return MessageAttachmentProcessor(file_handler=file_handler, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Tests: __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_stores_file_handler(self):
        handler = _make_file_handler()
        processor = MessageAttachmentProcessor(file_handler=handler, dry_run=False)
        assert processor.file_handler is handler

    def test_stores_dry_run_flag(self):
        processor = MessageAttachmentProcessor(
            file_handler=_make_file_handler(), dry_run=True
        )
        assert processor.dry_run is True

    def test_dry_run_defaults_false(self):
        processor = MessageAttachmentProcessor(file_handler=_make_file_handler())
        assert processor.dry_run is False


# ---------------------------------------------------------------------------
# Tests: _get_current_channel
# ---------------------------------------------------------------------------


class TestGetCurrentChannel:
    def test_returns_channel_when_available(self):
        handler = _make_file_handler()
        handler.state.context.current_channel = "random"
        processor = _make_processor(file_handler=handler)
        assert processor._get_current_channel() == "random"

    def test_returns_none_when_current_channel_not_set(self):
        handler = _make_file_handler()
        handler.state = MigrationState()
        processor = MessageAttachmentProcessor(file_handler=handler, dry_run=False)
        assert processor._get_current_channel() is None


# ---------------------------------------------------------------------------
# Tests: count_message_files
# ---------------------------------------------------------------------------


class TestCountMessageFiles:
    def test_count_with_files(self):
        processor = _make_processor()
        message = {"files": [{"id": "F1"}, {"id": "F2"}, {"id": "F3"}]}
        assert processor.count_message_files(message) == 3

    def test_count_with_no_files_key(self):
        processor = _make_processor()
        assert processor.count_message_files({"text": "hello"}) == 0

    def test_count_with_empty_files(self):
        processor = _make_processor()
        assert processor.count_message_files({"files": []}) == 0

    def test_count_with_none_message(self):
        processor = _make_processor()
        assert processor.count_message_files(None) == 0

    def test_count_with_non_dict_message(self):
        processor = _make_processor()
        assert processor.count_message_files("not a dict") == 0

    def test_count_with_empty_dict(self):
        processor = _make_processor()
        assert processor.count_message_files({}) == 0


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — no files
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsNoFiles:
    def test_returns_empty_when_no_files(self):
        processor = _make_processor()
        result = processor.process_message_attachments(
            message={"text": "hello"}, channel="general"
        )
        assert result == []

    def test_returns_empty_when_files_empty(self):
        processor = _make_processor()
        result = processor.process_message_attachments(
            message={"files": []}, channel="general"
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — forwarded message files
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsForwardedFiles:
    def test_collects_files_from_forwarded_attachments(self):
        """Files in forwarded/shared message attachments are included."""
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="drv_123",
            name="forwarded.txt",
        )
        processor = _make_processor(file_handler=handler)

        message = {
            "attachments": [
                {
                    "is_share": True,
                    "files": [{"id": "F1", "name": "forwarded.txt"}],
                }
            ],
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 1

    def test_collects_files_from_msg_unfurl_attachments(self):
        """Files in is_msg_unfurl attachments are included."""
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="drv_456",
            name="unfurled.png",
        )
        processor = _make_processor(file_handler=handler)

        message = {
            "attachments": [
                {
                    "is_msg_unfurl": True,
                    "files": [{"id": "F2", "name": "unfurled.png"}],
                }
            ],
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 1

    def test_ignores_non_share_attachments(self):
        """Attachments without is_share or is_msg_unfurl are ignored."""
        processor = _make_processor()
        message = {
            "attachments": [
                {"files": [{"id": "F1", "name": "ignored.txt"}]},
            ],
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []

    def test_combines_direct_and_forwarded_files(self):
        """Both top-level files and forwarded attachment files are processed."""
        handler = _make_file_handler()
        call_count = 0

        def upload_side_effect(file_obj, channel, space, user_service, sender_email):
            nonlocal call_count
            call_count += 1
            return UploadResult(
                upload_type="drive",
                drive_id=f"drv_{call_count}",
                name=file_obj.get("name", "unknown"),
            )

        handler.upload_attachment.side_effect = upload_side_effect
        processor = _make_processor(file_handler=handler)

        message = {
            "files": [{"id": "F1", "name": "direct.txt"}],
            "attachments": [
                {
                    "is_share": True,
                    "files": [{"id": "F2", "name": "forwarded.txt"}],
                }
            ],
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 2
        assert handler.upload_attachment.call_count == 2


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — dry run
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsDryRun:
    def test_dry_run_returns_mock_attachments(self):
        processor = _make_processor(dry_run=True)
        message = {
            "files": [
                {"id": "F1", "name": "report.pdf"},
                {"id": "F2", "name": "image.png"},
            ]
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 2

    def test_dry_run_attachment_structure(self):
        processor = _make_processor(dry_run=True)
        message = {"files": [{"id": "F1", "name": "report.pdf"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        att = result[0]
        assert "driveDataRef" in att
        assert "driveFileId" in att["driveDataRef"]
        assert att["contentName"] == "report.pdf"
        assert att["contentType"] == "application/octet-stream"
        assert att["name"].startswith("attachment-dry-")

    def test_dry_run_uses_fallback_name(self):
        processor = _make_processor(dry_run=True)
        message = {"files": [{"id": "F1"}]}  # no "name" key
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result[0]["contentName"] == "file_0"

    def test_dry_run_does_not_call_upload(self):
        handler = _make_file_handler()
        processor = _make_processor(file_handler=handler, dry_run=True)
        message = {"files": [{"id": "F1", "name": "test.txt"}]}
        processor.process_message_attachments(message=message, channel="general")
        handler.upload_attachment.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — drive upload
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsDriveUpload:
    def test_drive_upload_with_drive_id(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="abc123",
            name="file.txt",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "file.txt"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 1
        assert result[0] == {"driveDataRef": {"driveFileId": "abc123"}}

    def test_drive_upload_missing_drive_id_returns_none(self):
        """Drive upload with no drive_id returns nothing."""
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            name="file.txt",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "file.txt"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — direct upload
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsDirectUpload:
    def test_direct_upload_returns_ref(self):
        handler = _make_file_handler()
        attachment_ref = {
            "attachmentDataRef": {"resourceName": "media/12345"},
            "contentName": "photo.jpg",
            "contentType": "image/jpeg",
        }
        handler.upload_attachment.return_value = UploadResult(
            upload_type="direct",
            attachment_ref=attachment_ref,
            name="photo.jpg",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "photo.jpg"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 1
        assert result[0] is attachment_ref

    def test_direct_upload_missing_ref(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="direct",
            name="photo.jpg",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "photo.jpg"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []

    def test_direct_upload_ref_not_a_dict(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="direct",
            attachment_ref="not_a_dict",
            name="photo.jpg",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "photo.jpg"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — skip results
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsSkip:
    def test_skip_result_is_excluded(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="skip",
            skip_reason="google_doc",
            name="My Doc",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "My Doc"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — unknown upload type
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsUnknownType:
    def test_unknown_type_returns_nothing(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="something_new",
            name="file.bin",
        )
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "file.bin"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — upload failures
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsFailures:
    def test_upload_returns_error_result(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(error="Upload failed")
        processor = _make_processor(file_handler=handler)
        message = {"files": [{"id": "F1", "name": "fail.txt"}]}
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []

    def test_upload_raises_exception_continues(self):
        """An exception on one file should not prevent other files from processing."""
        handler = _make_file_handler()
        handler.upload_attachment.side_effect = [
            RuntimeError("network error"),
            UploadResult(upload_type="drive", drive_id="ok_file", name="second.txt"),
        ]
        processor = _make_processor(file_handler=handler)
        message = {
            "files": [
                {"id": "F1", "name": "first.txt"},
                {"id": "F2", "name": "second.txt"},
            ]
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 1
        assert result[0]["driveDataRef"]["driveFileId"] == "ok_file"

    def test_all_uploads_fail_returns_empty(self):
        handler = _make_file_handler()
        handler.upload_attachment.side_effect = RuntimeError("fail")
        processor = _make_processor(file_handler=handler)
        message = {
            "files": [{"id": "F1", "name": "a.txt"}, {"id": "F2", "name": "b.txt"}]
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert result == []


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — user_id passthrough
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsUserId:
    def test_sets_user_on_file_when_missing(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="d1",
            name="f.txt",
        )
        processor = _make_processor(file_handler=handler)
        file_obj = {"id": "F1", "name": "f.txt"}
        message = {"files": [file_obj]}
        processor.process_message_attachments(
            message=message, channel="general", user_id="U999"
        )
        uploaded = handler.upload_attachment.call_args[0][0]
        assert uploaded["user"] == "U999"
        assert "user" not in file_obj  # the export data is not modified

    def test_does_not_override_existing_user(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="d1",
            name="f.txt",
        )
        processor = _make_processor(file_handler=handler)
        file_obj = {"id": "F1", "name": "f.txt", "user": "U111"}
        message = {"files": [file_obj]}
        processor.process_message_attachments(
            message=message, channel="general", user_id="U999"
        )
        assert file_obj["user"] == "U111"

    def test_no_user_id_does_not_set_user(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="d1",
            name="f.txt",
        )
        processor = _make_processor(file_handler=handler)
        file_obj = {"id": "F1", "name": "f.txt"}
        message = {"files": [file_obj]}
        processor.process_message_attachments(
            message=message, channel="general", user_id=None
        )
        assert "user" not in file_obj


# ---------------------------------------------------------------------------
# Tests: process_message_attachments — arguments forwarded to upload
# ---------------------------------------------------------------------------


class TestProcessMessageAttachmentsUploadArgs:
    def test_forwards_space_user_service_and_email(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive",
            drive_id="d1",
            name="f.txt",
        )
        processor = _make_processor(file_handler=handler)
        user_svc = MagicMock()
        message = {"files": [{"id": "F1", "name": "f.txt"}]}
        processor.process_message_attachments(
            message=message,
            channel="general",
            space="spaces/abc",
            user_service=user_svc,
            sender_email="alice@example.com",
        )
        handler.upload_attachment.assert_called_once_with(
            {"id": "F1", "name": "f.txt"},
            "general",
            "spaces/abc",
            user_svc,
            "alice@example.com",
        )


# ---------------------------------------------------------------------------
# Tests: _create_attachment_from_result — edge cases
# ---------------------------------------------------------------------------


class TestCreateAttachmentFromResult:
    def test_drive_with_drive_id(self):
        processor = _make_processor()
        result = processor._create_attachment_from_result(
            UploadResult(upload_type="drive", drive_id="abc123")
        )
        assert result == {"driveDataRef": {"driveFileId": "abc123"}}

    def test_drive_without_drive_id_returns_none(self):
        processor = _make_processor()
        result = processor._create_attachment_from_result(
            UploadResult(upload_type="drive")
        )
        assert result is None

    def test_direct_with_attachment_ref(self):
        processor = _make_processor()
        ref = {"attachmentDataRef": {"resourceName": "media/12345"}}
        result = processor._create_attachment_from_result(
            UploadResult(upload_type="direct", attachment_ref=ref)
        )
        assert result is ref

    def test_direct_without_attachment_ref_returns_none(self):
        processor = _make_processor()
        result = processor._create_attachment_from_result(
            UploadResult(upload_type="direct")
        )
        assert result is None

    def test_direct_with_non_dict_ref_returns_none(self):
        processor = _make_processor()
        result = processor._create_attachment_from_result(
            UploadResult(upload_type="direct", attachment_ref="not_a_dict")
        )
        assert result is None

    def test_unknown_type_returns_none(self):
        processor = _make_processor()
        result = processor._create_attachment_from_result(
            UploadResult(upload_type="unknown")
        )
        assert result is None


# ---------------------------------------------------------------------------
# Tests: multiple files in single message
# ---------------------------------------------------------------------------


class TestMultipleFiles:
    def test_processes_all_files(self):
        handler = _make_file_handler()
        counter = {"n": 0}

        def upload_side_effect(file_obj, channel, space, user_service, sender_email):
            counter["n"] += 1
            return UploadResult(
                upload_type="drive",
                drive_id=f"drv_{counter['n']}",
                name=file_obj["name"],
            )

        handler.upload_attachment.side_effect = upload_side_effect
        processor = _make_processor(file_handler=handler)

        message = {
            "files": [
                {"id": "F1", "name": "a.txt"},
                {"id": "F2", "name": "b.txt"},
                {"id": "F3", "name": "c.txt"},
            ]
        }
        result = processor.process_message_attachments(
            message=message, channel="general"
        )
        assert len(result) == 3
        drive_ids = [att["driveDataRef"]["driveFileId"] for att in result]
        assert drive_ids == ["drv_1", "drv_2", "drv_3"]


# ---------------------------------------------------------------------------
# Forwarded files are counted (I37) and the input is left alone
# ---------------------------------------------------------------------------


class TestForwardedFileCounting:
    def test_count_matches_uploads(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive", drive_id="D", name="n"
        )
        processor = _make_processor(file_handler=handler)
        message = {
            "files": [{"id": "F1", "name": "a"}],
            "attachments": [{"is_share": True, "files": [{"id": "F2", "name": "b"}]}],
        }

        assert processor.count_message_files(message) == 2
        processor.process_message_attachments(message, "c", "spaces/S")
        assert handler.upload_attachment.call_count == 2

    def test_processing_does_not_mutate_message(self):
        handler = _make_file_handler()
        handler.upload_attachment.return_value = UploadResult(
            upload_type="drive", drive_id="D"
        )
        processor = _make_processor(file_handler=handler)
        message = {
            "files": [{"id": "F1", "name": "a"}],
            "attachments": [{"is_share": True, "files": [{"id": "F2", "name": "b"}]}],
        }

        processor.process_message_attachments(message, "c", "spaces/S", user_id="U1")
        processor.process_message_attachments(message, "c", "spaces/S", user_id="U1")

        assert message == {
            "files": [{"id": "F1", "name": "a"}],
            "attachments": [{"is_share": True, "files": [{"id": "F2", "name": "b"}]}],
        }
        assert processor.count_message_files(message) == 2

    def test_duplicate_file_ids_are_listed_once(self):
        message = {
            "files": [{"id": "F1"}],
            "attachments": [
                {"is_msg_unfurl": True, "files": [{"id": "F1"}, {"id": "F3"}]}
            ],
        }
        assert [f["id"] for f in collect_message_files(message)] == ["F1", "F3"]

    def test_tolerates_odd_shapes(self):
        assert collect_message_files(None) == []
        assert collect_message_files({"files": None, "attachments": None}) == []
        assert collect_message_files({"attachments": ["x", {"is_share": True}]}) == []


# ---------------------------------------------------------------------------
# Files that couldn't be attached are reported (for I03 placeholders)
# ---------------------------------------------------------------------------


class TestAttachmentIssues:
    def _run(self, *results):
        handler = _make_file_handler()
        handler.upload_attachment.side_effect = list(results)
        processor = _make_processor(file_handler=handler)
        files = [{"id": f"F{i}", "name": f"file{i}.pdf"} for i in range(len(results))]
        return processor.process_message_attachments({"files": files}, "c", "spaces/S")

    def test_returns_a_list_with_issues(self):
        result = self._run(UploadResult(upload_type="drive", drive_id="D1"))
        assert isinstance(result, AttachmentList)
        assert isinstance(result, list)
        assert result == [{"driveDataRef": {"driveFileId": "D1"}}]
        assert result.issues == []

    def test_unavailable_and_failed_files_are_named(self):
        result = self._run(
            UploadResult(
                upload_type="skip",
                skip_reason="unavailable",
                name="file0.pdf",
                metadata={"unavailable_reason": "deleted"},
            ),
            UploadResult(error="Download failed: http_503", name="file1.pdf"),
            UploadResult(upload_type="drive", drive_id="D2"),
            UploadResult(
                upload_type="skip",
                skip_reason="unavailable",
                url="https://www.dropbox.com/s/x",
                metadata={"unavailable_reason": "external"},
            ),
        )

        assert len(result) == 1
        assert result.unavailable == [
            AttachmentIssue("F0", "file0.pdf", "unavailable", "deleted"),
            AttachmentIssue(
                "F3",
                "file3.pdf",
                "unavailable",
                "external",
                "https://www.dropbox.com/s/x",
            ),
        ]
        assert result.failed == [
            AttachmentIssue("F1", "file1.pdf", "failed", "Download failed: http_503")
        ]
        assert [i.placeholder for i in result.issues] == [
            "[File unavailable: file0.pdf]",
            "[File unavailable: file1.pdf]",
            "[File unavailable: file3.pdf] https://www.dropbox.com/s/x",
        ]

    def test_google_docs_links_are_not_issues(self):
        result = self._run(
            UploadResult(upload_type="skip", skip_reason="google_docs_link", name="d")
        )
        assert result == []
        assert result.issues == []

    def test_exception_is_reported_as_failed(self):
        result = self._run(RuntimeError("boom"))
        assert result.failed == [AttachmentIssue("F0", "file0.pdf", "failed", "boom")]

    def test_dry_run_reports_no_issues(self):
        processor = _make_processor(dry_run=True)
        result = processor.process_message_attachments(
            {"files": [{"id": "F1", "mode": "tombstone"}]}, "c"
        )
        assert len(result) == 1
        assert result.issues == []
