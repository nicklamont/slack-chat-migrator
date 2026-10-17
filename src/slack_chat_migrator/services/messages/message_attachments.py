"""
Integrated file attachment service for message processing.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from slack_chat_migrator.types import UploadResult
from slack_chat_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_chat_migrator.services.chat_adapter import ChatAdapter
    from slack_chat_migrator.services.files.file import FileHandler

#: AttachmentIssue.status values.
ISSUE_UNAVAILABLE = "unavailable"
ISSUE_FAILED = "failed"


@dataclass(frozen=True)
class AttachmentIssue:
    """A file of a message that couldn't be attached.

    Attributes:
        file_id: Slack file ID.
        name: File name, for the placeholder.
        status: ``"unavailable"`` (deleted, hidden, external, expired link...)
            or ``"failed"`` (an upload or network error).
        reason: Short reason, e.g. ``deleted``, ``external``, ``html_page``,
            ``http_403``, or the error text.
        url: Link to keep in the message, when there is one (external files).
    """

    file_id: str
    name: str
    status: str
    reason: str
    url: str | None = None

    @property
    def placeholder(self) -> str:
        """One line to show in the message instead of the file."""
        text = f"[File unavailable: {self.name}]"
        return f"{text} {self.url}" if self.url else text


class AttachmentList(list[dict[str, Any]]):
    """Attachment payloads for a message, plus the files that couldn't be attached.

    It is a plain list of Chat attachment objects, so existing callers keep
    working; :attr:`issues` lists each file that was unavailable or failed,
    in message order.
    """

    def __init__(
        self,
        attachments: Iterable[dict[str, Any]] = (),
        issues: Iterable[AttachmentIssue] = (),
    ) -> None:
        super().__init__(attachments)
        self.issues: list[AttachmentIssue] = list(issues)

    @property
    def unavailable(self) -> list[AttachmentIssue]:
        """Files that could not be fetched (not counted as failures)."""
        return [i for i in self.issues if i.status == ISSUE_UNAVAILABLE]

    @property
    def failed(self) -> list[AttachmentIssue]:
        """Files whose upload failed."""
        return [i for i in self.issues if i.status == ISSUE_FAILED]


def collect_message_files(message: dict[str, Any]) -> list[dict[str, Any]]:
    """Return a message's own files plus files of shared/forwarded messages.

    The message is not modified.  A file that appears twice (same ID) is
    listed once.

    Args:
        message: Slack message.

    Returns:
        A new list of Slack file objects.
    """
    if not message or not isinstance(message, dict):
        return []
    files: list[dict[str, Any]] = list(message.get("files") or [])
    for attachment in message.get("attachments") or []:
        if not isinstance(attachment, dict):
            continue
        if attachment.get("is_share") or attachment.get("is_msg_unfurl"):
            files.extend(attachment.get("files") or [])

    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for file_obj in files:
        if not isinstance(file_obj, dict):
            continue
        file_id = file_obj.get("id")
        if file_id:
            if file_id in seen:
                continue
            seen.add(file_id)
        unique.append(file_obj)
    return unique


class MessageAttachmentProcessor:
    """Handles file attachments during message creation."""

    def __init__(self, file_handler: FileHandler, dry_run: bool = False) -> None:
        """Initialize the attachment processor.

        Args:
            file_handler: The FileHandler instance
            dry_run: Whether to run in dry run mode
        """
        self.file_handler = file_handler
        self.dry_run = dry_run

    def _get_current_channel(self) -> str | None:
        """Return the current channel name for logging context."""
        return self.file_handler.state.context.current_channel

    def process_message_attachments(
        self,
        message: dict[str, Any],
        channel: str,
        space: str | None = None,
        user_id: str | None = None,
        user_service: ChatAdapter | None = None,
        sender_email: str | None = None,
    ) -> AttachmentList:
        """Process all file attachments for a message and return attachment payload list.

        Files of forwarded/shared messages are included.  The message is not
        modified.

        Args:
            message: The Slack message containing files
            channel: Channel name for context
            space: Optional space ID where files will be used
            user_id: User ID of the message sender (for external user handling)
            user_service: Optional user-specific Chat service to use for uploads
            sender_email: Optional email of the message sender

        Returns:
            The attachment objects for the Chat message payload, as an
            :class:`AttachmentList` whose ``issues`` name the files that were
            unavailable or failed, so the caller can add a placeholder.
        """
        files = collect_message_files(message)
        if not files:
            return AttachmentList()

        forwarded = len(files) - len(message.get("files") or [])
        if forwarded > 0:
            log_with_context(
                logging.DEBUG,
                f"Found {forwarded} files in forwarded message attachments",
                channel=channel,
            )

        if self.dry_run:
            log_with_context(
                logging.DEBUG,
                f"[DRY RUN] Would process {len(files)} attachments",
                channel=channel,
            )
            # Return mock attachment objects for dry run
            mock_attachments = []
            for i, file_obj in enumerate(files):
                file_name = file_obj.get("name", f"file_{i}")
                mock_attachments.append(
                    {
                        "driveDataRef": {"driveFileId": f"DRY_FILE_{i}_{file_name}"},
                        "contentName": file_name,
                        "contentType": "application/octet-stream",
                        "name": f"attachment-dry-{i}",
                    }
                )
            return AttachmentList(mock_attachments)

        result = AttachmentList()

        log_with_context(
            logging.DEBUG,
            f"Processing {len(files)} attachments for message",
            channel=channel,
        )

        for file_obj in files:
            file_id = file_obj.get("id", "unknown")
            name = file_obj.get("name") or f"file_{file_id}"
            try:
                # Attribute the file to the sender if it names no user,
                # without changing the export data.
                if "user" not in file_obj and user_id:
                    file_obj = {**file_obj, "user": user_id}

                # Upload the file using FileHandler
                upload_result = self.file_handler.upload_attachment(
                    file_obj, channel, space, user_service, sender_email
                )

                if upload_result.skipped:
                    log_with_context(
                        logging.DEBUG,
                        f"Skipping attachment (reason: {upload_result.skip_reason or 'unknown'}): {upload_result.name or 'unknown'}",
                        channel=channel,
                        file_id=file_id,
                    )
                    if upload_result.skip_reason == ISSUE_UNAVAILABLE:
                        result.issues.append(
                            AttachmentIssue(
                                file_id,
                                name,
                                ISSUE_UNAVAILABLE,
                                str(
                                    upload_result.metadata.get("unavailable_reason")
                                    or "unknown"
                                ),
                                upload_result.url,
                            )
                        )
                    continue

                if upload_result.success:
                    attachment = self._create_attachment_from_result(upload_result)
                    if attachment:
                        result.append(attachment)
                        log_with_context(
                            logging.DEBUG,
                            f"Added attachment to message: {upload_result.name or 'unknown'}",
                            channel=channel,
                            file_id=file_id,
                        )
                        continue
                    log_with_context(
                        logging.WARNING,
                        f"Failed to create attachment from upload result for file: {name}",
                        channel=channel,
                        file_id=file_id,
                        upload_result_type=upload_result.upload_type or "unknown",
                    )
                    reason = "invalid upload result"
                else:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to upload file: {name}",
                        channel=channel,
                        file_id=file_id,
                    )
                    reason = upload_result.error or "upload failed"
                result.issues.append(
                    AttachmentIssue(file_id, name, ISSUE_FAILED, reason)
                )

            except Exception as e:
                log_with_context(
                    logging.ERROR,
                    f"Error processing file attachment: {name} - {e!s}",
                    channel=channel,
                    file_id=file_id,
                    error=str(e),
                )
                result.issues.append(
                    AttachmentIssue(file_id, name, ISSUE_FAILED, str(e))
                )
                # Continue processing other files even if one fails
                continue

        return result

    def _create_attachment_from_result(
        self, upload_result: UploadResult
    ) -> dict[str, Any] | None:
        """Create Google Chat attachment object from upload result.

        Args:
            upload_result: UploadResult from FileHandler.upload_attachment()

        Returns:
            Google Chat attachment object or None if failed
        """
        log_with_context(
            logging.DEBUG,
            f"Creating attachment from upload result: type={upload_result.upload_type}",
            upload_type=upload_result.upload_type,
            channel=self._get_current_channel(),
        )

        if upload_result.upload_type == "drive":
            drive_id = upload_result.drive_id

            log_with_context(
                logging.DEBUG,
                f"Processing Drive attachment: drive_id={drive_id}, file_name={upload_result.name}",
                drive_id=drive_id,
                file_name=upload_result.name,
                channel=self._get_current_channel(),
            )

            if drive_id:
                attachment = {"driveDataRef": {"driveFileId": drive_id}}

                log_with_context(
                    logging.DEBUG,
                    f"Created Drive attachment with driveFileId: {drive_id}",
                    drive_id=drive_id,
                    file_name=upload_result.name,
                    channel=self._get_current_channel(),
                )

                return attachment
            else:
                log_with_context(
                    logging.WARNING,
                    f"Drive upload result missing drive file ID: {upload_result}",
                    channel=self._get_current_channel(),
                )

        elif upload_result.upload_type == "direct":
            attachment_ref = upload_result.attachment_ref
            if attachment_ref and isinstance(attachment_ref, dict):
                log_with_context(
                    logging.DEBUG,
                    f"Using direct upload attachment: {attachment_ref}",
                    attachment_ref=attachment_ref,
                    channel=self._get_current_channel(),
                )
                return attachment_ref
            else:
                log_with_context(
                    logging.WARNING,
                    f"Direct upload result missing or invalid attachment_ref: {upload_result}",
                    channel=self._get_current_channel(),
                )
        else:
            log_with_context(
                logging.WARNING,
                f"Unknown upload result type: {upload_result.upload_type}",
                channel=self._get_current_channel(),
            )

        return None

    def count_message_files(self, message: dict[str, Any]) -> int:
        """Count the files in a message, including forwarded ones.

        Args:
            message: The Slack message object

        Returns:
            Number of files that :meth:`process_message_attachments` handles
        """
        return len(collect_message_files(message))
