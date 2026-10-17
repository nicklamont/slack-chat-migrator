"""
Google Chat API file upload functionality.
"""

from __future__ import annotations

import json
import logging
import mimetypes
import os
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

from slack_chat_migrator.constants import (
    CHAT_MEDIA_MAX_BYTES,
    DIRECT_UPLOAD_MAX_BYTES,
    RESUMABLE_UPLOAD_THRESHOLD_BYTES,
)
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.chat_adapter import ChatAdapter


class ChatFileUploader:
    """Handles direct file uploads to Google Chat API."""

    def __init__(self, chat_service: ChatAdapter) -> None:
        """Initialize the ChatFileUploader.

        Args:
            chat_service: Google Chat API service instance (ChatAdapter)
        """
        self.chat_service = chat_service
        # Set by FileHandler before each channel is processed
        self.current_channel: str | None = None

    def _get_current_channel(self) -> str | None:
        """Return the current channel name for logging context."""
        return self.current_channel

    def upload_file_to_chat(
        self,
        file_path: str,
        filename: str,
        parent_space: str | None = None,
        mime_type: str | None = None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """Upload a file directly to Google Chat API.

        Args:
            file_path: Path to the local file
            filename: Name for the uploaded file
            parent_space: The space ID where the file should be uploaded (e.g., "spaces/AAAAy2-BTIA")
            mime_type: Content type (Slack's ``mimetype``); guessed from the
                file name when omitted

        Returns:
            Tuple of (attachment_token, attachment_metadata) if successful, (None, None) otherwise
        """
        try:
            if not parent_space:
                log_with_context(
                    logging.ERROR,
                    f"No parent space provided for Chat API upload of {filename}",
                    channel=self._get_current_channel(),
                )
                return (None, None)

            # Get file size and MIME type
            file_size = os.path.getsize(file_path)
            mime_type = _resolve_mime_type(filename, mime_type)

            log_with_context(
                logging.DEBUG,
                f"Uploading file {filename} directly to Chat API (size: {file_size}, MIME: {mime_type})",
                channel=self._get_current_channel(),
            )

            # The 200 MB limit applies to Chat media uploads only, not Drive.
            if file_size > CHAT_MEDIA_MAX_BYTES:
                log_with_context(
                    logging.WARNING,
                    f"File {filename} ({file_size} bytes) exceeds Chat API limit ({CHAT_MEDIA_MAX_BYTES} bytes)",
                    channel=self._get_current_channel(),
                )
                return (None, None)

            resumable = file_size > RESUMABLE_UPLOAD_THRESHOLD_BYTES
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=resumable)

            log_with_context(
                logging.DEBUG,
                f"Executing Chat API media upload request for {filename}",
                api_data=json.dumps(
                    {
                        "parent_space": parent_space,
                        "resumable": resumable,
                        "file_size_mb": round(file_size / (1024 * 1024), 2),
                    }
                ),
                channel=self._get_current_channel(),
            )

            response = self.chat_service.upload_media(
                parent=parent_space,
                body={"filename": filename},
                media_body=media,
            )

            # According to Google Chat API documentation, return the complete response
            # The documentation states: "Set attachment as the response from calling the upload method"
            attachment_metadata = {
                "name": filename,
                "mimeType": mime_type,
                "sizeBytes": str(file_size),
            }

            log_with_context(
                logging.DEBUG,
                f"Successfully uploaded file {filename} to Chat API",
                channel=self._get_current_channel(),
            )

            # Return the complete upload response as per Google Chat API documentation
            # The documentation states: "Set attachment as the response from calling the upload method"
            return (response, attachment_metadata)

        except (HttpError, OSError) as e:
            # Get detailed error information for better debugging
            error_info = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "file": filename,
                "parent_space": parent_space,
            }

            # Log detailed error information
            log_with_context(
                logging.ERROR,
                f"Failed to upload file {filename} to Chat API: {e}",
                api_data=json.dumps(error_info),
                channel=self._get_current_channel(),
            )

            return (None, None)

    def create_attachment_for_message(
        self, upload_response: dict[str, Any], attachment_metadata: dict[str, Any]
    ) -> dict[str, Any]:
        """Create an attachment object for a Chat message.

        Args:
            upload_response: Complete response from Chat media upload
            attachment_metadata: Metadata about the uploaded file

        Returns:
            Complete upload response object (as per Google Chat API documentation)
        """
        # According to the official Google Chat API documentation:
        # "Set attachment as the response from calling the upload method"

        # Log the attachment creation details
        log_with_context(
            logging.DEBUG,
            "Creating attachment from upload response",
            api_data=json.dumps(
                {
                    "upload_response_type": type(upload_response).__name__,
                    "attachment_metadata": attachment_metadata,
                }
            ),
            channel=self._get_current_channel(),
        )

        # Check if this is a Drive file attachment (which has a different format)
        if "driveDataRef" in upload_response or "driveFileId" in upload_response:
            # For Drive attachments, we must use the driveDataRef format
            log_with_context(
                logging.DEBUG,
                "Creating Drive attachment reference",
                api_data=json.dumps(upload_response),
                channel=self._get_current_channel(),
            )

            # Check if there's a nested driveFileId
            if (
                "driveDataRef" in upload_response
                and "driveFileId" in upload_response["driveDataRef"]
            ):
                # Already in correct format
                return upload_response
            elif "driveFileId" in upload_response:
                # Convert to proper format
                attachment = {
                    "driveDataRef": {"driveFileId": upload_response["driveFileId"]}
                }
                log_with_context(
                    logging.DEBUG,
                    "Formatted Drive attachment for message",
                    api_data=json.dumps(attachment),
                    channel=self._get_current_channel(),
                )
                return attachment

        # For Chat API direct uploads, log and return as is
        log_with_context(
            logging.DEBUG,
            "Using direct upload attachment for message",
            api_data=json.dumps(upload_response),
            channel=self._get_current_channel(),
        )

        # Return the complete upload response object
        return upload_response

    def get_supported_mime_types(self) -> list[str]:
        """Get list of MIME types supported by Chat API direct upload.

        Returns:
            List of supported MIME types
        """
        # MIME types supported by Google Chat direct upload
        # Note: This is more comprehensive than what FileHandler uses for direct upload
        # FileHandler has additional size/context restrictions
        return [
            # Images (most commonly used for direct upload)
            "image/jpeg",
            "image/png",
            "image/gif",
            "image/webp",
            "image/bmp",
            # Documents
            "application/pdf",
            "text/plain",
            "text/csv",
            "application/msword",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "application/vnd.ms-powerpoint",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            # Archives
            "application/zip",
            "application/x-rar-compressed",
            "application/x-7z-compressed",
            # Audio/Video (small files only)
            "audio/mpeg",
            "audio/wav",
            "video/mp4",
            "video/quicktime",
            # Code files
            "text/javascript",
            "text/css",
            "text/html",
            "application/json",
            "text/xml",
            # Other
            "application/octet-stream",
        ]

    def is_supported_file_type(
        self, filename: str, mime_type: str | None = None
    ) -> bool:
        """Check if a file type is supported for direct Chat upload.

        Args:
            filename: Name of the file to check
            mime_type: Slack's MIME type for the file; the name is only used
                to guess one when this is missing

        Returns:
            True if supported, False otherwise
        """
        mime_type = _resolve_mime_type(filename, mime_type, default=None)

        # Log the MIME type detection
        log_with_context(
            logging.DEBUG,
            f"Checking if file type is supported for Chat API upload: {filename}",
            api_data=json.dumps(
                {"filename": filename, "detected_mime_type": mime_type or "None"}
            ),
            channel=self._get_current_channel(),
        )

        if not mime_type:
            log_with_context(
                logging.DEBUG,
                f"Could not determine MIME type for {filename}, defaulting to unsupported",
                channel=self._get_current_channel(),
            )
            return False

        supported = mime_type in self.get_supported_mime_types()
        if not supported:
            log_with_context(
                logging.DEBUG,
                f"MIME type {mime_type} for {filename} is not supported by Chat API",
                channel=self._get_current_channel(),
            )

        return supported

    def is_suitable_for_direct_upload(
        self, filename: str, file_size: int, mime_type: str | None = None
    ) -> bool:
        """Check if a file is suitable for direct Chat upload considering both type and size.

        Args:
            filename: Name of the file to check
            file_size: Size of the file in bytes
            mime_type: Slack's MIME type for the file, if known

        Returns:
            True if suitable for direct upload, False otherwise
        """
        log_with_context(
            logging.DEBUG,
            f"Checking if file is suitable for direct Chat API upload: {filename}",
            api_data=json.dumps(
                {
                    "filename": filename,
                    "file_size": file_size,
                    "file_size_mb": round(file_size / (1024 * 1024), 2),
                }
            ),
            channel=self._get_current_channel(),
        )

        # Check MIME type support
        if not self.is_supported_file_type(filename, mime_type):
            log_with_context(
                logging.DEBUG,
                f"File {filename} has unsupported MIME type for direct Chat API upload",
                channel=self._get_current_channel(),
            )
            return False

        # Direct uploads are kept small; bigger files go to Drive.
        if file_size > DIRECT_UPLOAD_MAX_BYTES:
            log_with_context(
                logging.DEBUG,
                f"File {filename} exceeds size limit for direct Chat API upload: {file_size} bytes > {DIRECT_UPLOAD_MAX_BYTES} bytes",
                channel=self._get_current_channel(),
            )
            return False

        log_with_context(
            logging.DEBUG,
            f"File {filename} is suitable for direct Chat API upload",
            channel=self._get_current_channel(),
        )
        return True


def _resolve_mime_type(
    filename: str,
    mime_type: str | None,
    default: str | None = "application/octet-stream",
) -> str | None:
    """Prefer Slack's MIME type; otherwise guess from the file name."""
    if mime_type and mime_type != "null":
        return mime_type
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or default
