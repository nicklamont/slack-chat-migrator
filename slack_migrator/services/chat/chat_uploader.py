"""
Google Chat API file upload functionality.
"""

import json
import logging
import mimetypes
import os
from typing import Any, Dict, Optional, Tuple

from googleapiclient.http import MediaFileUpload

from slack_migrator.utils.logging import (
    log_with_context,
)


class ChatFileUploader:
    """Handles direct file uploads to Google Chat API."""

    def __init__(self, chat_service, dry_run: bool = False):
        """Initialize the ChatFileUploader.

        Args:
            chat_service: Google Chat API service instance
            dry_run: Whether to run in dry run mode
        """
        self.chat_service = chat_service
        self.dry_run = dry_run
        self.migrator = None  # Will be set by the migrator when this service is used

    def _get_current_channel(self):
        """Helper method to get the current channel from the migrator.

        Returns:
            Current channel name or None if not available
        """
        if (
            hasattr(self, "migrator")
            and self.migrator
            and hasattr(self.migrator, "current_channel")
        ):
            return self.migrator.current_channel
        return None

    def upload_file_to_chat(
        self, file_path: str, filename: str, parent_space: Optional[str] = None
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """Upload a file directly to Google Chat API.

        Args:
            file_path: Path to the local file
            filename: Name for the uploaded file
            parent_space: The space ID where the file should be uploaded (e.g., "spaces/AAAAy2-BTIA")

        Returns:
            Tuple of (attachment_token, attachment_metadata) if successful, (None, None) otherwise
        """
        if self.dry_run:
            return (
                f"DRY_CHAT_TOKEN_{filename}",
                {"name": filename, "driveFile": {"name": f"DRY_CHAT_FILE_{filename}"}},
            )

        try:
            # Get file size and MIME type
            file_size = os.path.getsize(file_path)
            mime_type, _ = mimetypes.guess_type(filename)
            if not mime_type:
                mime_type = "application/octet-stream"

            log_with_context(
                logging.DEBUG,
                f"Uploading file {filename} directly to Chat API (size: {file_size}, MIME: {mime_type})",
                channel=self._get_current_channel(),
            )

            # Chat API has file size limits - typically 200MB
            max_size = 200 * 1024 * 1024  # 200MB
            if file_size > max_size:
                log_with_context(
                    logging.WARNING,
                    f"File {filename} ({file_size} bytes) exceeds Chat API limit ({max_size} bytes)",
                    channel=self._get_current_channel(),
                )
                return (None, None)

            # Use Chat API media upload endpoint
            # Upload the file using Google Chat API media upload
            # Create media upload object
            media = MediaFileUpload(
                file_path,
                mimetype=mime_type,
                resumable=(
                    True if file_size > 5 * 1024 * 1024 else False
                ),  # Use resumable for files > 5MB
            )

            # Upload file to Chat API media endpoint
            # The Chat API uses the media.upload endpoint for file uploads
            # The filename should be passed in the request body
            upload_request = self.chat_service.media().upload(
                parent=parent_space,  # Required: specify the space where the file should be uploaded
                media_body=media,
                body={
                    "filename": filename
                },  # Filename in request body as per Chat API requirements
            )

            # Log the request right before execution
            log_with_context(
                logging.DEBUG,
                f"Executing Chat API media upload request for {filename}",
                api_data=json.dumps(
                    {
                        "parent_space": parent_space,
                        "resumable": file_size > 5 * 1024 * 1024,
                        "file_size_mb": round(file_size / (1024 * 1024), 2),
                    }
                ),
                channel=self._get_current_channel(),
            )

            # Execute the upload
            response = upload_request.execute()

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

        except Exception as e:
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

            # Log as API response error with appropriate status code
            from googleapiclient.errors import HttpError

            # Get the actual HTTP status code if available
            status_code = 500  # Default to 500 for general errors
            if (
                isinstance(e, HttpError)
                and hasattr(e, "resp")
                and hasattr(e.resp, "status")
            ):
                status_code = e.resp.status
            return (None, None)

    def create_attachment_for_message(
        self, upload_response: Dict[str, Any], attachment_metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
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
            f"Creating attachment from upload response",
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

    def get_supported_mime_types(self) -> list:
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

    def is_supported_file_type(self, filename: str) -> bool:
        """Check if a file type is supported for direct Chat upload.

        Args:
            filename: Name of the file to check

        Returns:
            True if supported, False otherwise
        """
        mime_type, _ = mimetypes.guess_type(filename)

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

    def is_suitable_for_direct_upload(self, filename: str, file_size: int) -> bool:
        """Check if a file is suitable for direct Chat upload considering both type and size.

        Args:
            filename: Name of the file to check
            file_size: Size of the file in bytes

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
        if not self.is_supported_file_type(filename):
            log_with_context(
                logging.DEBUG,
                f"File {filename} has unsupported MIME type for direct Chat API upload",
                channel=self._get_current_channel(),
            )
            return False

        # Check size limits - Chat API typically has smaller limits than Drive
        # 25MB is a reasonable limit for direct upload to maintain performance
        max_direct_upload_size = 25 * 1024 * 1024  # 25MB
        if file_size > max_direct_upload_size:
            log_with_context(
                logging.DEBUG,
                f"File {filename} exceeds size limit for direct Chat API upload: {file_size} bytes > {max_direct_upload_size} bytes",
                channel=self._get_current_channel(),
            )
            return False

        log_with_context(
            logging.DEBUG,
            f"File {filename} is suitable for direct Chat API upload",
            channel=self._get_current_channel(),
        )
        return True
