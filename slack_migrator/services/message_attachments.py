"""
Integrated file attachment service for message processing.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from slack_migrator.utils.logging import log_with_context


class MessageAttachmentProcessor:
    """Handles file attachments during message creation."""

    def __init__(self, file_handler, dry_run: bool = False):
        """Initialize the attachment processor.

        Args:
            file_handler: The FileHandler instance
            dry_run: Whether to run in dry run mode
        """
        self.file_handler = file_handler
        self.dry_run = dry_run

    def _get_current_channel(self):
        """Helper method to get the current channel from the migrator.

        Returns:
            Current channel name or None if not available
        """
        if (
            hasattr(self, "file_handler")
            and hasattr(self.file_handler, "migrator")
            and hasattr(self.file_handler.migrator, "current_channel")
        ):
            return self.file_handler.migrator.current_channel
        return None

    def process_message_attachments(
        self,
        message: Dict[str, Any],
        channel: str,
        space: Optional[str] = None,
        user_id: Optional[str] = None,
        user_service: Optional[Any] = None,
        sender_email: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Process all file attachments for a message and return attachment payload list.

        Args:
            message: The Slack message containing files
            channel: Channel name for context
            space: Optional space ID where files will be used
            user_id: User ID of the message sender (for external user handling)
            user_service: Optional user-specific Chat service to use for uploads
            sender_email: Optional email of the message sender

        Returns:
            List of attachment objects for Google Chat message payload
        """
        files = message.get("files", [])
        if not files:
            return []

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
            return mock_attachments

        attachments = []

        log_with_context(
            logging.DEBUG,
            f"Processing {len(files)} attachments for message",
            channel=channel,
        )

        for file_obj in files:
            try:
                # Ensure the file has the user ID from the message if it doesn't have one
                if "user" not in file_obj and user_id:
                    file_obj["user"] = user_id

                # Upload the file using FileHandler
                upload_result = self.file_handler.upload_attachment(
                    file_obj, channel, space, user_service, sender_email
                )

                if upload_result:
                    # Check if this is a skip result (e.g., Google Docs files)
                    if upload_result.get("type") == "skip":
                        log_with_context(
                            logging.DEBUG,
                            f"Skipping attachment (reason: {upload_result.get('reason', 'unknown')}): {upload_result.get('name', 'unknown')}",
                            channel=channel,
                            file_id=file_obj.get("id", "unknown"),
                        )
                        continue  # Skip this attachment but don't log it as an error

                    attachment = self._create_attachment_from_result(upload_result)
                    if attachment:
                        attachments.append(attachment)
                        log_with_context(
                            logging.DEBUG,
                            f"Added attachment to message: {upload_result.get('name', 'unknown')}",
                            channel=channel,
                            file_id=file_obj.get("id", "unknown"),
                        )
                    else:
                        log_with_context(
                            logging.WARNING,
                            f"Failed to create attachment from upload result for file: {file_obj.get('name', 'unknown')}",
                            channel=channel,
                            file_id=file_obj.get("id", "unknown"),
                            upload_result_type=upload_result.get("type", "unknown"),
                        )
                else:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to upload file: {file_obj.get('name', 'unknown')}",
                        channel=channel,
                        file_id=file_obj.get("id", "unknown"),
                    )

            except Exception as e:
                log_with_context(
                    logging.ERROR,
                    f"Error processing file attachment: {file_obj.get('name', 'unknown')} - {str(e)}",
                    channel=channel,
                    file_id=file_obj.get("id", "unknown"),
                    error=str(e),
                )
                # Continue processing other files even if one fails
                continue

        return attachments

    def _create_attachment_from_result(
        self, upload_result: Dict[str, Any]
    ) -> Optional[Dict[str, Union[str, Dict[str, str]]]]:
        """Create Google Chat attachment object from upload result.

        Args:
            upload_result: Result from FileHandler.upload_attachment()

        Returns:
            Google Chat attachment object or None if failed
        """
        if not upload_result or not isinstance(upload_result, dict):
            # Try to get channel from parent migrator
            current_channel = None
            if (
                hasattr(self, "file_handler")
                and hasattr(self.file_handler, "migrator")
                and hasattr(self.file_handler.migrator, "current_channel")
            ):
                current_channel = self.file_handler.migrator.current_channel

            log_with_context(
                logging.WARNING,
                f"Invalid upload result: {upload_result}",
                upload_result_type=type(upload_result).__name__,
                channel=current_channel,
            )
            return None

        upload_type = upload_result.get("type")

        log_with_context(
            logging.DEBUG,
            f"Creating attachment from upload result: type={upload_type}, result={upload_result}",
            upload_type=upload_type,
            channel=self._get_current_channel(),
        )

        if upload_type == "drive":
            # For Drive uploads, use only the driveDataRef with driveFileId
            # According to the API: https://developers.google.com/workspace/chat/api/reference/rest/v1/DriveDataRef

            # First check if the drive ID is in the ref structure (new format)
            ref = upload_result.get("ref", {})
            if isinstance(ref, dict) and "driveFileId" in ref:
                drive_id = ref["driveFileId"]
            else:
                # Fallback to old format
                drive_id = upload_result.get("drive_id")

            file_name = upload_result.get("name")

            log_with_context(
                logging.DEBUG,
                f"Processing Drive attachment: ref={ref}, drive_id={drive_id}, file_name={file_name}",
                drive_id=drive_id,
                file_name=file_name,
                channel=self._get_current_channel(),
            )

            if drive_id:
                # Create the attachment object with ONLY the driveDataRef field
                # The Google Chat API will handle the rest of the metadata
                attachment = {"driveDataRef": {"driveFileId": drive_id}}

                log_with_context(
                    logging.DEBUG,
                    f"Created Drive attachment with driveFileId: {drive_id}",
                    drive_id=drive_id,
                    file_name=file_name,
                    channel=self._get_current_channel(),
                )

                return attachment
            else:
                log_with_context(
                    logging.WARNING,
                    f"Drive upload result missing drive file ID in both drive_id and ref.driveFileId: {upload_result}",
                    channel=self._get_current_channel(),
                )

        elif upload_type == "direct":
            # For direct uploads, the ref contains the complete attachment object
            attachment_ref = upload_result.get("ref")
            if attachment_ref and isinstance(attachment_ref, dict):
                # The attachment_ref is already a complete attachment object from ChatFileUploader
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
                    f"Direct upload result missing or invalid ref: {upload_result}",
                    channel=self._get_current_channel(),
                )
        else:
            log_with_context(
                logging.WARNING,
                f"Unknown upload result type: {upload_type} in result: {upload_result}",
                channel=self._get_current_channel(),
            )

        return None

    def count_message_files(self, message: Dict) -> int:
        """Count the number of files in a message.

        Args:
            message: The Slack message object

        Returns:
            Number of files in the message
        """
        if not message or not isinstance(message, dict):
            return 0
        return len(message.get("files", []))

    def has_files(self, message: Dict) -> bool:
        """Check if a message has file attachments.

        Args:
            message: The Slack message object

        Returns:
            True if message has files, False otherwise
        """
        return self.count_message_files(message) > 0
