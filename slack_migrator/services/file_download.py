"""File download and content retrieval from Slack exports."""

from __future__ import annotations

import logging
from typing import Any

import requests

from slack_migrator.constants import (
    HTTP_FORBIDDEN,
    HTTP_OK,
    HTTP_UNAUTHORIZED,
)
from slack_migrator.utils.logging import log_with_context

logger = logging.getLogger("slack_migrator")


def download_file(
    file_obj: dict[str, Any],
    channel: str | None,
) -> bytes | None:
    """Download a file from Slack export or URL.

    Handles Google Docs links (returns sentinel ``b"__GOOGLE_DOCS_SKIP__"``)
    and Google Drive files (returns sentinel ``b"__GOOGLE_DRIVE_FILE__"``).

    Args:
        file_obj: The file object from Slack.
        channel: Current channel name for logging context.

    Returns:
        File content as bytes, a sentinel marker, or None if download failed.
    """
    try:
        file_id = file_obj.get("id", "unknown")
        name = file_obj.get("name", f"file_{file_id}")
        url_private = file_obj.get("url_private")

        if not url_private:
            log_with_context(
                logging.WARNING,
                f"No URL found for file: {name}",
                file_id=file_id,
                channel=channel,
            )
            return None

        # Skip Google Docs links - these should not be processed as file attachments
        # Google Docs URLs in Slack messages are text links, not downloadable files
        # Only skip if they are actual Google Docs/Sheets/Slides documents
        is_google_docs = (
            ("docs.google.com/document" in url_private)
            or ("docs.google.com/spreadsheets" in url_private)
            or ("docs.google.com/presentation" in url_private)
            or ("sheets.google.com" in url_private and "/edit" in url_private)
            or ("slides.google.com" in url_private and "/edit" in url_private)
        )

        # Check if this is a Google Drive file that we should reference directly
        is_google_drive_file = (
            "drive.google.com/file/d/" in url_private
            or "drive.google.com/open?id=" in url_private
        )

        if is_google_docs:
            log_with_context(
                logging.DEBUG,
                f"Skipping Google Docs link - not a downloadable file: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                file_id=file_id,
                file_name=name,
                channel=channel,
            )
            return b"__GOOGLE_DOCS_SKIP__"

        if is_google_drive_file:
            log_with_context(
                logging.DEBUG,
                f"Google Drive file detected - will create direct reference instead of downloading: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                file_id=file_id,
                file_name=name,
                channel=channel,
            )
            # Return a special marker to indicate this is a Drive file
            return b"__GOOGLE_DRIVE_FILE__"

        log_with_context(
            logging.DEBUG,
            f"Downloading file from URL: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
            file_id=file_id,
            file_name=name,
            channel=channel,
        )

        # For files in the export, the URL might already contain a token
        # We'll try to download using requests with default headers
        headers: dict[str, str] = {}

        # Note: Slack token authentication removed as not needed
        # Export URLs already contain authentication tokens

        response = requests.get(url_private, headers=headers, stream=True, timeout=60)

        if response.status_code != HTTP_OK:
            log_with_context(
                logging.WARNING,
                f"Failed to download file {name}: HTTP {response.status_code}",
                file_id=file_id,
                http_status=response.status_code,
                channel=channel,
            )
            # Raise an exception to trigger the retry
            response.raise_for_status()
            return None

        # Get content length if available
        content_length = response.headers.get("Content-Length")
        if content_length:
            log_with_context(
                logging.DEBUG,
                f"File size from headers: {content_length} bytes",
                file_id=file_id,
                channel=channel,
            )

        # Return the actual file content
        content = response.content
        log_with_context(
            logging.DEBUG,
            f"Successfully downloaded file: {name} (Size: {len(content)} bytes)",
            file_id=file_id,
            channel=channel,
        )
        return content

    except requests.exceptions.RequestException as e:
        # Check for authentication errors (401, 403) which are unlikely to be resolved by retrying
        if (
            hasattr(e, "response")
            and e.response
            and e.response.status_code in (HTTP_UNAUTHORIZED, HTTP_FORBIDDEN)
        ):
            log_with_context(
                logging.WARNING,
                f"Authentication error downloading file, not retrying: {e!s}",
                file_id=file_obj.get("id", "unknown"),
                file_name=file_obj.get("name", "unknown"),
                error=str(e),
                status_code=e.response.status_code,
                channel=channel,
            )
            # Return None instead of re-raising to prevent further retries for auth errors
            return None

        # For other network errors, log and re-raise to trigger retry
        log_with_context(
            logging.WARNING,
            f"Error downloading file: {e!s}",
            file_id=file_obj.get("id", "unknown"),
            file_name=file_obj.get("name", "unknown"),
            error=str(e),
            channel=channel,
        )
        raise  # Re-raise to trigger retry
    except Exception as e:
        log_with_context(
            logging.ERROR,
            f"Error downloading file: {e!s}",
            file_id=file_obj.get("id", "unknown"),
            file_name=file_obj.get("name", "unknown"),
            error=str(e),
            channel=channel,
        )
        return None


def create_drive_reference(
    file_obj: dict[str, Any],
    channel: str | None,
    processed_files: dict[str, Any],
    file_stats: dict[str, Any],
) -> dict[str, Any] | None:
    """Create a direct reference to an existing Google Drive file.

    Instead of downloading and re-uploading, this creates a reference to a
    file already hosted on Google Drive.

    Args:
        file_obj: The file object from Slack containing Google Drive URL.
        channel: Optional channel name for context.
        processed_files: Cache dict mapping file IDs to results (mutated in place).
        file_stats: Statistics dict with upload counters (mutated in place).

    Returns:
        Dict with drive reference details if successful, None otherwise.
    """
    try:
        file_id = file_obj.get("id", "unknown")
        name = file_obj.get("name", f"file_{file_id}")
        url_private = file_obj.get("url_private", "")

        # Extract Google Drive file ID from the URL
        drive_file_id = None

        if "drive.google.com/file/d/" in url_private:
            # Format: https://drive.google.com/file/d/FILE_ID/view
            try:
                drive_file_id = url_private.split("/file/d/")[1].split("/")[0]
            except IndexError:
                pass
        elif "drive.google.com/open?id=" in url_private:
            # Format: https://drive.google.com/open?id=FILE_ID
            try:
                drive_file_id = url_private.split("id=")[1].split("&")[0]
            except IndexError:
                pass

        if not drive_file_id:
            log_with_context(
                logging.WARNING,
                f"Could not extract Drive file ID from URL: {url_private}",
                channel=channel,
                file_id=file_id,
            )
            return None

        log_with_context(
            logging.DEBUG,
            f"Created direct Drive reference for existing file: {name} (Drive ID: {drive_file_id})",
            channel=channel,
            file_id=file_id,
            drive_file_id=drive_file_id,
        )

        # Update statistics
        file_stats["drive_uploads"] += 1

        # Create the result format
        drive_result: dict[str, Any] = {
            "type": "drive",
            "link": url_private,
            "drive_id": drive_file_id,
            "name": name,
            "mime_type": file_obj.get("mimetype", "application/octet-stream"),
            "is_reference": True,  # Flag to indicate this is a reference, not an upload
        }

        # Cache the result
        processed_files[file_id] = drive_result

        return drive_result

    except Exception as e:
        log_with_context(
            logging.ERROR,
            f"Error creating Drive reference: {e}",
            channel=channel,
            file_id=file_obj.get("id", "unknown"),
            error=str(e),
        )
        return None
