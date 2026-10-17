"""MIME type resolution utilities for file uploads.

Pure functions that determine MIME types for Slack files being uploaded
to Google Drive, including special handling for Google Docs/Sheets/Slides
links and Slack's own document types (canvases and lists).
"""

from __future__ import annotations

import logging
import mimetypes
from typing import Any

from slack_chat_migrator.utils.logging import log_with_context


def resolve_drive_mime_type(
    file_obj: dict[str, Any],
    name: str,
    channel: str | None,
    file_id: str,
) -> str:
    """Determine the correct MIME type for a file being uploaded to Drive.

    Handles Google Docs/Sheets/Slides links specially and falls back
    to guessing from filename for regular files.

    Args:
        file_obj: Slack file object with ``mimetype`` and ``url_private`` fields.
        name: Display name of the file.
        channel: Channel name for logging context.
        file_id: Slack file ID for logging context.

    Returns:
        The resolved MIME type string.
    """
    mime_type: str = file_obj.get("mimetype", "application/octet-stream")
    url_private: str = file_obj.get("url_private", "")

    is_google_docs_link = (
        "docs.google.com" in url_private
        or "drive.google.com" in url_private
        or "sheets.google.com" in url_private
        or "slides.google.com" in url_private
    )

    if is_google_docs_link:
        mime_type = resolve_google_docs_mime_type(url_private, mime_type, name)
        log_with_context(
            logging.DEBUG,
            f"Detected Google Docs link, using MIME type {mime_type} for file {name}",
            channel=channel,
            file_id=file_id,
            url=url_private[:100],
        )
    elif not mime_type or mime_type == "null":
        guessed_type, _ = mimetypes.guess_type(name)
        mime_type = guessed_type if guessed_type else "application/octet-stream"
        log_with_context(
            logging.DEBUG,
            f"Using guessed MIME type {mime_type} for file {name}",
            channel=channel,
            file_id=file_id,
        )

    return mime_type


def resolve_google_docs_mime_type(
    url_private: str, current_mime: str, name: str
) -> str:
    """Map a Google Docs/Drive URL to its correct MIME type.

    Args:
        url_private: The private URL from Slack's file object.
        current_mime: The current MIME type (used as fallback).
        name: File name for MIME guessing when the URL is a generic Drive link.

    Returns:
        The resolved Google Docs MIME type, or *current_mime* as fallback.
    """
    if "docs.google.com/document" in url_private:
        return "application/vnd.google-apps.document"
    if (
        "docs.google.com/spreadsheets" in url_private
        or "sheets.google.com" in url_private
    ):
        return "application/vnd.google-apps.spreadsheet"
    if "docs.google.com/presentation" in url_private:
        return "application/vnd.google-apps.presentation"
    if "drive.google.com" in url_private:
        if not current_mime or current_mime == "application/octet-stream":
            guessed_type, _ = mimetypes.guess_type(name)
            return guessed_type or "application/vnd.google-apps.document"
    return current_mime


#: Slack ``filetype``/``mimetype`` values of canvases (exported as HTML).
CANVAS_FILETYPES = frozenset({"quip", "canvas"})
CANVAS_MIME_TYPES = frozenset({"application/vnd.slack-docs"})
#: Slack ``filetype``/``mimetype`` values of lists (downloadable as CSV).
LIST_FILETYPES = frozenset({"list", "slack_list"})
LIST_MIME_TYPES = frozenset({"application/vnd.slack-list"})


def is_slack_canvas(file_obj: dict[str, Any]) -> bool:
    """Whether a Slack file is a canvas (``filetype`` quip/canvas).

    Args:
        file_obj: Slack file object.

    Returns:
        True for canvases, whose download is an HTML rendering.
    """
    return (
        str(file_obj.get("filetype") or "").lower() in CANVAS_FILETYPES
        or str(file_obj.get("mimetype") or "").lower() in CANVAS_MIME_TYPES
    )


def is_slack_list(file_obj: dict[str, Any]) -> bool:
    """Whether a Slack file is a Slack List.

    Recognised by ``filetype`` list/slack_list, the
    ``application/vnd.slack-list`` MIME type or a ``list_csv_download_url``.
    Plain CSV attachments are not lists.

    Args:
        file_obj: Slack file object.

    Returns:
        True for Slack Lists.
    """
    return (
        str(file_obj.get("filetype") or "").lower() in LIST_FILETYPES
        or str(file_obj.get("mimetype") or "").lower() in LIST_MIME_TYPES
        or bool(file_obj.get("list_csv_download_url"))
    )
