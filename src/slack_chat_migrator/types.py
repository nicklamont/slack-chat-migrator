"""Shared type definitions for the Slack to Google Chat migration tool.

Provides TypedDicts for structured data flowing through the migration pipeline:
Slack export JSON shapes, Google API response shapes, and internal tracking types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypedDict

# ---------------------------------------------------------------------------
# Slack export types (from users.json, channels.json, messages/*.json)
# ---------------------------------------------------------------------------


class SlackUserProfile(TypedDict, total=False):
    """Profile block nested inside a Slack user record."""

    real_name: str
    email: str


class SlackUser(TypedDict, total=False):
    """A user record from the Slack export ``users.json``."""

    id: str
    name: str
    real_name: str
    is_bot: bool
    is_app_user: bool
    deleted: bool
    profile: SlackUserProfile


class SlackReaction(TypedDict, total=False):
    """A reaction entry inside a Slack message."""

    name: str
    users: list[str]
    count: int


class SlackFile(TypedDict, total=False):
    """A file attachment inside a Slack message."""

    id: str
    name: str
    user: str
    mimetype: str
    url_private_download: str
    url_private: str
    size: int
    mode: str
    filetype: str


class SlackMessage(TypedDict, total=False):
    """A message record from a Slack channel export JSON file."""

    ts: str
    user: str
    type: str
    subtype: str
    text: str
    thread_ts: str
    edited: dict[str, str]
    files: list[SlackFile]
    attachments: list[dict[str, object]]
    reactions: list[SlackReaction]
    username: str


class SlackChannel(TypedDict, total=False):
    """A channel record from the Slack export ``channels.json``."""

    id: str
    name: str
    created: int
    is_general: bool
    members: list[str]
    purpose: dict[str, str]
    topic: dict[str, str]


# ---------------------------------------------------------------------------
# Internal tracking types
# ---------------------------------------------------------------------------


class MigrationSummary(TypedDict):
    """Aggregate migration counters."""

    channels_processed: list[str]
    spaces_created: int
    messages_created: int
    reactions_created: int
    files_created: int


class FailedMessage(TypedDict):
    """A message that failed to migrate."""

    channel: str
    ts: str
    error: str
    error_details: str
    payload: dict[str, object]


class SkippedReaction(TypedDict):
    """A reaction skipped due to unmapped user."""

    user_id: str
    reaction: str
    message_ts: str
    channel: str


class UserWithoutEmail(TypedDict):
    """Info about a Slack user with no email in the export."""

    id: str
    name: str
    real_name: str
    is_bot: bool
    is_app_user: bool
    deleted: bool


class FileUploadStats(TypedDict):
    """File upload statistics returned by FileHandler."""

    total_files_processed: int
    successful_uploads: int
    failed_uploads: int
    drive_uploads: int
    direct_uploads: int
    external_user_files: int
    ownership_transferred: int
    success_rate: float


# ---------------------------------------------------------------------------
# Service result types (structured returns at API boundaries)
# ---------------------------------------------------------------------------


class MessageResult(str, Enum):
    """Why a message was skipped rather than sent to the Chat API."""

    IGNORED_BOT = "IGNORED_BOT"
    ALREADY_SENT = "ALREADY_SENT"
    SKIPPED = "SKIPPED"


@dataclass
class SendResult:
    """Structured result from :func:`send_message`.

    Three-state model: *success* (message created), *skipped* (intentionally
    not sent, e.g. bot or duplicate), or *failed* (error occurred).
    The ``failed`` property exists because ``not success`` conflates
    skips and errors — callers almost always need to distinguish the two.
    """

    message_name: str | None = None
    skipped: MessageResult | None = None
    error: str | None = None
    error_code: int | None = None
    retryable: bool = False

    @property
    def success(self) -> bool:
        """True when the message was created in Google Chat."""
        return self.message_name is not None

    @property
    def failed(self) -> bool:
        """True when the message was neither sent nor intentionally skipped."""
        return not self.success and self.skipped is None


@dataclass
class UploadResult:
    """Structured result from :func:`upload_attachment`.

    Covers three outcomes: direct Chat upload, Drive upload, or skip
    (e.g. Google Docs links that don't need re-uploading).
    """

    upload_type: str | None = None  # "direct", "drive", "skip", or None
    drive_id: str | None = None
    url: str | None = None
    attachment_ref: dict[str, Any] | None = None
    name: str | None = None
    mime_type: str | None = None
    skip_reason: str | None = None
    error: str | None = None
    cached: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        """True when the file was uploaded (direct or Drive)."""
        return self.upload_type in ("direct", "drive")

    @property
    def skipped(self) -> bool:
        """True when the file was intentionally skipped."""
        return self.upload_type == "skip"
