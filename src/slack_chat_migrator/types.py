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
    """File upload statistics returned by ``FileHandler.get_file_statistics``.

    They appear as ``file_upload_details`` in the migration report.
    """

    #: Attachments handled, one per ``upload_attachment`` call.
    total_files_processed: int
    #: Files uploaded to Drive or Chat, or linked again from Drive.
    successful_uploads: int
    #: Files that exist but couldn't be uploaded (see ``UploadResult.retryable``).
    failed_uploads: int
    #: Files no retry or token can fetch: deleted, hidden, external, no
    #: URL, or answering 404/410.
    unavailable_files: int
    #: Files deliberately not uploaded (Google Docs and Sheets links).
    skipped_files: int
    #: Successful uploads (and relinks) that went to Drive.
    drive_uploads: int
    #: Successful uploads that went to Chat directly.
    direct_uploads: int
    #: Attachments uploaded to Slack by users outside the workspace.
    external_user_files: int
    #: Ownership transfers (``transfer_file_ownership``) that worked.
    ownership_transferred: int
    #: Ownership transfers that failed (flagged for the end-of-run sweep).
    ownership_transfer_failed: int
    #: Poster writer grants that failed (flagged for the end-of-run sweep).
    poster_grant_failures: int
    #: Sweep attempts (grants and transfers) that now worked.
    pending_retried: int
    #: Sweep attempts that still failed; their flags are kept.
    pending_retry_failed: int
    #: Pending flags that couldn't be stored on their Drive file.
    pending_flag_failures: int
    #: Flags of successful retries that couldn't be cleared (cleared by the
    #: next run's sweep).
    pending_clear_failures: int
    #: Sweeps whose search for flagged files failed, that a used-up quota
    #: ended, or that crashed.
    pending_sweep_failures: int
    #: Flags kept without a retry because the recipient wasn't stored.
    pending_skipped_unknown: int
    #: Flags kept because the Slack user is no longer mapped.
    pending_skipped_unmapped: int
    #: Flags kept because the recipient is now external.
    pending_skipped_external: int
    #: Pending transfers kept because transfers are off (or on a shared drive).
    pending_skipped_transfers_off: int
    #: Pending transfers kept because someone other than the admin owns the
    #: file, and only its owner can transfer it.
    pending_skipped_not_owner: int
    #: Drive folders whose file listing failed at least once.
    folder_listing_failures: int
    #: Channel-folder grants to external members that Drive refused.
    external_folder_grant_failures: int
    #: Channel-folder permission syncs that failed (one per channel).
    folder_sync_failures: int
    #: Channel-folder grants to internal members that failed.
    folder_grant_failures: int
    #: Channel-folder removals of former members that failed.
    folder_removal_failures: int
    #: Files not uploaded because their private conversation's folder
    #: couldn't be made limited-access.
    restricted_folder_failures: int
    #: Files not uploaded because their channel folder couldn't be found or
    #: created.
    channel_folder_failures: int
    #: Downloads refused with 401/403 or a sign-in page.
    download_auth_failures: int
    #: Files per channel.
    files_by_channel: dict[str, int]
    #: Share of uploads that succeeded, out of those that succeeded or
    #: failed (unavailable and skipped files don't lower it).
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
    #: For a failed upload: True when a later attempt may succeed (a network
    #: or server error, a rate limit, a folder or Drive unavailable for such
    #: a reason, a dedup search that failed), so the message should be held
    #: back and retried.  False for permanent failures (a refused or
    #: wrong-sized download, a host off the allow-list, a 4xx refusal).
    retryable: bool = False

    def __post_init__(self) -> None:
        """Reject ``retryable`` on an upload that didn't fail."""
        if self.retryable and (self.success or self.skipped):
            raise ValueError("only a failed upload can be retryable")

    @property
    def success(self) -> bool:
        """True when the file was uploaded (direct or Drive)."""
        return self.upload_type in ("direct", "drive")

    @property
    def skipped(self) -> bool:
        """True when the file was intentionally skipped."""
        return self.upload_type == "skip"
