"""Shared type definitions for the Slack to Google Chat migration tool.

Provides TypedDicts for structured data flowing through the migration pipeline:
Slack export JSON shapes, Google API response shapes, and internal tracking types.
"""

from __future__ import annotations

from typing import TypedDict

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
