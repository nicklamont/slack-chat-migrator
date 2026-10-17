"""
Folder management for Google Drive integration.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Callable

from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DRIVE_FILES_PAGE_SIZE,
    DRIVE_FOLDER_MIME_TYPE,
    DRIVE_PROTECTED_ROLES,
    DRIVE_ROLE_READER,
    HTTP_BAD_REQUEST,
)
from slack_chat_migrator.services.drive_adapter import PermissionChange
from slack_chat_migrator.utils.api import escape_drive_query_value
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter


class FolderManager:
    """Manages folder creation and organization in Google Drive."""

    def __init__(
        self,
        drive_service: DriveAdapter,
        workspace_domain: str | None = None,
        *,
        is_external: Callable[[str], bool] | None = None,
        protected_emails: Callable[[], Iterable[str]] | None = None,
    ) -> None:
        """Initialize the FolderManager.

        Args:
            drive_service: DriveAdapter instance for Google Drive API calls
            workspace_domain: The workspace domain for permissions
            is_external: Tells whether an email belongs to an external user,
                who is never granted access.  Defaults to comparing the
                email's domain with *workspace_domain*.
            protected_emails: Returns accounts whose folder access a
                permission sync must never remove (the workspace admin).
        """
        self.drive_service = drive_service
        self.workspace_domain = workspace_domain
        self._is_external = is_external or self._is_outside_workspace_domain
        self._protected_emails = protected_emails or (lambda: ())
        # (parent_folder_id, folder name) -> folder ID, shared by every lookup.
        # Folders are created or found once per run; the cache is trusted.
        self.folder_cache: dict[tuple[str, str], str] = {}
        # Folders known to have inherited permissions disabled, and folders
        # Drive refused to restrict (so it isn't retried for every file).
        self.restricted_folders: set[str] = set()
        self._restriction_failed: set[str] = set()

    def _is_outside_workspace_domain(self, email: str) -> bool:
        """Default external check: the email's domain isn't the workspace's."""
        if not self.workspace_domain:
            return False
        return email.rsplit("@", 1)[-1].lower() != self.workspace_domain.lower()

    def remember_folder(
        self,
        parent_folder_id: str,
        name: str,
        folder_id: str,
        *,
        restricted: bool = False,
    ) -> None:
        """Record a known folder so lookups by name need no API call.

        The first folder seen for a name wins, matching what a search returns.

        Args:
            parent_folder_id: Parent folder ID.
            name: Folder name.
            folder_id: Folder ID.
            restricted: Whether the folder has ``inheritedPermissionsDisabled``.
        """
        self.folder_cache.setdefault(
            (parent_folder_id, self._sanitize_folder_name(name)), folder_id
        )
        if restricted:
            self.restricted_folders.add(folder_id)

    def ensure_restricted(
        self, folder_id: str, channel: str, shared_drive_id: str | None = None
    ) -> bool:
        """Turn *folder_id* into a limited-access folder, if it isn't one yet.

        A limited-access folder (``inheritedPermissionsDisabled``) is open
        only to people with direct access, plus shared drive managers, not
        to every member of the shared drive.

        Args:
            folder_id: Folder to restrict.
            channel: Channel name, for logging.
            shared_drive_id: ID of the shared drive (if applicable).

        Returns:
            True if the folder is restricted.
        """
        if folder_id in self.restricted_folders:
            return True
        if folder_id in self._restriction_failed:
            return False
        try:
            response = self.drive_service.update_file(
                folder_id,
                {"inheritedPermissionsDisabled": True},
                fields="id, inheritedPermissionsDisabled",
                supports_all_drives=bool(shared_drive_id),
            )
        except HttpError as e:
            self._restriction_failed.add(folder_id)
            log_with_context(
                logging.WARNING,
                f"Could not limit access to the {channel} folder ({e}); its files"
                " are visible to every member of the shared drive",
                channel=channel,
                folder_id=folder_id,
            )
            return False
        if response.get("inheritedPermissionsDisabled") is False:
            self._restriction_failed.add(folder_id)
            log_with_context(
                logging.WARNING,
                f"Drive did not limit access to the {channel} folder; its files"
                " are visible to every member of the shared drive",
                channel=channel,
                folder_id=folder_id,
            )
            return False
        self.restricted_folders.add(folder_id)
        log_with_context(
            logging.INFO,
            f"Limited access to the {channel} folder to its members",
            channel=channel,
            folder_id=folder_id,
        )
        return True

    def list_child_folders(
        self, parent_folder_id: str, shared_drive_id: str | None = None
    ) -> list[dict[str, Any]]:
        """List every non-trashed folder directly inside *parent_folder_id*.

        All pages are read.

        Args:
            parent_folder_id: Parent folder (or shared drive root) ID.
            shared_drive_id: ID of the shared drive (if applicable).

        Returns:
            Folder resources with ``id``, ``name`` and
            ``inheritedPermissionsDisabled``.

        Raises:
            HttpError: If a page can't be listed.
        """
        safe_parent = escape_drive_query_value(parent_folder_id)
        query = (
            f"'{safe_parent}' in parents and mimeType = '{DRIVE_FOLDER_MIME_TYPE}'"
            " and trashed = false"
        )
        list_kwargs: dict[str, Any] = {
            "q": query,
            "fields": "nextPageToken, files(id, name, inheritedPermissionsDisabled)",
            "page_size": DRIVE_FILES_PAGE_SIZE,
        }
        if shared_drive_id:
            list_kwargs.update(
                {
                    "corpora": "drive",
                    "drive_id": shared_drive_id,
                    "include_items_from_all_drives": True,
                    "supports_all_drives": True,
                }
            )

        folders: list[dict[str, Any]] = []
        page_token: str | None = None
        while True:
            response = self.drive_service.list_files(
                **list_kwargs, page_token=page_token
            )
            folders.extend(f for f in response.get("files", []) if f.get("id"))
            page_token = response.get("nextPageToken")
            if not page_token:
                return folders

    def create_regular_drive_folder(self, folder_name: str) -> str | None:
        """Create a regular Drive folder with domain permissions as fallback.

        Args:
            folder_name: Name of the folder to create

        Returns:
            Folder ID if successful, None otherwise
        """
        try:
            # Search for existing folder
            safe_name = escape_drive_query_value(folder_name)
            query = f"name = '{safe_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"

            log_with_context(
                logging.DEBUG,
                f"Searching for existing regular Drive folder: {folder_name}",
            )

            results = self.drive_service.list_files(q=query, fields="files(id, name)")

            files = results.get("files", [])
            if files:
                existing_folder_id: str | None = files[0]["id"]
                log_with_context(
                    logging.INFO,
                    f"Found existing regular Drive folder: {folder_name} (ID: {existing_folder_id})",
                )

                # Note: No domain-wide permissions set to avoid org-wide access
                # Individual channel folders will have their own space-specific permissions
                return existing_folder_id

            # Create new folder
            log_with_context(
                logging.INFO, f"Creating regular Drive folder: {folder_name}"
            )

            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
            }
            folder = self.drive_service.create_file(body=folder_metadata, fields="id")

            new_folder_id: str | None = folder.get("id")
            # Note: No domain-wide permissions set to avoid org-wide access
            # Individual channel folders will have their own space-specific permissions

            log_with_context(
                logging.INFO,
                f"Successfully created regular Drive folder: {folder_name} (ID: {new_folder_id})",
            )

            return new_folder_id

        except HttpError as e:
            log_with_context(
                logging.ERROR, f"Failed to create regular Drive folder: {e}"
            )
            return None

    def _folder_query(self, name: str, parent_folder_id: str) -> str:
        """Drive query for a non-trashed folder named *name* in a parent."""
        safe_name = escape_drive_query_value(name)
        safe_parent = escape_drive_query_value(parent_folder_id)
        return (
            f"name = '{safe_name}' and mimeType = '{DRIVE_FOLDER_MIME_TYPE}'"
            f" and '{safe_parent}' in parents and trashed = false"
        )

    def _search_folder(
        self, name: str, parent_folder_id: str, shared_drive_id: str | None
    ) -> str | None:
        """Find a non-trashed folder by name; cache and return its ID.

        Raises:
            HttpError: If the search fails.
        """
        query = self._folder_query(name, parent_folder_id)
        fields = "files(id, name, inheritedPermissionsDisabled)"
        if shared_drive_id:
            results = self.drive_service.list_files(
                q=query,
                corpora="drive",
                drive_id=shared_drive_id,
                include_items_from_all_drives=True,
                supports_all_drives=True,
                fields=fields,
            )
        else:
            results = self.drive_service.list_files(q=query, fields=fields)
        items = results.get("files", [])
        found: str | None = items[0].get("id") if items else None
        if found:
            self.remember_folder(
                parent_folder_id,
                name,
                found,
                restricted=bool(items[0].get("inheritedPermissionsDisabled")),
            )
        return found

    def get_or_create_channel_folder(
        self,
        channel: str,
        parent_folder_id: str,
        shared_drive_id: str | None = None,
        *,
        restricted: bool = False,
    ) -> str | None:
        """Get or create a channel-specific folder.

        Args:
            channel: The channel name
            parent_folder_id: ID of the parent folder
            shared_drive_id: ID of the shared drive (if applicable)
            restricted: Make it a limited-access folder
                (``inheritedPermissionsDisabled``), for private channels and
                DMs.  An existing folder is restricted too.

        Returns:
            The folder ID if successful, None otherwise
        """
        name = self._sanitize_folder_name(channel)
        folder_id = self.folder_cache.get((parent_folder_id, name))

        try:
            if not folder_id:
                log_with_context(
                    logging.DEBUG,
                    f"Searching for existing channel folder: {channel}",
                    channel=channel,
                )
                folder_id = self._search_folder(name, parent_folder_id, shared_drive_id)
                if folder_id:
                    log_with_context(
                        logging.DEBUG,
                        f"Found existing channel folder: {channel} (ID: {folder_id})",
                        channel=channel,
                    )
            if not folder_id:
                folder_id = self._create_channel_folder(
                    channel, name, parent_folder_id, shared_drive_id, restricted
                )
                if not folder_id:
                    return None
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to get or create channel folder {channel}: {e}",
                channel=channel,
                error=str(e),
            )
            return None

        if restricted:
            self.ensure_restricted(folder_id, channel, shared_drive_id)
        return folder_id

    def _create_channel_folder(
        self,
        channel: str,
        name: str,
        parent_folder_id: str,
        shared_drive_id: str | None,
        restricted: bool,
    ) -> str | None:
        """Create a channel folder, limited-access when *restricted*.

        Raises:
            HttpError: If the folder can't be created.
        """
        log_with_context(
            logging.INFO, f"Creating channel folder: {channel}", channel=channel
        )
        folder_metadata: dict[str, Any] = {
            "name": name,
            "mimeType": DRIVE_FOLDER_MIME_TYPE,
            "parents": [parent_folder_id],
        }
        if restricted:
            folder_metadata["inheritedPermissionsDisabled"] = True
        fields = "id, inheritedPermissionsDisabled"
        supports_all_drives = bool(shared_drive_id)
        try:
            folder = self.drive_service.create_file(
                body=folder_metadata,
                fields=fields,
                supports_all_drives=supports_all_drives,
            )
        except HttpError as e:
            if not restricted or e.resp.status != HTTP_BAD_REQUEST:
                raise
            # Create it open, then restrict it with an update.
            folder_metadata.pop("inheritedPermissionsDisabled")
            folder = self.drive_service.create_file(
                body=folder_metadata,
                fields=fields,
                supports_all_drives=supports_all_drives,
            )

        created_folder_id: str | None = folder.get("id")
        if not created_folder_id:
            log_with_context(
                logging.WARNING,
                f"Failed to create channel folder: {channel}",
                channel=channel,
            )
            return None

        log_with_context(
            logging.INFO,
            f"Successfully created channel folder: {channel} (ID: {created_folder_id})",
            channel=channel,
        )
        # Members are granted access after the migration, with
        # set_channel_folder_permissions.
        self.remember_folder(
            parent_folder_id,
            name,
            created_folder_id,
            restricted=bool(folder.get("inheritedPermissionsDisabled")),
        )
        return created_folder_id

    def get_channel_folder_id(
        self, channel: str, parent_folder_id: str, shared_drive_id: str | None = None
    ) -> str | None:
        """Get the ID of a channel folder if it exists.

        This is a read-only operation that doesn't create a folder.  It uses
        the same cache as the upload path, and never returns a trashed
        folder.

        Args:
            channel: Name of the channel
            parent_folder_id: ID of the parent folder
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            Folder ID if found, None otherwise
        """
        name = self._sanitize_folder_name(channel)
        cached = self.folder_cache.get((parent_folder_id, name))
        if cached:
            log_with_context(
                logging.DEBUG,
                f"Found cached channel folder ID for {channel}: {cached}",
                channel=channel,
            )
            return cached

        try:
            found_folder_id = self._search_folder(
                name, parent_folder_id, shared_drive_id
            )
            if found_folder_id:
                log_with_context(
                    logging.DEBUG,
                    f"Found existing channel folder: {channel} (ID: {found_folder_id})",
                    channel=channel,
                )
                return found_folder_id

            log_with_context(
                logging.DEBUG,
                f"No existing channel folder found for {channel}",
                channel=channel,
            )
            return None

        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Failed to get channel folder ID for {channel}: {e}",
                channel=channel,
                error=str(e),
            )
            return None

    def set_channel_folder_permissions(
        self,
        folder_id: str,
        channel: str,
        user_emails: list[str],
        shared_drive_id: str | None = None,
    ) -> bool:
        """Make the folder's member list match *user_emails*.

        Members without access get ``reader``; direct user permissions of
        people who are no longer members are removed.  Never removed: the
        workspace admin (and other protected accounts), owners, organizers
        and file organizers, inherited permissions, and non-user permissions.
        External users are never granted access.  Files inside inherit the
        folder's permissions, so only posters get per-file grants.

        Args:
            folder_id: ID of the channel folder
            channel: Channel name (for logging)
            user_emails: Emails of the people who should have access
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            True if every change succeeded, False otherwise
        """
        supports_all_drives = bool(shared_drive_id)
        protected = {email.lower() for email in self._protected_emails() if email}

        wanted: dict[str, str] = {}
        for email in user_emails:
            if not email:
                continue
            if self._is_external(email):
                log_with_context(
                    logging.DEBUG,
                    f"Not granting folder access to external user {email}",
                    channel=channel,
                    folder_id=folder_id,
                )
                continue
            wanted.setdefault(email.lower(), email)

        try:
            existing = self.drive_service.list_permissions(
                folder_id, supports_all_drives=supports_all_drives
            )
        except HttpError as e:
            log_with_context(
                logging.WARNING,
                f"Could not list permissions of the {channel} folder ({e});"
                " only adding members",
                channel=channel,
                folder_id=folder_id,
            )
            existing = []

        changes: list[PermissionChange] = []
        has_access: set[str] = set()
        for perm in existing:
            email = (perm.get("emailAddress") or "").lower()
            if perm.get("type") != "user" or not email:
                continue
            if email in wanted:
                has_access.add(email)
            elif not self._is_protected(perm, email, protected):
                changes.append(
                    PermissionChange(
                        folder_id,
                        permission_id=perm["id"],
                        supports_all_drives=supports_all_drives,
                    )
                )

        removals = len(changes)
        for key, email in wanted.items():
            if key not in has_access:
                changes.append(
                    PermissionChange(
                        folder_id,
                        body={
                            "type": "user",
                            "role": DRIVE_ROLE_READER,
                            "emailAddress": email,
                        },
                        supports_all_drives=supports_all_drives,
                    )
                )

        errors = self.drive_service.apply_permission_changes(changes) if changes else []
        failed_count = 0
        for change, error in zip(changes, errors):
            if error is None:
                continue
            failed_count += 1
            target = (change.body or {}).get("emailAddress") or change.permission_id
            action = "remove" if change.permission_id else "grant"
            log_with_context(
                logging.WARNING,
                f"Failed to {action} access for {target} on channel folder {channel}: {error}",
                channel=channel,
                folder_id=folder_id,
            )

        log_with_context(
            logging.INFO,
            f"Synced channel folder permissions for {channel}:"
            f" {len(changes) - removals} added, {removals} removed,"
            f" {failed_count} failed",
            channel=channel,
            folder_id=folder_id,
            success_count=len(changes) - failed_count,
            failed_count=failed_count,
        )

        return failed_count == 0

    @staticmethod
    def _is_protected(perm: dict[str, Any], email: str, protected: set[str]) -> bool:
        """Whether a sync must keep this permission."""
        if email in protected or perm.get("role") in DRIVE_PROTECTED_ROLES:
            return True
        details = perm.get("permissionDetails") or []
        return any(detail.get("inherited") for detail in details)

    def _sanitize_folder_name(self, folder_name: str) -> str:
        """Sanitize folder name for consistent matching.

        Args:
            folder_name: Name of the folder

        Returns:
            Sanitized folder name
        """
        # Replace special characters with underscore
        return folder_name.strip()
