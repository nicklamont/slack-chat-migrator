"""
Folder management for Google Drive integration.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from googleapiclient.errors import HttpError

from slack_chat_migrator.constants import (
    DRIVE_FILES_PAGE_SIZE,
    DRIVE_FOLDER_MIME_TYPE,
    DRIVE_PROTECTED_ROLES,
    DRIVE_ROLE_READER,
    HTTP_BAD_REQUEST,
)
from slack_chat_migrator.services.drive_adapter import (
    DRIVE_CALL_CANDIDATES,
    PermissionChange,
    http_status,
    is_drive_call_error,
    is_transient_error,
)
from slack_chat_migrator.utils.api import escape_drive_query_value
from slack_chat_migrator.utils.domains import (
    internal_domains,
    is_external_email,
)
from slack_chat_migrator.utils.logging import (
    log_with_context,
)

if TYPE_CHECKING:
    from slack_chat_migrator.services.drive_adapter import DriveAdapter


@dataclass(frozen=True)
class FolderLookup:
    """Outcome of finding (or creating) a channel folder.

    Attributes:
        folder_id: The folder, or None when there is none or it can't be used.
        error: Why the folder couldn't be found, created or used; None when
            the lookup worked (a read-only lookup may then still find no
            folder).
        restriction_failed: The folder exists but couldn't be made
            limited-access, so a private conversation's files must not go in.
        retryable: The failure may go away (a network or server error, a
            rate limit); False for a refusal such as a 403 or 404.
    """

    folder_id: str | None
    error: str | None = None
    restriction_failed: bool = False
    retryable: bool = False

    def __post_init__(self) -> None:
        """Reject a folder with an error, and failure flags without one."""
        if self.folder_id is not None and self.error is not None:
            raise ValueError("a FolderLookup with a folder has no error")
        if (self.restriction_failed or self.retryable) and not self.error:
            raise ValueError(
                "a failed restriction or a retryable failure needs an error"
            )


@dataclass(frozen=True)
class FolderSyncResult:
    """Outcome of syncing a channel folder's members (check :attr:`ok`).

    Attributes:
        folder_id: The channel folder; None when there is none (no files were
            uploaded for the channel) or the lookup failed.
        granted: Members given ``reader`` access.
        removed: Former members whose access was removed.
        failed_grants: Internal members whose grant failed.
        failed_removals: Former members (email, or permission ID) whose
            access couldn't be removed.
        external_failures: External members whose grant failed.  Outside the
            migration's control (sharing policy, no Google account), so they
            don't make the sync fail.
        lookup_error: Why the folder couldn't be looked up.
        listing_error: Why the folder's permissions couldn't be listed.
            Members were still granted access, but no one was removed.
    """

    folder_id: str | None = None
    granted: int = 0
    removed: int = 0
    failed_grants: tuple[str, ...] = ()
    failed_removals: tuple[str, ...] = ()
    external_failures: tuple[str, ...] = ()
    lookup_error: str | None = None
    listing_error: str | None = None

    @property
    def ok(self) -> bool:
        """True when every change succeeded, except grants to external members."""
        return not (
            self.lookup_error
            or self.listing_error
            or self.failed_grants
            or self.failed_removals
        )

    def describe(self) -> str:
        """One-line summary of what went wrong (empty when :attr:`ok`)."""
        problems: list[str] = []
        if self.lookup_error:
            problems.append(f"folder lookup failed: {self.lookup_error}")
        if self.listing_error:
            problems.append(
                f"permissions not listed, former members kept: {self.listing_error}"
            )
        if self.failed_grants:
            problems.append(f"{len(self.failed_grants)} member grant(s) failed")
        if self.failed_removals:
            problems.append(f"{len(self.failed_removals)} removal(s) failed")
        return "; ".join(problems)


class FolderManager:
    """Manages folder creation and organization in Google Drive."""

    def __init__(
        self,
        drive_service: DriveAdapter,
        workspace_domain: str | None = None,
        *,
        workspace_domains: Iterable[str] = (),
        is_external: Callable[[str], bool] | None = None,
        protected_emails: Callable[[], Iterable[str]] | None = None,
        on_sync_result: Callable[[str, FolderSyncResult], None] | None = None,
    ) -> None:
        """Initialize the FolderManager.

        Args:
            drive_service: DriveAdapter instance for Google Drive API calls
            workspace_domain: The workspace admin's domain.
            workspace_domains: Further internal domains
                (``config.workspace_domains``).
            is_external: Tells whether an email belongs to a user outside the
                workspace (e.g. a Slack Connect member); pass
                ``UserResolver.is_external_user``.  Defaults to checking the
                email's domain against *workspace_domain* and
                *workspace_domains*, as the resolver does.
            protected_emails: Returns accounts whose folder access a
                permission sync must never remove (the workspace admin).
            on_sync_result: Called with the channel and the result of every
                folder permission sync, including failed lookups, so the
                caller can count failures.
        """
        self.drive_service = drive_service
        self.workspace_domain = workspace_domain
        self._internal_domains = internal_domains(workspace_domain, workspace_domains)
        self._is_external = is_external or self._is_outside_workspace_domains
        self._protected_emails = protected_emails or (lambda: ())
        self._on_sync_result = on_sync_result or (lambda _channel, _result: None)
        # (parent_folder_id, folder name) -> folder ID, shared by every lookup.
        # Folders are created or found once per run; the cache is trusted.
        self.folder_cache: dict[tuple[str, str], str] = {}
        # Folders known to have inherited permissions disabled, and how often
        # restricting each other folder has failed (it is retried each time).
        self.restricted_folders: set[str] = set()
        self.restriction_failures: dict[str, int] = {}
        # Whether each folder's last failed restriction may go away.
        self.restriction_retryable: dict[str, bool] = {}

    def _is_outside_workspace_domains(self, email: str) -> bool:
        """Default external check: the email is on no internal domain."""
        return is_external_email(email, self._internal_domains)

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
        to every member of the shared drive.  A failure isn't cached: the
        next call (the next file, or the next run) tries again.  Only a
        response that confirms ``inheritedPermissionsDisabled`` counts.

        Args:
            folder_id: Folder to restrict.
            channel: Channel name, for logging.
            shared_drive_id: ID of the shared drive (if applicable).

        Returns:
            True if the folder is restricted.
        """
        if folder_id in self.restricted_folders:
            return True
        try:
            response = self.drive_service.update_file(
                folder_id,
                {"inheritedPermissionsDisabled": True},
                fields="id, inheritedPermissionsDisabled",
                supports_all_drives=bool(shared_drive_id),
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            self.restriction_retryable[folder_id] = is_transient_error(e)
            self._log_restriction_failure(folder_id, channel, str(e))
            return False
        if response.get("inheritedPermissionsDisabled") is not True:
            # Fail closed: a response that doesn't confirm it isn't proof.
            self.restriction_retryable[folder_id] = False
            self._log_restriction_failure(
                folder_id, channel, "Drive didn't confirm inherited permissions off"
            )
            return False
        self.restricted_folders.add(folder_id)
        self.restriction_failures.pop(folder_id, None)
        self.restriction_retryable.pop(folder_id, None)
        log_with_context(
            logging.INFO,
            f"Limited access to the {channel} folder to its members",
            channel=channel,
            folder_id=folder_id,
        )
        return True

    def _log_restriction_failure(
        self, folder_id: str, channel: str, cause: str
    ) -> None:
        """Count a failed restriction; log it at ERROR the first time."""
        count = self.restriction_failures.get(folder_id, 0) + 1
        self.restriction_failures[folder_id] = count
        log_with_context(
            logging.ERROR if count == 1 else logging.DEBUG,
            f"Not uploading files of private conversation {channel}: its Drive"
            f" folder could not be made limited-access ({cause}). Make the"
            " workspace admin a Manager of the shared drive, or set"
            " restrict_private_channel_folders: false to accept drive-wide"
            " visibility. Tried again with the next file.",
            channel=channel,
            folder_id=folder_id,
        )

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
            Any of ``DRIVE_CALL_ERRORS`` if a page can't be listed.
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

    def find_or_create_root_folder(self, folder_name: str) -> FolderLookup:
        """Find or create a regular Drive folder, used when there is no shared drive.

        No domain-wide permission is set, to avoid org-wide access: each
        channel folder gets its members' permissions.

        Args:
            folder_name: Name of the folder.

        Returns:
            The folder, or why it couldn't be found or created.
        """
        try:
            safe_name = escape_drive_query_value(folder_name)
            query = (
                f"name = '{safe_name}' and mimeType = '{DRIVE_FOLDER_MIME_TYPE}'"
                " and trashed = false"
            )
            log_with_context(
                logging.DEBUG,
                f"Searching for existing regular Drive folder: {folder_name}",
            )
            files = self.drive_service.list_files(
                q=query, fields="files(id, name)"
            ).get("files", [])
            if files and files[0].get("id"):
                existing_folder_id: str = files[0]["id"]
                log_with_context(
                    logging.INFO,
                    f"Found existing regular Drive folder: {folder_name}"
                    f" (ID: {existing_folder_id})",
                )
                return FolderLookup(existing_folder_id)

            log_with_context(
                logging.INFO, f"Creating regular Drive folder: {folder_name}"
            )
            folder = self.drive_service.create_file(
                body={"name": folder_name, "mimeType": DRIVE_FOLDER_MIME_TYPE},
                fields="id",
            )
            new_folder_id: str | None = folder.get("id")
            if not new_folder_id:
                return FolderLookup(None, "Drive returned no folder ID", retryable=True)
            log_with_context(
                logging.INFO,
                f"Successfully created regular Drive folder: {folder_name}"
                f" (ID: {new_folder_id})",
            )
            return FolderLookup(new_folder_id)

        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.ERROR, f"Failed to create regular Drive folder: {e}"
            )
            return FolderLookup(
                None,
                f"could not find or create folder {folder_name!r}: {e}",
                retryable=is_transient_error(e),
            )

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
            Any of ``DRIVE_CALL_ERRORS`` if the search fails.
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

    def find_or_create_channel_folder(
        self,
        channel: str,
        parent_folder_id: str,
        shared_drive_id: str | None = None,
        *,
        restricted: bool = False,
    ) -> FolderLookup:
        """Get or create a channel-specific folder.

        Args:
            channel: The channel name
            parent_folder_id: ID of the parent folder
            shared_drive_id: ID of the shared drive (if applicable)
            restricted: Make it a limited-access folder
                (``inheritedPermissionsDisabled``), for private channels and
                DMs.  An existing folder is restricted too.  If that fails,
                no folder is returned (fail closed), so the conversation's
                files are never stored in an open folder.

        Returns:
            The folder, or the reason there is none.
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
                    return FolderLookup(
                        None, "Drive returned no folder ID", retryable=True
                    )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.WARNING,
                f"Failed to get or create channel folder {channel}: {e}",
                channel=channel,
                error=str(e),
            )
            return FolderLookup(
                None,
                f"could not find or create the folder: {e}",
                retryable=is_transient_error(e),
            )

        if restricted and not self.ensure_restricted(
            folder_id, channel, shared_drive_id
        ):
            return FolderLookup(
                None,
                "the private conversation's folder could not be made limited-access",
                restriction_failed=True,
                retryable=self.restriction_retryable.get(folder_id, False),
            )
        return FolderLookup(folder_id)

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
            Any of ``DRIVE_CALL_ERRORS`` if the folder can't be created.
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
            if not restricted or http_status(e) != HTTP_BAD_REQUEST:
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
            Folder ID if found, None if there is no such folder.

        Raises:
            Any of ``DRIVE_CALL_ERRORS`` if the search fails (it never
            means "no folder").  :meth:`find_channel_folder` returns the
            error instead.
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

        found_folder_id = self._search_folder(name, parent_folder_id, shared_drive_id)
        if found_folder_id:
            log_with_context(
                logging.DEBUG,
                f"Found existing channel folder: {channel} (ID: {found_folder_id})",
                channel=channel,
            )
        else:
            log_with_context(
                logging.DEBUG,
                f"No existing channel folder found for {channel}",
                channel=channel,
            )
        return found_folder_id

    def find_channel_folder(
        self, channel: str, parent_folder_id: str, shared_drive_id: str | None = None
    ) -> FolderLookup:
        """Look up a channel folder without creating it.

        Like :meth:`get_channel_folder_id`, but a failed search is returned
        as ``FolderLookup(None, error)`` rather than raised, so it can't be
        mistaken for "no folder".
        """
        try:
            return FolderLookup(
                self.get_channel_folder_id(channel, parent_folder_id, shared_drive_id)
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.WARNING,
                f"Failed to look up the Drive folder of {channel}: {e}",
                channel=channel,
                error=str(e),
            )
            return FolderLookup(
                None,
                f"could not look up the folder: {e}",
                retryable=is_transient_error(e),
            )

    def sync_channel_folder(
        self,
        channel: str,
        parent_folder_id: str,
        user_emails: list[str],
        shared_drive_id: str | None = None,
    ) -> FolderSyncResult:
        """Find a channel's folder and make its members match *user_emails*.

        A channel without a folder (no files were uploaded) needs no sync and
        gives an ``ok`` result with no ``folder_id``.  A failed lookup gives
        a result with ``lookup_error``.  Every result, including those two,
        is passed to *on_sync_result*.

        Args:
            channel: Channel name.
            parent_folder_id: ID of the folder that holds the channel folders.
            user_emails: Emails of the people who should have access.
            shared_drive_id: ID of the shared drive (if applicable).

        Returns:
            What changed and what failed; see :class:`FolderSyncResult`.
        """
        lookup = self.find_channel_folder(channel, parent_folder_id, shared_drive_id)
        if lookup.error:
            result = FolderSyncResult(lookup_error=lookup.error)
            self._report_sync(channel, result)
            return result
        if not lookup.folder_id:
            result = FolderSyncResult()
            self._report_sync(channel, result)
            return result
        return self.set_channel_folder_permissions(
            lookup.folder_id, channel, user_emails, shared_drive_id
        )

    def set_channel_folder_permissions(
        self,
        folder_id: str,
        channel: str,
        user_emails: list[str],
        shared_drive_id: str | None = None,
    ) -> FolderSyncResult:
        """Make the folder's member list match *user_emails*.

        Every member with an email without access gets ``reader``, internal
        and external (e.g. Slack Connect) members alike; direct user
        permissions of people who are no longer members are removed.  Never
        removed: the workspace admin (and other protected accounts), owners,
        organizers and file organizers, inherited permissions, and non-user
        permissions.  Files inside inherit the folder's permissions, so only
        posters get per-file grants.

        A grant to an external address can fail for reasons outside the
        migration (domain sharing policy, no Google account).  Such failures
        are logged as warnings and don't make the sync fail.  If the
        folder's permissions can't be listed, members are still granted
        access but no one is removed, and the sync fails.  The result is
        also passed to *on_sync_result*.

        Args:
            folder_id: ID of the channel folder
            channel: Channel name (for logging)
            user_emails: Emails of the people who should have access
            shared_drive_id: ID of the shared drive (if applicable)

        Returns:
            What changed and what failed; :attr:`FolderSyncResult.ok` if
            every change succeeded, except grants to external members.
        """
        supports_all_drives = bool(shared_drive_id)
        protected = {email.lower() for email in self._protected_emails() if email}

        wanted: dict[str, str] = {}
        for email in user_emails:
            if email:
                wanted.setdefault(email.lower(), email)

        listing_error: str | None = None
        try:
            existing = self.drive_service.list_permissions(
                folder_id, supports_all_drives=supports_all_drives
            )
        except DRIVE_CALL_CANDIDATES as e:
            if not is_drive_call_error(e):
                raise
            log_with_context(
                logging.WARNING,
                f"Could not list permissions of the {channel} folder ({e});"
                " only adding members, former members keep their access",
                channel=channel,
                folder_id=folder_id,
            )
            existing = []
            listing_error = str(e)

        changes: list[PermissionChange] = []
        has_access: set[str] = set()
        removed_emails: dict[str, str] = {}  # permission ID -> email
        for perm in existing:
            email = (perm.get("emailAddress") or "").lower()
            if perm.get("type") != "user" or not email:
                continue
            if email in wanted:
                has_access.add(email)
            elif not self._is_protected(perm, email, protected):
                removed_emails[perm["id"]] = email
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
        failed_grants: list[str] = []
        failed_removals: list[str] = []
        external_failures: list[str] = []
        for change, error in zip(changes, errors, strict=True):
            if error is None:
                continue
            grantee = (change.body or {}).get("emailAddress")
            if grantee and self._is_external(grantee):
                # Outside our control (sharing policy, no Google account).
                external_failures.append(grantee)
                log_with_context(
                    logging.WARNING,
                    f"Could not give external member {grantee} access to the"
                    f" {channel} folder: {error}",
                    channel=channel,
                    folder_id=folder_id,
                )
                continue
            if grantee:
                failed_grants.append(grantee)
                action, target = "grant", grantee
            else:
                permission_id = change.permission_id or ""
                target = removed_emails.get(permission_id) or permission_id
                failed_removals.append(target)
                action = "remove"
            log_with_context(
                logging.WARNING,
                f"Failed to {action} access for {target} on channel folder"
                f" {channel}: {error}",
                channel=channel,
                folder_id=folder_id,
            )

        grants = len(changes) - removals
        result = FolderSyncResult(
            folder_id=folder_id,
            granted=grants - len(failed_grants) - len(external_failures),
            removed=removals - len(failed_removals),
            failed_grants=tuple(failed_grants),
            failed_removals=tuple(failed_removals),
            external_failures=tuple(external_failures),
            listing_error=listing_error,
        )
        log_with_context(
            logging.INFO,
            f"Synced channel folder permissions for {channel}:"
            f" {result.granted} added, {result.removed} removed,"
            f" {len(failed_grants) + len(failed_removals)} failed,"
            f" {len(external_failures)} external grants failed",
            channel=channel,
            folder_id=folder_id,
            success_count=result.granted + result.removed,
            failed_count=len(failed_grants) + len(failed_removals),
            external_failed_count=len(external_failures),
        )
        self._report_sync(channel, result)
        return result

    def _report_sync(self, channel: str, result: FolderSyncResult) -> None:
        """Log a failed sync and pass every result to *on_sync_result*."""
        if not result.ok:
            log_with_context(
                logging.ERROR,
                f"Channel folder permissions of {channel} are not in sync"
                f" ({result.describe()}); members may not be able to open its"
                " files, or former members may keep access.",
                channel=channel,
                folder_id=result.folder_id or "",
            )
        self._on_sync_result(channel, result)

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
