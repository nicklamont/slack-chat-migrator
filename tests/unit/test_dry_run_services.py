"""Tests for dry-run no-op API services.

Verifies that DryRunChatService and DryRunDriveService implement the
same method-chain interface as the real Google APIs and return response
shapes that callers expect.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from slack_migrator.core.state import MigrationState
from slack_migrator.services.chat.dry_run_service import (
    DryRunChatService,
    DryRunMedia,
    DryRunMembers,
    DryRunMessages,
    DryRunReactions,
    DryRunRequest,
    DryRunSpaces,
)
from slack_migrator.services.drive.dry_run_service import (
    DryRunDrives,
    DryRunDriveService,
    DryRunFiles,
    DryRunPermissions,
)

# ===================================================================
# DryRunRequest
# ===================================================================


class TestDryRunRequest:
    def test_execute_returns_data(self):
        req = DryRunRequest({"name": "spaces/abc"})
        assert req.execute() == {"name": "spaces/abc"}

    def test_execute_returns_empty_dict(self):
        req = DryRunRequest({})
        assert req.execute() == {}


# ===================================================================
# DryRunChatService — top-level wiring
# ===================================================================


class TestDryRunChatService:
    def test_spaces_returns_dry_run_spaces(self):
        svc = DryRunChatService(MigrationState())
        assert isinstance(svc.spaces(), DryRunSpaces)

    def test_media_returns_dry_run_media(self):
        svc = DryRunChatService(MigrationState())
        assert isinstance(svc.media(), DryRunMedia)


# ===================================================================
# Spaces
# ===================================================================


class TestDryRunSpaces:
    def _make_spaces(self) -> DryRunSpaces:
        return DryRunSpaces(MigrationState())

    def test_create_returns_space_name(self):
        spaces = self._make_spaces()
        result = spaces.create(body={"displayName": "general"}).execute()
        assert "name" in result
        assert result["name"].startswith("spaces/dry-run-")

    def test_create_increments_counter(self):
        spaces = self._make_spaces()
        r1 = spaces.create(body={"displayName": "a"}).execute()
        r2 = spaces.create(body={"displayName": "b"}).execute()
        assert r1["name"] != r2["name"]

    def test_list_returns_empty_spaces(self):
        result = self._make_spaces().list(pageSize=50).execute()
        assert result["spaces"] == []
        assert "nextPageToken" in result

    def test_get_returns_space_shape(self):
        result = self._make_spaces().get(name="spaces/abc").execute()
        assert result["name"] == "spaces/abc"
        assert "displayName" in result
        assert "spaceType" in result

    def test_patch_returns_empty(self):
        result = (
            self._make_spaces()
            .patch(
                name="spaces/abc", updateMask="displayName", body={"displayName": "new"}
            )
            .execute()
        )
        assert result == {}

    def test_delete_returns_empty(self):
        result = self._make_spaces().delete(name="spaces/abc").execute()
        assert result == {}

    def test_complete_import_returns_empty(self):
        result = self._make_spaces().completeImport(name="spaces/abc").execute()
        assert result == {}

    def test_members_returns_dry_run_members(self):
        assert isinstance(self._make_spaces().members(), DryRunMembers)

    def test_messages_returns_dry_run_messages(self):
        assert isinstance(self._make_spaces().messages(), DryRunMessages)


# ===================================================================
# Members
# ===================================================================


class TestDryRunMembers:
    def _make_members(self) -> DryRunMembers:
        return DryRunMembers(MigrationState())

    def test_create_returns_member_name(self):
        result = (
            self._make_members()
            .create(
                parent="spaces/abc", body={"member": {"name": "users/user@test.com"}}
            )
            .execute()
        )
        assert "name" in result
        assert "dry-run" in result["name"]

    def test_list_returns_empty_memberships(self):
        result = self._make_members().list(parent="spaces/abc").execute()
        assert result["memberships"] == []

    def test_delete_returns_empty(self):
        result = self._make_members().delete(name="spaces/abc/members/123").execute()
        assert result == {}


# ===================================================================
# Messages
# ===================================================================


class TestDryRunMessages:
    def _make_messages(self) -> DryRunMessages:
        return DryRunMessages(MigrationState())

    def test_create_returns_name_and_thread(self):
        result = (
            self._make_messages()
            .create(parent="spaces/abc", body={"text": "hello"})
            .execute()
        )
        assert "name" in result
        assert "thread" in result
        assert "name" in result["thread"]

    def test_create_increments_counter(self):
        msgs = self._make_messages()
        r1 = msgs.create(parent="spaces/abc", body={"text": "a"}).execute()
        r2 = msgs.create(parent="spaces/abc", body={"text": "b"}).execute()
        assert r1["name"] != r2["name"]

    def test_list_returns_empty_messages(self):
        result = self._make_messages().list(parent="spaces/abc").execute()
        assert result["messages"] == []

    def test_reactions_returns_dry_run_reactions(self):
        assert isinstance(self._make_messages().reactions(), DryRunReactions)


# ===================================================================
# Reactions
# ===================================================================


class TestDryRunReactions:
    def test_create_returns_empty(self):
        result = (
            DryRunReactions()
            .create(
                parent="spaces/abc/messages/123",
                body={"emoji": {"unicode": "👍"}},
            )
            .execute()
        )
        assert result == {}


# ===================================================================
# Media
# ===================================================================


class TestDryRunMedia:
    def test_upload_returns_attachment_ref(self):
        result = (
            DryRunMedia()
            .upload(
                parent="spaces/abc",
                media_body=MagicMock(),
                body={"filename": "test.txt"},
            )
            .execute()
        )
        assert "attachmentDataRef" in result
        assert "resourceName" in result["attachmentDataRef"]

    def test_upload_increments_counter(self):
        media = DryRunMedia()
        r1 = media.upload(body={"filename": "a.txt"}).execute()
        r2 = media.upload(body={"filename": "b.txt"}).execute()
        assert (
            r1["attachmentDataRef"]["resourceName"]
            != r2["attachmentDataRef"]["resourceName"]
        )


# ===================================================================
# Full chain integration — mimics real usage patterns
# ===================================================================


class TestChatFullChains:
    """Test that the full method chains work end-to-end like real API calls."""

    def test_create_space_chain(self):
        svc = DryRunChatService(MigrationState())
        result = svc.spaces().create(body={"displayName": "test"}).execute()
        assert result["name"].startswith("spaces/")

    def test_send_message_chain(self):
        svc = DryRunChatService(MigrationState())
        result = (
            svc.spaces()
            .messages()
            .create(parent="spaces/abc", body={"text": "hello"})
            .execute()
        )
        assert "name" in result

    def test_add_member_chain(self):
        svc = DryRunChatService(MigrationState())
        result = (
            svc.spaces()
            .members()
            .create(
                parent="spaces/abc",
                body={"member": {"name": "users/user@test.com", "type": "HUMAN"}},
            )
            .execute()
        )
        assert "name" in result

    def test_add_reaction_chain(self):
        svc = DryRunChatService(MigrationState())
        result = (
            svc.spaces()
            .messages()
            .reactions()
            .create(
                parent="spaces/abc/messages/123",
                body={"emoji": {"unicode": "👍"}},
            )
            .execute()
        )
        assert result == {}

    def test_upload_media_chain(self):
        svc = DryRunChatService(MigrationState())
        result = (
            svc.media()
            .upload(
                parent="spaces/abc",
                media_body=MagicMock(),
                body={"filename": "photo.jpg"},
            )
            .execute()
        )
        assert "attachmentDataRef" in result

    def test_list_spaces_chain(self):
        svc = DryRunChatService(MigrationState())
        result = svc.spaces().list(pageSize=100).execute()
        assert result["spaces"] == []

    def test_complete_import_chain(self):
        svc = DryRunChatService(MigrationState())
        result = svc.spaces().completeImport(name="spaces/abc").execute()
        assert result == {}


# ===================================================================
# DryRunDriveService — top-level wiring
# ===================================================================


class TestDryRunDriveService:
    def test_files_returns_dry_run_files(self):
        svc = DryRunDriveService()
        assert isinstance(svc.files(), DryRunFiles)

    def test_permissions_returns_dry_run_permissions(self):
        svc = DryRunDriveService()
        assert isinstance(svc.permissions(), DryRunPermissions)

    def test_drives_returns_dry_run_drives(self):
        svc = DryRunDriveService()
        assert isinstance(svc.drives(), DryRunDrives)


# ===================================================================
# Files
# ===================================================================


class TestDryRunFiles:
    def test_list_returns_empty_files(self):
        result = DryRunFiles().list(q="test", fields="files(id)").execute()
        assert result["files"] == []

    def test_get_returns_file_shape(self):
        result = DryRunFiles().get(fileId="abc123", fields="id,webViewLink").execute()
        assert result["id"] == "abc123"
        assert "webViewLink" in result

    def test_create_returns_file_shape(self):
        result = (
            DryRunFiles()
            .create(body={"name": "test.txt"}, media_body=MagicMock())
            .execute()
        )
        assert "id" in result
        assert "webViewLink" in result

    def test_delete_returns_empty(self):
        result = DryRunFiles().delete(fileId="abc123").execute()
        assert result == {}


# ===================================================================
# Permissions
# ===================================================================


class TestDryRunPermissions:
    def test_create_returns_id(self):
        result = (
            DryRunPermissions()
            .create(
                fileId="abc",
                body={"type": "user", "role": "reader", "emailAddress": "a@b.com"},
            )
            .execute()
        )
        assert "id" in result


# ===================================================================
# Drives (shared drives)
# ===================================================================


class TestDryRunDrivesResource:
    def test_get_returns_drive_shape(self):
        result = DryRunDrives().get(driveId="drive123").execute()
        assert result["id"] == "drive123"
        assert "name" in result

    def test_list_returns_empty_drives(self):
        result = DryRunDrives().list().execute()
        assert result["drives"] == []

    def test_create_returns_drive_id(self):
        result = (
            DryRunDrives().create(body={"name": "My Drive"}, requestId="uuid").execute()
        )
        assert "id" in result


# ===================================================================
# Drive full chain integration
# ===================================================================


class TestDriveFullChains:
    def test_create_file_chain(self):
        svc = DryRunDriveService()
        result = (
            svc.files()
            .create(body={"name": "doc.pdf"}, media_body=MagicMock())
            .execute()
        )
        assert "id" in result

    def test_list_files_chain(self):
        svc = DryRunDriveService()
        result = svc.files().list(q="trashed=false").execute()
        assert result["files"] == []

    def test_set_permission_chain(self):
        svc = DryRunDriveService()
        result = (
            svc.permissions()
            .create(
                fileId="abc",
                body={"type": "user", "role": "reader", "emailAddress": "a@b.com"},
            )
            .execute()
        )
        assert "id" in result

    def test_validate_shared_drive_chain(self):
        svc = DryRunDriveService()
        result = svc.drives().get(driveId="drive123").execute()
        assert result["id"] == "drive123"
