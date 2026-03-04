"""Contract tests for DryRunChatService response shapes.

Validates that the dry-run stubs return the same keys and structure
that the production pipeline reads.  Catches drift if someone modifies
a stub without updating its response fields.
"""

from __future__ import annotations

import pytest

from slack_chat_migrator.core.state import MigrationState
from slack_chat_migrator.services.chat.dry_run_service import DryRunChatService

pytestmark = pytest.mark.integration


@pytest.fixture()
def svc() -> DryRunChatService:
    return DryRunChatService(MigrationState())


class TestMessageResponseContract:
    """``spaces().messages().create()`` must return ``name`` and ``thread.name``."""

    def test_create_message_response_has_name(self, svc: DryRunChatService) -> None:
        resp = svc.spaces().messages().create(parent="spaces/s1").execute()
        assert "name" in resp
        assert resp["name"].startswith("spaces/s1/messages/")

    def test_create_message_response_has_thread(self, svc: DryRunChatService) -> None:
        resp = svc.spaces().messages().create(parent="spaces/s1").execute()
        assert "thread" in resp
        assert "name" in resp["thread"]
        assert resp["thread"]["name"].startswith("spaces/s1/threads/")


class TestSpaceResponseContract:
    """``spaces().create()`` must return ``name``."""

    def test_create_space_response_has_name(self, svc: DryRunChatService) -> None:
        resp = svc.spaces().create(body={"displayName": "test"}).execute()
        assert "name" in resp
        assert resp["name"].startswith("spaces/")


class TestMembershipResponseContract:
    """``spaces().members().create()`` must return ``name``."""

    def test_create_membership_response_has_name(self, svc: DryRunChatService) -> None:
        resp = (
            svc.spaces()
            .members()
            .create(parent="spaces/s1", body={"member": {}})
            .execute()
        )
        assert "name" in resp
        assert resp["name"].startswith("spaces/s1/members/")


class TestMediaResponseContract:
    """``media().upload()`` must return ``attachmentDataRef.resourceName``."""

    def test_upload_response_has_attachment_data_ref(
        self, svc: DryRunChatService
    ) -> None:
        resp = (
            svc.media()
            .upload(parent="spaces/s1", body={"filename": "test.txt"})
            .execute()
        )
        assert "attachmentDataRef" in resp
        assert "resourceName" in resp["attachmentDataRef"]


class TestSingletonSubObjects:
    """``spaces()`` and sub-objects return the same instance across calls."""

    def test_spaces_returns_singleton(self, svc: DryRunChatService) -> None:
        assert svc.spaces() is svc.spaces()

    def test_messages_returns_singleton(self, svc: DryRunChatService) -> None:
        assert svc.spaces().messages() is svc.spaces().messages()

    def test_members_returns_singleton(self, svc: DryRunChatService) -> None:
        assert svc.spaces().members() is svc.spaces().members()


class TestListResponseContracts:
    """List endpoints must return the keys the pipeline reads."""

    def test_list_messages_returns_messages_key(self, svc: DryRunChatService) -> None:
        resp = svc.spaces().messages().list(parent="spaces/s1").execute()
        assert "messages" in resp
        assert isinstance(resp["messages"], list)

    def test_list_spaces_returns_spaces_key(self, svc: DryRunChatService) -> None:
        resp = svc.spaces().list().execute()
        assert "spaces" in resp
        assert "nextPageToken" in resp
        assert isinstance(resp["spaces"], list)
