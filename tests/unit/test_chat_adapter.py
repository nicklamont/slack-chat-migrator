"""Unit tests for the ChatAdapter."""

from unittest.mock import MagicMock

import pytest

from slack_migrator.services.chat_adapter import ChatAdapter


@pytest.fixture()
def mock_service():
    """Return a deeply-mocked Chat API service."""
    return MagicMock(name="chat_service")


@pytest.fixture()
def adapter(mock_service):
    """Return a ChatAdapter wrapping the mock service."""
    return ChatAdapter(mock_service)


# ---------------------------------------------------------------------------
# Spaces
# ---------------------------------------------------------------------------


class TestListSpaces:
    def test_default_page_size(self, adapter, mock_service):
        adapter.list_spaces()

        mock_service.spaces().list.assert_called_once_with(pageSize=100)
        mock_service.spaces().list().execute.assert_called_once()

    def test_custom_page_size_and_token(self, adapter, mock_service):
        adapter.list_spaces(page_size=50, page_token="abc")

        mock_service.spaces().list.assert_called_once_with(pageSize=50, pageToken="abc")

    def test_returns_api_response(self, adapter, mock_service):
        mock_service.spaces().list().execute.return_value = {
            "spaces": [{"name": "spaces/AAA"}]
        }
        result = adapter.list_spaces()
        assert result == {"spaces": [{"name": "spaces/AAA"}]}


class TestGetSpace:
    def test_calls_get_with_name(self, adapter, mock_service):
        adapter.get_space("spaces/AAA")
        mock_service.spaces().get.assert_called_once_with(name="spaces/AAA")


class TestCreateSpace:
    def test_calls_create_with_body(self, adapter, mock_service):
        body = {"displayName": "Test"}
        adapter.create_space(body)
        mock_service.spaces().create.assert_called_once_with(body=body)


class TestPatchSpace:
    def test_calls_patch_with_all_args(self, adapter, mock_service):
        adapter.patch_space("spaces/AAA", "displayName", {"displayName": "New"})
        mock_service.spaces().patch.assert_called_once_with(
            name="spaces/AAA",
            updateMask="displayName",
            body={"displayName": "New"},
        )


class TestCompleteImport:
    def test_calls_complete_import(self, adapter, mock_service):
        adapter.complete_import("spaces/AAA")
        mock_service.spaces().completeImport.assert_called_once_with(name="spaces/AAA")


class TestDeleteSpace:
    def test_calls_delete(self, adapter, mock_service):
        adapter.delete_space("spaces/AAA")
        mock_service.spaces().delete.assert_called_once_with(name="spaces/AAA")


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class TestCreateMessage:
    def test_minimal_call(self, adapter, mock_service):
        body = {"text": "hello"}
        adapter.create_message("spaces/AAA", body)
        mock_service.spaces().messages().create.assert_called_once_with(
            parent="spaces/AAA", body=body
        )

    def test_with_message_id_and_reply_option(self, adapter, mock_service):
        body = {"text": "reply"}
        adapter.create_message(
            "spaces/AAA",
            body,
            message_id="client-123",
            message_reply_option="REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD",
        )
        mock_service.spaces().messages().create.assert_called_once_with(
            parent="spaces/AAA",
            body=body,
            messageId="client-123",
            messageReplyOption="REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD",
        )

    def test_omits_none_optional_args(self, adapter, mock_service):
        adapter.create_message("spaces/AAA", {"text": "hi"})
        kwargs = mock_service.spaces().messages().create.call_args.kwargs
        assert "messageId" not in kwargs
        assert "messageReplyOption" not in kwargs


class TestListMessages:
    def test_default_args(self, adapter, mock_service):
        adapter.list_messages("spaces/AAA")
        mock_service.spaces().messages().list.assert_called_once_with(
            parent="spaces/AAA", pageSize=25
        )

    def test_with_order_by(self, adapter, mock_service):
        adapter.list_messages("spaces/AAA", page_size=1, order_by="createTime desc")
        mock_service.spaces().messages().list.assert_called_once_with(
            parent="spaces/AAA", pageSize=1, orderBy="createTime desc"
        )


# ---------------------------------------------------------------------------
# Reactions
# ---------------------------------------------------------------------------


class TestCreateReaction:
    def test_calls_reactions_create(self, adapter, mock_service):
        body = {"emoji": {"unicode": "👍"}}
        adapter.create_reaction("spaces/AAA/messages/BBB", body)
        (
            mock_service.spaces()
            .messages()
            .reactions()
            .create.assert_called_once_with(parent="spaces/AAA/messages/BBB", body=body)
        )


class TestBuildCreateReactionRequest:
    def test_returns_uneexecuted_request(self, adapter, mock_service):
        body = {"emoji": {"unicode": "👍"}}
        result = adapter.build_create_reaction_request("spaces/AAA/messages/BBB", body)
        # Should NOT call .execute()
        (
            mock_service.spaces()
            .messages()
            .reactions()
            .create.assert_called_once_with(parent="spaces/AAA/messages/BBB", body=body)
        )
        assert result is not None


class TestNewBatchHttpRequest:
    def test_with_callback(self, adapter, mock_service):
        cb = MagicMock()
        adapter.new_batch_http_request(callback=cb)
        mock_service.new_batch_http_request.assert_called_once_with(callback=cb)

    def test_without_callback(self, adapter, mock_service):
        adapter.new_batch_http_request()
        mock_service.new_batch_http_request.assert_called_once_with()


# ---------------------------------------------------------------------------
# Memberships
# ---------------------------------------------------------------------------


class TestCreateMembership:
    def test_calls_members_create(self, adapter, mock_service):
        body = {"member": {"name": "users/123", "type": "HUMAN"}}
        adapter.create_membership("spaces/AAA", body)
        mock_service.spaces().members().create.assert_called_once_with(
            parent="spaces/AAA", body=body
        )


class TestListMemberships:
    def test_default_args(self, adapter, mock_service):
        adapter.list_memberships("spaces/AAA")
        mock_service.spaces().members().list.assert_called_once_with(
            parent="spaces/AAA", pageSize=100
        )

    def test_with_page_token(self, adapter, mock_service):
        adapter.list_memberships("spaces/AAA", page_size=50, page_token="tok")
        mock_service.spaces().members().list.assert_called_once_with(
            parent="spaces/AAA", pageSize=50, pageToken="tok"
        )


class TestDeleteMembership:
    def test_calls_members_delete(self, adapter, mock_service):
        adapter.delete_membership("spaces/AAA/members/BBB")
        mock_service.spaces().members().delete.assert_called_once_with(
            name="spaces/AAA/members/BBB"
        )
