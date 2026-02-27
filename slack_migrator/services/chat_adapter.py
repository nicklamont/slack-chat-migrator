"""Typed adapter for the Google Chat API.

Replaces raw ``chat.spaces().messages().create(...).execute()`` chains
with explicit method calls that are easier to mock, test, and type-check.

The adapter delegates to a pre-built (and retry-wrapped) Chat service
object, so it does **not** add its own retry logic.
"""

from __future__ import annotations

from typing import Any


class ChatAdapter:
    """Thin typed wrapper around the Google Chat API service."""

    def __init__(self, service: Any) -> None:
        self._svc = service

    # -- Spaces ---------------------------------------------------------------

    def list_spaces(
        self,
        page_size: int = 100,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        """List spaces visible to the authenticated user.

        Args:
            page_size: Maximum spaces per page.
            page_token: Continuation token from a previous response.

        Returns:
            Raw API response dict with ``spaces`` and ``nextPageToken`` keys.
        """
        kwargs: dict[str, Any] = {"pageSize": page_size}
        if page_token:
            kwargs["pageToken"] = page_token
        result: dict[str, Any] = self._svc.spaces().list(**kwargs).execute()
        return result

    def get_space(self, name: str) -> dict[str, Any]:
        """Get a single space by resource name.

        Args:
            name: Space resource name (e.g. ``spaces/AAAA``).

        Returns:
            Space resource dict.
        """
        result: dict[str, Any] = self._svc.spaces().get(name=name).execute()
        return result

    def create_space(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a new space.

        Args:
            body: Space resource body.

        Returns:
            Created space resource dict.
        """
        result: dict[str, Any] = self._svc.spaces().create(body=body).execute()
        return result

    def patch_space(
        self,
        name: str,
        update_mask: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """Update a space.

        Args:
            name: Space resource name.
            update_mask: Comma-separated field mask.
            body: Fields to update.

        Returns:
            Updated space resource dict.
        """
        result: dict[str, Any] = (
            self._svc.spaces()
            .patch(name=name, updateMask=update_mask, body=body)
            .execute()
        )
        return result

    def complete_import(self, name: str) -> dict[str, Any]:
        """Complete import mode for a space.

        Args:
            name: Space resource name.

        Returns:
            API response dict.
        """
        result: dict[str, Any] = self._svc.spaces().completeImport(name=name).execute()
        return result

    def delete_space(self, name: str) -> dict[str, Any]:
        """Delete a space.

        Args:
            name: Space resource name.

        Returns:
            API response dict (typically empty on success).
        """
        result: dict[str, Any] = self._svc.spaces().delete(name=name).execute()
        return result

    # -- Messages -------------------------------------------------------------

    def create_message(
        self,
        parent: str,
        body: dict[str, Any],
        message_id: str | None = None,
        message_reply_option: str | None = None,
    ) -> dict[str, Any]:
        """Create a message in a space.

        Args:
            parent: Space resource name (e.g. ``spaces/AAAA``).
            body: Message resource body.
            message_id: Optional client-assigned message ID.
            message_reply_option: Optional reply threading option.

        Returns:
            Created message resource dict.
        """
        kwargs: dict[str, Any] = {"parent": parent, "body": body}
        if message_id is not None:
            kwargs["messageId"] = message_id
        if message_reply_option is not None:
            kwargs["messageReplyOption"] = message_reply_option
        result: dict[str, Any] = (
            self._svc.spaces().messages().create(**kwargs).execute()
        )
        return result

    def list_messages(
        self,
        parent: str,
        page_size: int = 25,
        order_by: str | None = None,
    ) -> dict[str, Any]:
        """List messages in a space.

        Args:
            parent: Space resource name.
            page_size: Maximum messages per page.
            order_by: Optional ordering (e.g. ``"createTime desc"``).

        Returns:
            Raw API response dict with ``messages`` key.
        """
        kwargs: dict[str, Any] = {"parent": parent, "pageSize": page_size}
        if order_by is not None:
            kwargs["orderBy"] = order_by
        result: dict[str, Any] = self._svc.spaces().messages().list(**kwargs).execute()
        return result

    # -- Reactions ------------------------------------------------------------

    def create_reaction(
        self,
        parent: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """Add a reaction to a message.

        Args:
            parent: Message resource name (e.g.
                ``spaces/AAAA/messages/BBBB``).
            body: Reaction body with ``emoji`` field.

        Returns:
            Created reaction resource dict.
        """
        result: dict[str, Any] = (
            self._svc.spaces()
            .messages()
            .reactions()
            .create(parent=parent, body=body)
            .execute()
        )
        return result

    def build_create_reaction_request(
        self,
        parent: str,
        body: dict[str, Any],
    ) -> Any:
        """Build a reaction create request without executing it.

        Useful for batching multiple reactions into a single HTTP request.

        Args:
            parent: Message resource name.
            body: Reaction body.

        Returns:
            An un-executed API request object suitable for batching.
        """
        return (
            self._svc.spaces().messages().reactions().create(parent=parent, body=body)
        )

    def new_batch_http_request(self, callback: Any = None) -> Any:
        """Create a new batch HTTP request.

        Args:
            callback: Optional callback for batch results.

        Returns:
            A ``BatchHttpRequest`` object.
        """
        if callback is not None:
            return self._svc.new_batch_http_request(callback=callback)
        return self._svc.new_batch_http_request()

    # -- Memberships ----------------------------------------------------------

    def create_membership(
        self,
        parent: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """Add a member to a space.

        Args:
            parent: Space resource name.
            body: Membership body (must include ``member`` field).

        Returns:
            Created membership resource dict.
        """
        result: dict[str, Any] = (
            self._svc.spaces().members().create(parent=parent, body=body).execute()
        )
        return result

    def list_memberships(
        self,
        parent: str,
        page_size: int = 100,
        page_token: str | None = None,
    ) -> dict[str, Any]:
        """List memberships in a space.

        Args:
            parent: Space resource name.
            page_size: Maximum members per page.
            page_token: Continuation token.

        Returns:
            Raw API response dict with ``memberships`` key.
        """
        kwargs: dict[str, Any] = {"parent": parent, "pageSize": page_size}
        if page_token:
            kwargs["pageToken"] = page_token
        result: dict[str, Any] = self._svc.spaces().members().list(**kwargs).execute()
        return result

    def delete_membership(self, name: str) -> dict[str, Any]:
        """Remove a member from a space.

        Args:
            name: Membership resource name
                (e.g. ``spaces/AAAA/members/BBBB``).

        Returns:
            API response dict.
        """
        result: dict[str, Any] = (
            self._svc.spaces().members().delete(name=name).execute()
        )
        return result
