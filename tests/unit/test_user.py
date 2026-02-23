"""Unit tests for the user mapping module."""

import json
from pathlib import Path

import pytest

from slack_migrator.services.user import generate_user_map


def _write_users_json(tmpdir: Path, users: list) -> Path:
    """Helper to write a users.json file in a temp directory."""
    users_file = tmpdir / "users.json"
    users_file.write_text(json.dumps(users))
    return users_file


class TestGenerateUserMap:
    """Tests for generate_user_map()."""

    def test_basic_mapping(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
            {
                "id": "U002",
                "name": "bob",
                "profile": {"email": "bob@example.com"},
            },
        ]
        _write_users_json(tmp_path, users)

        user_map, without_email = generate_user_map(tmp_path, {})

        assert user_map["U001"] == "alice@example.com"
        assert user_map["U002"] == "bob@example.com"
        assert len(without_email) == 0

    def test_email_domain_override(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@old.com"},
            },
        ]
        _write_users_json(tmp_path, users)
        config = {"email_domain_override": "new.com"}

        user_map, _ = generate_user_map(tmp_path, config)

        assert user_map["U001"] == "alice@new.com"

    def test_user_mapping_overrides(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
        ]
        _write_users_json(tmp_path, users)
        config = {"user_mapping_overrides": {"U001": "override@example.com"}}

        user_map, _ = generate_user_map(tmp_path, config)

        assert user_map["U001"] == "override@example.com"

    def test_override_for_user_not_in_users_json(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
        ]
        _write_users_json(tmp_path, users)
        config = {"user_mapping_overrides": {"U999": "external@other.com"}}

        user_map, _ = generate_user_map(tmp_path, config)

        assert user_map["U999"] == "external@other.com"

    def test_missing_email_tracked(self, tmp_path):
        users = [
            {"id": "U001", "name": "noemail", "profile": {}},
        ]
        _write_users_json(tmp_path, users)

        user_map, without_email = generate_user_map(
            tmp_path, {"user_mapping_overrides": {"U001": "fallback@co.com"}}
        )

        # Override takes precedence, so user IS in user_map
        assert user_map["U001"] == "fallback@co.com"

    def test_missing_email_without_override(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
            {"id": "U002", "name": "noemail", "profile": {}},
        ]
        _write_users_json(tmp_path, users)

        user_map, without_email = generate_user_map(tmp_path, {})

        assert "U002" not in user_map
        assert len(without_email) == 1
        assert without_email[0]["id"] == "U002"

    def test_ignore_bots(self, tmp_path):
        users = [
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
            {
                "id": "B001",
                "name": "botuser",
                "is_bot": True,
                "profile": {"email": "bot@example.com"},
            },
        ]
        _write_users_json(tmp_path, users)
        config = {"ignore_bots": True}

        user_map, _ = generate_user_map(tmp_path, config)

        assert "U001" in user_map
        assert "B001" not in user_map

    def test_bots_included_by_default(self, tmp_path):
        users = [
            {
                "id": "B001",
                "name": "botuser",
                "is_bot": True,
                "profile": {"email": "bot@example.com"},
            },
        ]
        _write_users_json(tmp_path, users)

        user_map, _ = generate_user_map(tmp_path, {})

        assert "B001" in user_map

    def test_missing_users_json_exits(self, tmp_path):
        with pytest.raises(SystemExit):
            generate_user_map(tmp_path, {})

    def test_user_without_id_skipped(self, tmp_path):
        users = [
            {"name": "noid", "profile": {"email": "noid@example.com"}},
            {
                "id": "U001",
                "name": "alice",
                "profile": {"email": "alice@example.com"},
            },
        ]
        _write_users_json(tmp_path, users)

        user_map, _ = generate_user_map(tmp_path, {})

        assert len(user_map) == 1
        assert "U001" in user_map
