"""Unit tests for the unified permission validation system."""

from unittest.mock import MagicMock, patch

import pytest
from googleapiclient.errors import HttpError

from slack_migrator.exceptions import PermissionCheckError
from slack_migrator.utils.permissions import (
    PermissionCheckContext,
    PermissionValidator,
    check_permissions_standalone,
    validate_permissions,
)

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------


def _make_context(chat=None, drive=None, admin="admin@example.com"):
    """Build a PermissionCheckContext with mocked services."""
    return PermissionCheckContext(
        chat=chat or MagicMock(),
        drive=drive or MagicMock(),
        workspace_admin=admin,
    )


def _http_error(status=400, reason="Bad Request", content=b"error"):
    """Create an HttpError for testing."""
    resp = MagicMock()
    resp.status = status
    resp.reason = reason
    return HttpError(resp=resp, content=content)


def _setup_space_creation(chat, space_name="spaces/test123"):
    """Configure chat mock so space creation succeeds."""
    chat.spaces().create().execute.return_value = {"name": space_name}


def _setup_space_and_member(chat, space_name="spaces/test123"):
    """Configure chat mock so both space creation and member ops succeed."""
    _setup_space_creation(chat, space_name)
    chat.spaces().members().list().execute.return_value = {"memberships": []}
    chat.spaces().members().create().execute.return_value = {
        "name": f"{space_name}/members/m1"
    }


# -------------------------------------------------------------------
# TestPermissionValidatorInit
# -------------------------------------------------------------------


class TestPermissionValidatorInit:
    """Tests for PermissionValidator.__init__."""

    def test_accepts_permission_check_context(self):
        """Accepts a PermissionCheckContext dataclass."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        assert validator.migrator is ctx
        assert validator.permission_errors == []
        assert validator.test_resources == {}

    def test_accepts_migrator_like_object(self):
        """Accepts any object with .chat, .drive, .workspace_admin."""
        migrator = MagicMock()
        migrator.chat = MagicMock()
        migrator.drive = MagicMock()
        migrator.workspace_admin = "admin@example.com"
        validator = PermissionValidator(migrator)
        assert validator.migrator is migrator


# -------------------------------------------------------------------
# TestSpaceOperations
# -------------------------------------------------------------------


class TestSpaceOperations:
    """Tests for _test_space_operations."""

    def test_happy_path_space_created_and_listed(self):
        """Space creation and listing both succeed."""
        ctx = _make_context()
        _setup_space_creation(ctx.chat)
        ctx.chat.spaces().list().execute.return_value = {"spaces": []}

        validator = PermissionValidator(ctx)
        validator._test_space_operations()

        assert validator.permission_errors == []
        assert validator.test_resources["space"] == "spaces/test123"

    def test_creation_http_error(self):
        """Space creation HttpError logged, no crash, early return."""
        ctx = _make_context()
        ctx.chat.spaces().create().execute.side_effect = _http_error(403, "Forbidden")

        validator = PermissionValidator(ctx)
        validator._test_space_operations()

        assert len(validator.permission_errors) == 1
        assert "Space creation failed" in validator.permission_errors[0]
        assert "space" not in validator.test_resources

    def test_listing_http_error(self):
        """Space listing HttpError logged but doesn't prevent further tests."""
        ctx = _make_context()
        _setup_space_creation(ctx.chat)
        ctx.chat.spaces().list().execute.side_effect = _http_error(403, "Forbidden")

        validator = PermissionValidator(ctx)
        validator._test_space_operations()

        assert len(validator.permission_errors) == 1
        assert "Space listing failed" in validator.permission_errors[0]
        # Space was still created
        assert "space" in validator.test_resources


# -------------------------------------------------------------------
# TestMemberOperations
# -------------------------------------------------------------------


class TestMemberOperations:
    """Tests for _test_member_operations."""

    def test_happy_path_member_listed_and_created(self):
        """Member listing and creation both succeed."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources["space"] = "spaces/test123"
        validator.test_resources["space_create_time"] = "2024-01-01T00:00:00Z"

        ctx.chat.spaces().members().list().execute.return_value = {"memberships": []}
        ctx.chat.spaces().members().create().execute.return_value = {
            "name": "spaces/test123/members/m1"
        }

        validator._test_member_operations()

        assert validator.permission_errors == []

    def test_skips_when_no_test_space(self):
        """Skips member tests when no test space exists."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        # No space in test_resources

        validator._test_member_operations()

        assert validator.permission_errors == []
        ctx.chat.spaces().members().list.assert_not_called()

    def test_409_conflict_treated_as_success(self):
        """409 Already a member treated as success."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources["space"] = "spaces/test123"
        validator.test_resources["space_create_time"] = "2024-01-01T00:00:00Z"

        ctx.chat.spaces().members().list().execute.return_value = {"memberships": []}
        ctx.chat.spaces().members().create().execute.side_effect = _http_error(
            409, "Conflict"
        )

        validator._test_member_operations()

        assert validator.permission_errors == []

    def test_import_mode_normal_membership_treated_as_success(self):
        """'Adding normal memberships isn't supported' is expected in import mode."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources["space"] = "spaces/test123"
        validator.test_resources["space_create_time"] = "2024-01-01T00:00:00Z"

        ctx.chat.spaces().members().list().execute.return_value = {"memberships": []}
        err = _http_error(
            400,
            "Bad Request",
            b"Adding normal memberships isn't supported",
        )
        ctx.chat.spaces().members().create().execute.side_effect = err

        validator._test_member_operations()

        assert validator.permission_errors == []

    def test_insufficient_scopes_treated_as_error(self):
        """'insufficient authentication scopes' is a real error."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources["space"] = "spaces/test123"
        validator.test_resources["space_create_time"] = "2024-01-01T00:00:00Z"

        err = _http_error(
            403,
            "Forbidden",
            b"insufficient authentication scopes",
        )
        ctx.chat.spaces().members().list().execute.side_effect = err

        validator._test_member_operations()

        assert len(validator.permission_errors) == 1
        assert "Member listing failed" in validator.permission_errors[0]


# -------------------------------------------------------------------
# TestMessageOperations
# -------------------------------------------------------------------


class TestMessageOperations:
    """Tests for _test_message_operations."""

    def test_happy_path_message_created(self):
        """Message creation succeeds."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources["space"] = "spaces/test123"

        ctx.chat.spaces().messages().create().execute.return_value = {
            "name": "spaces/test123/messages/msg1"
        }

        validator._test_message_operations()

        assert validator.permission_errors == []
        assert validator.test_resources["message"] == "spaces/test123/messages/msg1"

    def test_skips_when_no_test_space(self):
        """Skips message tests when no test space exists."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)

        validator._test_message_operations()

        assert validator.permission_errors == []

    def test_creation_failure_logged(self):
        """Message creation failure adds to permission_errors."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources["space"] = "spaces/test123"

        ctx.chat.spaces().messages().create().execute.side_effect = _http_error(
            403, "Forbidden"
        )

        validator._test_message_operations()

        assert len(validator.permission_errors) == 1
        assert "Message creation failed" in validator.permission_errors[0]


# -------------------------------------------------------------------
# TestDriveOperations
# -------------------------------------------------------------------


class TestDriveOperations:
    """Tests for _test_drive_operations."""

    def test_happy_path_file_created_and_permission_set(self):
        """File creation and permission setting succeed."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)

        ctx.drive.files().create().execute.return_value = {"id": "file123"}
        ctx.drive.permissions().create().execute.return_value = {"id": "perm1"}

        validator._test_drive_operations()

        assert validator.permission_errors == []
        assert validator.test_resources["drive_file"] == "file123"

    def test_file_creation_fails(self):
        """File creation failure adds to permission_errors."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)

        ctx.drive.files().create().execute.side_effect = _http_error(403, "Forbidden")

        validator._test_drive_operations()

        assert len(validator.permission_errors) == 1
        assert "Drive operations failed" in validator.permission_errors[0]

    def test_permission_creation_fails(self):
        """Permission creation failure adds to permission_errors."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)

        ctx.drive.files().create().execute.return_value = {"id": "file123"}
        ctx.drive.permissions().create().execute.side_effect = _http_error(
            403, "Forbidden"
        )

        validator._test_drive_operations()

        assert len(validator.permission_errors) == 1
        assert "Drive operations failed" in validator.permission_errors[0]


# -------------------------------------------------------------------
# TestCleanup
# -------------------------------------------------------------------


class TestCleanup:
    """Tests for _cleanup_test_resources."""

    def test_cleans_up_both_space_and_drive_file(self):
        """Cleans up both space and drive file when both exist."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {
            "space": "spaces/test123",
            "drive_file": "file123",
        }

        validator._cleanup_test_resources()

        ctx.chat.spaces().delete.assert_called_once()
        ctx.drive.files().delete.assert_called_once()

    def test_handles_import_mode_deletion_errors(self):
        """Import mode deletion error logged gracefully."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {"space": "spaces/test123"}

        ctx.chat.spaces().delete().execute.side_effect = Exception(
            "import mode: cannot delete"
        )

        # Should not raise
        validator._cleanup_test_resources()

    def test_noop_when_no_resources(self):
        """No-op when no resources to clean up."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {}

        validator._cleanup_test_resources()

        ctx.chat.spaces().delete.assert_not_called()
        ctx.drive.files().delete.assert_not_called()

    def test_partial_cleanup_one_fails_other_still_attempted(self):
        """When drive cleanup fails, space cleanup still attempted."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {
            "drive_file": "file123",
            "space": "spaces/test123",
        }

        ctx.drive.files().delete().execute.side_effect = Exception("drive error")

        # Should not raise — space cleanup still attempted
        validator._cleanup_test_resources()

        ctx.chat.spaces().delete.assert_called_once()


# -------------------------------------------------------------------
# TestReportResults
# -------------------------------------------------------------------


class TestReportResults:
    """Tests for _report_results."""

    def test_no_errors_returns_true(self):
        """No errors means success."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.permission_errors = []

        assert validator._report_results() is True

    def test_has_errors_raises_permission_check_error(self):
        """Errors raise PermissionCheckError."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.permission_errors = ["error1", "error2"]

        with pytest.raises(PermissionCheckError, match="2 errors"):
            validator._report_results()

    def test_error_message_includes_count(self):
        """Error message includes number of accumulated errors."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.permission_errors = ["a", "b", "c"]

        with pytest.raises(PermissionCheckError, match="3 errors"):
            validator._report_results()


# -------------------------------------------------------------------
# TestValidateAllPermissions
# -------------------------------------------------------------------


class TestValidateAllPermissions:
    """Tests for validate_all_permissions end-to-end."""

    def test_all_pass_returns_true(self):
        """All operations pass returns True."""
        ctx = _make_context()
        # Space ops
        _setup_space_creation(ctx.chat)
        ctx.chat.spaces().list().execute.return_value = {"spaces": []}
        # Member ops
        ctx.chat.spaces().members().list().execute.return_value = {"memberships": []}
        ctx.chat.spaces().members().create().execute.return_value = {
            "name": "spaces/test123/members/m1"
        }
        # Message ops
        ctx.chat.spaces().messages().create().execute.return_value = {
            "name": "spaces/test123/messages/msg1"
        }
        # Drive ops
        ctx.drive.files().create().execute.return_value = {"id": "file123"}
        ctx.drive.permissions().create().execute.return_value = {"id": "perm1"}

        validator = PermissionValidator(ctx)
        assert validator.validate_all_permissions() is True

    def test_critical_exception_caught_and_reported(self):
        """Unexpected exception during validation is caught and reported."""
        ctx = _make_context()
        ctx.chat.spaces().create().execute.side_effect = RuntimeError("unexpected")

        validator = PermissionValidator(ctx)
        with pytest.raises(PermissionCheckError):
            validator.validate_all_permissions()

        assert any(
            "Critical validation error" in e for e in validator.permission_errors
        )

    def test_cleanup_runs_even_on_failure(self):
        """Cleanup runs even when validation raises."""
        ctx = _make_context()
        _setup_space_creation(ctx.chat)
        ctx.chat.spaces().list().execute.return_value = {"spaces": []}
        # Fail on member listing
        ctx.chat.spaces().members().list().execute.side_effect = _http_error(
            403,
            "Forbidden",
            b"insufficient authentication scopes",
        )
        # Message ops fail because member error
        ctx.chat.spaces().messages().create().execute.return_value = {
            "name": "spaces/test123/messages/msg1"
        }
        # Drive ops succeed
        ctx.drive.files().create().execute.return_value = {"id": "file123"}
        ctx.drive.permissions().create().execute.return_value = {"id": "perm1"}

        validator = PermissionValidator(ctx)
        with pytest.raises(PermissionCheckError):
            validator.validate_all_permissions()

        # Space was created, so cleanup should have been called
        ctx.chat.spaces().delete.assert_called()


# -------------------------------------------------------------------
# TestValidatePermissions
# -------------------------------------------------------------------


class TestValidatePermissions:
    """Tests for the validate_permissions convenience function."""

    def test_calls_initialize_then_delegates(self):
        """Calls _initialize_api_services() then delegates to validator."""
        migrator = MagicMock()
        migrator.chat = MagicMock()
        migrator.drive = MagicMock()
        migrator.workspace_admin = "admin@example.com"

        # Make all ops succeed
        _setup_space_creation(migrator.chat)
        migrator.chat.spaces().list().execute.return_value = {"spaces": []}
        migrator.chat.spaces().members().list().execute.return_value = {
            "memberships": []
        }
        migrator.chat.spaces().members().create().execute.return_value = {
            "name": "spaces/t/members/m"
        }
        migrator.chat.spaces().messages().create().execute.return_value = {
            "name": "spaces/t/messages/m"
        }
        migrator.drive.files().create().execute.return_value = {"id": "f1"}
        migrator.drive.permissions().create().execute.return_value = {"id": "p1"}

        result = validate_permissions(migrator)

        migrator._initialize_api_services.assert_called_once()
        assert result is True

    def test_propagates_permission_check_error(self):
        """Propagates PermissionCheckError from validator."""
        migrator = MagicMock()
        migrator.chat = MagicMock()
        migrator.drive = MagicMock()
        migrator.workspace_admin = "admin@example.com"

        migrator.chat.spaces().create().execute.side_effect = _http_error(
            403, "Forbidden"
        )

        with pytest.raises(PermissionCheckError):
            validate_permissions(migrator)


# -------------------------------------------------------------------
# TestCheckPermissionsStandalone
# -------------------------------------------------------------------


class TestCheckPermissionsStandalone:
    """Tests for check_permissions_standalone."""

    @patch("slack_migrator.utils.permissions.get_gcp_service")
    @patch("slack_migrator.utils.permissions.load_config")
    def test_happy_path(self, mock_load_config, mock_get_service, tmp_path):
        """Happy path with mocked services."""
        mock_config = MagicMock()
        mock_config.max_retries = 3
        mock_config.retry_delay = 1
        mock_load_config.return_value = mock_config

        chat_service = MagicMock()
        drive_service = MagicMock()

        # get_gcp_service called twice (chat, drive)
        mock_get_service.side_effect = [chat_service, drive_service]

        # Make all ops pass
        _setup_space_creation(chat_service)
        chat_service.spaces().list().execute.return_value = {"spaces": []}
        chat_service.spaces().members().list().execute.return_value = {
            "memberships": []
        }
        chat_service.spaces().members().create().execute.return_value = {
            "name": "spaces/t/members/m"
        }
        chat_service.spaces().messages().create().execute.return_value = {
            "name": "spaces/t/messages/m"
        }
        drive_service.files().create().execute.return_value = {"id": "f1"}
        drive_service.permissions().create().execute.return_value = {"id": "p1"}

        result = check_permissions_standalone(
            "/fake/creds.json", "admin@example.com", "config.yaml"
        )

        assert result is True
        assert mock_get_service.call_count == 2

    @patch("slack_migrator.utils.permissions.get_gcp_service")
    @patch("slack_migrator.utils.permissions.load_config")
    def test_config_not_found(self, mock_load_config, mock_get_service):
        """Config file not found raises error."""
        mock_load_config.side_effect = FileNotFoundError("config.yaml not found")

        with pytest.raises(FileNotFoundError):
            check_permissions_standalone(
                "/fake/creds.json", "admin@example.com", "missing.yaml"
            )

    @patch("slack_migrator.utils.permissions.get_gcp_service")
    @patch("slack_migrator.utils.permissions.load_config")
    def test_api_service_creation_fails(self, mock_load_config, mock_get_service):
        """API service creation failure propagates."""
        mock_config = MagicMock()
        mock_config.max_retries = 3
        mock_config.retry_delay = 1
        mock_load_config.return_value = mock_config

        mock_get_service.side_effect = ValueError("Invalid credentials")

        with pytest.raises(ValueError, match="Invalid credentials"):
            check_permissions_standalone(
                "/bad/creds.json", "admin@example.com", "config.yaml"
            )


# -------------------------------------------------------------------
# TestMemberOperationsWithoutSpaceCreateTime
# -------------------------------------------------------------------


class TestMemberOperationsEdgeCases:
    """Edge-case tests for _test_member_operations."""

    def test_fallback_when_space_create_time_missing(self):
        """Uses fallback times when space_create_time is not in test_resources."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {"space": "spaces/test123"}
        # Intentionally no space_create_time

        ctx.chat.spaces().members().list().execute.return_value = {"memberships": []}
        ctx.chat.spaces().members().create().execute.return_value = {
            "name": "spaces/test123/members/m1"
        }

        validator._test_member_operations()

        assert validator.permission_errors == []

    def test_generic_member_creation_error(self):
        """Generic HttpError during member creation is recorded."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {
            "space": "spaces/test123",
            "space_create_time": "2024-01-01T00:00:00Z",
        }

        ctx.chat.spaces().members().list().execute.return_value = {"memberships": []}
        ctx.chat.spaces().members().create().execute.side_effect = _http_error(
            500, "Internal Server Error"
        )

        validator._test_member_operations()

        assert len(validator.permission_errors) == 1
        assert "Member creation failed" in validator.permission_errors[0]

    def test_member_listing_import_mode_not_available(self):
        """'not available' in import mode is expected, not an error."""
        ctx = _make_context()
        validator = PermissionValidator(ctx)
        validator.test_resources = {
            "space": "spaces/test123",
            "space_create_time": "2024-01-01T00:00:00Z",
        }

        err = _http_error(400, "Bad Request", b"not available in import mode")
        ctx.chat.spaces().members().list().execute.side_effect = err
        ctx.chat.spaces().members().create().execute.return_value = {
            "name": "spaces/test123/members/m1"
        }

        validator._test_member_operations()

        # "not available" is expected, should NOT be added as an error
        assert validator.permission_errors == []
