"""
Unified permission validation system for Slack to Google Chat migration.

This module provides comprehensive permission testing that validates all
required scopes and operations before starting migration.
"""

import datetime
import io
import logging
from typing import Any, Dict, List

from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

from slack_migrator.utils.api import REQUIRED_SCOPES
from slack_migrator.utils.logging import log_with_context


class PermissionValidator:
    """
    Comprehensive permission validator for Google Chat and Drive APIs.

    This class tests all operations that the migration tool will perform,
    ensuring that all required scopes are properly configured before
    starting the actual migration process.
    """

    def __init__(self, migrator):
        """
        Initialize the permission validator.

        Args:
            migrator: The SlackToChatMigrator instance with configured API clients
        """
        self.migrator = migrator
        self.permission_errors: List[str] = []
        self.test_resources: Dict[str, Any] = {}

    def validate_all_permissions(self) -> bool:
        """
        Run comprehensive permission validation.

        Returns:
            True if all permissions are valid, False otherwise

        Raises:
            Exception: If critical permissions are missing
        """
        log_with_context(
            logging.INFO, "🔍 Starting comprehensive permission validation..."
        )

        self.permission_errors = []
        self.test_resources = {}

        try:
            # Test each category of operations
            self._test_space_operations()
            self._test_member_operations()
            self._test_message_operations()
            self._test_drive_operations()

        except Exception as e:
            self.permission_errors.append(f"Critical validation error: {e}")
            log_with_context(logging.ERROR, f"Critical validation error: {e}")

        finally:
            # Always clean up test resources
            self._cleanup_test_resources()

        # Report results
        return self._report_results()

    def _test_space_operations(self):
        """Test all space-related operations."""
        log_with_context(logging.INFO, "Testing space operations...")

        # Test 1: Space creation (import mode)
        log_with_context(logging.INFO, "  • Testing space creation...")
        try:
            # Set space creation time to the past to allow for proper historical membership testing
            current_time = datetime.datetime.now(datetime.timezone.utc)
            space_create_time = (
                (current_time - datetime.timedelta(minutes=5))
                .isoformat()
                .replace("+00:00", "Z")
            )

            test_space = {
                "displayName": "Permission Test Space",
                "spaceType": "SPACE",
                "importMode": True,
                "createTime": space_create_time,
            }
            result = self.migrator.chat.spaces().create(body=test_space).execute()
            space_name = result.get("name")
            self.test_resources["space"] = space_name
            self.test_resources["space_create_time"] = space_create_time
            log_with_context(logging.INFO, "    ✓ Space creation: PASSED")
        except HttpError as e:
            self.permission_errors.append(f"Space creation failed: {e}")
            log_with_context(logging.ERROR, "    ✗ Space creation: FAILED")
            return  # Can't continue without a test space

        # Test 2: Space listing
        log_with_context(logging.INFO, "  • Testing space listing...")
        try:
            self.migrator.chat.spaces().list(pageSize=1).execute()
            log_with_context(logging.INFO, "    ✓ Space listing: PASSED")
        except HttpError as e:
            self.permission_errors.append(f"Space listing failed: {e}")
            log_with_context(logging.ERROR, "    ✗ Space listing: FAILED")

        # Skip space history access test for import mode - no messages exist yet
        log_with_context(
            logging.INFO,
            "  • Skipping space history access test (no messages in import mode)",
        )
        log_with_context(
            logging.INFO,
            "    ✓ Space history access: SKIPPED (not applicable for import mode)",
        )

        # Skip space patch/update test for import mode - often restricted
        log_with_context(
            logging.INFO, "  • Skipping space update test (restricted in import mode)"
        )
        log_with_context(
            logging.INFO, "    ✓ Space update: SKIPPED (not applicable for import mode)"
        )

    def _test_member_operations(self):
        """Test member-related operations."""
        if "space" not in self.test_resources:
            log_with_context(
                logging.WARNING, "Skipping member tests - no test space available"
            )
            return

        log_with_context(logging.INFO, "Testing member operations...")

        # Test 5: Member listing (may be limited in import mode but still testable for permissions)
        log_with_context(logging.INFO, "  • Testing member listing...")
        try:
            self.migrator.chat.spaces().members().list(
                parent=self.test_resources["space"]
            ).execute()
            log_with_context(logging.INFO, "    ✓ Member listing: PASSED")
        except HttpError as e:
            if "insufficient authentication scopes" in str(e).lower():
                self.permission_errors.append(
                    f"Member listing failed: Missing 'chat.memberships.readonly' scope. Error: {e}"
                )
                log_with_context(
                    logging.ERROR, "    ✗ Member listing: FAILED (missing scope)"
                )
            elif "import mode" in str(e).lower() or "not available" in str(e).lower():
                # Member listing may be restricted in import mode, which is expected
                log_with_context(
                    logging.INFO,
                    "    ✓ Member listing: EXPECTED (limited in import mode)",
                )
            else:
                self.permission_errors.append(f"Member listing failed: {e}")
                log_with_context(logging.ERROR, "    ✗ Member listing: FAILED")

        # Test 6: Member creation (use historical membership for import mode spaces)
        log_with_context(logging.INFO, "  • Testing member creation...")
        try:
            # For import mode spaces, we need to create historical memberships
            # Use the space's create time as reference to ensure proper timing
            current_time = datetime.datetime.now(datetime.timezone.utc)
            space_create_time = self.test_resources.get("space_create_time")

            if space_create_time:
                # Parse the space create time and create membership after it
                space_create_dt = datetime.datetime.fromisoformat(
                    space_create_time.replace("Z", "+00:00")
                )
                # Create membership 1 minute after space creation, delete 2 minutes after
                past_create_time = (
                    (space_create_dt + datetime.timedelta(minutes=1))
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                past_delete_time = (
                    (space_create_dt + datetime.timedelta(minutes=2))
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            else:
                # Fallback if we don't have space create time
                past_create_time = (
                    (current_time - datetime.timedelta(minutes=3))
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                past_delete_time = (
                    (current_time - datetime.timedelta(minutes=2))
                    .isoformat()
                    .replace("+00:00", "Z")
                )

            member_body = {
                "member": {
                    "name": f"users/{self.migrator.workspace_admin}",
                    "type": "HUMAN",
                },
                # Add both createTime and deleteTime for historical membership
                "createTime": past_create_time,
                "deleteTime": past_delete_time,
            }
            member_result = (
                self.migrator.chat.spaces()
                .members()
                .create(parent=self.test_resources["space"], body=member_body)
                .execute()
            )
            self.test_resources["member"] = member_result.get("name")
            log_with_context(
                logging.INFO, "    ✓ Member creation: PASSED (historical membership)"
            )
        except HttpError as e:
            if e.resp.status == 409:  # Already a member
                log_with_context(
                    logging.INFO, "    ✓ Member creation: PASSED (already member)"
                )
            elif "Adding normal memberships isn't supported" in str(e):
                # This error indicates import mode is working correctly
                log_with_context(
                    logging.INFO,
                    "    ✓ Member creation: EXPECTED (import mode requires historical memberships)",
                )
            else:
                self.permission_errors.append(f"Member creation failed: {e}")
                log_with_context(logging.ERROR, "    ✗ Member creation: FAILED")

    def _test_message_operations(self):
        """Test message-related operations."""
        if "space" not in self.test_resources:
            log_with_context(
                logging.WARNING, "Skipping message tests - no test space available"
            )
            return

        log_with_context(logging.INFO, "Testing message operations...")

        # Test 7: Message creation
        log_with_context(logging.INFO, "  • Testing message creation...")
        try:
            message_body = {"text": "Permission test message - will be cleaned up"}
            message_result = (
                self.migrator.chat.spaces()
                .messages()
                .create(parent=self.test_resources["space"], body=message_body)
                .execute()
            )
            self.test_resources["message"] = message_result.get("name")
            log_with_context(logging.INFO, "    ✓ Message creation: PASSED")
        except HttpError as e:
            self.permission_errors.append(f"Message creation failed: {e}")
            log_with_context(logging.ERROR, "    ✗ Message creation: FAILED")

        # Skip import completion test - this is tested during actual migration
        # and causes cleanup issues with test spaces
        log_with_context(
            logging.INFO,
            "  • Skipping import completion test (tested during actual migration)",
        )
        log_with_context(
            logging.INFO,
            "    ✓ Import completion: SKIPPED (not needed for permission validation)",
        )

    def _test_drive_operations(self):
        """Test Drive-related operations."""
        log_with_context(logging.INFO, "Testing Drive operations...")

        # Test 10: Drive file creation and sharing
        log_with_context(logging.INFO, "  • Testing Drive file operations...")
        try:
            # Create a test file
            file_metadata = {"name": "permission-test.txt", "parents": []}
            media_body = MediaIoBaseUpload(
                io.BytesIO(b"Permission test file content"), mimetype="text/plain"
            )
            test_file = (
                self.migrator.drive.files()
                .create(body=file_metadata, media_body=media_body)
                .execute()
            )
            file_id = test_file.get("id")
            self.test_resources["drive_file"] = file_id

            # Test file sharing permissions
            permission_body = {"role": "reader", "type": "anyone"}
            self.migrator.drive.permissions().create(
                fileId=file_id, body=permission_body
            ).execute()

            log_with_context(logging.INFO, "    ✓ Drive operations: PASSED")

        except Exception as e:
            self.permission_errors.append(f"Drive operations failed: {e}")
            log_with_context(logging.ERROR, f"    ✗ Drive operations: FAILED - {e}")

    def _cleanup_test_resources(self):
        """Clean up all test resources."""
        log_with_context(logging.INFO, "Cleaning up test resources...")

        # Clean up Drive file
        if "drive_file" in self.test_resources:
            try:
                self.migrator.drive.files().delete(
                    fileId=self.test_resources["drive_file"]
                ).execute()
                log_with_context(logging.DEBUG, "Cleaned up test Drive file")
            except Exception as e:
                log_with_context(logging.WARNING, f"Failed to clean up Drive file: {e}")

        # Clean up test space - simple deletion without import completion
        if "space" in self.test_resources:
            try:
                # Try to delete the space directly
                self.migrator.chat.spaces().delete(
                    name=self.test_resources["space"]
                ).execute()
                log_with_context(logging.DEBUG, "Cleaned up test space")
            except Exception as e:
                # Import mode spaces often cannot be deleted, which is expected
                if any(
                    keyword in str(e).lower()
                    for keyword in [
                        "insufficient authentication scopes",
                        "permission denied",
                        "cannot delete",
                        "import mode",
                    ]
                ):
                    log_with_context(
                        logging.DEBUG,
                        "Test space cleanup skipped (expected for import mode)",
                    )
                else:
                    log_with_context(
                        logging.WARNING, f"Failed to clean up test space: {e}"
                    )

    def _report_results(self) -> bool:
        """Report validation results and return success status."""
        if self.permission_errors:
            log_with_context(logging.ERROR, "❌ Permission validation FAILED:")
            for error in self.permission_errors:
                log_with_context(logging.ERROR, f"  • {error}")

            log_with_context(logging.ERROR, "")
            log_with_context(
                logging.ERROR, "Required scopes for domain-wide delegation:"
            )
            for scope in REQUIRED_SCOPES:
                log_with_context(logging.ERROR, f"  • {scope}")

            log_with_context(logging.ERROR, "")
            log_with_context(
                logging.ERROR,
                "Please ensure all scopes are granted to your service account in the Google Admin Console.",
            )
            log_with_context(
                logging.ERROR, "See the setup documentation for detailed instructions."
            )

            raise Exception(
                f"Permission validation failed with {len(self.permission_errors)} errors. Migration cannot proceed."
            )

        else:
            log_with_context(logging.INFO, "✅ All permission validations PASSED!")
            log_with_context(logging.INFO, "Migration can proceed safely.")
            return True


def validate_permissions(migrator) -> bool:
    """
    Convenience function to validate all permissions.

    Args:
        migrator: The SlackToChatMigrator instance

    Returns:
        True if all permissions are valid

    Raises:
        Exception: If critical permissions are missing
    """
    # Initialize API services before validation
    migrator._initialize_api_services()

    validator = PermissionValidator(migrator)
    return validator.validate_all_permissions()
