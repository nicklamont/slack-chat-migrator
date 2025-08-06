#!/usr/bin/env python3
"""
Main execution module for the Slack to Google Chat migration tool.

This module provides the command-line interface for the migration tool,
handling argument parsing, configuration loading, and executing the
migration process with appropriate error handling.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import logging

from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.utils.logging import setup_logger, log_with_context
from slack_migrator.utils.permissions import validate_permissions

# Create logger instance
logger = logging.getLogger("slack_migrator")


class MigrationOrchestrator:
    """Orchestrates the migration process with validation and error handling."""

    def __init__(self, args):
        self.args = args
        self.migrator = None
        self.dry_run_migrator = None
        self.output_dir: Optional[str] = None

    def create_migrator(self, force_dry_run: bool = False) -> SlackToChatMigrator:
        """Create a migrator instance with the given parameters."""
        migrator = SlackToChatMigrator(
            self.args.creds_path,
            self.args.export_path,
            self.args.workspace_admin,
            self.args.config,
            dry_run=force_dry_run or self.args.dry_run,
            verbose=self.args.verbose,
            update_mode=self.args.update_mode,
            debug_api=self.args.debug_api,
        )

        # Set output directory if we have one
        if self.output_dir:
            migrator.output_dir = self.output_dir

        return migrator

    def validate_prerequisites(self):
        """Validate all prerequisites before migration."""
        # Check credentials file
        creds_path = Path(self.args.creds_path)
        if not creds_path.exists():
            log_with_context(
                logging.ERROR, f"Credentials file not found: {self.args.creds_path}"
            )
            log_with_context(
                logging.INFO,
                "Make sure your service account JSON key file exists and has the correct path.",
            )
            sys.exit(1)

        # Initialize main migrator
        self.migrator = self.create_migrator()

        # Run permission checks BEFORE any expensive operations
        if not self.args.skip_permission_check:
            log_with_context(logging.INFO, "Checking permissions before proceeding...")
            try:
                validate_permissions(self.migrator)
                log_with_context(logging.INFO, "Permission checks passed!")

                # Now that permissions are validated, initialize drive structures
                if (
                    hasattr(self.migrator, "file_handler")
                    and self.migrator.file_handler
                ):
                    self.migrator.file_handler.ensure_drive_initialized()
                    log_with_context(
                        logging.INFO, "Drive structures initialized successfully"
                    )

            except Exception as e:
                log_with_context(logging.ERROR, f"Permission checks failed: {e}")
                log_with_context(
                    logging.ERROR,
                    "Fix the issues or run with --skip_permission_check if you're sure.",
                )
                sys.exit(1)
        else:
            log_with_context(
                logging.WARNING,
                "Permission checks skipped. This may cause issues during migration.",
            )
            # Still initialize drive structures even if permission checks are skipped
            if hasattr(self.migrator, "file_handler") and self.migrator.file_handler:
                self.migrator.file_handler.ensure_drive_initialized()
                log_with_context(
                    logging.INFO, "Drive structures initialized successfully"
                )

    def check_unmapped_users(self, migrator_instance: SlackToChatMigrator) -> bool:
        """Check for unmapped users and return True if any found."""
        return (
            hasattr(migrator_instance, "unmapped_user_tracker")
            and migrator_instance.unmapped_user_tracker.has_unmapped_users()
        )

    def report_validation_issues(
        self, migrator_instance: SlackToChatMigrator, is_explicit_dry_run: bool = False
    ) -> bool:
        """Report validation issues and ask user if they want to proceed anyway."""
        log_with_context(logging.INFO, "")
        log_with_context(logging.INFO, "🚨 VALIDATION ISSUES DETECTED!")
        log_with_context(
            logging.INFO,
            f"Found {migrator_instance.unmapped_user_tracker.get_unmapped_count()} unmapped user(s).",
        )
        log_with_context(logging.INFO, "")
        log_with_context(
            logging.INFO, "⚠️  WARNING: If you proceed without fixing these mappings:"
        )
        log_with_context(
            logging.INFO,
            "   • Messages from unmapped users will be sent by the workspace admin",
        )
        log_with_context(
            logging.INFO, "   • Attribution prefixes will indicate the original sender"
        )
        log_with_context(
            logging.INFO,
            "   • Reactions from unmapped users will be skipped and logged",
        )
        log_with_context(logging.INFO, "")
        log_with_context(logging.INFO, "📋 Recommended steps to fix:")
        log_with_context(logging.INFO, "1. Review the unmapped users listed above")
        log_with_context(
            logging.INFO, "2. Add them to user_mapping_overrides in your config.yaml"
        )

        if is_explicit_dry_run:
            log_with_context(
                logging.INFO, "3. Run the migration again (without --dry_run)"
            )
            log_with_context(logging.INFO, "")
            return False  # In explicit dry run, just report and exit
        else:
            log_with_context(logging.INFO, "3. Run the migration again")
            log_with_context(logging.INFO, "")

            # Ask user if they want to proceed anyway
            try:
                response = (
                    input(
                        "⚠️  Proceed anyway despite unmapped users? (NOT RECOMMENDED) (y/N): "
                    )
                    .strip()
                    .lower()
                )
                if response in ["y", "yes"]:
                    log_with_context(
                        logging.WARNING,
                        "Proceeding with unmapped users - messages will be attributed to workspace admin",
                    )
                    return True
                else:
                    log_with_context(
                        logging.INFO,
                        "Migration cancelled. Please fix the user mappings and try again.",
                    )
                    return False
            except KeyboardInterrupt:
                log_with_context(logging.INFO, "\nMigration cancelled by user.")
                return False

    def report_validation_success(self, is_explicit_dry_run: bool = False):
        """Report successful validation."""
        log_with_context(logging.INFO, "")
        log_with_context(logging.INFO, "✅ Validation completed successfully!")
        log_with_context(logging.INFO, "   • All users mapped correctly")
        log_with_context(logging.INFO, "   • File attachments accessible")
        log_with_context(logging.INFO, "   • Channel structure validated")
        log_with_context(logging.INFO, "   • Migration scope confirmed")
        log_with_context(logging.INFO, "")

        if is_explicit_dry_run:
            log_with_context(
                logging.INFO,
                "You can now run the migration without --dry_run to perform the actual migration.",
            )

        log_with_context(logging.INFO, "")

    def run_validation(self) -> bool:
        """Run comprehensive validation. Returns True if validation passes."""
        log_with_context(logging.INFO, "")
        log_with_context(
            logging.INFO, "🔍 STEP 1: Running comprehensive validation (dry run)..."
        )
        log_with_context(
            logging.INFO, "   • Validating user mappings and detecting unmapped users"
        )
        log_with_context(logging.INFO, "   • Checking file attachments and permissions")
        log_with_context(
            logging.INFO, "   • Verifying channel structure and memberships"
        )
        log_with_context(logging.INFO, "   • Testing message formatting and content")
        log_with_context(
            logging.INFO, "   • Estimating migration scope and requirements"
        )
        log_with_context(logging.INFO, "")

        # Create and run dry run migrator
        self.dry_run_migrator = self.create_migrator(force_dry_run=True)

        try:
            self.dry_run_migrator.migrate()
        except Exception as e:
            log_with_context(logging.ERROR, f"Validation (dry run) failed: {e}")
            log_with_context(
                logging.ERROR,
                "Please fix the issues identified during validation before proceeding.",
            )
            sys.exit(1)

        # Check validation results
        if self.check_unmapped_users(self.dry_run_migrator):
            return self.report_validation_issues(self.dry_run_migrator)

        return True

    def get_user_confirmation(self) -> bool:
        """Get user confirmation to proceed with migration."""
        log_with_context(
            logging.INFO, "🚀 STEP 2: Ready to proceed with actual migration"
        )
        log_with_context(logging.INFO, "")

        try:
            response = (
                input("Proceed with the actual migration? (y/N): ").strip().lower()
            )
            return response in ["y", "yes"]
        except KeyboardInterrupt:
            log_with_context(logging.INFO, "\nMigration cancelled by user.")
            return False

    def run_migration(self):
        """Execute the main migration logic."""
        if self.args.dry_run:
            # Explicit dry run mode
            try:
                self.migrator.migrate()

                if self.check_unmapped_users(self.migrator):
                    if self.report_validation_issues(
                        self.migrator, is_explicit_dry_run=True
                    ):
                        # User should not be able to proceed in explicit dry run mode
                        log_with_context(
                            logging.INFO,
                            "Use normal migration mode to proceed with unmapped users.",
                        )
                        sys.exit(1)
                    else:
                        sys.exit(1)
                else:
                    self.report_validation_success(is_explicit_dry_run=True)

            except Exception as e:
                log_with_context(logging.ERROR, f"Validation failed: {e}")
                sys.exit(1)
        else:
            # Full migration with automatic validation
            if self.run_validation():
                self.report_validation_success()

                if self.get_user_confirmation():
                    try:
                        self.migrator.migrate()

                    except Exception as e:
                        log_with_context(logging.ERROR, f"Migration failed: {e}")
                        raise
                else:
                    log_with_context(logging.INFO, "Migration cancelled by user.")
                    sys.exit(0)

    def cleanup(self):
        """Perform cleanup operations."""
        if self.migrator:
            try:
                log_with_context(logging.INFO, "Performing cleanup operations...")

                # Always clean up channel handlers, regardless of dry run mode
                if hasattr(self.migrator, "_cleanup_channel_handlers"):
                    try:
                        self.migrator._cleanup_channel_handlers()
                    except Exception as handler_cleanup_e:
                        log_with_context(
                            logging.ERROR,
                            f"Failed to clean up channel handlers: {handler_cleanup_e}",
                            exc_info=True,
                        )

                # Only perform space cleanup if not in dry run mode
                if not self.args.dry_run:
                    try:
                        self.migrator.cleanup()
                    except Exception as space_cleanup_e:
                        log_with_context(
                            logging.ERROR,
                            f"Failed to clean up spaces: {space_cleanup_e}",
                            exc_info=True,
                        )
                        log_with_context(
                            logging.WARNING,
                            "Some spaces may still be in import mode and require manual cleanup",
                        )

                log_with_context(logging.INFO, "Cleanup completed successfully.")
            except Exception as cleanup_e:
                log_with_context(
                    logging.ERROR, f"Overall cleanup failed: {cleanup_e}", exc_info=True
                )
                log_with_context(
                    logging.INFO,
                    "You may need to manually clean up temporary resources.",
                )
                log_with_context(
                    logging.INFO,
                    "Check Google Chat admin console for spaces that may still be in import mode.",
                )


def setup_argument_parser() -> argparse.ArgumentParser:
    """Set up and return the argument parser."""
    parser = argparse.ArgumentParser(description="Migrate Slack export to Google Chat")
    parser.add_argument(
        "--creds_path", required=True, help="Path to service account credentials JSON"
    )
    parser.add_argument(
        "--export_path", required=True, help="Path to Slack export directory"
    )
    parser.add_argument(
        "--workspace_admin",
        required=True,
        help="Email of workspace admin to impersonate",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to config YAML (default: config.yaml)",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Validation-only mode - performs comprehensive validation without making changes",
    )
    parser.add_argument(
        "--update_mode",
        action="store_true",
        help="Update mode - update existing spaces instead of creating new ones",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose console logging (shows DEBUG level messages)",
    )
    parser.add_argument(
        "--debug_api",
        action="store_true",
        help="Enable detailed API request/response logging (creates very large log files)",
    )
    parser.add_argument(
        "--skip_permission_check",
        action="store_true",
        help="Skip permission checks (not recommended)",
    )
    return parser


def log_startup_info(args):
    """Log startup information."""
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = Path.cwd() / args.config

    log_with_context(logging.INFO, "Starting migration with the following parameters:")
    log_with_context(logging.INFO, f"- Export path: {args.export_path}")
    log_with_context(logging.INFO, f"- Workspace admin: {args.workspace_admin}")
    log_with_context(logging.INFO, f"- Config: {config_path}")
    log_with_context(logging.INFO, f"- Dry run: {args.dry_run}")
    log_with_context(logging.INFO, f"- Update mode: {args.update_mode}")
    log_with_context(logging.INFO, f"- Verbose logging: {args.verbose}")
    log_with_context(logging.INFO, f"- Debug API calls: {args.debug_api}")


def handle_http_error(e):
    """Handle HTTP errors with specific messages."""

    if e.resp.status == 403 and "PERMISSION_DENIED" in str(e):
        log_with_context(logging.ERROR, f"Permission denied error: {e}")
        log_with_context(
            logging.INFO,
            "\nThe service account doesn't have sufficient permissions. Please ensure:",
        )
        log_with_context(
            logging.INFO,
            "1. The service account has the 'Chat API Admin' role in your GCP project",
        )
        log_with_context(
            logging.INFO,
            "2. Domain-wide delegation is configured properly in your Google Workspace admin console",
        )
        log_with_context(
            logging.INFO, "3. The following scopes are granted to the service account:"
        )
        log_with_context(
            logging.INFO, "   - https://www.googleapis.com/auth/chat.import"
        )
        log_with_context(
            logging.INFO, "   - https://www.googleapis.com/auth/chat.spaces"
        )
        log_with_context(logging.INFO, "   - https://www.googleapis.com/auth/drive")
    elif e.resp.status == 429:
        log_with_context(logging.ERROR, f"Rate limit exceeded: {e}")
        log_with_context(
            logging.INFO,
            "The migration hit API rate limits. Consider using --update_mode to resume.",
        )
    elif e.resp.status >= 500:
        log_with_context(logging.ERROR, f"Server error from Google API: {e}")
        log_with_context(
            logging.INFO, "This is likely a temporary issue. Please try again later."
        )
    else:
        log_with_context(logging.ERROR, f"API error during migration: {e}")


def handle_exception(e):
    """Handle different types of exceptions."""
    from googleapiclient.errors import HttpError

    if isinstance(e, HttpError):
        handle_http_error(e)
    elif isinstance(e, FileNotFoundError):
        log_with_context(logging.ERROR, f"File not found: {e}")
        log_with_context(
            logging.INFO,
            "Please check that all required files exist and paths are correct.",
        )
    elif isinstance(e, KeyboardInterrupt):
        log_with_context(logging.WARNING, "Migration interrupted by user.")
        log_with_context(
            logging.INFO,
            "📋 Check the partial migration report in the output directory.",
        )
        log_with_context(
            logging.INFO, "🔄 You can resume the migration with --update_mode."
        )
        log_with_context(
            logging.INFO, "📝 All progress and logs have been saved to disk."
        )
    else:
        log_with_context(logging.ERROR, f"Migration failed: {e}", exc_info=True)


def show_security_warning():
    """Show security warning about tokens in export files."""
    log_with_context(
        logging.WARNING,
        "\nSECURITY WARNING: Your Slack export files contain authentication tokens in the URLs.",
    )
    log_with_context(
        logging.WARNING,
        "Consider securing or deleting these files after the migration is complete.",
    )
    log_with_context(
        logging.WARNING,
        "See README.md for more information on security best practices.",
    )


def create_migration_output_directory():
    """Create output directory for migration with timestamp."""
    import datetime
    import os

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"migration_logs/run_{timestamp}"

    # Create subdirectories
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "channel_logs"), exist_ok=True)

    return output_dir


def main():
    """
    Main entry point for the Slack to Google Chat migration tool.

    Parses command line arguments, sets up logging, performs permission checks,
    initializes the migrator, and executes the migration process.

    The function handles errors during migration and provides appropriate
    error messages and cleanup operations.
    """
    # Parse arguments and setup
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Create output directory early so all operations are logged to file
    output_dir = create_migration_output_directory()

    # Set up logger with output directory for file logging
    logger = setup_logger(args.verbose, args.debug_api, output_dir)

    log_startup_info(args)
    log_with_context(logging.INFO, f"Output directory: {output_dir}")

    # Create orchestrator and run migration
    orchestrator = MigrationOrchestrator(args)
    # Set the output directory so migrator doesn't create its own
    orchestrator.output_dir = output_dir

    try:
        orchestrator.validate_prerequisites()
        orchestrator.run_migration()
    except Exception as e:
        handle_exception(e)
    finally:
        orchestrator.cleanup()
        show_security_warning()


if __name__ == "__main__":
    main()
