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
from typing import NoReturn

from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.utils.logging import logger, setup_logger
from slack_migrator.utils.permissions import validate_permissions


class MigrationOrchestrator:
    """Orchestrates the migration process with validation and error handling."""

    def __init__(self, args):
        self.args = args
        self.migrator = None
        self.dry_run_migrator = None

    def create_migrator(self, force_dry_run: bool = False) -> SlackToChatMigrator:
        """Create a migrator instance with the given parameters."""
        return SlackToChatMigrator(
            self.args.creds_path,
            self.args.export_path,
            self.args.workspace_admin,
            self.args.config,
            dry_run=force_dry_run or self.args.dry_run,
            verbose=self.args.verbose,
            update_mode=self.args.update_mode,
            debug_api=self.args.debug_api,
        )

    def validate_prerequisites(self):
        """Validate all prerequisites before migration."""
        # Check credentials file
        creds_path = Path(self.args.creds_path)
        if not creds_path.exists():
            logger.error(f"Credentials file not found: {self.args.creds_path}")
            logger.info(
                "Make sure your service account JSON key file exists and has the correct path."
            )
            sys.exit(1)

        # Initialize main migrator (without expensive drive operations)
        self.migrator = self.create_migrator()

        # Run permission checks BEFORE any expensive operations
        if not self.args.skip_permission_check:
            logger.info("Checking permissions before proceeding...")
            try:
                validate_permissions(self.migrator)
                logger.info("Permission checks passed!")

                # Now that permissions are validated, initialize drive structures
                if (
                    hasattr(self.migrator, "file_handler")
                    and self.migrator.file_handler
                ):
                    logger.info("Initializing drive structures...")
                    self.migrator.file_handler.ensure_drive_initialized()
                    logger.info("Drive structures initialized!")

            except Exception as e:
                logger.error(f"Permission checks failed: {e}")
                logger.error(
                    "Fix the issues or run with --skip_permission_check if you're sure."
                )
                sys.exit(1)
        else:
            logger.warning(
                "Permission checks skipped. This may cause issues during migration."
            )
            # Still initialize drive structures even if permission checks are skipped
            if hasattr(self.migrator, "file_handler") and self.migrator.file_handler:
                logger.info("Initializing drive structures...")
                self.migrator.file_handler.ensure_drive_initialized()
                logger.info("Drive structures initialized!")

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
        logger.info("")
        logger.info("🚨 VALIDATION ISSUES DETECTED!")
        logger.info(
            f"Found {migrator_instance.unmapped_user_tracker.get_unmapped_count()} unmapped user(s)."
        )
        logger.info("")
        logger.info("⚠️  WARNING: If you proceed without fixing these mappings:")
        logger.info(
            "   • Messages from unmapped users will be sent by the workspace admin"
        )
        logger.info("   • Attribution prefixes will indicate the original sender")
        logger.info("   • Reactions from unmapped users will be skipped and logged")
        logger.info("")
        logger.info("📋 Recommended steps to fix:")
        logger.info("1. Review the unmapped users listed above")
        logger.info("2. Add them to user_mapping_overrides in your config.yaml")

        if is_explicit_dry_run:
            logger.info("3. Run the migration again (without --dry_run)")
            logger.info("")
            return False  # In explicit dry run, just report and exit
        else:
            logger.info("3. Run the migration again")
            logger.info("")

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
                    logger.warning(
                        "Proceeding with unmapped users - messages will be attributed to workspace admin"
                    )
                    return True
                else:
                    logger.info(
                        "Migration cancelled. Please fix the user mappings and try again."
                    )
                    return False
            except KeyboardInterrupt:
                logger.info("\nMigration cancelled by user.")
                return False

    def report_validation_success(self, is_explicit_dry_run: bool = False):
        """Report successful validation."""
        logger.info("")
        logger.info("✅ Validation completed successfully!")
        logger.info("   • All users mapped correctly")
        logger.info("   • File attachments accessible")
        logger.info("   • Channel structure validated")
        logger.info("   • Migration scope confirmed")
        logger.info("")

        if is_explicit_dry_run:
            logger.info(
                "You can now run the migration without --dry_run to perform the actual migration."
            )

        logger.info("")

    def run_validation(self) -> bool:
        """Run comprehensive validation. Returns True if validation passes."""
        logger.info("")
        logger.info("🔍 STEP 1: Running comprehensive validation (dry run)...")
        logger.info("   • Validating user mappings and detecting unmapped users")
        logger.info("   • Checking file attachments and permissions")
        logger.info("   • Verifying channel structure and memberships")
        logger.info("   • Testing message formatting and content")
        logger.info("   • Estimating migration scope and requirements")
        logger.info("")

        # Create and run dry run migrator
        self.dry_run_migrator = self.create_migrator(force_dry_run=True)

        try:
            self.dry_run_migrator.migrate()
        except Exception as e:
            logger.error(f"Validation (dry run) failed: {e}")
            logger.error(
                "Please fix the issues identified during validation before proceeding."
            )
            sys.exit(1)

        # Check validation results
        if self.check_unmapped_users(self.dry_run_migrator):
            return self.report_validation_issues(self.dry_run_migrator)

        return True

    def get_user_confirmation(self) -> bool:
        """Get user confirmation to proceed with migration."""
        logger.info("🚀 STEP 2: Ready to proceed with actual migration")
        logger.info("")

        try:
            response = (
                input("Proceed with the actual migration? (y/N): ").strip().lower()
            )
            return response in ["y", "yes"]
        except KeyboardInterrupt:
            logger.info("\nMigration cancelled by user.")
            return False

    def run_migration(self):
        """Execute the main migration logic."""
        if self.args.dry_run:
            # Explicit dry run mode
            try:
                # Set up output directory and update logger
                self._setup_output_logging()
                self.migrator.migrate()

                if self.check_unmapped_users(self.migrator):
                    if self.report_validation_issues(
                        self.migrator, is_explicit_dry_run=True
                    ):
                        # User should not be able to proceed in explicit dry run mode
                        logger.info(
                            "Use normal migration mode to proceed with unmapped users."
                        )
                        sys.exit(1)
                    else:
                        sys.exit(1)
                else:
                    self.report_validation_success(is_explicit_dry_run=True)

            except Exception as e:
                logger.error(f"Validation failed: {e}")
                sys.exit(1)
        else:
            # Full migration with automatic validation
            if self.run_validation():
                self.report_validation_success()

                if self.get_user_confirmation():
                    try:
                        # Set up output directory and update logger
                        self._setup_output_logging()
                        self.migrator.migrate()
                        logger.info("")
                        logger.info("🎉 Migration completed successfully!")
                        logger.info("")
                    except Exception as e:
                        logger.error(f"Migration failed: {e}")
                        raise
                else:
                    logger.info("Migration cancelled by user.")
                    sys.exit(0)

    def _setup_output_logging(self):
        """Set up output directory and update logger with file logging."""
        from slack_migrator.cli.report import create_output_directory

        # Create output directory (this will be used by the migrator)
        output_dir = create_output_directory(self.migrator)

        # Set the output directory on the migrator so it doesn't create its own
        self.migrator.output_dir = output_dir

        # Update the logger to include file logging
        global logger
        logger = setup_logger(self.args.verbose, self.args.debug_api, output_dir)

        logger.info(f"Output directory created: {output_dir}")

    def cleanup(self):
        """Perform cleanup operations."""
        if self.migrator and not self.args.dry_run:
            try:
                logger.info("Performing cleanup operations...")
                self.migrator.cleanup()
                logger.info("Cleanup completed successfully.")
            except Exception as cleanup_e:
                logger.error(f"Cleanup failed: {cleanup_e}", exc_info=True)
                logger.info("You may need to manually clean up temporary resources.")


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
        "--verbose", "-v", action="store_true", help="Enable verbose (debug) logging"
    )
    parser.add_argument(
        "--debug_api",
        action="store_true",
        help="Enable detailed API request/response logging (generates large log files)",
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

    logger.info("Starting migration with the following parameters:")
    logger.info(f"- Export path: {args.export_path}")
    logger.info(f"- Workspace admin: {args.workspace_admin}")
    logger.info(f"- Config: {config_path}")
    logger.info(f"- Dry run: {args.dry_run}")
    logger.info(f"- Update mode: {args.update_mode}")
    logger.info(f"- Verbose logging: {args.verbose}")
    logger.info(f"- Debug API calls: {args.debug_api}")


def handle_http_error(e):
    """Handle HTTP errors with specific messages."""

    if e.resp.status == 403 and "PERMISSION_DENIED" in str(e):
        logger.error(f"Permission denied error: {e}")
        logger.info(
            "\nThe service account doesn't have sufficient permissions. Please ensure:"
        )
        logger.info(
            "1. The service account has the 'Chat API Admin' role in your GCP project"
        )
        logger.info(
            "2. Domain-wide delegation is configured properly in your Google Workspace admin console"
        )
        logger.info("3. The following scopes are granted to the service account:")
        logger.info("   - https://www.googleapis.com/auth/chat.import")
        logger.info("   - https://www.googleapis.com/auth/chat.spaces")
        logger.info("   - https://www.googleapis.com/auth/drive")
    elif e.resp.status == 429:
        logger.error(f"Rate limit exceeded: {e}")
        logger.info(
            "The migration hit API rate limits. Consider using --update_mode to resume."
        )
    elif e.resp.status >= 500:
        logger.error(f"Server error from Google API: {e}")
        logger.info("This is likely a temporary issue. Please try again later.")
    else:
        logger.error(f"API error during migration: {e}")


def handle_exception(e):
    """Handle different types of exceptions."""
    from googleapiclient.errors import HttpError

    if isinstance(e, HttpError):
        handle_http_error(e)
    elif isinstance(e, FileNotFoundError):
        logger.error(f"File not found: {e}")
        logger.info("Please check that all required files exist and paths are correct.")
    elif isinstance(e, KeyboardInterrupt):
        logger.warning("Migration interrupted by user.")
        logger.info("You can resume the migration with --update_mode.")
    else:
        logger.error(f"Migration failed: {e}", exc_info=True)


def show_security_warning():
    """Show security warning about tokens in export files."""
    logger.warning(
        "\nSECURITY WARNING: Your Slack export files contain authentication tokens in the URLs."
    )
    logger.warning(
        "Consider securing or deleting these files after the migration is complete."
    )
    logger.warning("See README.md for more information on security best practices.")


def main() -> NoReturn:
    """
    Main entry point for the Slack to Google Chat migration tool.

    Parses command line arguments, sets up logging, performs permission checks,
    initializes the migrator, and executes the migration process.

    The function handles errors during migration and provides appropriate
    error messages and cleanup operations.

    Returns:
        NoReturn: The function exits with sys.exit()
    """
    # Parse arguments and setup
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Set up logger with verbosity level from command line
    global logger
    logger = setup_logger(args.verbose, args.debug_api)

    log_startup_info(args)

    # Create orchestrator and run migration
    orchestrator = MigrationOrchestrator(args)

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
