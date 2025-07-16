#!/usr/bin/env python3
"""
Main execution module for the Slack to Google Chat migration tool
"""

import sys
import argparse
from pathlib import Path

from slack_migrator.utils.logging import logger, setup_logger
from slack_migrator.core.migrator import SlackToChatMigrator
from slack_migrator.core.config import load_config
from slack_migrator.cli.report import generate_report, print_dry_run_summary
from slack_migrator.cli.permission import check_permissions


def main():
    """Main entry point for the migration tool."""
    parser = argparse.ArgumentParser(description='Migrate Slack export to Google Chat')
    parser.add_argument('--creds_path', required=True, help='Path to service account credentials JSON')
    parser.add_argument('--export_path', required=True, help='Path to Slack export directory')
    parser.add_argument('--workspace_admin', required=True, help='Email of workspace admin to impersonate')
    parser.add_argument('--config', required=True, help='Path to config YAML')
    parser.add_argument('--slack_token', help='Slack token for downloading files (optional, will be extracted from URLs if not provided)')
    parser.add_argument('--dry_run', action='store_true', help='Dry run mode - no changes will be made')
    parser.add_argument('--update_mode', action='store_true', help='Update mode - update existing spaces instead of creating new ones')
    parser.add_argument('--report_file', help='Path to save the dry run report (default: migration_report.yaml)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose (debug) logging')
    parser.add_argument('--debug_api', action='store_true', help='Enable detailed API request/response logging (generates large log files)')
    parser.add_argument('--skip_permission_check', action='store_true', help='Skip permission checks (not recommended)')
    
    args = parser.parse_args()
    
    # Set up logger with verbosity level from command line
    global logger
    logger = setup_logger(args.verbose, args.debug_api)
    
    migrator = None
    
    try:
        # Check if credentials file exists
        creds_path = Path(args.creds_path)
        if not creds_path.exists():
            logger.error(f"Credentials file not found: {args.creds_path}")
            logger.info("Make sure your service account JSON key file exists and has the correct path.")
            sys.exit(1)
            
        logger.info("Starting migration with the following parameters:")
        logger.info(f"- Export path: {args.export_path}")
        logger.info(f"- Workspace admin: {args.workspace_admin}")
        logger.info(f"- Config: {args.config}")
        logger.info(f"- Dry run: {args.dry_run}")
        logger.info(f"- Update mode: {args.update_mode}")
        logger.info(f"- Verbose logging: {args.verbose}")
        logger.info(f"- Debug API calls: {args.debug_api}")
        
        # Run permission checks before proceeding
        if not args.skip_permission_check:
            logger.info("Checking permissions before proceeding...")
            if not check_permissions(args.creds_path, args.workspace_admin):
                logger.error("Permission checks failed. Fix the issues or run with --skip_permission_check if you're sure.")
                sys.exit(1)
        else:
            logger.warning("Permission checks skipped. This may cause issues during migration.")
        
        migrator = SlackToChatMigrator(
            args.creds_path, 
            args.export_path, 
            args.workspace_admin,
            args.config,
            args.slack_token,
            args.dry_run,
            args.verbose,
            args.update_mode
        )
        
        # Run the migration
        migrator.migrate()
        
        # Report is already generated in migrator.migrate()
    except Exception as e:
        from googleapiclient.errors import HttpError
        if isinstance(e, HttpError) and e.resp.status == 403 and 'PERMISSION_DENIED' in str(e):
            logger.error(f"Permission denied error: {e}")
            logger.info("\nThe service account doesn't have sufficient permissions. Please ensure:")
            logger.info("1. The service account has the 'Chat API Admin' role in your GCP project")
            logger.info("2. Domain-wide delegation is configured properly in your Google Workspace admin console")
            logger.info("3. The following scopes are granted to the service account:")
            logger.info("   - https://www.googleapis.com/auth/chat.import")
            logger.info("   - https://www.googleapis.com/auth/drive.file")
        else:
            logger.error(f"Migration failed: {e}")
    finally:
        # Cleanup resources
        if migrator and not args.dry_run:
            try:
                migrator.cleanup()
            except Exception as cleanup_e:
                logger.error(f"Cleanup failed: {cleanup_e}")
            
        # Show the security warning about tokens in export files
        logger.warning("SECURITY WARNING: Your Slack export files contain authentication tokens in the URLs.")
        logger.warning("Consider securing or deleting these files after the migration is complete.")
        logger.warning("See README.md for more information on security best practices.")


if __name__ == '__main__':
    main() 