"""
Permission Checker for Slack Chat Migration

This module provides functionality to check if the service account has the necessary
permissions to perform the Slack to Google Chat migration.
"""

import json
import logging
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from slack_migrator.utils.logging import logger, log_with_context

# Required scopes for the migration
REQUIRED_SCOPES = [
    "https://www.googleapis.com/auth/chat.import",
    "https://www.googleapis.com/auth/chat.spaces",
    "https://www.googleapis.com/auth/drive"  # Full Drive scope covers all drive.file permissions plus shared drives
]

class PermissionChecker:
    """Checks if the service account has the necessary permissions for migration."""
    
    def __init__(self, creds_path: str, workspace_admin: str):
        """
        Initialize the permission checker.
        
        Args:
            creds_path: Path to the service account credentials JSON file
            workspace_admin: Email of the workspace admin to impersonate
        """
        self.creds_path = Path(creds_path)
        self.workspace_admin = workspace_admin
        self.service_account_email = None
        self.project_id = None
        
    def check_all(self) -> bool:
        """
        Run all permission checks.
        
        Returns:
            bool: True if all checks pass, False otherwise
        """
        logger.info("Running permission checks before migration...")
        
        # Check if credentials file exists and can be loaded
        if not self._check_credentials():
            return False
            
        # Check Chat API permissions
        chat_ok = self._check_chat_api()
        
        # Check Drive API permissions
        drive_ok = self._check_drive_api()
        
        # Summary
        if chat_ok and drive_ok:
            logger.info("✅ All permission checks passed!")
            return True
        else:
            logger.error("❌ Some permission checks failed. Please fix the issues above.")
            self._print_common_issues()
            return False
    
    def _check_credentials(self) -> bool:
        """
        Check if credentials file exists and can be loaded.
        
        Returns:
            bool: True if credentials are valid, False otherwise
        """
        if not self.creds_path.exists():
            logger.error(f"Credentials file not found: {self.creds_path}")
            return False
            
        try:
            with open(self.creds_path) as f:
                creds_data = json.load(f)
                
            self.service_account_email = creds_data.get('client_email')
            self.project_id = creds_data.get('project_id')
            
            logger.info(f"Credentials loaded successfully")
            logger.info(f"Service Account: {self.service_account_email}")
            logger.info(f"Project ID: {self.project_id}")
            return True
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in credentials file")
            return False
        except Exception as e:
            logger.error(f"Failed to load credentials: {e}")
            return False
    
    def _get_service(self, api: str, version: str, scopes: Optional[List[str]] = None) -> Any:
        """
        Get a Google API client service using service account impersonation.
        
        Args:
            api: The API name (e.g., "chat", "drive")
            version: The API version (e.g., "v1", "v3")
            scopes: The OAuth scopes to request
            
        Returns:
            The API service object or None if failed
        """
        if scopes is None:
            scopes = REQUIRED_SCOPES
            
        try:
            creds = service_account.Credentials.from_service_account_file(
                str(self.creds_path), 
                scopes=scopes
            )
            delegated = creds.with_subject(self.workspace_admin)
            return build(api, version, credentials=delegated, cache_discovery=False)
        except Exception as e:
            logger.error(f"Failed to create {api} service: {e}")
            return None
    
    def _check_chat_api(self) -> bool:
        """
        Check if Chat API is accessible and permissions are correct.
        
        Returns:
            bool: True if Chat API permissions are correct, False otherwise
        """
        logger.info(f"Testing Chat API access as {self.workspace_admin}...")
        
        chat_service = self._get_service("chat", "v1")
        if not chat_service:
            return False
            
        # Test listing spaces (basic permission)
        try:
            spaces = chat_service.spaces().list(pageSize=10).execute()
            logger.info(f"✅ Successfully listed spaces: {len(spaces.get('spaces', []))} spaces found")
        except HttpError as e:
            logger.error(f"❌ Failed to list spaces: {e}")
            logger.error("This indicates a problem with domain-wide delegation or Chat API permissions")
            return False
        
        # Test creating a space with import mode
        try:
            logger.info("Testing space creation with import mode...")
            test_space = {
                'displayName': 'Test Import Space',
                'spaceType': 'SPACE',
                'importMode': True
            }
            result = chat_service.spaces().create(body=test_space).execute()
            space_name = result.get('name')
            logger.info(f"✅ Successfully created test space: {space_name}")
            
            # For import mode, we don't need to test message creation
            # The import mode permission is confirmed if we can create a space with importMode=True
            logger.info("✅ Import mode permissions confirmed")
            
            # Clean up by deleting the test space
            try:
                chat_service.spaces().delete(name=space_name).execute()
                logger.info("✅ Test space deleted successfully")
            except HttpError as e:
                logger.error(f"❌ Failed to delete test space: {e}")
                
            return True
        except HttpError as e:
            logger.error(f"❌ Failed to create test space: {e}")
            logger.error("This indicates a problem with Chat API admin permissions")
            return False
    
    def _check_drive_api(self) -> bool:
        """
        Check if Drive API is accessible and permissions are correct.
        
        Returns:
            bool: True if Drive API permissions are correct, False otherwise
        """
        logger.info(f"Testing Drive API access as {self.workspace_admin}...")
        
        drive_service = self._get_service("drive", "v3")
        if not drive_service:
            return False
            
        # Test listing files (basic permission)
        try:
            files = drive_service.files().list(pageSize=10).execute()
            logger.info(f"✅ Successfully listed files: {len(files.get('files', []))} files found")
        except HttpError as e:
            logger.error(f"❌ Failed to list files: {e}")
            logger.error("This indicates a problem with domain-wide delegation or Drive API permissions")
            return False
        
        # Test creating a folder
        try:
            logger.info("Testing folder creation...")
            folder_metadata = {
                'name': 'Test Slack Migration Folder',
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = drive_service.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            logger.info(f"✅ Successfully created test folder: {folder_id}")
            
            # Clean up by deleting the test folder
            try:
                drive_service.files().delete(fileId=folder_id).execute()
                logger.info("✅ Test folder deleted successfully")
            except HttpError as e:
                logger.error(f"❌ Failed to delete test folder: {e}")
                
            return True
        except HttpError as e:
            logger.error(f"❌ Failed to create test folder: {e}")
            logger.error("This indicates a problem with Drive API permissions")
            return False
    
    def _print_common_issues(self) -> None:
        """Print common issues and solutions for permission problems."""
        logger.info("\nCommon issues:")
        logger.info("1. Domain-wide delegation not configured in Google Workspace admin console")
        logger.info("2. Required API scopes not authorized for the service account:")
        for scope in REQUIRED_SCOPES:
            logger.info(f"   - {scope}")
        logger.info("3. Service account doesn't have the necessary IAM roles:")
        logger.info("   - Chat API Admin")
        logger.info("   - Drive API permissions")
        logger.info("\nTo fix these issues, run setup_permissions.sh and follow the instructions.")


def check_permissions(creds_path: str, workspace_admin: str) -> bool:
    """
    Check if the service account has the necessary permissions.
    
    Args:
        creds_path: Path to the service account credentials JSON file
        workspace_admin: Email of the workspace admin to impersonate
        
    Returns:
        bool: True if all checks pass, False otherwise
    """
    checker = PermissionChecker(creds_path, workspace_admin)
    return checker.check_all()


def main():
    """Command-line entry point for running permission checks."""
    parser = argparse.ArgumentParser(description='Check permissions for Slack to Google Chat migration')
    parser.add_argument('--creds_path', required=True, help='Path to service account credentials JSON')
    parser.add_argument('--workspace_admin', required=True, help='Email of workspace admin to impersonate')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Configure logging - but only if we're running as a standalone script
    # When imported as a module, use the existing logger
    if __name__ == '__main__':
        logging_level = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(
            level=logging_level,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    print("\n=== Slack to Google Chat Migration Permission Checker ===\n")
    
    result = check_permissions(args.creds_path, args.workspace_admin)
    
    if result:
        print("\n✅ All permission checks passed! You're ready to run the migration.")
        sys.exit(0)
    else:
        print("\n❌ Some permission checks failed. Please fix the issues above before running the migration.")
        sys.exit(1)


if __name__ == '__main__':
    main() 