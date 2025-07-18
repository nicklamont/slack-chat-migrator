"""
File handling module for the Slack to Google Chat migration tool
"""

import hashlib
import io
import logging
import mimetypes
import requests
from typing import Dict, Optional

from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

from slack_migrator.utils.logging import logger, log_with_context, log_api_request, log_api_response
from slack_migrator.utils.api import retry, set_global_retry_config


class FileHandler:
    """Handles file uploads and attachments during migration."""
    
    def __init__(self, drive_service, folder_id: str, migrator, slack_token: Optional[str] = None, dry_run: bool = False):
        """Initialize the file handler.
        
        Args:
            drive_service: Google Drive API service
            folder_id: ID of the Google Drive folder for attachments
            migrator: Reference to the migrator instance for stats tracking
            slack_token: Optional Slack token for file access
            dry_run: Whether this is a dry run
        """
        self.drive = drive_service
        self.drive_folder_id = folder_id
        self.slack_token = slack_token
        self.dry_run = dry_run
        self.migrator = migrator
        self.verbose = getattr(migrator, 'verbose', False)
        
        # Cache to avoid re-uploading same files
        self.drive_cache = {}
        
        # Initialize the set to track processed files
        self.processed_files = set()
        
        # Get retry configuration from migrator if available
        if hasattr(migrator, 'config'):
            set_global_retry_config(migrator.config)
        
        if self.verbose:
            logger.debug("FileHandler initialized with verbose logging")
    
    @retry()
    def create_folder(self, name: str) -> str:
        """Create a folder in Google Drive."""
        log_with_context(
            logging.INFO,
            f"Creating Drive folder: {name}"
        )
        
        body = {'name': name, 'mimeType': 'application/vnd.google-apps/folder'}
        log_api_request("POST", "drive.files.create", body)
        
        result = self.drive.files().create(
            body=body,
            fields='id'
        ).execute()
        
        folder_id = result.get('id')
        log_api_response(200, "drive.files.create", {"id": folder_id})
        
        log_with_context(
            logging.INFO,
            f"Created Drive folder: {name} (ID: {folder_id})"
        )
        return folder_id
    
    def ensure_drive_folder(self, folder_name: str) -> str:
        """Ensure the attachments folder exists in Google Drive."""
        if self.dry_run:
            log_with_context(
                logging.INFO,
                f"[DRY RUN] Would ensure Drive folder exists: {folder_name}"
            )
            return f"DRY_{folder_name}"
            
        # Search for existing folder
        query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        log_with_context(
            logging.DEBUG,
            f"Searching for Drive folder: {folder_name}"
        )
        log_api_request("GET", "drive.files.list", {"query": query})
        
        results = self.drive.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        items = results.get('files', [])
        log_api_response(200, "drive.files.list", results)
        
        # Use existing folder if found
        if items:
            folder_id = items[0]['id']
            log_with_context(
                logging.INFO,
                f"Using existing Drive folder: {folder_name} (ID: {folder_id})"
            )
            return folder_id
            
        # Create folder if not found
        log_with_context(
            logging.INFO,
            f"Drive folder not found, creating: {folder_name}"
        )
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        log_api_request("POST", "drive.files.create", folder_metadata)
        folder = self.drive.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')
        log_api_response(200, "drive.files.create", folder)
        
        log_with_context(
            logging.INFO,
            f"Created new Drive folder: {folder_name} (ID: {folder_id})"
        )
        return folder_id
    
    @retry()
    def upload_file(self, file_obj: Dict, channel: Optional[str] = None) -> Optional[str]:
        """Upload a file to Google Drive.
        
        Args:
            file_obj: Slack file object
            channel: Optional channel name for logging
            
        Returns:
            The Google Drive file ID, or None if upload failed
        """
        if self.dry_run:
            return "DRY_RUN_FILE_ID"
            
        # Extract file information
        fid = file_obj.get("id", "unknown")
        name = file_obj.get("name", "file")
        title = file_obj.get("title", name)
        
        # Check if we've already processed this file
        if fid in self.processed_files:
            log_with_context(
                logging.DEBUG,
                f"File {name} (ID: {fid}) already processed, skipping",
                channel=channel,
                file_id=fid
            )
            return self.drive_cache.get(fid)
            
        # Check for Google Docs links
        url_private = file_obj.get("url_private", "")
        if "docs.google.com" in url_private or file_obj.get("mimetype") == "application/vnd.google-apps.document":
            log_with_context(
                logging.INFO,
                f"Google Docs link detected: {url_private}",
                channel=channel,
                file_id=fid
            )
            
            # Instead of trying to upload the Google Docs file, create a text file with the link
            log_with_context(
                logging.INFO,
                f"Created placeholder for Google Docs link: {title}",
                channel=channel,
                file_id=fid
            )
            
            # For dry run, just return a placeholder
            if self.dry_run:
                return "DRY_RUN_GDOC_LINK"
                
            try:
                # Create a text file with the Google Docs link
                file_content = f"This is a link to a Google Doc: {url_private}\n\nPlease click the link to open the document."
                
                # Get or create channel-specific subfolder
                parent_folder_id = self.drive_folder_id
                if channel:
                    channel_folder_id = self._get_or_create_channel_folder(channel)
                    if channel_folder_id:
                        parent_folder_id = channel_folder_id
                
                # Create file metadata
                file_metadata = {
                    'name': f"{title} - Link.txt",
                    'parents': [parent_folder_id],
                    'description': f"Link to Google Doc from Slack: {title}"
                }
                
                # Create a memory buffer from the text content
                buffer = io.BytesIO(file_content.encode('utf-8'))
                media = MediaIoBaseUpload(buffer, mimetype='text/plain', resumable=True)
                
                # Upload the file
                file = self.drive.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                drive_file_id = file.get('id')
                
                if drive_file_id:
                    # Cache the Drive file ID
                    self.drive_cache[fid] = drive_file_id
                    
                    # Mark as processed
                    self.processed_files.add(fid)
                    
                    # Update statistics
                    self.migrator.migration_summary["files_created"] += 1
                    
                    return drive_file_id
            except Exception as e:
                log_with_context(
                    logging.ERROR,
                    f"Failed to create placeholder for Google Docs link {title}: {e}",
                    channel=channel,
                    file_id=fid,
                    error=str(e)
                )
            
            return None
            
        # Download the file from Slack export
        log_with_context(
            logging.DEBUG,
            f"Downloading file: {name} (ID: {fid})",
            channel=channel,
            file_id=fid
        )
        
        file_content = self._download_file(file_obj)
        if not file_content:
            log_with_context(
                logging.WARNING,
                f"Failed to download file: {name} (ID: {fid})",
                channel=channel,
                file_id=fid
            )
            return None
            
        try:
            # Upload to Google Drive
            log_with_context(
                logging.DEBUG,
                f"Uploading file to Drive: {name} (Size: {len(file_content)} bytes)",
                channel=channel,
                file_id=fid
            )
            
            # Get MIME type
            mime_type = file_obj.get("mimetype")
            if not mime_type:
                # Try to guess from file extension
                mime_type, _ = mimetypes.guess_type(name)
                if not mime_type:
                    mime_type = "application/octet-stream"
                    
            log_with_context(
                logging.DEBUG,
                f"File MIME type: {mime_type}",
                channel=channel,
                file_id=fid
            )
            
            # Get or create channel-specific subfolder
            parent_folder_id = self.drive_folder_id
            if channel:
                channel_folder_id = self._get_or_create_channel_folder(channel)
                if channel_folder_id:
                    parent_folder_id = channel_folder_id
            
            # Create file metadata
            file_metadata = {
                'name': name,
                'parents': [parent_folder_id],
                'description': f"Migrated from Slack: {title}"
            }
            
            # Create a memory buffer from the actual file content
            buffer = io.BytesIO(file_content)
            media = MediaIoBaseUpload(buffer, mimetype=mime_type, resumable=True)
            
            # Upload the file
            log_api_request("POST", "drive.files.create", file_metadata, channel=channel, file_id=fid)
            file = self.drive.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            drive_file_id = file.get('id')
            log_api_response(200, "drive.files.create", file, channel=channel, file_id=fid)
            
            if not drive_file_id:
                log_with_context(
                    logging.WARNING,
                    f"Failed to get Drive file ID for {name}",
                    channel=channel,
                    file_id=fid
                )
                return None
                
            # Cache the Drive file ID
            self.drive_cache[fid] = drive_file_id
            
            # Mark as processed
            self.processed_files.add(fid)
            
            # Update statistics
            self.migrator.migration_summary["files_created"] += 1
            
            log_with_context(
                logging.DEBUG,
                f"Successfully uploaded file to Drive: {name} → Drive ID: {drive_file_id}",
                channel=channel,
                file_id=fid,
                drive_file_id=drive_file_id
            )
            
            return drive_file_id
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to upload file {name}: {e}",
                channel=channel,
                file_id=fid,
                error=str(e)
            )
            return None
    
    def _get_or_create_channel_folder(self, channel: str) -> Optional[str]:
        """Get or create a channel-specific folder in Google Drive.
        
        Args:
            channel: The channel name
            
        Returns:
            The folder ID if successful, None otherwise
        """
        if self.dry_run:
            return None
            
        # Check if we've already cached this folder ID
        cache_key = f"folder_{channel}"
        if cache_key in self.drive_cache:
            return self.drive_cache[cache_key]
            
        try:
            # Check if the folder already exists
            query = f"name = '{channel}' and mimeType = 'application/vnd.google-apps.folder' and '{self.drive_folder_id}' in parents and trashed = false"
            
            log_api_request("GET", "drive.files.list", {"query": query}, channel=channel)
            results = self.drive.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
            items = results.get('files', [])
            
            # Use existing folder if found
            if items:
                folder_id = items[0]['id']
                log_with_context(
                    logging.DEBUG,
                    f"Using existing Drive folder for channel {channel}: {folder_id}",
                    channel=channel
                )
                # Cache the folder ID
                self.drive_cache[cache_key] = folder_id
                return folder_id
                
            # Create folder if not found
            log_with_context(
                logging.DEBUG,
                f"Creating Drive folder for channel {channel}",
                channel=channel
            )
            
            folder_metadata = {
                'name': channel,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.drive_folder_id]
            }
            
            log_api_request("POST", "drive.files.create", folder_metadata, channel=channel)
            folder = self.drive.files().create(body=folder_metadata, fields='id').execute()
            folder_id = folder.get('id')
            
            if folder_id:
                log_with_context(
                    logging.DEBUG,
                    f"Created Drive folder for channel {channel}: {folder_id}",
                    channel=channel
                )
                # Cache the folder ID
                self.drive_cache[cache_key] = folder_id
                return folder_id
            else:
                log_with_context(
                    logging.WARNING,
                    f"Failed to create Drive folder for channel {channel}",
                    channel=channel
                )
                return None
                
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Failed to get or create Drive folder for channel {channel}: {e}",
                channel=channel,
                error=str(e)
            )
            return None
    
    def share_file_with_members(self, drive_file_id: str, channel: str) -> bool:
        """Share a Drive file with all active members of a channel.
        
        Args:
            drive_file_id: The ID of the Drive file to share
            channel: The channel name to get members from
            
        Returns:
            True if sharing was successful, False otherwise
        """
        if not hasattr(self.migrator, "active_users_by_channel") or channel not in self.migrator.active_users_by_channel:
            log_with_context(
                logging.WARNING,
                f"No active users tracked for channel {channel}, can't share file",
                channel=channel
            )
            return False
            
        active_users = self.migrator.active_users_by_channel[channel]
        emails_to_share = []
        
        # Get emails for all active users
        for user_id in active_users:
            email = self.migrator.user_map.get(user_id)
            if email and not self.migrator._is_external_user(email):
                emails_to_share.append(email)
        
        if not emails_to_share:
            log_with_context(
                logging.WARNING,
                f"No valid emails found for active users in channel {channel}",
                channel=channel
            )
            return False
            
        try:
            # Share the file with each user
            log_with_context(
                logging.DEBUG,
                f"Sharing Drive file {drive_file_id} with {len(emails_to_share)} users",
                channel=channel
            )
            
            for email in emails_to_share:
                try:
                    # Create a permission for the user
                    permission = {
                        'type': 'user',
                        'role': 'reader',
                        'emailAddress': email
                    }
                    
                    log_api_request(
                        "POST", 
                        "drive.permissions.create", 
                        permission, 
                        channel=channel,
                        file_id=drive_file_id
                    )
                    
                    self.drive.permissions().create(
                        fileId=drive_file_id,
                        body=permission,
                        sendNotificationEmail=False
                    ).execute()
                    
                except Exception as e:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to share file with {email}: {e}",
                        channel=channel,
                        file_id=drive_file_id
                    )
            
            log_with_context(
                logging.DEBUG,
                f"Successfully shared Drive file with channel members",
                channel=channel,
                file_id=drive_file_id
            )
            return True
            
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Failed to share Drive file: {e}",
                channel=channel,
                file_id=drive_file_id
            )
            return False
    
    @retry()
    def process_attachments(self, chat_service, message_name: str, message_id: str, files: list, channel: Optional[str] = None) -> None:
        """Process file attachments for a message in import mode."""
        if self.dry_run:
            return
            
        log_with_context(
            logging.DEBUG,
            f"Processing {len(files)} attachments for message {message_id}",
            channel=channel,
            message_id=message_id
        )
        
        # Process each file
        for idx, file in enumerate(files, 1):
            file_name = file.get('name', f"attachment_{idx}")
            file_id = file.get('id', 'unknown')
            
            log_with_context(
                logging.DEBUG,
                f"Processing attachment {idx}/{len(files)}: {file_name}",
                channel=channel,
                message_id=message_id,
                file_id=file_id
            )
            
            # Upload file to Drive
            drive_file_id = self.upload_file(file, channel)
            
            if not drive_file_id:
                log_with_context(
                    logging.WARNING,
                    f"Skipping attachment {file_name} due to upload failure",
                    channel=channel,
                    message_id=message_id,
                    file_id=file_id
                )
                continue
            
            # Share the file with all members of the channel if channel is provided
            if channel:
                self.share_file_with_members(drive_file_id, channel)
                
            try:
                # Add attachment to message using the correct format for the Chat API import
                # Following documentation at https://developers.google.com/workspace/chat/import-data#attachments
                attachment_data = {
                    'attachmentDataRef': {
                        'resourceName': f"drives/0/files/{drive_file_id}"
                    }
                }
                
                if file.get('mimetype'):
                    attachment_data['contentType'] = file.get('mimetype')
                
                if file.get('name'):
                    attachment_data['contentName'] = file.get('name')
                
                log_with_context(
                    logging.DEBUG,
                    f"Attaching file {file_name} to message {message_id} with resourceName drives/0/files/{drive_file_id}",
                    channel=channel,
                    message_id=message_id,
                    file_id=file_id,
                    drive_file_id=drive_file_id
                )
                
                # Use the parent message name as the parent parameter
                log_api_request(
                    "POST", 
                    "chat.spaces.messages.attachments.create", 
                    attachment_data,
                    channel=channel,
                    message_id=message_id,
                    file_id=file_id
                )
                
                result = chat_service.spaces().messages().attachments().create(
                    parent=message_name,
                    body=attachment_data
                ).execute()
                
                log_api_response(
                    200, 
                    "chat.spaces.messages.attachments.create", 
                    result,
                    channel=channel,
                    message_id=message_id,
                    file_id=file_id
                )
                
                log_with_context(
                    logging.DEBUG,
                    f"Successfully attached file {file_name} to message {message_id}",
                    channel=channel,
                    message_id=message_id,
                    file_id=file_id,
                    drive_file_id=drive_file_id
                )
            except HttpError as e:
                log_with_context(
                    logging.WARNING,
                    f"Failed to attach file {file_name} to message {message_id}: {e}",
                    channel=channel,
                    message_id=message_id,
                    file_id=file_id,
                    drive_file_id=drive_file_id,
                    error_code=e.resp.status,
                    error_message=str(e)
                ) 
                
                # If we can't attach the file, add a message with a link to the file
                try:
                    # Create a link to the file in the message text
                    file_link = f"https://drive.google.com/file/d/{drive_file_id}/view"
                    file_message = f"File attachment: [{file_name}]({file_link})"
                    
                    # Get the original message's create time and add a small offset to avoid conflicts
                    try:
                        original_message = chat_service.spaces().messages().get(name=message_name).execute()
                        original_time = original_message.get("createTime", "")
                        
                        # Parse the timestamp and add a small offset (1 second per file index)
                        import datetime
                        from datetime import timezone
                        
                        if original_time.endswith('Z'):
                            original_time = original_time[:-1] + '+00:00'
                            
                        dt = datetime.datetime.fromisoformat(original_time)
                        new_dt = dt + datetime.timedelta(seconds=idx)
                        reply_time = new_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    except Exception as e:
                        # If we can't get the original time, use current time
                        import time
                        from slack_migrator.utils.api import slack_ts_to_rfc3339
                        reply_time = slack_ts_to_rfc3339(f"{time.time() + idx}.000000")
                    
                    # Extract the space name from the message name
                    space_name = message_name.split("/messages/")[0]
                    
                    # Add a reply to the message with the file link
                    reply_body = {
                        "text": file_message,
                        "createTime": reply_time,
                        "thread": {"name": space_name + "/threads/" + message_id}
                    }
                    
                    log_api_request(
                        "POST",
                        "chat.spaces.messages.create",
                        reply_body,
                        channel=channel,
                        message_id=message_id,
                        file_id=file_id
                    )
                    
                    # Create the message with the proper thread reference
                    request_params = {
                        "parent": space_name,
                        "body": reply_body,
                        "messageReplyOption": "REPLY_MESSAGE_OR_FAIL"
                    }
                    
                    chat_service.spaces().messages().create(**request_params).execute()
                    
                    log_with_context(
                        logging.INFO,
                        f"Added file link as a reply message instead: {file_link}",
                        channel=channel,
                        message_id=message_id,
                        file_id=file_id,
                        drive_file_id=drive_file_id
                    )
                except Exception as reply_error:
                    log_with_context(
                        logging.WARNING,
                        f"Failed to add file link as reply: {reply_error}",
                        channel=channel,
                        message_id=message_id,
                        file_id=file_id
                    )
                
    @retry()
    def _download_file(self, file_obj: Dict) -> Optional[bytes]:
        """Download a file from Slack export or URL.
        
        Args:
            file_obj: The file object from Slack
            
        Returns:
            File content as bytes, or None if download failed
        """
        try:
            file_id = file_obj.get('id', 'unknown')
            name = file_obj.get('name', f'file_{file_id}')
            url_private = file_obj.get('url_private')
            
            if not url_private:
                log_with_context(
                    logging.WARNING,
                    f"No URL found for file: {name}",
                    file_id=file_id
                )
                return None
                
            # Create a hash of the URL to use as cache key
            file_hash = hashlib.md5(url_private.encode()).hexdigest()
            
            # Special handling for Google Docs links
            if ('docs.google.com' in url_private or 
                'drive.google.com' in url_private or 
                'sheets.google.com' in url_private or 
                'slides.google.com' in url_private):
                log_with_context(
                    logging.INFO,
                    f"Google Docs link detected: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                    file_id=file_id,
                    file_name=name
                )
                
                # For Google Docs links, we can't download the actual file content
                # Instead, we'll create a placeholder file with the link
                placeholder_content = f"Google Docs link: {url_private}".encode('utf-8')
                
                log_with_context(
                    logging.INFO,
                    f"Created placeholder for Google Docs link: {name}",
                    file_id=file_id
                )
                
                return placeholder_content
            
            log_with_context(
                logging.DEBUG,
                f"Downloading file from URL: {url_private[:100]}{'...' if len(url_private) > 100 else ''}",
                file_id=file_id,
                file_name=name
            )
            
            # For files in the export, the URL might contain a token
            # We'll try to download using requests
            headers = {}
            if self.slack_token:
                headers['Authorization'] = f'Bearer {self.slack_token}'
                log_with_context(
                    logging.DEBUG,
                    "Using Slack token for authenticated download",
                    file_id=file_id
                )
                
            response = requests.get(url_private, headers=headers, stream=True)
            
            if response.status_code != 200:
                log_with_context(
                    logging.WARNING,
                    f"Failed to download file {name}: HTTP {response.status_code}",
                    file_id=file_id,
                    http_status=response.status_code
                )
                # Raise an exception to trigger the retry
                response.raise_for_status()
                return None
                
            # Get content length if available
            content_length = response.headers.get('Content-Length')
            if content_length:
                log_with_context(
                    logging.DEBUG,
                    f"File size from headers: {content_length} bytes",
                    file_id=file_id
                )
                
            # Return the actual file content
            content = response.content
            log_with_context(
                logging.DEBUG,
                f"Successfully downloaded file: {name} (Size: {len(content)} bytes)",
                file_id=file_id
            )
            return content
            
        except requests.exceptions.RequestException as e:
            # Check for authentication errors (401, 403) which are unlikely to be resolved by retrying
            if hasattr(e, 'response') and e.response and e.response.status_code in (401, 403):
                log_with_context(
                    logging.WARNING,
                    f"Authentication error downloading file, not retrying: {str(e)}",
                    file_id=file_obj.get('id', 'unknown'),
                    file_name=file_obj.get('name', 'unknown'),
                    error=str(e),
                    status_code=e.response.status_code
                )
                # Return None instead of re-raising to prevent further retries for auth errors
                return None
            
            # For other network errors, log and re-raise to trigger retry
            log_with_context(
                logging.WARNING,
                f"Error downloading file: {str(e)}",
                file_id=file_obj.get('id', 'unknown'),
                file_name=file_obj.get('name', 'unknown'),
                error=str(e)
            )
            raise  # Re-raise to trigger retry
        except Exception as e:
            log_with_context(
                logging.ERROR,
                f"Error downloading file: {str(e)}",
                file_id=file_obj.get('id', 'unknown'),
                file_name=file_obj.get('name', 'unknown'),
                error=str(e)
            )
            return None 