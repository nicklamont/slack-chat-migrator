## Slack to Google Chat Migration Tool

This tool migrates Slack JSON exports into Google Chat spaces via the Chat Import API.

### Features

- Imports all messages (including threaded replies)
- Uploads attachments into Google Drive with name+MD5 deduplication
- Migrates emoji reactions using per-user impersonation
- Posts channel metadata (purpose/topic) from `channels.json`
- Retries API calls on transient errors with exponential backoff
- Logs in structured JSON for easy Cloud Logging ingestion
- Filters channels via `config.yaml`
- Automatically generates user mapping from users.json
- Maps external emails to internal workspace emails
- Includes comprehensive reports for both validation runs and actual migrations
- Identifies users without email addresses for mapping
- Automatic permission checking before migration

### Prerequisites

- Python 3.9+ (with `venv` module for virtual environment management)
- Google Cloud SDK (`gcloud`) (optional, only needed for automated setup)
  - Required only if using the `setup_permissions.sh` script for automated permission setup
  - All permissions can also be configured manually through the Google Cloud Console
  - If installing:
    - macOS: `brew install --cask google-cloud-sdk`
    - Linux/Windows: [Official installation guide](https://cloud.google.com/sdk/docs/install)
    - After installation: Run `gcloud init` to configure
- GCP project with Chat & Drive APIs enabled
- Service account w/ domain-wide delegation and scopes:
  - https://www.googleapis.com/auth/chat.import
  - https://www.googleapis.com/auth/chat.spaces
  - https://www.googleapis.com/auth/chat.messages
  - https://www.googleapis.com/auth/chat.spaces.readonly
  - https://www.googleapis.com/auth/chat.memberships.readonly
  - https://www.googleapis.com/auth/drive (for file uploads and shared drive creation)
- Slack export folder:
  ```
  export_root/
    channels.json
    users.json
    <channel_name>/
      YYYY-MM-DD.json
  ```

> **Note:** Using a Python virtual environment is recommended for Python dependency management.
> The Google Cloud SDK is a system-level component that cannot be installed via pip.

### Installation

```bash
# Clone the repository
git clone https://github.com/nicklamont/slack-chat-migrator.git
cd slack-chat-migrator

# Create and activate a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the Python package in development mode
pip install -e .

# Or install directly from repository (still recommended to use a virtual environment)
pip install git+https://github.com/nicklamont/slack-chat-migrator.git
```

> Note: The `setup.py` file is used by pip during installation and shouldn't be run directly.

### Setting Up Permissions

Before running your first migration, you need to set up the Google Cloud permissions. The `setup_permissions.sh` script automates most of this process:

```bash
# Ensure the Google Cloud SDK (gcloud) is installed first
# Then run the permissions setup script
./setup_permissions.sh
```

**When to run setup_permissions.sh:**
- Once during initial setup, after installing the package
- If you're setting up the tool in a new Google Cloud project
- If you encounter permission errors during migration
- If you need to create a new service account

**Important Prerequisites:**
1. The script provides **automated** setup, but all steps can be done manually in the Google Cloud Console
2. If using the script, Google Cloud SDK must be installed as a system component (not via pip)
3. As an alternative to the script, follow the manual setup steps in the Troubleshooting section

**What the script does:**
1. Checks for the Google Cloud SDK installation (if using the automated approach)
2. Enables required Google Cloud APIs
3. Creates a service account with necessary permissions
4. Downloads a service account key file
5. Provides instructions for manual steps in Google Workspace admin console

**Important Notes:**
- The script is **optional** - all steps can be performed manually in the Google Cloud Console
- If you choose to use the script:
  - You need the Google Cloud SDK installed as a system component (not via pip)
  - After running the script, you'll still need to complete the domain-wide delegation setup in Google Workspace
- See the Troubleshooting section for manual setup instructions if you prefer not to use the script

### Configuration

Create a `config.yaml` file:

```yaml
# Shared Drive configuration for storing attachments
# Using a shared drive is recommended for organization-wide access
shared_drive:
  # Option 1: Specify an existing shared drive by ID
  # id: "0AInA6b6Ej1Q2Uk9PVA"  # Replace with your shared drive ID
  
  # Option 2: Specify a shared drive by name (will be created if it doesn't exist)
  name: "Imported Slack Attachments"
  
  # If neither id nor name is specified, a new shared drive will be created
  # with the name "Imported Slack Attachments"

# Optional: Channels to exclude from migration (by name)
exclude_channels:
  - "random"
  - "shitposting"

# Optional: Channels to include in migration (if specified, only these will be processed)
include_channels: []

# Optional: Override email domains for user mapping
# If not specified, emails from users.json will be used directly
email_domain_override: ""  # e.g. "company.com"

# Optional: User mapping overrides
# Use this to manually map specific Slack user IDs to Google Workspace emails
# This takes precedence over the automatic mapping from users.json
# You can also use this to map external emails to internal ones
user_mapping_overrides:
  # Map Slack user IDs to emails
  "U12345678": "user@example.com"
  # Map bot accounts that don't have emails
  "U87654321": "slackbot@company.com"

# User handling options
# Whether to skip importing bot messages and reactions
# When true, all bot messages and reactions will be excluded from migration
# When false (default), bot messages will be migrated if user mappings exist
ignore_bots: false

# Error handling configuration
# Whether to abort the entire migration if errors are encountered in a channel
abort_on_error: false

# Maximum percentage of message failures allowed per channel before skipping
# If more than this percentage of messages fail in a channel, the channel will be skipped
max_failure_percentage: 10

# Strategy for completing import mode when errors occur
# Options:
#   - "skip_on_error": Skip completing import mode if channel had errors (default)
#   - "force_complete": Complete import mode even if errors occurred
#   - "always_skip": Never complete import mode (useful for testing)
import_completion_strategy: "skip_on_error"

# Whether to delete spaces that had errors during migration
# If true, spaces with errors will be deleted during cleanup
cleanup_on_error: false

# Maximum number of retries for API calls
max_retries: 3

# Delay between retries (in seconds)
retry_delay: 2
```

> **Note:** Debug and logging options are controlled via command-line flags (`--verbose` and `--debug_api`) rather than configuration file settings. This ensures consistent logging behavior across all migration runs.

### Command-Line Reference

The `slack-migrator` command provides several options for different migration scenarios:

#### Available Command-Line Options

| Option | Required | Description |
|--------|----------|-------------|
| `--creds_path` | Yes | Path to the service account credentials JSON file |
| `--export_path` | Yes | Path to the Slack export directory |
| `--workspace_admin` | Yes | Email of workspace admin to impersonate |
| `--config` | No | Path to config YAML (default: config.yaml) |
| `--dry_run` | No | Validation-only mode - performs comprehensive validation without making changes |
| `--update_mode` | No | Update mode - update existing spaces instead of creating new ones |
| `--verbose` or `-v` | No | Enable verbose console logging (shows DEBUG level messages) |
| `--debug_api` | No | Enable detailed API request/response logging (creates very large log files) |
| `--skip_permission_check` | No | Skip permission checks (not recommended) |

#### Logging and Debug Options

The migration tool provides two levels of debug logging:

- **`--verbose` / `-v`**: Enables verbose console output showing DEBUG level messages. Useful for understanding the migration flow and troubleshooting issues.
- **`--debug_api`**: Enables detailed HTTP API request/response logging to files. This creates very large log files but is invaluable for diagnosing API-related issues or developing the tool. Only enable this when specifically needed.

Both options are independent and can be used together for maximum debugging information.

> **Note:** The `--skip_permission_check` option bypasses validation of service account permissions. Only use this if you're certain your service account is properly configured and you're encountering false positives in the permission check.

#### Basic Command Example

```bash
slack-migrator migrate \
  --creds_path ./slack-chat-migrator-sa-key.json \
  --export_path ./slack_export \
  --workspace_admin admin@company.com \
  --verbose
```

**Example commands for different scenarios:**

```bash
# Check permissions before migration (highly recommended)
slack-migrator-check-permissions \
  --creds_path /path/to/key.json \
  --workspace_admin admin@company.com

# Perform comprehensive validation before migration
slack-migrator \
  --creds_path /path/to/key.json \
  --export_path ./slack_export \
  --workspace_admin admin@company.com \
  --dry_run

# Execute the full migration
slack-migrator \
  --creds_path /path/to/key.json \
  --export_path ./slack_export \
  --workspace_admin admin@company.com

# Resume an interrupted migration
slack-migrator \
  --creds_path /path/to/key.json \
  --export_path ./slack_export \
  --workspace_admin admin@company.com \
  --update_mode

# Debug a problematic migration with detailed logging
slack-migrator \
  --creds_path /path/to/key.json \
  --export_path ./slack_export \
  --workspace_admin admin@company.com \
  --verbose

# Enable API debugging for development or complex troubleshooting
slack-migrator \
  --creds_path /path/to/key.json \
  --export_path ./slack_export \
  --workspace_admin admin@company.com \
  --verbose --debug_api
```

> **Note:** For backward compatibility, you can also use `python slack_to_chat_migration.py` with the same arguments.

### Migration Workflow

For a successful migration, follow this recommended workflow:

> **Note:** Using a Python virtual environment is recommended for Python package dependencies, but the Google Cloud SDK must be installed as a system component outside of any virtual environment.

1. **Install the Google Cloud SDK** (one-time system setup):
   ```bash
   # macOS
   brew install --cask google-cloud-sdk
   
   # Linux/Windows: Follow the instructions at https://cloud.google.com/sdk/docs/install
   # After installation, run:
   gcloud init
   ```

2. **Set up permissions** (one-time setup):
   ```bash
   # Run with default settings
   ./setup_permissions.sh
   
   # Or customize the setup
   ./setup_permissions.sh --project your-project-id --sa-name custom-sa-name --key-file custom-key.json
   ```
   
   After running the script, complete the domain-wide delegation setup in Google Workspace Admin Console as instructed.

2. **Verify permissions** (recommended before each migration):
   ```bash
   slack-migrator-check-permissions \
     --creds_path /path/to/credentials.json \
     --workspace_admin admin@domain.com
   ```

3. **Configure migration settings** (before first migration):
   ```bash
   # Create and edit your configuration
   cp config.yaml.example config.yaml
   nano config.yaml  # or your preferred editor
   ```

4. **Run comprehensive validation** (automatically performed before every migration):
   ```bash
   # The tool automatically runs a comprehensive validation first:
   # • Validates all user mappings and detects unmapped users
   # • Checks file attachments and permissions
   # • Verifies channel structure and memberships
   # • Tests message formatting and content
   # • Estimates migration scope and requirements
   
   # For validation-only runs:
   slack-migrator \
     --creds_path /path/to/credentials.json \
     --export_path ./slack_export \
     --workspace_admin admin@domain.com \
     --dry_run
     
   # Regular migration (automatically includes validation step):
   slack-migrator \
     --creds_path /path/to/credentials.json \
     --export_path ./slack_export \
     --workspace_admin admin@domain.com
   ```
   The validation identifies potential issues before any changes are made.

5. **Execute the migration** (validation runs automatically first):
   ```bash
   # The migration process includes automatic validation:
   # Step 1: Comprehensive validation (dry run)
   # Step 2: User confirmation to proceed
   # Step 3: Actual migration
   
   slack-migrator \
     --creds_path /path/to/credentials.json \
     --export_path ./slack_export \
     --workspace_admin admin@domain.com
   ```

6. **If migration is interrupted** (optional):
   ```bash
   # Resume migration with update mode
   slack-migrator \
     --creds_path /path/to/credentials.json \
     --export_path ./slack_export \
     --workspace_admin admin@domain.com \
     --update_mode
   ```
   
   Update mode will find existing spaces and only import messages that are newer than the last message 
   in each space. This approach is simple and reliable but has a known limitation: thread replies to
   older messages may be posted as new standalone messages instead of being properly threaded.
   
   If multiple Google Chat spaces exist with the same name (e.g., multiple "Slack #general" spaces), 
   the tool will detect this conflict and show you the details of each space. You'll need to add a 
   `space_mapping` section to your config.yaml file to specify which space to use for each channel:
   
   ```yaml
   space_mapping:
     "general": "AAAAAgcE123"  # Use this space ID for the general channel
     "random": "AAAABbTr456"   # Use this space ID for the random channel
   ```

> **Note:** You only need to run the permissions setup script once, unless you change Google Cloud projects, need a new service account, or encounter permission errors.

4. The migration tool will automatically check permissions before running. If you want to skip this check:
   ```bash
   slack-migrator --creds_path ... --export_path ... --workspace_admin ... --config ... --skip_permission_check
   ```

### Migration Process and Cleanup

The migration process follows these steps for each channel:

1. **Create Space**: Creates a Google Chat space in import mode
2. **Add Historical Members**: Adds users who were in the Slack channel
3. **Send Intro**: Posts channel metadata (purpose/topic) as the first message
4. **Import Messages**: Migrates all messages with their attachments and reactions
5. **Complete Import**: Finishes the import mode for the space
6. **Add Regular Members**: Adds all members back to the space as regular members

#### Error Handling

The tool provides several configurable options for handling errors during migration:

1. **Abort on Error**: When enabled (`abort_on_error: true`), the migration will stop after encountering errors in a channel. When disabled (default), the migration will continue processing other channels even if errors occur.

2. **Maximum Failure Percentage**: Controls how many message failures are tolerated within a channel before skipping the rest of that channel (`max_failure_percentage: 10` by default). If the failure rate exceeds this percentage, the channel processing will stop.

3. **Import Completion Strategy**: Determines how to handle import mode completion when errors occur:
   - `skip_on_error` (default): Don't complete import mode if there were errors
   - `force_complete`: Complete import mode even if there were errors
   - `always_skip`: Never complete import mode (useful for testing)

4. **Cleanup on Error**: When enabled (`cleanup_on_error: true`), spaces with errors will be deleted during cleanup. When disabled (default), spaces with errors will be kept (allowing manual completion).

5. **API Retry Settings**: Configure how API calls are retried when errors occur:
   - `max_retries: 3` (default): Maximum number of retry attempts for failed API calls
   - `retry_delay: 2` (default): Initial delay in seconds between retry attempts

These options can be configured in your `config.yaml` file:

```yaml
# Error handling configuration
abort_on_error: false
max_failure_percentage: 10
import_completion_strategy: "skip_on_error"
cleanup_on_error: false

# API retry settings
max_retries: 3
retry_delay: 2
```

#### Cleanup Process

After all channels are processed, a **cleanup process** runs to ensure all spaces are properly out of import mode. This cleanup:

1. Lists all spaces created by the migration tool
2. Identifies any spaces still in "import mode" that weren't properly completed
3. Completes the import mode for these spaces with retry logic
4. Preserves external user access settings where applicable
5. Adds regular members to these spaces

The cleanup process is important because spaces in import mode have limitations and will be automatically deleted after 90 days if not properly completed.

### Output Directory and Log Files

The migration tool automatically creates a timestamped output directory for each migration run to store logs, reports, and other output files:

```
migration_logs/
├── run_20250806_153200/          # Timestamped run directory
│   ├── migration.log             # Main migration log
│   ├── migration_report.yaml     # Summary report
│   ├── channel_logs/            # Per-channel detailed logs
│   │   ├── general_migration.log
│   │   └── random_migration.log
│   └── failed_messages.txt       # Failed messages (if any)
```

**Log File Types:**

- **migration.log**: Main log file containing overall migration progress, errors, and system messages
- **channel_logs/*.log**: Per-channel detailed logs with message-level details (when `--debug_api` is enabled)
- **migration_report.yaml**: Structured summary report with statistics and recommendations
- **failed_messages.txt**: Details of any messages that failed to migrate (created only if there are failures)

> **Note:** When using `--debug_api`, channel logs can become quite large as they include complete API request/response data.

### Migration Reports

The tool generates comprehensive reports in both validation mode and after actual migrations:

1. **Validation Report**: Generated when running with the `--dry_run` flag, shows comprehensive validation results including:
   - User mapping validation and unmapped user detection
   - File attachment accessibility checks
   - Channel structure verification
   - Message formatting validation
   - Migration scope estimation
   
2. **Migration Summary**: Generated after a real migration, shows what actually happened

The reports include:

1. **Channels**: Which channels were/will be processed and how many spaces were/will be created
2. **Messages**: Count of messages and reactions migrated/to be migrated
3. **Files**: Count of files uploaded/to be uploaded
4. **Users**:
   - External emails detected and suggested mappings
   - Users without email addresses that need mapping
5. **Recommendations**: Actionable suggestions to improve the migration

Example report:

```yaml
report_type: dry_run  # or migration_summary
timestamp: "2023-06-26T17:54:05.596Z"
workspace_admin: admin@company.com
export_path: /path/to/export
channels:
  to_process:
  - general
  - random
  total_count: 2
  spaces_to_create: 2
messages:
  to_create: 1250
  reactions_to_add: 78
files:
  to_upload: 15
users:
  external_emails:
    personal@gmail.com: personal@company.com
  external_email_count: 1
  users_without_email:
    U12345678:
      name: slackbot
      real_name: Slackbot
      type: Bot
      suggested_email: slackbot@company.com
  users_without_email_count: 1
recommendations:
- type: users_without_email
  message: Found 1 users without email addresses. Add them to user_mapping_overrides in your config.yaml.
  severity: warning
- type: external_emails
  message: Found 1 external email addresses. Consider mapping them to internal workspace emails using user_mapping_overrides in your config.yaml.
  severity: info
```

### User Mapping

The tool maps Slack users to Google Workspace users in several ways:

1. **Automatic mapping**: Uses the email addresses from Slack's `users.json` file
2. **Domain override**: Replaces the domain of all email addresses with a specified domain
3. **User mapping overrides**: Manually map specific Slack user IDs to Google Workspace emails

When users sign up for Slack with personal emails (like `personal@gmail.com`) but have a corresponding internal workspace email (like `work@company.com`), or when bots/integrations don't have email addresses, you can map them using `user_mapping_overrides`.

To identify users that need mapping:

1. Run comprehensive validation to generate a report:
   ```bash
   slack-migrator --creds_path ... --export_path ... --workspace_admin ... --config ... --dry_run
   ```
   This validates all aspects of the migration including user mappings, file access, and content formatting.

2. Review the validation report and add any required mappings to your `config.yaml` file under `user_mapping_overrides`

3. Run the migration with the updated config (validation runs automatically first)

### Package Structure

The codebase is organized into the following modules:

- `slack_migrator/__init__.py` - Package initialization
- `slack_migrator/__main__.py` - Main entry point
- `slack_migrator/core/` - Core functionality
  - `slack_migrator/core/migrator.py` - Main migration logic
  - `slack_migrator/core/config.py` - Configuration handling
- `slack_migrator/services/` - Service interactions
  - `slack_migrator/services/space.py` - Space creation and management
  - `slack_migrator/services/message.py` - Message handling and formatting
  - `slack_migrator/services/file.py` - File and attachment handling
  - `slack_migrator/services/user.py` - User mapping utilities
- `slack_migrator/cli/` - Command-line interface components
  - `slack_migrator/cli/commands.py` - Main CLI commands
  - `slack_migrator/cli/report.py` - Report generation
  - `slack_migrator/cli/permission.py` - Permission checking
- `slack_migrator/utils/` - Utility functions
    - `slack_migrator/utils/logging.py` - Logging utilities
    - `slack_migrator/utils/api.py` - API and retry utilities
    - `slack_migrator/utils/formatting.py` - Message formatting utilities


### GCP on Cloud Run

1. Enable APIs:
   ```bash
   gcloud services enable chat.googleapis.com drive.googleapis.com
   ```

2. Create service account & grant roles:
   ```bash
   gcloud iam service-accounts create slack-migrator-sa
   # grant chat.admin and drive permissions
   ```

3. Domain-wide delegation in Admin console with above scopes.

4. Build & push container:
   ```bash
   gcloud builds submit --tag gcr.io/$PROJECT_ID/slack-migrator
   ```

5. Create & execute Cloud Run job (mount export & map via Cloud Storage or volume).

### Known Issues

- **Thread Continuity in Update Mode**: When using update mode to resume an interrupted migration, messages that are part of threads started in the previous migration may not be correctly threaded with their parent thread. This occurs because the tool only imports messages newer than the last message in each space, and thread replies to older messages are posted as new standalone messages instead of being properly attached to their original thread context.

- **Limited External User Support**: The migration tool has several limitations when dealing with external users (users outside your Google Workspace domain):
  - External users cannot be impersonated due to Google Chat API restrictions, so their messages are posted by the workspace admin with attribution text indicating the original sender
  - Emoji reactions from external users are dropped and not migrated
  - External users are not automatically added to migrated spaces - only internal workspace users receive space memberships during migration

- **Attachment Handling**: When Slack attachment files are uploaded to Google Drive during migration, a link to the Google Drive file is appended to the end of the message content rather than being attached as a native Google Chat attachment. This preserves access to the files but changes how they appear in the migrated conversations.

 - **Formatting Limitations**: Caveats related to migrating Markdown formatting from Slack to Google Chat, including:
   - Nested bullet lists may not indent correctly in Google Chat, causing subbullets to appear as top-level bullets.
   - Bold styling around user mentions (e.g., `*<@USER>*`) is not supported by Google Chat and will display literal asterisks.
### Troubleshooting

#### Google Cloud SDK Issues

- **Error: "No matching distribution found for google-cloud-sdk"**:
  - The Google Cloud SDK cannot be installed via pip in a virtual environment
  - Install the SDK using system package managers:
    - macOS: `brew install --cask google-cloud-sdk`
    - Linux/Windows: Use the [Official Installation Guide](https://cloud.google.com/sdk/docs/install)
  - After installation, run `gcloud init` to configure your environment
  
- **Permission denied when running setup_permissions.sh**:
  - Make the script executable: `chmod +x setup_permissions.sh`
  - Run as: `./setup_permissions.sh`
  
#### Manual Google Cloud Setup (Without Using the Script)

If you prefer not to use the setup script, follow these steps manually:

1. **Enable required APIs in Google Cloud Console**:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Navigate to "APIs & Services" > "Library"
   - Search for and enable these APIs:
     - Google Chat API
     - Google Drive API

2. **Create a Service Account**:
   - Go to "IAM & Admin" > "Service Accounts"
   - Click "Create Service Account"
   - Give it a name (e.g., "slack-chat-migrator")
   - Grant these roles:
     - Chat Service Agent
     - Drive File Organizer

3. **Create and Download a Key**:
   - In the service account details page, go to the "Keys" tab
   - Click "Add Key" > "Create new key"
   - Select JSON format and download it
   - Save this file as `slack-chat-migrator-sa-key.json` in your project directory

4. **Set Up Domain-Wide Delegation**:
   - Go to your Google Workspace Admin Console
   - Navigate to Security > API Controls > Domain-wide Delegation
   - Add a new API client with the client ID from your service account
   - Grant these OAuth scopes:
     - https://www.googleapis.com/auth/chat.import
     - https://www.googleapis.com/auth/chat.spaces
     - https://www.googleapis.com/auth/chat.messages
     - https://www.googleapis.com/auth/chat.spaces.readonly
     - https://www.googleapis.com/auth/chat.memberships.readonly
     - https://www.googleapis.com/auth/drive

#### Migration Issues

- **Permission Errors during migration**:
  - Ensure the service account has proper domain-wide delegation configured
  - Check that all required APIs are enabled
  - Verify your config.yaml has the correct service account key file path
  - Run `slack-migrator check-permissions` to validate permissions

- **Files/Attachments Not Migrating**:
  - Ensure you have the `drive` scope in your service account permissions
  - Check for errors in the migration log related to file access
  - Verify shared drive settings if using a shared drive for storage

### License

MIT