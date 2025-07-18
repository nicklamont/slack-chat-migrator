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
- Includes comprehensive reports for both dry runs and actual migrations
- Identifies users without email addresses for mapping
- Automatic permission checking before migration

### Prerequisites

- Python 3.9+
- Google Cloud SDK (`gcloud`)
- GCP project with Chat & Drive APIs enabled
- Service account w/ domain-wide delegation and scopes:
  - https://www.googleapis.com/auth/chat.import
  - https://www.googleapis.com/auth/chat.spaces
  - https://www.googleapis.com/auth/drive.file
- Slack export folder:
  ```
  export_root/
    channels.json
    users.json
    <channel_name>/
      YYYY-MM-DD.json
  ```

### Installation

```bash
# Clone the repository
git clone https://github.com/nicklamont/slack-chat-migrator.git
cd slack-chat-migrator

# Install the Python package in development mode
python -m venv venv && source venv/bin/activate
pip install -e .

# Or install directly from repository
pip install git+https://github.com/nicklamont/slack-chat-migrator.git
```

> Note: The `setup.py` file is used by pip during installation and shouldn't be run directly.

### Configuration

Create a `config.yaml` file:

```yaml
# Folder where attachments will be stored in Google Drive
attachments_folder: "Slack Attachments"

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

### Usage

```bash
# Perform a dry run and generate a comprehensive report
slack-migrator \
  --creds_path $GOOGLE_APPLICATION_CREDENTIALS \
  --export_path ./export_root \
  --workspace_admin admin@domain.com \
  --config config.yaml \
  --dry_run \
  --report_file migration_report.yaml

# Execute the migration
slack-migrator \
  --creds_path $GOOGLE_APPLICATION_CREDENTIALS \
  --export_path ./export_root \
  --workspace_admin admin@domain.com \
  --config config.yaml \
  --slack_token xoxp-...   # optional

# Run with verbose (debug) logging
slack-migrator \
  --creds_path $GOOGLE_APPLICATION_CREDENTIALS \
  --export_path ./export_root \
  --workspace_admin admin@domain.com \
  --config config.yaml \
  --verbose  # or -v
```

For backward compatibility, you can also use the wrapper script:

```bash
python slack_to_chat_migration.py \
  --creds_path $GOOGLE_APPLICATION_CREDENTIALS \
  --export_path ./export_root \
  --workspace_admin admin@domain.com \
  --config config.yaml
```

### Permission Setup

Before running the migration, you need to set up the proper Google Cloud and Google Workspace permissions:

1. Run the permission setup script (requires Google Cloud SDK):
   ```bash
   ./setup_permissions.sh
   ```
   
   You can customize the setup with these options:
   ```bash
   ./setup_permissions.sh --project your-project-id --sa-name custom-sa-name --key-file custom-key.json
   ```

2. Follow the instructions to configure domain-wide delegation in the Google Workspace Admin Console.

3. After setting up permissions with the script above, verify your setup using the permission checker tool:
   ```bash
   slack-migrator-check-permissions --creds_path /path/to/credentials.json --workspace_admin admin@domain.com
   ```

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

### Migration Reports

The tool generates comprehensive reports in both dry run mode and after actual migrations:

1. **Dry Run Report**: Generated when running with the `--dry_run` flag, shows what would happen during migration
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

1. Run a dry run to generate a report:
   ```bash
   slack-migrator --creds_path ... --export_path ... --workspace_admin ... --config ... --dry_run
   ```

2. Review the report and add the mappings to your `config.yaml` file under `user_mapping_overrides`

3. Run the migration with the updated config

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
   # grant chat.admin and drive.file
   ```

3. Domain-wide delegation in Admin console with above scopes.

4. Build & push container:
   ```bash
   gcloud builds submit --tag gcr.io/$PROJECT_ID/slack-migrator
   ```

5. Create & execute Cloud Run job (mount export & map via Cloud Storage or volume).

### License

MIT