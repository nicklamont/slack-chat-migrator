#!/bin/bash
#
# Setup Permissions Script for Slack Chat Migration
# ------------------------------------------------
# This script automates the setup of Google Cloud permissions required
# for the Slack to Google Chat migration tool.
#
# WHEN TO RUN THIS SCRIPT:
# -----------------------
# Run this script ONCE during initial setup before using the migration tool:
#   1. After installing the slack-chat-migrator package
#   2. Before your first migration attempt
#   3. When setting up the tool in a new Google Cloud project
#   4. If you encounter permission errors during migration
#
# WHAT THIS SCRIPT DOES:
# ---------------------
# 1. Enables required Google Cloud APIs (Chat API, Drive API)
# 2. Creates a service account (no project-level IAM roles are granted: the
#    service account gets its access through domain-wide delegation)
# 3. Downloads a service account key file for authentication, or checks that
#    an existing key file belongs to the service account and that the key is
#    still active (not deleted, disabled or expired)
# 4. Provides instructions for setting up domain-wide delegation in Google Workspace
#
# The script stops with a non-zero exit status as soon as any step fails.
#
# PREREQUISITES:
# -------------
# - Google Cloud account with project admin access
# - Google Workspace admin access (for domain-wide delegation setup)
# - Google Cloud SDK (gcloud CLI) installed and configured
#   (Note: This must be installed as a system component, not via pip.
#    See: https://cloud.google.com/sdk/docs/install)
#
# Author: Nick Lamont
# License: MIT

set -euo pipefail

# Default configuration values (the project defaults to the current gcloud
# project, looked up once gcloud is known to be installed)
PROJECT_ID=""
SERVICE_ACCOUNT_NAME="slack-chat-migrator-sa"
KEY_FILE="slack-chat-migrator-sa-key.json"

# OAuth scopes for domain-wide delegation (keep in sync with
# REQUIRED_SCOPES in src/slack_chat_migrator/utils/api.py)
REQUIRED_SCOPES="https://www.googleapis.com/auth/chat.import,https://www.googleapis.com/auth/chat.spaces,https://www.googleapis.com/auth/chat.messages,https://www.googleapis.com/auth/chat.spaces.readonly,https://www.googleapis.com/auth/chat.memberships.readonly,https://www.googleapis.com/auth/drive"
CUSTOM_EMOJI_SCOPE="https://www.googleapis.com/auth/chat.customemojis.readonly"

# Print an error message and exit non-zero
fail() {
  echo "❌ $*" >&2
  exit 1
}

# Function to display script usage
show_help() {
  echo "Usage: $0 [options]"
  echo
  echo "Options:"
  echo "  --project PROJECT_ID    GCP project ID (default: current gcloud project)"
  echo "  --sa-name NAME          Service account name (default: slack-chat-migrator-sa)"
  echo "  --key-file FILE         Key file name (default: slack-chat-migrator-sa-key.json)"
  echo "  --help                  Show this help message"
  echo
  echo "Example:"
  echo "  $0 --project my-project-id --sa-name migration-service-account"
  exit 0
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --project)
      if [[ -z "${2:-}" || "${2:-}" == --* ]]; then
        fail "--project requires a value"
      fi
      PROJECT_ID="$2"
      shift 2
      ;;
    --sa-name)
      if [[ -z "${2:-}" || "${2:-}" == --* ]]; then
        fail "--sa-name requires a value"
      fi
      SERVICE_ACCOUNT_NAME="$2"
      shift 2
      ;;
    --key-file)
      if [[ -z "${2:-}" || "${2:-}" == --* ]]; then
        fail "--key-file requires a value"
      fi
      KEY_FILE="$2"
      shift 2
      ;;
    --help)
      show_help
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Use --help for usage information" >&2
      exit 1
      ;;
  esac
done

# Function to check for prerequisites and validate inputs
check_prerequisites() {
  echo "Checking prerequisites..."

  # Check if gcloud is installed (before anything calls it)
  if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: Google Cloud SDK (gcloud CLI) not found." >&2
    echo "The gcloud command must be installed as a system component, not via pip." >&2
    if [ -n "${VIRTUAL_ENV:-}" ]; then
      echo "Detected Python virtual environment: $(basename "$VIRTUAL_ENV")" >&2
      echo "The Google Cloud SDK can't be installed via pip in a virtual environment." >&2
    fi
    echo >&2
    echo "Install the Google Cloud SDK:" >&2
    echo "  macOS:   brew install --cask google-cloud-sdk" >&2
    echo "  Linux:   https://cloud.google.com/sdk/docs/install-sdk" >&2
    echo "  Windows: https://cloud.google.com/sdk/docs/install-sdk" >&2
    echo "Then run 'gcloud init' and this script again." >&2
    echo "Alternatively, use the interactive wizard: slack-chat-migrator setup" >&2
    exit 1
  fi

  # Default to the current gcloud project
  if [ -z "$PROJECT_ID" ]; then
    PROJECT_ID=$(gcloud config get-value project 2> /dev/null || true)
  fi

  # Validate project ID
  if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
    echo "❌ Error: No project ID specified and no default project set in gcloud" >&2
    echo "Please specify a project ID with --project or set a default project with:" >&2
    echo "  gcloud config set project PROJECT_ID" >&2
    exit 1
  fi

  # Verify gcloud auth (the command succeeds even when no account is active)
  local active_account
  active_account=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2> /dev/null || true)
  if [ -z "$active_account" ]; then
    echo "❌ Error: No active gcloud account found" >&2
    echo "Please run: gcloud auth login" >&2
    exit 1
  fi

  echo "✅ Prerequisites check passed"
}

# Run prerequisites check
check_prerequisites

# Set up service account email
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Print configuration
echo "======================================================================"
echo "Setting up permissions with the following configuration:"
echo "======================================================================"
echo "Project ID:            ${PROJECT_ID}"
echo "Service Account:       ${SERVICE_ACCOUNT_NAME}"
echo "Service Account Email: ${SERVICE_ACCOUNT_EMAIL}"
echo "Key File Path:         ${KEY_FILE}"
echo "======================================================================"
echo

# Function to enable required APIs
enable_apis() {
  echo "1️⃣ Enabling required Google Cloud APIs..."

  echo "   - Enabling Chat API"
  gcloud services enable chat.googleapis.com --project "${PROJECT_ID}" \
    || fail "Failed to enable Chat API"

  echo "   - Enabling Drive API"
  gcloud services enable drive.googleapis.com --project "${PROJECT_ID}" \
    || fail "Failed to enable Drive API"

  echo "✅ APIs enabled successfully"
  echo
}

# Function to set up service account
setup_service_account() {
  echo "2️⃣ Setting up service account..."

  # Check if service account exists
  if ! gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" --project "${PROJECT_ID}" &> /dev/null; then
    echo "   - Creating service account '${SERVICE_ACCOUNT_NAME}'..."
    gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
      --display-name="Slack Chat Migration Service Account" \
      --description="Service account for Slack to Google Chat migration" \
      --project "${PROJECT_ID}" \
      || fail "Failed to create service account"
    echo "   ✅ Service account created successfully"
  else
    echo "   ✅ Service account '${SERVICE_ACCOUNT_NAME}' already exists"
  fi

  # No project-level IAM roles are granted: the migration's access comes
  # entirely from domain-wide delegation scopes. (Project-wide roles such as
  # roles/iam.serviceAccountTokenCreator would let this account act as every
  # service account in the project.)
  echo
}

# Read a top-level string field from a JSON key file
json_field() {
  { grep -o "\"$1\": *\"[^\"]*\"" "$2" || true; } | head -n 1 | sed 's/.*: *"\([^"]*\)"/\1/'
}

# Check that an existing key file belongs to the service account and that the
# key is still usable: not deleted, not disabled and not expired.
verify_existing_key() {
  local key_email key_id keys line id disabled valid_before found now
  if [ ! -r "${KEY_FILE}" ]; then
    fail "Can't read key file '${KEY_FILE}'. The file was left unchanged; fix its permissions, or use --key-file to choose another path."
  fi
  if [ ! -s "${KEY_FILE}" ]; then
    fail "Key file '${KEY_FILE}' is empty (perhaps left by a failed key creation). Remove it and run this script again."
  fi
  key_email=$(json_field client_email "${KEY_FILE}")
  if [ -z "${key_email}" ]; then
    fail "Key file '${KEY_FILE}' is not a service account key file (no client_email). Use --key-file to choose another path, or move the file away."
  fi
  if [ "${key_email}" != "${SERVICE_ACCOUNT_EMAIL}" ]; then
    fail "Key file '${KEY_FILE}' belongs to '${key_email}', not ${SERVICE_ACCOUNT_EMAIL}. Use --key-file to choose another path, or move the old file away."
  fi

  key_id=$(json_field private_key_id "${KEY_FILE}")
  if [ -z "${key_id}" ]; then
    fail "Key file '${KEY_FILE}' is not a service account key (no private_key_id)."
  fi
  # A failure here says nothing about the key itself, so the file is kept.
  keys=$(gcloud iam service-accounts keys list \
    --iam-account="${SERVICE_ACCOUNT_EMAIL}" \
    --project "${PROJECT_ID}" \
    --managed-by=user \
    --format="csv[no-heading](name.basename(),disabled,validBeforeTime)") \
    || fail "Couldn't list the keys of ${SERVICE_ACCOUNT_EMAIL} (see the gcloud error above), so '${KEY_FILE}' couldn't be checked. The file was left unchanged; fix the problem (is the IAM API enabled, and may you list service account keys?) and run this script again."

  found=""
  while IFS=, read -r id disabled valid_before; do
    if [ "${id}" = "${key_id}" ]; then
      found=1
      break
    fi
  done <<< "${keys}"
  if [ -z "${found}" ]; then
    fail "Key file '${KEY_FILE}' holds key ${key_id}, which no longer exists for ${SERVICE_ACCOUNT_EMAIL} (it was deleted). Remove the file and run this script again to create a new key."
  fi
  case "${disabled}" in
    [Tt]rue)
      fail "Key file '${KEY_FILE}' holds key ${key_id} of ${SERVICE_ACCOUNT_EMAIL}, which is disabled. Enable the key again, or remove the file and run this script again to create a new key."
      ;;
  esac
  # An expiry that can't be read says nothing about the key: keep the file.
  if [ -n "${valid_before}" ] && ! [[ "${valid_before}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} ]]; then
    fail "Couldn't read the expiry time '${valid_before}' of key ${key_id} of ${SERVICE_ACCOUNT_EMAIL}, so '${KEY_FILE}' couldn't be checked. The file was left unchanged. Check the key in the Cloud Console, or remove the file and run this script again to create a new key."
  fi
  # ISO 8601 UTC timestamps compare correctly as strings.
  now=$(date -u +%Y-%m-%dT%H:%M:%S)
  if [ -n "${valid_before}" ] && [[ "${valid_before:0:19}" < "${now}" ]]; then
    fail "Key file '${KEY_FILE}' holds key ${key_id} of ${SERVICE_ACCOUNT_EMAIL}, which expired on ${valid_before}. Remove the file and run this script again to create a new key."
  fi
}

# Function to create and download service account key
create_service_account_key() {
  local requested_at created_here
  echo "3️⃣ Setting up service account key..."

  # Check if key file exists
  if [ ! -f "${KEY_FILE}" ]; then
    echo "   - Creating and downloading service account key to '${KEY_FILE}'..."
    # gcloud creates the output file before it calls IAM, so a failed
    # create leaves an empty file behind: remove it, so the next run creates
    # the key instead of stopping at an empty file.  Only a regular file this
    # run created is removed, never a symlink or a device the user named.
    created_here=""
    if [ ! -e "${KEY_FILE}" ] && [ ! -L "${KEY_FILE}" ]; then
      created_here=1
    fi
    requested_at=$(date -u '+%Y-%m-%d %H:%M')
    (umask 077 && gcloud iam service-accounts keys create "${KEY_FILE}" \
      --iam-account="${SERVICE_ACCOUNT_EMAIL}" \
      --project "${PROJECT_ID}") \
      || {
        if [ -n "${created_here}" ] && [ -f "${KEY_FILE}" ] && [ ! -L "${KEY_FILE}" ] \
          && [ ! -s "${KEY_FILE}" ]; then
          rm -f "${KEY_FILE}"
        fi
        fail "Failed to create service account key (see the gcloud error above). If it was a network or server (5xx) error, IAM may have created the key anyway: list the keys with 'gcloud iam service-accounts keys list --iam-account=${SERVICE_ACCOUNT_EMAIL} --managed-by=user' and delete only the one whose CREATED_AT is about ${requested_at} UTC, with 'gcloud iam service-accounts keys delete <KEY_ID> --iam-account=${SERVICE_ACCOUNT_EMAIL}'. Don't delete the account's other keys, which may be in use elsewhere."
      }
    echo "   ✅ Key file created and downloaded successfully"
  else
    verify_existing_key
    echo "   ✅ Key file '${KEY_FILE}' holds an active key of ${SERVICE_ACCOUNT_EMAIL} (using existing key)"
  fi
  # Secure the key file permissions
  chmod 600 "${KEY_FILE}"

  echo
}

# Function to extract client ID from key file
extract_client_id() {
  if [ ! -f "${KEY_FILE}" ]; then
    fail "Key file not found: ${KEY_FILE}"
  fi

  CLIENT_ID=$(json_field client_id "${KEY_FILE}")

  if [ -z "${CLIENT_ID}" ]; then
    fail "Could not extract client ID from key file"
  fi
}

# Function to display next steps
display_next_steps() {
  echo "======================================================================"
  echo "🎉 Setup completed successfully! 🎉"
  echo "======================================================================"
  echo
  echo "🔴 IMPORTANT: Manual steps required 🔴"
  echo "======================================================================"
  echo "A. Configure the Chat app in the Google Cloud console:"
  echo "   https://console.cloud.google.com/apis/api/chat.googleapis.com/hangouts-chat?project=${PROJECT_ID}"
  echo "   Set an app name, avatar URL and description, leave interactive features"
  echo "   disabled, and keep 'Markup syntax' set to 'Chat' (the tool sends Chat"
  echo "   markup and sets it on every message anyway). Click Save."
  echo
  echo "B. In the Google Workspace Admin Console:"
  echo "1️⃣ Go to https://admin.google.com/"
  echo "2️⃣ Navigate to Security → API Controls → Domain-wide Delegation"
  echo "3️⃣ Click 'Add new' to add your service account"
  echo "4️⃣ Enter the following Client ID:"
  echo
  echo "   ${CLIENT_ID}"
  echo
  echo "5️⃣ Enter the following OAuth scopes (copy and paste the entire line):"
  echo
  echo "   ${REQUIRED_SCOPES}"
  echo
  echo "   Only if you set 'use_chat_custom_emoji: true' in config.yaml, also add:"
  echo "   ${CUSTOM_EMOJI_SCOPE}"
  echo
  echo "6️⃣ Click 'Authorize'"
  echo "======================================================================"
  echo
  echo "🚀 NEXT STEPS - MIGRATION WORKFLOW 🚀"
  echo "======================================================================"
  echo "1. CREATE AND CUSTOMIZE CONFIG FILE (ONE-TIME SETUP)"
  echo "   Generate a config file from your Slack export:"
  echo
  echo "   slack-chat-migrator init --export_path /path/to/slack/export"
  echo
  echo "2. VALIDATE (RECOMMENDED BEFORE EACH MIGRATION)"
  echo "   Checks your export and, with credentials, tests the permissions"
  echo "   against the real APIs without migrating anything:"
  echo
  echo "   slack-chat-migrator validate --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                               --export_path /path/to/slack/export \\"
  echo "                               --workspace_admin your-admin@domain.com"
  echo
  echo "3. RUN THE ACTUAL MIGRATION"
  echo "   When you're ready to perform the actual migration:"
  echo
  echo "   slack-chat-migrator migrate --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                              --export_path /path/to/slack/export \\"
  echo "                              --workspace_admin your-admin@domain.com"
  echo
  echo "4. IF MIGRATION IS INTERRUPTED (OPTIONAL)"
  echo "   Resume an interrupted migration:"
  echo
  echo "   slack-chat-migrator migrate --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                              --export_path /path/to/slack/export \\"
  echo "                              --workspace_admin your-admin@domain.com \\"
  echo "                              --resume"
  echo "======================================================================"
  echo
  echo "📝 NOTE: You only need to run this setup script once, unless you:"
  echo "  - Change Google Cloud projects"
  echo "  - Need to create a new service account"
  echo "  - Experience permission errors with the existing setup"
  echo "======================================================================"
}

# Run setup steps
enable_apis
setup_service_account
create_service_account_key
extract_client_id
display_next_steps
