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
# 2. Creates a service account with necessary permissions
# 3. Downloads a service account key file for authentication
# 4. Provides instructions for setting up domain-wide delegation in Google Workspace
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

# Default configuration values
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_NAME="slack-migrator-sa"
KEY_FILE="slack-chat-migrator-sa-key.json"

# Function to display script usage
show_help() {
  echo "Usage: $0 [options]"
  echo
  echo "Options:"
  echo "  --project PROJECT_ID    GCP project ID (default: current gcloud project)"
  echo "  --sa-name NAME          Service account name (default: slack-migrator-sa)"
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
      if [[ -z "$2" || "$2" == --* ]]; then
        echo "Error: --project requires a value"
        exit 1
      fi
      PROJECT_ID="$2"
      shift 2
      ;;
    --sa-name)
      if [[ -z "$2" || "$2" == --* ]]; then
        echo "Error: --sa-name requires a value"
        exit 1
      fi
      SERVICE_ACCOUNT_NAME="$2"
      shift 2
      ;;
    --key-file)
      if [[ -z "$2" || "$2" == --* ]]; then
        echo "Error: --key-file requires a value"
        exit 1
      fi
      KEY_FILE="$2"
      shift 2
      ;;
    --help)
      show_help
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Function to check for prerequisites and validate inputs
check_prerequisites() {
  echo "Checking prerequisites..."
  
  # Check if gcloud is installed
  if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: Google Cloud SDK (gcloud CLI) not found."
    echo "The gcloud command must be installed as a system component, not via pip."
    
    # Check if in Python environment, but explain we need the full SDK
    if [ -n "$VIRTUAL_ENV" ]; then
      echo "Detected Python virtual environment: $(basename "$VIRTUAL_ENV")"
      echo "⚠️ The Google Cloud SDK needs to be installed manually as a system component."
      echo "It cannot be installed via pip in a virtual environment."
      echo
      echo "Please install the Google Cloud SDK by following these steps:"
      echo
      echo "For macOS:"
      echo "  brew install --cask google-cloud-sdk"
      echo "  # Or download from https://cloud.google.com/sdk/docs/install-sdk"
      echo
      echo "For Linux:"
      echo "  # Follow instructions at https://cloud.google.com/sdk/docs/install-sdk"
      echo
      echo "For Windows:"
      echo "  # Download installer from https://cloud.google.com/sdk/docs/install-sdk"
      echo
      echo "After installation, make sure to initialize the SDK:"
      echo "  gcloud init"
      echo
      echo "Would you like to continue without the Google Cloud SDK? (y/n)"
      read -r response
      if ! [[ "$response" =~ ^[Yy]$ ]]; then
        echo "Exiting. Please install the Google Cloud SDK and run this script again."
        exit 1
      fi
      echo "Continuing without gcloud. Some steps will be skipped."
    else
      echo "Please install the Google Cloud SDK manually:"
      echo "Visit: https://cloud.google.com/sdk/docs/install"
      echo "After installation, run 'gcloud init' to set up your environment."
      exit 1
    fi
  fi

  # Validate project ID
  if [ -z "$PROJECT_ID" ]; then
    echo "❌ Error: No project ID specified and no default project set in gcloud"
    echo "Please specify a project ID with --project or set a default project with:"
    echo "  gcloud config set project PROJECT_ID"
    exit 1
  fi
  
  # Verify gcloud auth
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
    echo "❌ Error: No active gcloud account found"
    echo "Please run: gcloud auth login"
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
  gcloud services enable chat.googleapis.com --project "${PROJECT_ID}" || {
    echo "❌ Failed to enable Chat API"
    exit 1
  }
  
  echo "   - Enabling Drive API"
  gcloud services enable drive.googleapis.com --project "${PROJECT_ID}" || {
    echo "❌ Failed to enable Drive API"
    exit 1
  }
  
  echo "✅ APIs enabled successfully"
  echo
}

# Function to set up service account
setup_service_account() {
  echo "2️⃣ Setting up service account..."
  
  # Check if service account exists
  if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT_EMAIL} --project "${PROJECT_ID}" &> /dev/null; then
    echo "   - Creating service account '${SERVICE_ACCOUNT_NAME}'..."
    gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
      --display-name="Slack Chat Migration Service Account" \
      --description="Service account for Slack to Google Chat migration" \
      --project "${PROJECT_ID}" || {
        echo "❌ Failed to create service account"
        exit 1
      }
    echo "   ✅ Service account created successfully"
  else
    echo "   ✅ Service account '${SERVICE_ACCOUNT_NAME}' already exists"
  fi
  
  echo
}

# Function to create and download service account key
create_service_account_key() {
  echo "3️⃣ Setting up service account key..."
  
  # Check if key file exists
  if [ ! -f "${KEY_FILE}" ]; then
    echo "   - Creating and downloading service account key to '${KEY_FILE}'..."
    gcloud iam service-accounts keys create "${KEY_FILE}" \
      --iam-account=${SERVICE_ACCOUNT_EMAIL} \
      --project "${PROJECT_ID}" || {
        echo "❌ Failed to create service account key"
        exit 1
      }
    echo "   ✅ Key file created and downloaded successfully"
    # Secure the key file permissions
    chmod 600 "${KEY_FILE}"
  else
    echo "   ✅ Key file '${KEY_FILE}' already exists (using existing key)"
  fi
  
  echo
}

# Run setup steps
enable_apis
setup_service_account
create_service_account_key

# Function to assign IAM roles to service account
assign_iam_roles() {
  echo "4️⃣ Assigning IAM roles to service account..."
  
  echo "   - Assigning Chat Owner role"
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/chat.owner" \
    --quiet || {
      echo "❌ Failed to assign Chat Owner role"
      # Don't exit on error, try the next role
    }
  
  echo "   - Assigning Service Account Token Creator role"
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/iam.serviceAccountTokenCreator" \
    --quiet || {
      echo "❌ Failed to assign Service Account Token Creator role"
      # Don't exit on error, try the next role
    }
  
  echo "   - Assigning Service Account User role"
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/iam.serviceAccountUser" \
    --quiet || {
      echo "❌ Failed to assign Service Account User role"
      # Don't exit on error, try the next role
    }
  
  echo "   ✅ IAM roles assignment completed"
  echo
}

# Function to extract client ID from key file
extract_client_id() {
  if [ ! -f "${KEY_FILE}" ]; then
    echo "❌ Key file not found: ${KEY_FILE}"
    exit 1
  fi
  
  CLIENT_ID=$(grep -o '"client_id": "[^"]*' "${KEY_FILE}" | cut -d'"' -f4)
  
  if [ -z "${CLIENT_ID}" ]; then
    echo "❌ Could not extract client ID from key file"
    exit 1
  fi
}

# Function to display next steps
display_next_steps() {
  echo "======================================================================"
  echo "🎉 Setup completed successfully! 🎉"
  echo "======================================================================"
  echo
  echo "🔴 IMPORTANT: Manual steps required in Google Workspace Admin Console 🔴"
  echo "======================================================================"
  echo "1️⃣ Go to https://admin.google.com/"
  echo "2️⃣ Navigate to Security → API Controls → Domain-wide Delegation"
  echo "3️⃣ Click 'Add new' to add your service account"
  echo "4️⃣ Enter the following Client ID:"
  echo 
  echo "   ${CLIENT_ID}"
  echo 
  echo "5️⃣ Enter the following OAuth scopes (copy and paste the entire line):"
  echo 
  echo "   https://www.googleapis.com/auth/chat.import,https://www.googleapis.com/auth/chat.spaces,https://www.googleapis.com/auth/chat.messages,https://www.googleapis.com/auth/chat.spaces.readonly,https://www.googleapis.com/auth/chat.memberships.readonly,https://www.googleapis.com/auth/drive"
  echo 
  echo "6️⃣ Click 'Authorize'"
  echo "======================================================================"
  echo
  echo "🚀 NEXT STEPS - MIGRATION WORKFLOW 🚀"
  echo "======================================================================"
  echo "1. VERIFY PERMISSIONS (ALWAYS DO THIS FIRST)"
  echo "   This confirms your service account is properly set up:"
  echo
  echo "   slack-migrator-check-permissions --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                                   --workspace_admin your-admin@domain.com"
  echo
  echo "2. CREATE AND CUSTOMIZE CONFIG FILE (ONE-TIME SETUP)"
  echo "   If you haven't already, create a config file to customize the migration:"
  echo
  echo "   cp config.yaml.example config.yaml"
  echo "   # Edit config.yaml to match your needs"
  echo
  echo "3. TEST WITH DRY RUN (RECOMMENDED BEFORE EACH MIGRATION)"
  echo "   This simulates the migration without making any changes:"
  echo
  echo "   slack-migrator --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                 --export_path /path/to/slack/export \\"
  echo "                 --workspace_admin your-admin@domain.com \\"
  echo "                 --config config.yaml --dry_run"
  echo
  echo "4. RUN THE ACTUAL MIGRATION"
  echo "   When you're ready to perform the actual migration:"
  echo
  echo "   slack-migrator --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                 --export_path /path/to/slack/export \\"
  echo "                 --workspace_admin your-admin@domain.com \\"
  echo "                 --config config.yaml"
  echo
  echo "5. IF MIGRATION IS INTERRUPTED (OPTIONAL)"
  echo "   Use update mode to continue an interrupted migration:"
  echo
  echo "   slack-migrator --creds_path \"$(pwd)/${KEY_FILE}\" \\"
  echo "                 --export_path /path/to/slack/export \\"
  echo "                 --workspace_admin your-admin@domain.com \\"
  echo "                 --config config.yaml --update_mode"
  echo "======================================================================"
  echo
  echo "📝 NOTE: You only need to run this setup script once, unless you:"
  echo "  - Change Google Cloud projects"
  echo "  - Need to create a new service account"
  echo "  - Experience permission errors with the existing setup"
  echo "======================================================================"
}

# Run remaining setup steps
assign_iam_roles
extract_client_id
display_next_steps 