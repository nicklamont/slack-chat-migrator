#!/bin/bash
# Script to set up the proper permissions for Slack Chat Migration

# Default values
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT_NAME="slack-migrator-sa"
KEY_FILE="slack-chat-migrator-sa-key.json"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --project)
      PROJECT_ID="$2"
      shift 2
      ;;
    --sa-name)
      SERVICE_ACCOUNT_NAME="$2"
      shift 2
      ;;
    --key-file)
      KEY_FILE="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --project PROJECT_ID    GCP project ID (default: current gcloud project)"
      echo "  --sa-name NAME          Service account name (default: slack-migrator-sa)"
      echo "  --key-file FILE         Key file name (default: slack-chat-migrator-sa-key.json)"
      echo "  --help                  Show this help message"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
  echo "Error: gcloud CLI not found. Please install Google Cloud SDK."
  echo "Visit: https://cloud.google.com/sdk/docs/install"
  exit 1
fi

# Validate project ID
if [ -z "$PROJECT_ID" ]; then
  echo "Error: No project ID specified and no default project set in gcloud"
  echo "Please specify a project ID with --project or set a default project with:"
  echo "  gcloud config set project PROJECT_ID"
  exit 1
fi

SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Setting up permissions for service account: ${SERVICE_ACCOUNT_EMAIL}"
echo "Project: ${PROJECT_ID}"
echo "Key file: ${KEY_FILE}"

# Enable required APIs
echo "Enabling required APIs..."
gcloud services enable chat.googleapis.com drive.googleapis.com --project "${PROJECT_ID}"

# Create service account if it doesn't exist
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT_EMAIL} --project "${PROJECT_ID}" &> /dev/null; then
  echo "Creating service account..."
  gcloud iam service-accounts create ${SERVICE_ACCOUNT_NAME} \
    --display-name="Slack Chat Migration Service Account" \
    --project "${PROJECT_ID}"
else
  echo "Service account already exists."
fi

# Download service account key if it doesn't exist
if [ ! -f "${KEY_FILE}" ]; then
  echo "Creating and downloading service account key..."
  gcloud iam service-accounts keys create "${KEY_FILE}" \
    --iam-account=${SERVICE_ACCOUNT_EMAIL} \
    --project "${PROJECT_ID}"
else
  echo "Key file already exists. Using existing key file."
fi

# Get client ID from the key file
CLIENT_ID=$(grep -o '"client_id": "[^"]*' "${KEY_FILE}" | cut -d'"' -f4)

# Set required IAM roles
echo "Setting required IAM roles..."
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
  --role="roles/chat.admin" \
  --quiet

echo "======================================================================"
echo "IMPORTANT: You must complete the following steps manually in the Google Workspace Admin Console:"
echo "======================================================================"
echo "1. Go to https://admin.google.com/"
echo "2. Navigate to Security → API Controls → Domain-wide Delegation"
echo "3. Click 'Add new' to add your service account"
echo "4. Enter the following Client ID: ${CLIENT_ID}"
echo "5. Enter the following OAuth scopes (copy and paste exactly):"
echo "   https://www.googleapis.com/auth/chat.import"
echo "   https://www.googleapis.com/auth/chat.spaces"
echo "   https://www.googleapis.com/auth/drive"
echo "   https://www.googleapis.com/auth/chat.spaces.readonly"
echo "   https://www.googleapis.com/auth/chat.messages"
echo "6. Click 'Authorize'"
echo ""
echo "After completing these steps, you can run the migration script with:"
echo ""
echo "# Set the credentials environment variable (optional)"
echo "export GOOGLE_APPLICATION_CREDENTIALS=\"$(pwd)/${KEY_FILE}\""
echo ""
echo "# Run the migration tool"
echo "slack-migrator --creds_path \"$(pwd)/${KEY_FILE}\" --export_path /path/to/export --workspace_admin your-admin@domain.com --config config.yaml"
echo ""
echo "# For a dry run (recommended before actual migration)"
echo "slack-migrator --creds_path \"$(pwd)/${KEY_FILE}\" --export_path /path/to/export --workspace_admin your-admin@domain.com --config config.yaml --dry_run"
echo "======================================================================" 