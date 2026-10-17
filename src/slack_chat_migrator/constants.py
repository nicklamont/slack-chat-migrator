"""Centralized constants for the Slack Chat Migrator."""

# --- HTTP Status Codes ---
HTTP_OK = 200
HTTP_BAD_REQUEST = 400
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409
HTTP_RATE_LIMIT = 429
HTTP_SERVER_ERROR_MIN = 500

# --- Slack Message Subtypes ---
BOT_MESSAGE_SUBTYPE = "bot_message"
APP_MESSAGE_SUBTYPE = "app_message"
CHANNEL_JOIN_SUBTYPE = "channel_join"
CHANNEL_LEAVE_SUBTYPE = "channel_leave"
BOT_SUBTYPES = frozenset({BOT_MESSAGE_SUBTYPE, APP_MESSAGE_SUBTYPE})
SYSTEM_SUBTYPES = frozenset({CHANNEL_JOIN_SUBTYPE, CHANNEL_LEAVE_SUBTYPE})

# --- File Size Limits ---
# Largest image sent as a direct Chat attachment; bigger files go to Drive.
DIRECT_UPLOAD_MAX_BYTES = 25 * 1024 * 1024  # 25 MB
RESUMABLE_UPLOAD_THRESHOLD_BYTES = 5 * 1024 * 1024  # 5 MB
# Chunk size for streaming Slack downloads to disk and for hashing files.
FILE_READ_CHUNK_BYTES = 1024 * 1024  # 1 MiB

# --- Slack File Downloads ---
# Hosts Slack serves export files from.  A host matches when it equals an
# entry or is a subdomain of one; downloads and redirects elsewhere are refused.
SLACK_FILE_HOST_SUFFIXES = ("slack.com", "slack-edge.com", "slack-files.com")
DOWNLOAD_MAX_REDIRECTS = 5
DOWNLOAD_RETRY_TOTAL = 5
DOWNLOAD_RETRY_BACKOFF_SECONDS = 1.0
DOWNLOAD_RETRY_STATUSES = (429, 500, 502, 503, 504)
DOWNLOAD_CONNECT_TIMEOUT_SECONDS = 10
DOWNLOAD_READ_TIMEOUT_SECONDS = 120
DOWNLOAD_POOL_SIZE = 16
# Attachment downloads are streamed into a per-run directory under
# ``<output_dir>/.attachment_tmp/`` (or the system temp dir), swept at start-up.
ATTACHMENT_TEMP_DIRNAME = ".attachment_tmp"
ATTACHMENT_TEMP_FALLBACK_DIRNAME = "slack_chat_migrator_attachments"

# --- API Pagination ---
SPACES_PAGE_SIZE = 100
DRIVE_FILES_PAGE_SIZE = 1000

# --- Space Configuration ---
SPACE_TYPE = "SPACE"
SPACE_THREADING_STATE = "THREADED_MESSAGES"
SPACE_NAME_PREFIX = "Slack #"
IMPORT_MODE_DAYS_LIMIT = 90

# --- Google Chat API ---
MEMBER_TYPE_HUMAN = "HUMAN"
# Message text is generated in Chat markup syntax (``*bold*``, ``<url|text>``).
# New Chat apps default to Markdown, so every create request pins this value.
MARKUP_SYNTAX = "MARKUP_SYNTAX_CHAT"

# --- Google Drive Roles ---
DRIVE_ROLE_EDITOR = "editor"
DRIVE_ROLE_READER = "reader"

# --- Membership Time Offsets ---
DEFAULT_FALLBACK_JOIN_TIME = "2020-01-01T00:00:00Z"
HISTORICAL_DELETE_TIME_OFFSET_SECONDS = 5
EARLIEST_MESSAGE_OFFSET_MINUTES = 2
FIRST_MESSAGE_OFFSET_MINUTES = 1

# --- Logging/Truncation Limits ---
RESPONSE_MAX_LENGTH = 2000
RESPONSE_FALLBACK_LENGTH = 1000
ERROR_DETAIL_MAX_LENGTH = 500
URL_TRUNCATION_LENGTH = 100
ID_HASH_LENGTH = 8

# --- Error Patterns ---
PERMISSION_DENIED_ERROR = "PERMISSION_DENIED"

# --- API Throttle Delays (seconds) ---
API_THROTTLE_MESSAGE_SECONDS = 0.05
API_THROTTLE_MEMBER_SECONDS = 0.1
API_THROTTLE_DISCOVERY_SECONDS = 0.2
