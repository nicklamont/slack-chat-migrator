## v0.4.1 (2026-03-14)

### Fix

- add explicit UTF-8 encoding to all file I/O — fixes `charmap` codec errors on Windows
- strip `#` prefix from channel names in init wizard and normalize defensively in config
- warn about unrecognized channel names in init wizard
- don't add excluded channels to `channels_processed` list in dry-run reports

## v0.4.0 (2026-03-06)

### BREAKING CHANGES

- **Package renamed**: Import path changed from `slack_migrator` to `slack_chat_migrator`. CLI command renamed from `slack-migrator` to `slack-chat-migrator`.
- **src layout adopted**: Package now uses the standard `src/` layout (`src/slack_chat_migrator/`).
- **Build system migrated**: Moved from `setup.py` to `pyproject.toml`-only build configuration.
- **CLI rewritten**: Migrated from argparse (single command) to Click with subcommands (`migrate`, `validate`, `init`, `setup`, `cleanup`).

### Feat

- **Click CLI with subcommands**: `migrate`, `validate`, `cleanup` replace the monolithic argparse interface
- **Interactive setup wizard** (`setup`): Guided GCP project creation, API enablement, service account provisioning, and domain-wide delegation — all via REST APIs from the CLI
- **Interactive config generator** (`init`): Step-by-step `config.yaml` creation with export path validation and optional post-init validation
- **Rich CLI output**: Rich panels, tables, and spinners across all commands
- **Live progress display**: Real-time migration progress with throughput metrics, error rates, member bars, and dry-run mode indicator
- **Migration resumption**: `--resume` flag with checkpoint system to resume interrupted migrations
- **Graceful interruption**: `InterruptHandler` catches Ctrl+C and completes the current channel before stopping
- **`--complete` flag**: Inline import-mode completion (replaces deprecated `cleanup` command)
- **Credential-free dry-run**: Run `migrate --dry-run` and `validate` without Google credentials
- **Export inspector**: Analyze Slack export structure with channel/message/file counts in `validate` output
- **ChatAdapter and DriveAdapter**: Typed wrappers over raw Google API services with consistent error handling
- **DryRunChatService and DryRunDriveService**: No-op service implementations injected in dry-run mode
- **Typed result dataclasses**: `SendResult` and `UploadResult` replace raw dicts for message and attachment operations
- **Checkpoint system**: Periodic state snapshots enable migration resumption after interruption

### Fix

- **Crash bugs and data-loss risks**: Fix `messages_created` counter increment, resolve crash paths across core modules
- **Security hardening**: Escape single quotes in Drive API query strings (injection prevention), case-insensitive Bearer token check, credential handling improvements
- **User mapping diagnostics**: Add context to `UserMappingError` for empty user maps, sample correct entries
- **Bot handling**: Filter bots in dry-run reactions, handle `workspace_admin=None` in membership
- **Silent exception handling**: Add debug logging to all previously silent exception handlers
- **Narrow exception handlers**: Replace broad `except Exception` with specific exception types throughout
- **Click 8.2+ compatibility**: Remove deprecated `mix_stderr` parameter
- **Type annotations**: Add type annotations across all CLI, service, and core modules; enforce mypy in CI
- **Input validation**: Python 3.9 compatibility, improved update mode logic
- **Migration report**: Surface skipped reactions from unmapped users

### Refactor

- **Dependency injection**: `MigrationContext` (frozen dataclass) carries immutable config; `MigrationState` (mutable) tracks runtime state. Services receive only what they need — no more `migrator` back-references
- **Package reorganization**: Services split into `services/messages/`, `services/spaces/`, `services/files/`, `services/setup/` subpackages
- **Module extraction**: `ChannelProcessor`, `UserResolver`, `MigrationState`, `MigrationContext`, `migration_logging`, `cleanup` extracted from monolithic `migrator.py`
- **Function decomposition**: Large functions decomposed into focused helpers across all modules
- **Type safety**: ~50 `Any` annotations replaced with concrete types, `TypedDict`s for state, enums for sentinels (`UserType`, `MessageResult`, `ImportCompletionStrategy`), strict mypy on core modules
- **Exception hierarchy**: Custom exceptions (`UserMappingError`, `SpacePermissionError`, etc.) replace `sys.exit()` calls and sentinel strings

### Test

- **Coverage raised from ~0% to 90%+** with 1541 unit tests
- Tier 1/2/3 integration tests, dry-run pipeline tests, and comprehensive unit tests
- Shared test fixtures and data builders

### CI

- Security scanning and pinned dependencies
- Coverage threshold enforced at 90%
- Dependabot: weekly updates for pip and GitHub Actions
- Commitizen conventional commit enforcement

### Docs

- Updated README for new CLI commands and Quick Start guide
- Google-style docstrings on all public methods
- SECURITY.md and CODEOWNERS added

### Dependencies

- New: `click` (CLI framework), `rich` (terminal rendering)
- New: `ruff` (linting/formatting, replaces black+isort+flake8)
- New optional `[setup]`: `google-cloud-resource-manager`, `google-cloud-service-usage`
- google-auth-httplib2 ~=0.3 (was ~=0.2)
- emoji ~=2.15 (was ~=2.14)
- pyyaml 6.0.3 (was 6.0.2)
- requests 2.32.5 (was 2.32.4)
- google-api-python-client 2.190.0 (was 2.177.0)

## v0.3.1 (2025-08-07)

### Fix

- Improve handling of forwarded messages (via attachments) in migration process.
- improve multi-level bullet formatting

## v0.3.0 (2025-08-07)

### Feat

- Implement shared channel folder cache and reset functionality
- Add handling for Google Docs file skips during attachment processing
- Add message deduplication to prevent processing thread replies multiple times
- Improve logging and cleanup processes for migration tool
- Enhance logging to include channel context for API requests and responses
- Refactor email handling to always return mapped emails and improve attribution for external users
- Enhance bot message handling to ignore specific message types during migration
- Add option to ignore bot messages and reactions during migration
- **migrator**: Improve attribution formatting and handle external user reactions gracefully
- **logging**: Add channel context to logging in file and space user management functions
- **logging**: Enhance logger initialization and add channel context to logging across services
- **permissions**: Implement unified permission validation system and remove legacy permission checker
- **discovery**: Implement space discovery and last message timestamp retrieval
- **message**: Enhance message sending logic in update mode

### Fix

- Update logging messages for clarity on member addition processes
- Enhance error handling and logging during migration and cleanup processes
- Improve rich text formatting and indentation handling in Slack message parsing
- Increment spaces created counter in create_space function
- Prevent logging attribute issues by filtering out reserved attributes in log_with_context
- **members**: Update logging for adding external users and improve visibility
- **api**: Update required scopes for Google Chat API

### Refactor

- Move API request and response logging from various modules to central api module
- Remove retry decorator and implement retry logic in API service wrapper
- **permissions**: Update IAM roles and refine OAuth scopes in setup script
- **migrator**: Remove intro message sending and update logging steps for space creation
- **message**: Remove unused thread and message ID mapping functions
