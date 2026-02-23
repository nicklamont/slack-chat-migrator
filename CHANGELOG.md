## 0.3.1 (2025-02-23)

### Fix

- Harden error handling in config, user, and file modules
- Add input validation, Python 3.9 compat, and improve update mode logic
- Address PR review feedback

### Refactor

- Harden credential handling and remove frame introspection in API utils

## v0.3.2 (2026-02-23)

### Fix

- bump setuptools minimum to >=61 and make version dynamic
- add type annotations to CLI/utility modules and enforce mypy in CI
- add type annotations to service modules
- add type annotations to core migrator module
- delete empty logging.pyi stub and add googleapiclient to mypy overrides
- resolve CI lint failures and strengthen emoji test assertion
- surface skipped reactions from unmapped users in migration report
- resolve ruff lint errors and fix CI workflow
- Address PR review feedback
- Add input validation, Python 3.9 compat, and improve update mode logic
- Harden error handling in config, user, and file modules
- Update broken config tests to match current schema
- Replace bare except clause with except Exception in report.py

### Refactor

- migrate to pyproject.toml-only build configuration
- narrow except Exception to specific exception types
- extract magic numbers into named constants in space.py
- add return type annotations to functions missing them
- Harden credential handling and remove frame introspection in API utils
- Fix bare except and improve HTTP debug logging in logging module

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
