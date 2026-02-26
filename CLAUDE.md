# Slack Chat Migrator

Tool for migrating Slack workspace exports to Google Chat spaces.

## Architecture

The codebase uses **dependency injection** — `SlackToChatMigrator` is the composition root that wires all services together. Service functions receive only the explicit dependencies they need via `MigrationContext` (immutable config) and `MigrationState` (mutable tracking).

In **dry-run mode**, `DryRunChatService`/`DryRunDriveService` are injected in place of real API services, eliminating scattered `if dry_run` checks.

```
slack_migrator/
├── cli/            # CLI entry points and report generation
│   ├── commands.py # Main CLI commands (migrate, validate, etc.)
│   └── report.py   # Migration report formatting
├── core/           # Core logic
│   ├── channel_processor.py # Per-channel migration orchestration
│   ├── cleanup.py           # Post-migration cleanup (import mode completion, members)
│   ├── config.py            # YAML config loading and validation
│   ├── context.py           # MigrationContext frozen dataclass (immutable config)
│   ├── migration_logging.py # Migration success/failure logging
│   ├── migrator.py          # Composition root — wires all deps, owns lifecycle
│   └── state.py             # MigrationState dataclass (mutable tracking state)
├── services/       # External API integrations
│   ├── chat/       # Google Chat API (spaces, messages)
│   │   └── dry_run_service.py  # No-op Chat API for dry-run mode
│   ├── drive/      # Google Drive API (file uploads, shared drives)
│   │   └── dry_run_service.py  # No-op Drive API for dry-run mode
│   ├── discovery.py          # Space discovery and mapping for migration resumption
│   ├── file.py               # Slack export file parsing
│   ├── membership_manager.py # Space membership (historical + regular members)
│   ├── message.py            # Message transformation (Slack → Chat format)
│   ├── message_attachments.py
│   ├── reaction_processor.py # Batch reaction processing
│   ├── space_creator.py      # Space creation, listing, and import mode cleanup
│   ├── user.py               # User mapping (Slack → Google)
│   └── user_resolver.py      # User identity resolution and impersonation
└── utils/          # Shared utilities
    ├── api.py      # API retry logic, credential handling
    ├── formatting.py
    ├── logging.py
    ├── permissions.py
    └── user_validation.py
tests/
├── unit/           # Fast, isolated tests
└── integration/    # Tests requiring external services
```

## Development Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Common Commands

```bash
# Lint and format
ruff check slack_migrator/ tests/          # lint
ruff check --fix slack_migrator/ tests/    # lint + autofix
ruff format slack_migrator/ tests/         # format

# Type check
mypy slack_migrator/

# Tests
pytest tests/ -v                           # all tests
pytest tests/unit/ -v                      # unit only
pytest tests/ --cov=slack_migrator         # with coverage

# Commit message validation
cz check --message "fix: description"
```

## Conventions

- **Python 3.9+** — no walrus operator in hot paths, use `from __future__ import annotations` sparingly
- **Conventional Commits** — enforced by commitizen pre-commit hook and CI
  - `feat:`, `fix:`, `refactor:`, `docs:`, `ci:`, `test:`, `chore:`
- **Trunk-based development** — work on feature branches, merge to `main`, release via tags
- **Line length** — 88 characters (ruff)
- **Import order** — managed by ruff (isort-compatible, black profile)

## Release Process

1. `cz bump` — bumps version in `__init__.py`, updates `CHANGELOG.md`, creates git tag
2. `git push && git push --tags`
3. Tag push triggers `.github/workflows/release.yml` → creates GitHub Release
4. GitHub Release triggers `.github/workflows/python-publish.yml` → publishes to PyPI
