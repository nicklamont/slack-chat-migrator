# Slack Chat Migrator

Tool for migrating Slack workspace exports to Google Chat spaces.

## Architecture

```
slack_migrator/
├── cli/            # CLI entry points and report generation
│   ├── commands.py # Main CLI commands (migrate, validate, etc.)
│   └── report.py   # Migration report formatting
├── core/           # Core logic
│   ├── config.py   # YAML config loading and validation
│   └── migrator.py # Main migration orchestrator
├── services/       # External API integrations
│   ├── chat/       # Google Chat API (spaces, messages)
│   ├── drive/      # Google Drive API (file uploads, shared drives)
│   ├── discovery.py
│   ├── file.py     # Slack export file parsing
│   ├── message.py  # Message transformation (Slack → Chat format)
│   ├── message_attachments.py
│   ├── space.py    # Space creation and management
│   └── user.py     # User mapping (Slack → Google)
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
