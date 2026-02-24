# Contributing

## Development Setup

```bash
# Clone and set up the environment
git clone https://github.com/nicklamont/slack-chat-migrator.git
cd slack-chat-migrator
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pre-commit install
```

## Running Checks

```bash
# Lint
ruff check slack_migrator/ tests/

# Format
ruff format slack_migrator/ tests/

# Type check
mypy slack_migrator/

# Tests
pytest tests/ -v

# Tests with coverage
pytest tests/ --cov=slack_migrator

# Integration tests (require GCP credentials)
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json pytest tests/integration/ -v
```

## Commit Messages

This project uses [Conventional Commits](https://www.conventionalcommits.org/),
enforced by [commitizen](https://commitizen-tools.github.io/commitizen/).

Format: `<type>: <description>`

| Type       | Use for                          |
|------------|----------------------------------|
| `feat`     | New features                     |
| `fix`      | Bug fixes                        |
| `refactor` | Code changes that don't add features or fix bugs |
| `test`     | Adding or updating tests         |
| `docs`     | Documentation changes            |
| `ci`       | CI/CD configuration              |
| `chore`    | Maintenance tasks                |

Validate before committing:

```bash
cz check --message "fix: resolve edge case in user mapping"
```

## Pull Requests

1. Create a feature branch from `main`
2. Make your changes with conventional commit messages
3. Ensure all checks pass (`ruff check`, `pytest`, `mypy`)
4. Open a PR against `main`
