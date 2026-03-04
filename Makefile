.PHONY: install lint format format-check typecheck test test-cov check fix clean

install:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check src/slack_chat_migrator/ tests/

fix:
	ruff check --fix src/slack_chat_migrator/ tests/
	ruff format src/slack_chat_migrator/ tests/

format:
	ruff format src/slack_chat_migrator/ tests/

format-check:
	ruff format --check src/slack_chat_migrator/ tests/

typecheck:
	mypy src/slack_chat_migrator/

test:
	pytest tests/ -v

test-cov:
	pytest tests/ --cov=slack_chat_migrator --cov-report=term-missing

check: lint format-check typecheck test

clean:
	rm -rf build/ dist/ *.egg-info .mypy_cache .pytest_cache .coverage coverage.xml htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
