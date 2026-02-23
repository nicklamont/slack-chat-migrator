.PHONY: install lint format format-check typecheck test test-cov check fix clean

install:
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check slack_migrator/ tests/

fix:
	ruff check --fix slack_migrator/ tests/
	ruff format slack_migrator/ tests/

format:
	ruff format slack_migrator/ tests/

format-check:
	ruff format --check slack_migrator/ tests/

typecheck:
	mypy slack_migrator/

test:
	pytest tests/ -v

test-cov:
	pytest tests/ --cov=slack_migrator --cov-report=term-missing

check: lint format-check typecheck test

clean:
	rm -rf build/ dist/ *.egg-info .mypy_cache .pytest_cache .coverage coverage.xml htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
