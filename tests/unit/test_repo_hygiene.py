"""Repository hygiene: CI pins, Python floor, release checks (issue #122)."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"
SUPPORTED = ["3.10", "3.11", "3.12", "3.13", "3.14"]


def _workflow(name: str) -> dict:
    return yaml.safe_load((WORKFLOWS / name).read_text())


def _uses_lines() -> list[tuple[str, str]]:
    found = []
    for path in sorted(WORKFLOWS.glob("*.yml")):
        for line in path.read_text().splitlines():
            stripped = line.strip().removeprefix("- ")
            if stripped.startswith("uses:"):
                found.append((path.name, stripped))
    return found


class TestWorkflows:
    def test_actions_pinned_by_sha_with_version_comment(self) -> None:
        uses = _uses_lines()
        assert uses
        for name, line in uses:
            assert re.fullmatch(
                r"uses: [\w.-]+/[\w./-]+@[0-9a-f]{40} # v\d+(\.\d+)*", line
            ), f"{name}: {line}"

    def test_ci_permissions_read_only(self) -> None:
        assert _workflow("ci.yml")["permissions"] == {"contents": "read"}

    def test_ci_matrix_covers_supported_pythons(self) -> None:
        matrix = _workflow("ci.yml")["jobs"]["test"]["strategy"]["matrix"]
        assert matrix["python-version"] == SUPPORTED

    def test_ruff_pinned_to_pre_commit_version(self) -> None:
        version = _workflow("ci.yml")["env"]["RUFF_VERSION"]
        pre_commit = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text())
        ruff_repo = next(
            r for r in pre_commit["repos"] if "ruff-pre-commit" in r["repo"]
        )
        assert ruff_repo["rev"] == f"v{version}"
        lint_steps = _workflow("ci.yml")["jobs"]["lint"]["steps"]
        assert any("ruff==${RUFF_VERSION}" in s.get("run", "") for s in lint_steps)

    def test_release_checks_tag_before_building(self) -> None:
        jobs = _workflow("release.yml")["jobs"]
        assert "check_release_version.py" in str(jobs["check-version"]["steps"])
        assert jobs["github-release"]["needs"] == "check-version"
        assert jobs["pypi-build"]["needs"] == "check-version"
        assert _workflow("release.yml")["permissions"] == {"contents": "read"}


def _release_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "check_release_version",
        ROOT / ".github" / "scripts" / "check_release_version.py",
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestReleaseVersionCheck:
    def test_matching_tag_passes(self) -> None:
        import slack_chat_migrator

        script = _release_script()
        assert script.main(["x", f"v{slack_chat_migrator.__version__}"]) == 0

    @pytest.mark.parametrize("tag", ["v0.0.0-other", "0.4.2", "vX"])
    def test_mismatched_tag_fails(self, tag: str) -> None:
        assert _release_script().main(["x", tag]) == 1


class TestPackaging:
    # Plain-text checks: tomllib is only in the standard library from 3.11.
    PYPROJECT = (ROOT / "pyproject.toml").read_text()

    def test_python_floor(self) -> None:
        assert 'requires-python = ">=3.10"' in self.PYPROJECT
        assert 'python_version = "3.10"' in self.PYPROJECT
        assert 'target-version = "py310"' in self.PYPROJECT

    def test_classifiers(self) -> None:
        for version in SUPPORTED:
            assert f'"Programming Language :: Python :: {version}"' in self.PYPROJECT
        assert "Programming Language :: Python :: 3.9" not in self.PYPROJECT

    def test_setuptools_supports_pep639_license(self) -> None:
        assert '"setuptools>=77"' in self.PYPROJECT


def test_tests_do_not_run_in_the_checkout() -> None:
    """The autouse fixture keeps migration_logs/ out of the repository."""
    assert Path.cwd().resolve() != ROOT
