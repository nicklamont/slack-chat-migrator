"""Run setup_permissions.sh against a stub gcloud (issue #121)."""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "setup_permissions.sh"
SA_EMAIL = "slack-chat-migrator-sa@demo-proj.iam.gserviceaccount.com"
KEY = {
    "type": "service_account",
    "client_email": SA_EMAIL,
    "client_id": "1234567890",
    "private_key_id": "abc123keyid",
}

#: ``keys list`` output for the key in KEY: enabled, never expires.
ACTIVE_KEY = "abc123keyid,,9999-12-31T23:59:59Z"

pytestmark = pytest.mark.skipif(
    os.name == "nt" or shutil.which("bash") is None, reason="needs bash"
)

# A stub gcloud: logs every call, fails the commands listed in $FAIL_ON, and
# writes a key file for "iam service-accounts keys create <path>".
STUB = r"""#!/bin/bash
echo "$*" >> "$GCLOUD_LOG"
# Like real gcloud, "keys create" creates its output file before calling IAM.
if [ "$1 $2 $3 $4" = "iam service-accounts keys create" ]; then
  : > "$5"
fi
for pattern in ${FAIL_ON:-}; do
  case "$*" in *"${pattern//_/ }"*) echo "ERROR: (gcloud) PERMISSION_DENIED" >&2; exit 1 ;; esac
done
case "$1 $2" in
  "config get-value") echo demo-proj ;;
  "auth list") echo me@example.com ;;
esac
if [ "$1 $2 $3 $4" = "iam service-accounts keys create" ]; then
  printf '%s' "$KEY_JSON" > "$5"
fi
if [ "$1 $2 $3 $4" = "iam service-accounts keys list" ]; then
  # The script parses this exact output shape; anything else is a bug.
  case " $* " in
    *" --managed-by=user "*) ;;
    *) echo "stub: keys list without --managed-by=user" >&2; exit 64 ;;
  esac
  case " $* " in
    *" --format=csv[no-heading](name.basename(),disabled,validBeforeTime) "*) ;;
    *) echo "stub: unexpected keys list --format" >&2; exit 64 ;;
  esac
  printf '%s\n' "$KEY_LIST"
fi
exit 0
"""


def _bin_dir(tmp_path: Path, with_gcloud: bool = True) -> Path:
    """A PATH directory with only the tools the script needs (+ the stub)."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    for tool in (
        "bash",
        "grep",
        "sed",
        "head",
        "basename",
        "chmod",
        "cat",
        "date",
        "rm",
    ):
        target = shutil.which(tool)
        assert target, tool
        link = bin_dir / tool
        if not link.exists():
            link.symlink_to(target)
    if with_gcloud:
        stub = bin_dir / "gcloud"
        stub.write_text(STUB)
        stub.chmod(0o755)
    return bin_dir


def _run(
    tmp_path: Path,
    *args: str,
    with_gcloud: bool = True,
    fail_on: str = "",
    key_list: str = ACTIVE_KEY,
) -> subprocess.CompletedProcess[str]:
    env = {
        "PATH": str(_bin_dir(tmp_path, with_gcloud)),
        "HOME": str(tmp_path),
        "GCLOUD_LOG": str(tmp_path / "gcloud.log"),
        "FAIL_ON": fail_on,
        "KEY_JSON": json.dumps(KEY),
        "KEY_LIST": key_list,
    }
    return subprocess.run(  # noqa: S603
        [str(SCRIPT), *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def _calls(tmp_path: Path) -> str:
    log = tmp_path / "gcloud.log"
    return log.read_text() if log.exists() else ""


class TestSetupPermissionsScript:
    def test_success_creates_private_key_and_no_project_roles(
        self, tmp_path: Path
    ) -> None:
        result = _run(tmp_path)
        assert result.returncode == 0, result.stderr
        key = tmp_path / "slack-chat-migrator-sa-key.json"
        assert stat.S_IMODE(key.stat().st_mode) == 0o600
        assert "add-iam-policy-binding" not in _calls(tmp_path)
        assert "Setup completed successfully" in result.stdout
        assert "1234567890" in result.stdout
        assert "chat.customemojis.readonly" in result.stdout
        assert "Markup syntax" in result.stdout

    def test_next_steps_use_current_commands(self, tmp_path: Path) -> None:
        result = _run(tmp_path)
        assert "check-permissions" not in result.stdout
        assert "--update_mode" not in result.stdout
        assert "validate --creds_path" in result.stdout
        assert "--resume" in result.stdout

    def test_scopes_match_required_scopes(self, tmp_path: Path) -> None:
        from slack_chat_migrator.utils.api import REQUIRED_SCOPES

        result = _run(tmp_path)
        assert ",".join(REQUIRED_SCOPES) in result.stdout

    def test_missing_gcloud_fails_cleanly(self, tmp_path: Path) -> None:
        result = _run(tmp_path, with_gcloud=False)
        assert result.returncode != 0
        assert "command not found" not in result.stderr
        assert "gcloud CLI) not found" in result.stderr

    @pytest.mark.parametrize(
        "step",
        ["services_enable", "service-accounts_create", "keys_create"],
    )
    def test_failed_step_exits_non_zero(self, tmp_path: Path, step: str) -> None:
        # Force "service-accounts describe" to fail too, so "create" runs
        fail_on = step if step != "service-accounts_create" else "describe " + step
        result = _run(tmp_path, fail_on=fail_on)
        assert result.returncode != 0
        assert "Setup completed successfully" not in result.stdout

    def test_failed_create_leaves_no_empty_file(self, tmp_path: Path) -> None:
        key = tmp_path / "slack-chat-migrator-sa-key.json"
        result = _run(tmp_path, fail_on="keys_create")
        assert result.returncode != 0
        assert "Failed to create service account key" in result.stderr
        assert "--managed-by=user" in result.stderr
        assert "delete only the one whose CREATED_AT is about" in result.stderr
        assert "no file for" not in result.stderr
        assert not key.exists()
        # So the next run creates the key instead of blaming "unknown".
        result = _run(tmp_path)
        assert result.returncode == 0, result.stderr
        assert json.loads(key.read_text()) == KEY

    @pytest.mark.parametrize(
        ("content", "message"),
        [
            ("", "is empty (perhaps left by a failed key creation)"),
            ("not json", "not a service account key file (no client_email)"),
        ],
    )
    def test_empty_or_foreign_file_gets_its_own_message(
        self, tmp_path: Path, content: str, message: str
    ) -> None:
        (tmp_path / "slack-chat-migrator-sa-key.json").write_text(content)
        result = _run(tmp_path)
        assert result.returncode != 0
        assert message in result.stderr
        assert "belongs to" not in result.stderr
        assert "keys create" not in _calls(tmp_path)

    def test_reused_key_of_another_account_is_rejected(self, tmp_path: Path) -> None:
        other = {**KEY, "client_email": "someone@else.iam.gserviceaccount.com"}
        (tmp_path / "slack-chat-migrator-sa-key.json").write_text(json.dumps(other))
        result = _run(tmp_path)
        assert result.returncode != 0
        assert "belongs to" in result.stderr

    @pytest.mark.parametrize(
        ("key_list", "message"),
        [
            ("somethingelse,,9999-12-31T23:59:59Z", "it was deleted"),
            ("", "it was deleted"),
            ("abc123keyid,True,9999-12-31T23:59:59Z", "which is disabled"),
            ("abc123keyid,,2020-01-01T00:00:00Z", "expired on 2020-01-01"),
        ],
    )
    def test_reused_unusable_key_is_rejected(
        self, tmp_path: Path, key_list: str, message: str
    ) -> None:
        key = tmp_path / "slack-chat-migrator-sa-key.json"
        key.write_text(json.dumps(KEY))
        result = _run(tmp_path, key_list=key_list)
        assert result.returncode != 0
        assert message in result.stderr
        assert "keys create" not in _calls(tmp_path)
        assert json.loads(key.read_text()) == KEY

    def test_key_that_cannot_be_checked_is_kept(self, tmp_path: Path) -> None:
        key = tmp_path / "slack-chat-migrator-sa-key.json"
        key.write_text(json.dumps(KEY))
        result = _run(tmp_path, fail_on="keys_list")
        assert result.returncode != 0
        assert "couldn't be checked" in result.stderr
        assert "left unchanged" in result.stderr
        assert "keys create" not in _calls(tmp_path)
        assert json.loads(key.read_text()) == KEY

    def test_key_without_expiry_is_active(self, tmp_path: Path) -> None:
        (tmp_path / "slack-chat-migrator-sa-key.json").write_text(json.dumps(KEY))
        result = _run(tmp_path, key_list="abc123keyid,,")
        assert result.returncode == 0, result.stderr
        assert "holds an active key" in result.stdout

    def test_unreadable_expiry_keeps_the_file(self, tmp_path: Path) -> None:
        key = tmp_path / "slack-chat-migrator-sa-key.json"
        key.write_text(json.dumps(KEY))
        result = _run(tmp_path, key_list="abc123keyid,,soon")
        assert result.returncode != 0
        assert "Couldn't read the expiry time 'soon'" in result.stderr
        assert "left unchanged" in result.stderr
        assert "Check the key in the Cloud Console" in result.stderr
        assert "keys create" not in _calls(tmp_path)
        assert json.loads(key.read_text()) == KEY

    def test_keys_are_listed_in_the_parsed_format(self, tmp_path: Path) -> None:
        (tmp_path / "slack-chat-migrator-sa-key.json").write_text(json.dumps(KEY))
        result = _run(tmp_path)
        assert result.returncode == 0, result.stderr
        listed = [c for c in _calls(tmp_path).splitlines() if "keys list" in c]
        assert len(listed) == 1
        assert f"--iam-account={SA_EMAIL}" in listed[0]
        assert "--managed-by=user" in listed[0]
        assert (
            "--format=csv[no-heading](name.basename(),disabled,validBeforeTime)"
            in listed[0]
        )

    def test_disabled_false_and_future_expiry_is_active(self, tmp_path: Path) -> None:
        (tmp_path / "slack-chat-migrator-sa-key.json").write_text(json.dumps(KEY))
        result = _run(
            tmp_path,
            key_list="other,,9999-12-31T23:59:59Z\nabc123keyid,False,2999-01-01T00:00:00Z",
        )
        assert result.returncode == 0, result.stderr
        assert "holds an active key" in result.stdout

    def test_reused_valid_key_is_kept_and_made_private(self, tmp_path: Path) -> None:
        key = tmp_path / "slack-chat-migrator-sa-key.json"
        key.write_text(json.dumps(KEY))
        key.chmod(0o644)
        result = _run(tmp_path)
        assert result.returncode == 0, result.stderr
        assert "keys create" not in _calls(tmp_path)
        assert stat.S_IMODE(key.stat().st_mode) == 0o600

    def test_missing_option_value(self, tmp_path: Path) -> None:
        result = _run(tmp_path, "--project")
        assert result.returncode != 0
        assert "requires a value" in result.stderr
