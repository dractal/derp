"""Tests for `derp env pull <env>`."""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from derp.cli.main import app

runner = CliRunner()


def _make_fake_terraform(
    bin_dir: Path,
    output_json: dict[str, str] | str,
    exit_code: int = 0,
) -> None:
    """Drop a fake `terraform` shim on PATH that prints `output_json` for
    `output -json env_vars` calls and exits with `exit_code`.

    `output_json` may be a dict (serialized to JSON) or a raw string.
    """
    payload = json.dumps(output_json) if isinstance(output_json, dict) else output_json
    script = f"""#!/usr/bin/env bash
if [[ "$1" == "output" && "$2" == "-json" && "$3" == "env_vars" ]]; then
    cat <<'__DERP_TF_EOF__'
{payload}
__DERP_TF_EOF__
    exit {exit_code}
fi
echo "fake terraform: unexpected args $@" >&2
exit 2
"""
    bin_path = bin_dir / "terraform"
    bin_path.write_text(script)
    bin_path.chmod(bin_path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


@pytest.fixture
def fake_terraform_env(tmp_path: Path):
    """Yield (project_root, bin_dir, env) with a fake terraform on PATH."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    project = tmp_path / "project"
    project.mkdir()
    (project / "infra" / "dev").mkdir(parents=True)
    (project / "infra" / "prod").mkdir(parents=True)

    old_path = os.environ.get("PATH", "")
    os.environ["PATH"] = f"{bin_dir}{os.pathsep}{old_path}"
    old_cwd = os.getcwd()
    os.chdir(project)
    try:
        yield project, bin_dir
    finally:
        os.chdir(old_cwd)
        os.environ["PATH"] = old_path


def test_env_pull_writes_env_file(fake_terraform_env) -> None:
    project, bin_dir = fake_terraform_env
    _make_fake_terraform(
        bin_dir,
        {
            "DATABASE_URL": "postgres://x",
            "WORKOS_API_KEY": "sk_live_xxx",
            "VERCEL_AI_API_KEY": "vck_yyy",
        },
    )

    result = runner.invoke(app, ["env", "pull", "dev"])
    assert result.exit_code == 0, result.stdout
    assert "Wrote .env.dev" in result.stdout

    written = (project / ".env.dev").read_text()
    # Sorted by key
    lines = [line for line in written.splitlines() if line and not line.startswith("#")]
    keys = [line.split("=", 1)[0] for line in lines]
    assert keys == sorted(keys)
    assert 'DATABASE_URL="postgres://x"' in written
    assert 'WORKOS_API_KEY="sk_live_xxx"' in written
    assert 'VERCEL_AI_API_KEY="vck_yyy"' in written


def test_env_pull_stdout_does_not_write_file(fake_terraform_env) -> None:
    project, bin_dir = fake_terraform_env
    _make_fake_terraform(bin_dir, {"FOO": "bar"})

    result = runner.invoke(app, ["env", "pull", "dev", "--stdout"])
    assert result.exit_code == 0
    assert 'FOO="bar"' in result.stdout
    assert not (project / ".env.dev").exists()


def test_env_pull_requires_env_arg(fake_terraform_env) -> None:
    _, _ = fake_terraform_env
    result = runner.invoke(app, ["env", "pull"])
    # Missing required positional argument → Typer/Click usage error
    assert result.exit_code != 0
    assert isinstance(result.exception, SystemExit)


def test_env_pull_rejects_bogus_env(fake_terraform_env) -> None:
    _, _ = fake_terraform_env
    result = runner.invoke(app, ["env", "pull", "staging"])
    assert result.exit_code != 0
    # Should mention valid choices
    assert "dev" in result.stdout or "dev" in (result.stderr or "")


def test_env_pull_refuses_to_overwrite_prod_without_force(fake_terraform_env) -> None:
    project, bin_dir = fake_terraform_env
    _make_fake_terraform(bin_dir, {"X": "y"})

    (project / ".env.prod").write_text("EXISTING=already_here\n")

    result = runner.invoke(app, ["env", "pull", "prod"])
    assert result.exit_code != 0
    assert "already exists" in result.stdout or "already exists" in (
        result.stderr or ""
    )

    # Contents should be unchanged
    assert (project / ".env.prod").read_text() == "EXISTING=already_here\n"


def test_env_pull_overwrites_prod_with_force(fake_terraform_env) -> None:
    project, bin_dir = fake_terraform_env
    _make_fake_terraform(bin_dir, {"X": "y"})

    (project / ".env.prod").write_text("EXISTING=already_here\n")

    result = runner.invoke(app, ["env", "pull", "prod", "--force"])
    assert result.exit_code == 0, result.stdout
    assert 'X="y"' in (project / ".env.prod").read_text()


def test_env_pull_missing_infra_dir(tmp_path: Path) -> None:
    """If `infra/dev/` doesn't exist, fail with a clear message."""
    old_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        result = runner.invoke(app, ["env", "pull", "dev"])
    finally:
        os.chdir(old_cwd)
    assert result.exit_code != 0
    msg = result.stdout + (result.stderr or "")
    assert "does not exist" in msg or "init --infra" in msg


def test_env_pull_terraform_failure_surfaces_stderr(fake_terraform_env) -> None:
    """Non-zero exit from terraform propagates with helpful text."""
    project, bin_dir = fake_terraform_env
    # Fake script that always fails
    (bin_dir / "terraform").write_text(
        "#!/usr/bin/env bash\necho 'no state file' >&2\nexit 1\n"
    )
    (bin_dir / "terraform").chmod(0o755)

    result = runner.invoke(app, ["env", "pull", "dev"])
    assert result.exit_code != 0
    msg = result.stdout + (result.stderr or "")
    assert "terraform output" in msg or "terraform init" in msg


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="bash shim test is POSIX-only",
)
def test_env_pull_smoke_runs_on_posix() -> None:
    """Sanity: the test infra above uses bash; ensure the platform check is honest."""
    assert sys.platform != "win32"
