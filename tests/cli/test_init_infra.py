"""Tests for `derp init --infra --yes` (greenfield-everything scaffold)."""

from __future__ import annotations

import os
from pathlib import Path

from typer.testing import CliRunner

from derp.cli.main import app
from derp.cli.scaffolding.infra import default_yes_choices
from derp.cli.scaffolding.infra.generator import GITIGNORE_LINES

runner = CliRunner()


def test_yes_scaffold_writes_full_tree(temp_dir: Path) -> None:
    """--infra --yes should produce derp.toml + infra/{dev,prod}/."""
    os.chdir(temp_dir)
    result = runner.invoke(app, ["init", "demo-app", "--infra", "--yes"])

    assert result.exit_code == 0, result.stdout

    expected_files = [
        "derp.toml",
        "infra/versions.tf",
        "infra/README.md",
        "infra/dev/main.tf",
        "infra/dev/variables.tf",
        "infra/dev/outputs.tf",
        "infra/dev/backend.tf",
        "infra/dev/terraform.tfvars.example",
        "infra/prod/main.tf",
        "infra/prod/variables.tf",
        "infra/prod/outputs.tf",
        "infra/prod/backend.tf",
        "infra/prod/terraform.tfvars.example",
    ]
    for rel in expected_files:
        assert (temp_dir / rel).is_file(), f"missing {rel}"


def test_yes_scaffold_tier_differs_dev_vs_prod(temp_dir: Path) -> None:
    """dev defaults to `tier = "hobby"`, prod to `tier = "production"`."""
    os.chdir(temp_dir)
    runner.invoke(app, ["init", "demo-app", "--infra", "--yes"])

    dev_main = (temp_dir / "infra/dev/main.tf").read_text()
    prod_main = (temp_dir / "infra/prod/main.tf").read_text()

    assert 'tier        = "hobby"' in dev_main
    assert 'environment = "dev"' in dev_main
    assert 'tier        = "production"' in prod_main
    assert 'environment = "prod"' in prod_main


def test_yes_scaffold_gitignore_added(temp_dir: Path) -> None:
    """All required gitignore lines get added (idempotently)."""
    os.chdir(temp_dir)
    runner.invoke(app, ["init", "demo-app", "--infra", "--yes"])

    gitignore = (temp_dir / ".gitignore").read_text()
    for line in GITIGNORE_LINES:
        assert line in gitignore, f"{line!r} missing from .gitignore"


def test_yes_scaffold_derp_toml_has_all_provisioned_sections(temp_dir: Path) -> None:
    """Greenfield derp.toml includes a section for every Provision/BYO service."""
    os.chdir(temp_dir)
    runner.invoke(app, ["init", "demo-app", "--infra", "--yes"])

    derp_toml = (temp_dir / "derp.toml").read_text()
    # Provisioned in --yes mode
    assert "[database]" in derp_toml
    assert "[auth.workos]" in derp_toml
    assert "[storage]" in derp_toml
    assert "[kv.valkey]" in derp_toml
    assert "[payments]" in derp_toml
    assert "[ai]" in derp_toml
    # BYO (email)
    assert "[email]" in derp_toml
    # Always-on Vercel queue config
    assert "[queue.vercel]" in derp_toml
    # AI gateway uses the dedicated var, not OPENAI_*
    assert "$VERCEL_AI_API_KEY" in derp_toml
    assert "$OPENAI_API_KEY" not in derp_toml


def test_yes_scaffold_app_name_defaults_to_cwd(temp_dir: Path) -> None:
    """No positional name → use the directory basename."""
    target = temp_dir / "my-cool-app"
    target.mkdir()
    os.chdir(target)

    result = runner.invoke(app, ["init", "--infra", "--yes"])
    assert result.exit_code == 0, result.stdout

    main_tf = (target / "infra/dev/main.tf").read_text()
    assert 'app_name    = "my-cool-app"' in main_tf


def test_force_required_for_overwrite(temp_dir: Path) -> None:
    """Re-running --infra --yes without --force is an error."""
    os.chdir(temp_dir)
    first = runner.invoke(app, ["init", "demo-app", "--infra", "--yes"])
    assert first.exit_code == 0

    second = runner.invoke(app, ["init", "demo-app", "--infra", "--yes"])
    assert second.exit_code == 1
    assert "already exists" in second.stdout

    third = runner.invoke(app, ["init", "demo-app", "--infra", "--yes", "--force"])
    assert third.exit_code == 0


def test_default_yes_choices_provisions_everything_except_email() -> None:
    """default_yes_choices is the source of truth for greenfield defaults."""
    from derp.cli.scaffolding.infra.specs import ServiceMode, WellLitService

    choices = default_yes_choices("demo-app")
    assert choices.app_name == "demo-app"

    modes = {c.service: c.mode for c in choices.modes}
    # Email is Resend-BYO-only — never Provision
    assert modes[WellLitService.EMAIL] == ServiceMode.BYO
    # Every other service defaults to Provision
    for svc in WellLitService:
        if svc == WellLitService.EMAIL:
            continue
        assert modes[svc] == ServiceMode.PROVISION, (
            f"{svc.value} expected Provision in greenfield default"
        )
