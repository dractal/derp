"""Tests for `derp init --infra` interactive prompts.

Drives the prompts via Typer's CliRunner with scripted stdin, then asserts
the generated files reflect each mode (Provision / Import / BYO / Skip)
correctly per service.
"""

from __future__ import annotations

import os
from pathlib import Path

from typer.testing import CliRunner

from derp.cli.main import app
from derp.cli.scaffolding.infra import WELL_LIT_SERVICES, ServiceMode

runner = CliRunner()


def _scripted_input(modes: dict[str, ServiceMode]) -> str:
    """Compose stdin matching the WELL_LIT_SERVICES prompt order.

    Each prompt is `<label> [<allowed>]: ` with a default; we feed the
    explicit value for the test to remove ambiguity.
    """
    lines: list[str] = []
    for spec in WELL_LIT_SERVICES:
        chosen = modes.get(spec.key.value, ServiceMode.PROVISION)
        # If the spec doesn't allow the chosen mode, fall back to its first
        # allowed mode — tests should only pass valid modes.
        if chosen not in spec.allowed_modes:
            chosen = spec.allowed_modes[0]
        lines.append(chosen.value)
    return "\n".join(lines) + "\n"


def test_skip_auth_omits_section(temp_dir: Path) -> None:
    os.chdir(temp_dir)
    stdin = _scripted_input({"auth": ServiceMode.SKIP})

    result = runner.invoke(app, ["init", "demo-app", "--infra"], input=stdin)
    assert result.exit_code == 0, result.stdout

    derp_toml = (temp_dir / "derp.toml").read_text()
    assert "[auth.workos]" not in derp_toml

    main_tf = (temp_dir / "infra/dev/main.tf").read_text()
    assert 'mode_auth = "skip"' in main_tf
    assert "byo_workos_api_key" not in main_tf
    assert "import_workos_application_id" not in main_tf

    # Vendor WorkOS isn't needed for provisioning if auth is skipped
    variables_tf = (temp_dir / "infra/dev/variables.tf").read_text()
    assert "workos_management_token" not in variables_tf


def test_byo_database_relays_url(temp_dir: Path) -> None:
    os.chdir(temp_dir)
    stdin = _scripted_input({"db": ServiceMode.BYO})

    result = runner.invoke(app, ["init", "demo-app", "--infra"], input=stdin)
    assert result.exit_code == 0, result.stdout

    main_tf = (temp_dir / "infra/dev/main.tf").read_text()
    assert 'mode_db = "byo"' in main_tf
    assert "byo_database_url = var.byo_database_url" in main_tf

    variables_tf = (temp_dir / "infra/dev/variables.tf").read_text()
    assert 'variable "byo_database_url"' in variables_tf

    tfvars_example = (temp_dir / "infra/dev/terraform.tfvars.example").read_text()
    assert "byo_database_url" in tfvars_example

    derp_toml = (temp_dir / "derp.toml").read_text()
    # DB section still present; references $DATABASE_URL the BYO will emit
    assert 'db_url = "$DATABASE_URL"' in derp_toml


def test_import_storage_emits_import_block(temp_dir: Path) -> None:
    os.chdir(temp_dir)
    stdin = _scripted_input({"storage": ServiceMode.IMPORT})

    result = runner.invoke(app, ["init", "demo-app", "--infra"], input=stdin)
    assert result.exit_code == 0, result.stdout

    main_tf = (temp_dir / "infra/dev/main.tf").read_text()
    assert 'mode_storage = "import"' in main_tf
    assert "import_r2_bucket_id = var.import_r2_bucket_id" in main_tf
    # Declarative import block at the bottom
    assert "import {" in main_tf
    assert "to = module.derp.storage.this" in main_tf
    assert "id = var.import_r2_bucket_id" in main_tf

    variables_tf = (temp_dir / "infra/dev/variables.tf").read_text()
    assert 'variable "import_r2_bucket_id"' in variables_tf


def test_provision_payments_standard_shape(temp_dir: Path) -> None:
    os.chdir(temp_dir)
    stdin = _scripted_input({"payments": ServiceMode.PROVISION})

    result = runner.invoke(app, ["init", "demo-app", "--infra"], input=stdin)
    assert result.exit_code == 0, result.stdout

    main_tf = (temp_dir / "infra/dev/main.tf").read_text()
    assert 'mode_payments = "provision"' in main_tf
    # No byo or import vars for a Provision-mode service
    assert "byo_stripe" not in main_tf
    assert "import_stripe" not in main_tf

    variables_tf = (temp_dir / "infra/dev/variables.tf").read_text()
    # Stripe vendor credential is required
    assert 'variable "stripe_api_key"' in variables_tf


def test_mixed_modes_tfvars_example_only_lists_needed(temp_dir: Path) -> None:
    """Combined modes produce a coherent tfvars.example without leftovers."""
    os.chdir(temp_dir)
    stdin = _scripted_input(
        {
            "auth": ServiceMode.SKIP,
            "db": ServiceMode.BYO,
            "storage": ServiceMode.IMPORT,
            "kv": ServiceMode.SKIP,
            "payments": ServiceMode.PROVISION,
            "ai": ServiceMode.PROVISION,
            "email": ServiceMode.BYO,
        }
    )

    result = runner.invoke(app, ["init", "demo-app", "--infra"], input=stdin)
    assert result.exit_code == 0, result.stdout

    tfvars = (temp_dir / "infra/dev/terraform.tfvars.example").read_text()

    # Auth skipped → no WorkOS credentials (provisioning or BYO)
    assert "workos_management_token" not in tfvars
    assert "byo_workos_api_key" not in tfvars
    # DB BYO → byo var present, planetscale credentials absent
    assert "byo_database_url" in tfvars
    assert "planetscale_service_token" not in tfvars
    # Storage Import → import var + cloudflare vendor credentials
    assert "import_r2_bucket_id" in tfvars
    assert "cloudflare_api_token" in tfvars
    # KV skipped → no Vercel-marketplace KV vars
    assert "upstash" not in tfvars.lower()
    # Payments Provision → stripe vendor credential present
    assert "stripe_api_key" in tfvars
    # AI Provision → vercel vendor credentials present (also always-on)
    assert "vercel_api_token" in tfvars
    # Email BYO → resend byo vars present
    assert "byo_resend_api_key" in tfvars
    assert "byo_resend_from_email" in tfvars


def test_email_only_offers_byo_or_skip(temp_dir: Path) -> None:
    """Email's allowed_modes is restricted to BYO + Skip (Resend has no TF provider)."""
    from derp.cli.scaffolding.infra import WellLitService, service_spec

    email = service_spec(WellLitService.EMAIL)
    assert set(email.allowed_modes) == {ServiceMode.BYO, ServiceMode.SKIP}
