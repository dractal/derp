"""Contract test: the well-lit-path-scaffolded derp.toml's $VAR_NAME refs
must exactly match the env_vars keys the Terraform module is expected to emit.

This catches drift between the framework (which reads `$VAR_NAME` references)
and the `derp-infra` module repo (which emits keys via its `env_vars` output).
"""

from __future__ import annotations

import re

from derp.cli.scaffolding.infra import (
    InfraChoices,
    ServiceMode,
    ServiceModeChoice,
    WellLitService,
    default_yes_choices,
    render_derp_toml,
)
from derp.cli.scaffolding.infra.generator import InfraLayout
from derp.cli.scaffolding.infra.specs import (
    ALWAYS_EMITTED_ENV_VARS,
    WELL_LIT_SERVICES,
    service_spec,
)

_VAR_RE = re.compile(r"\$([A-Z][A-Z0-9_]*)")


def _vars_referenced_in_toml(toml_text: str) -> set[str]:
    """Extract every $VAR_NAME reference in a TOML string."""
    return set(_VAR_RE.findall(toml_text))


def _expected_env_vars(choices: InfraChoices) -> set[str]:
    expected = set(ALWAYS_EMITTED_ENV_VARS)
    for spec in choices.enabled_services():
        expected.update(spec.env_vars)
    return expected


def test_greenfield_toml_var_refs_match_module_output() -> None:
    """Every $VAR in --yes derp.toml maps to a key in the env_vars output."""
    choices = default_yes_choices("contract-app")
    toml_text = render_derp_toml(choices)

    referenced = _vars_referenced_in_toml(toml_text)
    expected = _expected_env_vars(choices)

    missing = referenced - expected
    assert not missing, (
        f"derp.toml references vars the module won't emit: {sorted(missing)}"
    )


def test_no_raw_openai_env_var_in_well_lit_toml() -> None:
    """Vercel AI Gateway uses dedicated env var names — never OPENAI_*."""
    choices = default_yes_choices("contract-app")
    toml_text = render_derp_toml(choices)
    referenced = _vars_referenced_in_toml(toml_text)

    assert "VERCEL_AI_API_KEY" in referenced
    assert "VERCEL_AI_BASE_URL" in referenced
    assert "OPENAI_API_KEY" not in referenced
    assert "OPENAI_BASE_URL" not in referenced


def test_byo_only_config_only_references_byo_keys() -> None:
    """A BYO-everything (where allowed) config only references BYO env vars."""
    modes = []
    for spec in WELL_LIT_SERVICES:
        # Pick BYO when available, otherwise Skip
        mode = (
            ServiceMode.BYO
            if ServiceMode.BYO in spec.allowed_modes
            else ServiceMode.SKIP
        )
        modes.append(ServiceModeChoice(service=spec.key, mode=mode))

    choices = InfraChoices(app_name="byo-app", modes=tuple(modes))
    toml_text = render_derp_toml(choices)
    referenced = _vars_referenced_in_toml(toml_text)
    expected = _expected_env_vars(choices)

    # All referenced vars are emitted (provisioned or relayed BYO — same map)
    assert not (referenced - expected), (
        f"Unexpected vars referenced in BYO-everything config: "
        f"{sorted(referenced - expected)}"
    )

    # Specifically, BYO services contribute their env vars too
    byo_specs = [s for s in choices.enabled_services()]
    for spec in byo_specs:
        for env_var in spec.env_vars:
            assert env_var in expected, f"BYO {spec.label}: {env_var} missing"


def test_skip_omits_service_env_vars() -> None:
    """Skipping a service drops its env var references from derp.toml."""
    modes = []
    for spec in WELL_LIT_SERVICES:
        if spec.key == WellLitService.KV:
            mode = ServiceMode.SKIP
        elif spec.key == WellLitService.EMAIL:
            mode = ServiceMode.BYO
        else:
            mode = ServiceMode.PROVISION
        modes.append(ServiceModeChoice(service=spec.key, mode=mode))

    choices = InfraChoices(app_name="no-kv", modes=tuple(modes))
    toml_text = render_derp_toml(choices)
    referenced = _vars_referenced_in_toml(toml_text)

    kv_spec = service_spec(WellLitService.KV)
    for kv_var in kv_spec.env_vars:
        assert kv_var not in referenced, f"Skipped KV but found {kv_var} in derp.toml"


def test_layout_env_vars_match_expected_set() -> None:
    """InfraLayout.env_vars_emitted() agrees with the contract."""
    choices = default_yes_choices("layout-app")
    layout = InfraLayout(env="dev", tier="hobby", choices=choices)
    emitted = set(layout.env_vars_emitted())
    expected = _expected_env_vars(choices)
    assert emitted == expected
