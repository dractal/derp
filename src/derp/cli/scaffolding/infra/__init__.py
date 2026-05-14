"""Terraform scaffolding for the derp well-lit path.

Generates a `derp.toml` + `infra/dev/` + `infra/prod/` tree wired to the
`derp-infra` Terraform module, based on per-service mode selections.

Public API:

- :data:`WELL_LIT_SERVICES` — the ordered list of services the user picks modes for
- :class:`ServiceMode` — Provision / Import / BYO / Skip
- :func:`generate_infra_scaffold` — write the full file tree for a chosen layout
- :func:`render_derp_toml` — render just the `derp.toml` text for a chosen layout
"""

from __future__ import annotations

from derp.cli.scaffolding.infra.generator import (
    InfraLayout,
    generate_infra_scaffold,
    render_derp_toml,
)
from derp.cli.scaffolding.infra.specs import (
    WELL_LIT_SERVICES,
    InfraChoices,
    ServiceMode,
    ServiceModeChoice,
    ServiceSpec,
    WellLitService,
    default_yes_choices,
    service_spec,
)

__all__ = [
    "WELL_LIT_SERVICES",
    "InfraChoices",
    "InfraLayout",
    "ServiceMode",
    "ServiceModeChoice",
    "ServiceSpec",
    "WellLitService",
    "default_yes_choices",
    "generate_infra_scaffold",
    "render_derp_toml",
    "service_spec",
]
