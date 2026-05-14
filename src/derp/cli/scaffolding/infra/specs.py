"""Service specs for the derp well-lit-path infrastructure scaffolding.

These specs are the single source of truth for what each service contributes:
which `derp.toml` section it produces, which env vars it emits, which Terraform
inputs it needs per mode (Provision / Import / BYO), and which modes are valid
for it.

The same specs drive:
- the interactive prompts in `derp init --infra`
- the rendered `derp.toml` (only enabled services appear)
- the rendered Terraform `main.tf` / `variables.tf` / `terraform.tfvars.example`
  (only relevant variables appear per chosen mode)
- the contract test that asserts every `$VAR_NAME` in `derp.toml` has a
  matching key in the Terraform module's `env_vars` output

Keep this file dependency-free (stdlib only) so it can be imported anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ServiceMode(StrEnum):
    """How a service is wired into the well-lit path for one environment."""

    PROVISION = "provision"
    IMPORT = "import"
    BYO = "byo"
    SKIP = "skip"


class WellLitService(StrEnum):
    """The services that make up the well-lit path."""

    AUTH = "auth"
    DB = "db"
    STORAGE = "storage"
    KV = "kv"
    EMAIL = "email"
    PAYMENTS = "payments"
    AI = "ai"


class Vendor(StrEnum):
    """Vendors whose Terraform providers we provision through."""

    VERCEL = "vercel"
    CLOUDFLARE = "cloudflare"
    WORKOS = "workos"
    PLANETSCALE = "planetscale"
    STRIPE = "stripe"


@dataclass(frozen=True)
class TfVariable:
    """A variable in `infra/<env>/variables.tf` + `terraform.tfvars.example`."""

    name: str
    description: str
    example: str = ""
    sensitive: bool = True


@dataclass(frozen=True)
class ImportResource:
    """A resource ID needed when a service is in Import mode."""

    var_name: str
    description: str
    example: str


@dataclass(frozen=True)
class ByoCredential:
    """A credential the user supplies when a service is in BYO mode.

    `env_var` is the env var name the credential becomes in the rendered
    `env_vars` output (matching the `$VAR_NAME` references in `derp.toml`).
    """

    var_name: str
    env_var: str
    description: str
    example: str = ""


@dataclass(frozen=True)
class ServiceSpec:
    """Spec for one service in the well-lit path."""

    key: WellLitService
    label: str
    vendors: tuple[Vendor, ...]
    env_vars: tuple[str, ...]
    import_resources: tuple[ImportResource, ...] = ()
    byo_credentials: tuple[ByoCredential, ...] = ()
    allowed_modes: tuple[ServiceMode, ...] = (
        ServiceMode.PROVISION,
        ServiceMode.IMPORT,
        ServiceMode.BYO,
        ServiceMode.SKIP,
    )
    # A renderer for the derp.toml block. Returns the section text (no
    # trailing newline). Takes nothing — sections that need user-tunable
    # config keep them as commented placeholders so the rendered file is
    # always valid TOML.
    toml_section: str = ""


# Vendor-level provisioning credentials. These are required whenever any
# service for that vendor is in Provision or Import mode. Always-provisioned
# resources (the Vercel project + Queues + Cron) make Vercel mandatory in
# every scaffold.
VENDOR_CREDENTIALS: dict[Vendor, tuple[TfVariable, ...]] = {
    Vendor.VERCEL: (
        TfVariable(
            name="vercel_api_token",
            description=(
                "Vercel API token from https://vercel.com/account/tokens — "
                "needs project + integration scopes"
            ),
            example="vercel_xxx",
        ),
        TfVariable(
            name="vercel_team_id",
            description="Vercel team ID (find under Team Settings → General)",
            example="team_xxx",
            sensitive=False,
        ),
    ),
    Vendor.CLOUDFLARE: (
        TfVariable(
            name="cloudflare_api_token",
            description=(
                "Cloudflare API token with R2:Edit + Zone:Edit (DNS) — "
                "create at https://dash.cloudflare.com/profile/api-tokens"
            ),
            example="cloudflare_xxx",
        ),
        TfVariable(
            name="cloudflare_account_id",
            description="Cloudflare account ID (right sidebar of any domain)",
            example="0123456789abcdef",
            sensitive=False,
        ),
    ),
    Vendor.WORKOS: (
        TfVariable(
            name="workos_management_token",
            description=(
                "WorkOS API key from https://dashboard.workos.com — "
                "used by Terraform to create/manage the WorkOS application"
            ),
            example="sk_test_xxx",
        ),
    ),
    Vendor.PLANETSCALE: (
        TfVariable(
            name="planetscale_service_token_id",
            description="PlanetScale service token ID (from Account → Service tokens)",
            example="abc123",
            sensitive=False,
        ),
        TfVariable(
            name="planetscale_service_token",
            description="PlanetScale service token secret",
            example="pscale_tkn_xxx",
        ),
        TfVariable(
            name="planetscale_organization",
            description="PlanetScale organization slug",
            example="my-org",
            sensitive=False,
        ),
    ),
    Vendor.STRIPE: (
        TfVariable(
            name="stripe_api_key",
            description=(
                "Stripe secret API key (use test mode for dev, live for prod) — "
                "from https://dashboard.stripe.com/apikeys"
            ),
            example="sk_test_xxx",
        ),
    ),
}


_AUTH = ServiceSpec(
    key=WellLitService.AUTH,
    label="Auth (WorkOS)",
    vendors=(Vendor.WORKOS,),
    env_vars=("WORKOS_API_KEY", "WORKOS_CLIENT_ID"),
    import_resources=(
        ImportResource(
            var_name="import_workos_application_id",
            description="Existing WorkOS Application ID to adopt under Terraform",
            example="app_01H...",
        ),
    ),
    byo_credentials=(
        ByoCredential(
            var_name="byo_workos_api_key",
            env_var="WORKOS_API_KEY",
            description="Existing WorkOS API key",
            example="sk_test_xxx",
        ),
        ByoCredential(
            var_name="byo_workos_client_id",
            env_var="WORKOS_CLIENT_ID",
            description="Existing WorkOS Client ID",
            example="client_01H...",
        ),
    ),
    toml_section=(
        "[auth.workos]\n"
        'api_key = "$WORKOS_API_KEY"\n'
        'client_id = "$WORKOS_CLIENT_ID"\n'
        '# redirect_uri = "https://yourapp.com/callback"\n'
    ),
)


_DB = ServiceSpec(
    key=WellLitService.DB,
    label="Database (PlanetScale)",
    vendors=(Vendor.PLANETSCALE,),
    env_vars=("DATABASE_URL",),
    import_resources=(
        ImportResource(
            var_name="import_planetscale_database_id",
            description="Existing PlanetScale database name to adopt",
            example="my-app-db",
        ),
    ),
    byo_credentials=(
        ByoCredential(
            var_name="byo_database_url",
            env_var="DATABASE_URL",
            description="Existing PlanetScale (or any Postgres) connection URL",
            example="postgres://user:pass@host:5432/dbname",
        ),
    ),
    toml_section=(
        "[database]\n"
        'db_url = "$DATABASE_URL"\n'
        'schema_path = "app/schema.py"\n'
        'migrations_dir = "./migrations"\n'
    ),
)


_STORAGE = ServiceSpec(
    key=WellLitService.STORAGE,
    label="Object storage (Cloudflare R2)",
    vendors=(Vendor.CLOUDFLARE,),
    env_vars=(
        "R2_BUCKET",
        "R2_ENDPOINT_URL",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
    ),
    import_resources=(
        ImportResource(
            var_name="import_r2_bucket_id",
            description="Existing R2 bucket name to adopt",
            example="my-app-uploads",
        ),
    ),
    byo_credentials=(
        ByoCredential(
            var_name="byo_r2_bucket",
            env_var="R2_BUCKET",
            description="Existing R2 bucket name",
            example="my-app-uploads",
        ),
        ByoCredential(
            var_name="byo_r2_endpoint_url",
            env_var="R2_ENDPOINT_URL",
            description="R2 S3-compatible endpoint URL",
            example="https://<account>.r2.cloudflarestorage.com",
        ),
        ByoCredential(
            var_name="byo_r2_access_key_id",
            env_var="R2_ACCESS_KEY_ID",
            description="R2 API token (Access Key ID)",
            example="...",
        ),
        ByoCredential(
            var_name="byo_r2_secret_access_key",
            env_var="R2_SECRET_ACCESS_KEY",
            description="R2 API token secret",
            example="...",
        ),
    ),
    toml_section=(
        "[storage]\n"
        'endpoint_url = "$R2_ENDPOINT_URL"\n'
        'access_key_id = "$R2_ACCESS_KEY_ID"\n'
        'secret_access_key = "$R2_SECRET_ACCESS_KEY"\n'
        'region = "auto"\n'
    ),
)


_KV = ServiceSpec(
    key=WellLitService.KV,
    label="KV (Upstash Redis via Vercel marketplace)",
    vendors=(Vendor.VERCEL,),
    env_vars=("UPSTASH_REDIS_HOST", "UPSTASH_REDIS_PORT", "UPSTASH_REDIS_PASSWORD"),
    import_resources=(
        ImportResource(
            var_name="import_upstash_integration_id",
            description=(
                "Existing Vercel marketplace Upstash integration ID — "
                "find via `vercel integration ls`"
            ),
            example="icfg_xxx",
        ),
    ),
    byo_credentials=(
        ByoCredential(
            var_name="byo_upstash_redis_host",
            env_var="UPSTASH_REDIS_HOST",
            description="Upstash Redis host",
            example="usw1-foo-12345.upstash.io",
        ),
        ByoCredential(
            var_name="byo_upstash_redis_port",
            env_var="UPSTASH_REDIS_PORT",
            description="Upstash Redis port",
            example="6379",
        ),
        ByoCredential(
            var_name="byo_upstash_redis_password",
            env_var="UPSTASH_REDIS_PASSWORD",
            description="Upstash Redis password",
            example="...",
        ),
    ),
    toml_section=(
        "[kv.valkey]\n"
        'addresses = [["$UPSTASH_REDIS_HOST", "$UPSTASH_REDIS_PORT"]]\n'
        'password = "$UPSTASH_REDIS_PASSWORD"\n'
        "use_tls = true\n"
    ),
)


# Email is special-cased: Resend has no documented Terraform provider, so the
# well-lit path never provisions or imports the Resend resource itself — only
# BYO (relay the user-supplied API key) or Skip. DNS records (DKIM/SPF/DMARC/
# MX) are still managed by the email submodule via the Cloudflare provider.
_EMAIL = ServiceSpec(
    key=WellLitService.EMAIL,
    label="Email (Resend, DNS via Cloudflare)",
    vendors=(Vendor.CLOUDFLARE,),
    env_vars=("RESEND_API_KEY", "RESEND_FROM_EMAIL"),
    byo_credentials=(
        ByoCredential(
            var_name="byo_resend_api_key",
            env_var="RESEND_API_KEY",
            description=(
                "Resend API key (create one at https://resend.com/api-keys "
                "after registering your sending domain)"
            ),
            example="re_xxx",
        ),
        ByoCredential(
            var_name="byo_resend_from_email",
            env_var="RESEND_FROM_EMAIL",
            description="Verified sender address on your Resend domain",
            example="noreply@yourdomain.com",
        ),
    ),
    allowed_modes=(ServiceMode.BYO, ServiceMode.SKIP),
    toml_section=(
        "[email]\n"
        'site_name = "My App"\n'
        'site_url = "https://yourdomain.com"\n'
        'from_email = "$RESEND_FROM_EMAIL"\n'
        'smtp_host = "smtp.resend.com"\n'
        "smtp_port = 587\n"
        'smtp_user = "resend"\n'
        'smtp_password = "$RESEND_API_KEY"\n'
        "use_tls = true\n"
    ),
)


_PAYMENTS = ServiceSpec(
    key=WellLitService.PAYMENTS,
    label="Payments (Stripe)",
    vendors=(Vendor.STRIPE,),
    env_vars=("STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET"),
    import_resources=(
        ImportResource(
            var_name="import_stripe_webhook_endpoint_id",
            description="Existing Stripe webhook endpoint ID to adopt",
            example="we_xxx",
        ),
    ),
    byo_credentials=(
        ByoCredential(
            var_name="byo_stripe_secret_key",
            env_var="STRIPE_SECRET_KEY",
            description="Stripe secret key (test or live)",
            example="sk_test_xxx",
        ),
        ByoCredential(
            var_name="byo_stripe_webhook_secret",
            env_var="STRIPE_WEBHOOK_SECRET",
            description="Stripe webhook signing secret",
            example="whsec_xxx",
        ),
    ),
    toml_section=(
        "[payments]\n"
        'api_key = "$STRIPE_SECRET_KEY"\n'
        'webhook_secret = "$STRIPE_WEBHOOK_SECRET"\n'
    ),
)


_AI = ServiceSpec(
    key=WellLitService.AI,
    label="AI gateway (Vercel AI Gateway, OpenAI-compatible)",
    vendors=(Vendor.VERCEL,),
    env_vars=("VERCEL_AI_API_KEY", "VERCEL_AI_BASE_URL"),
    import_resources=(
        ImportResource(
            var_name="import_vercel_ai_gateway_id",
            description="Existing Vercel AI Gateway slug to adopt",
            example="my-gateway",
        ),
    ),
    byo_credentials=(
        ByoCredential(
            var_name="byo_vercel_ai_api_key",
            env_var="VERCEL_AI_API_KEY",
            description="Vercel AI Gateway API key (NOT a raw OpenAI key)",
            example="vck_xxx",
        ),
        ByoCredential(
            var_name="byo_vercel_ai_base_url",
            env_var="VERCEL_AI_BASE_URL",
            description="Vercel AI Gateway base URL (OpenAI-compatible endpoint)",
            example="https://ai-gateway.vercel.sh/v1",
        ),
    ),
    toml_section=(
        '[ai]\napi_key = "$VERCEL_AI_API_KEY"\nbase_url = "$VERCEL_AI_BASE_URL"\n'
    ),
)


# Order matters: this is the order interactive prompts walk through.
WELL_LIT_SERVICES: tuple[ServiceSpec, ...] = (
    _DB,
    _AUTH,
    _STORAGE,
    _KV,
    _PAYMENTS,
    _AI,
    _EMAIL,
)


def service_spec(key: WellLitService) -> ServiceSpec:
    """Look up a service spec by key."""
    for spec in WELL_LIT_SERVICES:
        if spec.key == key:
            return spec
    raise KeyError(key)


# Env vars always emitted by the always-provisioned compute layer (Vercel
# project + Queues + Cron). These appear in `env_vars` regardless of which
# optional services are enabled.
ALWAYS_EMITTED_ENV_VARS: tuple[str, ...] = (
    "VERCEL_PROJECT_ID",
    "VERCEL_QUEUE_TOKEN",
    "VERCEL_QUEUE_DEFAULT",
)


@dataclass(frozen=True)
class ServiceModeChoice:
    """A user's chosen mode for one service."""

    service: WellLitService
    mode: ServiceMode


@dataclass(frozen=True)
class InfraChoices:
    """All choices made for one well-lit-path scaffold.

    The same choices apply to both dev and prod environments — mode shape is
    shared, only the credentials/region/tier differ at runtime.
    """

    app_name: str
    modes: tuple[ServiceModeChoice, ...] = field(default_factory=tuple)

    def mode_for(self, service: WellLitService) -> ServiceMode:
        for choice in self.modes:
            if choice.service == service:
                return choice.mode
        return ServiceMode.SKIP

    def enabled_services(self) -> tuple[ServiceSpec, ...]:
        """Services whose mode is not Skip."""
        return tuple(
            service_spec(c.service) for c in self.modes if c.mode != ServiceMode.SKIP
        )

    def vendors_in_use(self) -> set[Vendor]:
        """Vendors whose provisioning credentials are required.

        Vercel is always required because the Vercel project + Queues + Cron
        are always provisioned (they're how the app runs).
        """
        vendors: set[Vendor] = {Vendor.VERCEL}
        for choice in self.modes:
            if choice.mode in (ServiceMode.PROVISION, ServiceMode.IMPORT):
                spec = service_spec(choice.service)
                vendors.update(spec.vendors)
        return vendors


def default_yes_choices(app_name: str) -> InfraChoices:
    """Greenfield-everything defaults for `derp init --infra --yes`.

    All services that allow Provision get Provision; email (which only allows
    BYO/Skip) defaults to BYO.
    """
    modes: list[ServiceModeChoice] = []
    for spec in WELL_LIT_SERVICES:
        if ServiceMode.PROVISION in spec.allowed_modes:
            mode = ServiceMode.PROVISION
        else:
            mode = ServiceMode.BYO
        modes.append(ServiceModeChoice(service=spec.key, mode=mode))
    return InfraChoices(app_name=app_name, modes=tuple(modes))
