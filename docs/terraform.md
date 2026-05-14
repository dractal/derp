# Terraform — the well-lit path

derp ships a Terraform-based scaffold for an opinionated default stack
("the well-lit path") that you can provision in one command. The flow is:

```bash
derp init my-app --infra        # interactive: pick a mode per service
cd infra/dev
cp terraform.tfvars.example terraform.tfvars && $EDITOR terraform.tfvars
terraform init && terraform apply
cd ../..
derp env pull dev               # writes .env.dev with all $VAR_NAMEs
uv run uvicorn app.main:app --env-file .env.dev
```

Two artifacts get scaffolded:

- **`derp.toml`** — tailored to the services you chose. Each `$VAR_NAME`
  reference matches a key the Terraform module emits.
- **`infra/dev/` and `infra/prod/`** — independent Terraform roots, one
  per environment, with separate state, separate vendor-side resources,
  and separate API keys. Never share state or credentials across the two.

## The stack

| Layer        | Vendor                                  | Notes |
| ------------ | --------------------------------------- | ----- |
| Frontend     | Vercel                                  | Out of scope for derp — bring your own. |
| Backend      | Vercel Fluid Compute (Python)           | Colocated with PlanetScale on AWS us-east-1. |
| Database     | PlanetScale (Postgres, AWS us-east-1)   | |
| Storage      | Cloudflare R2                           | Zero-egress, S3-compatible API. |
| Auth         | WorkOS                                  | Same provider used by OpenAI, Anthropic, Vercel. |
| KV           | Upstash Redis via Vercel marketplace    | Conditional — see notes. |
| Queue + Cron | Vercel Queues + Vercel Cron             | Native to Vercel Fluid Compute. |
| Email        | Resend (BYO) + Cloudflare DNS           | Resend has no TF provider; DNS records are TF-managed. |
| Payments     | Stripe                                  | Webhook endpoints + product/price stubs are TF-managed. |
| AI Gateway   | Vercel AI Gateway                       | OpenAI-compatible from Python via HTTP. |

## Modes per service

For each service, `derp init --infra` asks how Terraform should handle it:

- **`provision`** — Terraform creates a new resource for you (greenfield default).
- **`import`** — Terraform adopts an existing resource into state and manages
  it going forward. Provide the resource ID in `terraform.tfvars`; the
  scaffolded `main.tf` includes a TF 1.5+ declarative `import` block that
  runs on the next `terraform apply`. After import, edits in the vendor UI
  will be reverted on the next apply.
- **`byo`** — Terraform only relays your existing credentials into the
  `env_vars` output. The resource itself stays under your manual management.
- **`skip`** — service is not part of this stack; not referenced in
  `derp.toml` either.

Pass `--yes` to accept greenfield-Provision defaults non-interactively
(email defaults to BYO since Resend has no Provision mode).

## Mode transitions worth knowing about

- **BYO → Import** is the natural upgrade: flip `mode_<service> = "byo" → "import"`,
  add the matching `import_<service>_*_id` variable, run `terraform apply`.
  The declarative import block does the adoption.
- **BYO → Provision** is destructive in intent: it creates a *new* resource,
  doesn't adopt your existing one.
- **Import → BYO** is the rollback: remove the `import` block, set the mode
  back to `byo`, provide `byo_<service>_*` values. Run `terraform state rm`
  to detach without destroying.
- **Skip → anything** requires re-running `derp init --infra` (or editing
  `main.tf` by hand). The framework's `DerpClient` factory gracefully handles
  added/removed sections in `derp.toml`.

## State backend

Generated `backend.tf` defaults to **local state**. This is fine for first
exploration, **not acceptable for prod**. Switch to S3 + DynamoDB (or
Terraform Cloud) before sharing state with anyone. The commented blocks
in `backend.tf` are scoped per app + environment.

## The `derp-infra` module

The scaffolded `infra/<env>/main.tf` references
`source = "derp/derp/aws"` — a separate module repo. The module composes
existing vendor providers (`vercel/vercel`, `cloudflare/cloudflare`,
`planetscale/planetscale`, etc.) and exposes a single `env_vars` map
output. The map's keys match the `$VAR_NAME` references in the generated
`derp.toml`; the contract test
(`tests/cli/test_env_pull_contract.py`) keeps the two repos honest.

## Why these specific naming choices

A few decisions that look arbitrary but aren't:

- **`VERCEL_AI_API_KEY`, not `OPENAI_API_KEY`.** The Vercel AI Gateway is
  OpenAI-compatible, but using OpenAI's env var name invites users to
  paste their real OpenAI key into `.env`, after which the gateway URL
  (`ai-gateway.vercel.sh`) starts returning auth errors. Distinct names
  avoid the footgun.
- **Resend stays BYO even though Cloudflare's email service is shipping.**
  Deliverability is a multi-year reputation game; CF Email is still in
  private beta and Workers-binding-only. Switching would mean either silent
  inbox-placement regressions or writing a bridging Worker just for email.
  Resend is the safer pick until both ship.
- **KV is conditional.** Upstash Redis is provisioned via the Vercel
  marketplace integration if it's terraform-able; otherwise omitted from
  the well-lit path. Users who explicitly want KV can wire Upstash directly.

## Alternative: Stripe Projects

If you're a true greenfield project and want billing consolidation,
[Stripe Projects](https://docs.stripe.com/projects) is a reasonable
alternative — it provisions a similar catalog (Vercel, PlanetScale,
WorkOS, Upstash, Cloudflare) with consolidated billing through Stripe.
The catch is that Stripe Projects can't adopt existing resources — every
provider gets a freshly-created account. If you have any existing
infrastructure to migrate, the Terraform path here is the better fit.
The env var contract (`$WORKOS_API_KEY`, `$DATABASE_URL`, etc.) is the
same either way, so you can swap source-of-truth without touching the app.
