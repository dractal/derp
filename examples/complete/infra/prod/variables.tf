# variables.tf — inputs for prod. Values live in terraform.tfvars
# (gitignored) or are passed via TF_VAR_* env vars in CI.

# === Vendor provisioning credentials ===

variable "cloudflare_api_token" {
  description = "Cloudflare API token with R2:Edit + Zone:Edit (DNS) — create at https://dash.cloudflare.com/profile/api-tokens"
  type        = string
  sensitive   = true
}

variable "cloudflare_account_id" {
  description = "Cloudflare account ID (right sidebar of any domain)"
  type        = string
}

variable "planetscale_service_token_id" {
  description = "PlanetScale service token ID (from Account → Service tokens)"
  type        = string
}

variable "planetscale_service_token" {
  description = "PlanetScale service token secret"
  type        = string
  sensitive   = true
}

variable "planetscale_organization" {
  description = "PlanetScale organization slug"
  type        = string
}

variable "stripe_api_key" {
  description = "Stripe secret API key (use test mode for dev, live for prod) — from https://dashboard.stripe.com/apikeys"
  type        = string
  sensitive   = true
}

variable "vercel_api_token" {
  description = "Vercel API token from https://vercel.com/account/tokens — needs project + integration scopes"
  type        = string
  sensitive   = true
}

variable "vercel_team_id" {
  description = "Vercel team ID (find under Team Settings → General)"
  type        = string
}

variable "workos_management_token" {
  description = "WorkOS API key from https://dashboard.workos.com — used by Terraform to create/manage the WorkOS application"
  type        = string
  sensitive   = true
}

# === BYO credentials (relayed into env_vars output) ===

variable "byo_resend_api_key" {
  description = "BYO Email (Resend, DNS via Cloudflare): existing RESEND_API_KEY value"
  type        = string
  sensitive   = true
}

variable "byo_resend_from_email" {
  description = "BYO Email (Resend, DNS via Cloudflare): existing RESEND_FROM_EMAIL value"
  type        = string
  sensitive   = true
}
