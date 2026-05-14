# backend.tf — Terraform state backend for dev.
#
# DEFAULT IS LOCAL — fine for first-time setup, NOT acceptable for prod.
# Pick a remote backend and uncomment the matching block before running
# `terraform init` in any shared environment. The block below is sized to
# scope state to this environment (`demo-app/dev`).

terraform {
  backend "local" {
    path = "terraform.tfstate"
  }

  # === Recommended for production: S3 + DynamoDB lock ===
  # backend "s3" {
  #   bucket         = "your-tf-state-bucket"
  #   key            = "demo-app/dev/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }

  # === Alternative: Terraform Cloud / HCP ===
  # cloud {
  #   organization = "your-org"
  #   workspaces { name = "demo-app-dev" }
  # }
}
