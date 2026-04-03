terraform {
  required_version = ">= 1.5.0"

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

variable "account_id" {
  description = "Cloudflare Account ID"
  type        = string
  sensitive   = true
}

variable "r2_access_key_id" {
  description = "R2 S3-compatible access key ID (used only by ETL — not the Worker)"
  type        = string
  sensitive   = true
}

variable "r2_secret_access_key" {
  description = "R2 S3-compatible secret access key (used only by ETL — not the Worker)"
  type        = string
  sensitive   = true
}

variable "worker_name" {
  description = "Name of the Cloudflare Worker"
  type        = string
  default     = "icon-api-worker"
}

variable "r2_bucket_name" {
  description = "Name of the R2 bucket"
  type        = string
  default     = "icon-global-databank"
}

variable "kv_namespace_title" {
  description = "Title of the KV namespace for metadata caching"
  type        = string
  default     = "icon-metadata-cache"
}

variable "worker_script_path" {
  description = "Path to compiled Worker script (index.js)"
  type        = string
  default     = "../worker/dist/index.js"
}

# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------

provider "cloudflare" {
  account_id = var.account_id
}

# ---------------------------------------------------------------------------
# R2 Bucket
# ---------------------------------------------------------------------------

resource "cloudflare_r2_bucket" "databank" {
  account_id = var.account_id
  name       = var.r2_bucket_name
  location   = "WNAM" # Western North America; change to suit your region

  # NOTE: Lifecycle/expiration is NOT configured here.
  # Data retention is managed exclusively by the ETL Janitor logic.
}

# ---------------------------------------------------------------------------
# Workers KV Namespace — metadata caching (latest run pointer, etc.)
# ---------------------------------------------------------------------------

resource "cloudflare_workers_kv_namespace" "metadata_cache" {
  account_id = var.account_id
  title      = var.kv_namespace_title
}

# ---------------------------------------------------------------------------
# Cloudflare Worker Script
# ---------------------------------------------------------------------------

resource "cloudflare_worker_script" "api" {
  account_id = var.account_id
  name       = var.worker_name
  content    = file(var.worker_script_path)

  # Bind the R2 bucket — Worker accesses data via binding, never via public URL
  r2_bucket_binding {
    name        = "DATABANK"
    bucket_name = cloudflare_r2_bucket.databank.name
  }

  # Bind the KV namespace for sub-millisecond metadata reads
  kv_namespace_binding {
    name         = "METADATA"
    namespace_id = cloudflare_workers_kv_namespace.metadata_cache.id
  }
}

# ---------------------------------------------------------------------------
# Worker Route (optional — attach to a custom domain)
# ---------------------------------------------------------------------------
# Uncomment and set zone_id + pattern if you want to serve on a custom domain.
#
# resource "cloudflare_worker_route" "api_route" {
#   zone_id     = var.zone_id
#   pattern     = "weather-api.example.com/*"
#   script_name = cloudflare_worker_script.api.name
# }

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "r2_bucket_name" {
  description = "R2 bucket name"
  value       = cloudflare_r2_bucket.databank.name
}

output "r2_endpoint" {
  description = "R2 S3-compatible endpoint for ETL script"
  value       = "https://${var.account_id}.r2.cloudflarestorage.com"
  sensitive   = true
}

output "kv_namespace_id" {
  description = "KV namespace ID for metadata caching"
  value       = cloudflare_workers_kv_namespace.metadata_cache.id
}

output "worker_name" {
  description = "Deployed Worker name"
  value       = cloudflare_worker_script.api.name
}
