# Serverless Global Weather Data Bank

Fully automated, production-grade ETL pipeline for ICON Global / ICON-EPS numerical weather prediction data, served via a Cloudflare Workers edge API.

---

## Architecture Overview

```
DWD opendata (GRIB2/bz2)
        │
        ▼
GitHub Actions (cron × 4/day)
├── Matrix: 8 parameters × parallel jobs
│   └── etl.py: download → decompress → CDO regrid → Zarr → R2 staging
│
└── Post-matrix: validate → promote → janitor
        │
        ▼
Cloudflare R2  (icon-global-databank)
├── staging/{YYYYMMDDHH}/{param}/    ← in-progress
├── runs/{YYYYMMDDHH}/{param}/       ← validated & promoted
├── latest/_LATEST_RUN               ← pointer to latest run
└── spatial_index.json
        │
        ▼
Cloudflare Worker (icon-api-worker)
└── GET /?lat=&lon=&param=[&ensemble=true][&run=YYYYMMDDHH]
```

### Key Design Decisions

| Concern | Choice | Reason |
|---|---|---|
| Grid | Icosahedral → 0.125° lat/lon | ICON native grid is unstructured; regridding enables Zarr spatial chunking |
| Regridding tool | CDO `remapbil` | Battle-tested, handles ICON GRIB natively, bilinear interpolation |
| Storage format | Zarr + Blosc (zstd, BITSHUFFLE) | Cloud-optimised, byte-range reads, excellent compression for float32 grids |
| Chunking | `{lat: 100, lon: 100, step: -1}` | ~1.8 MB per spatial chunk; step unchunked for full time-series in one read |
| Idempotency | 3-phase: staging → validation → promotion | Guarantees `/latest/` is never stale; safe to re-run any job |
| Cleanup | Post-success Janitor only | Never deletes data during or after a failed run |
| Worker data access | R2 binding (byte-range) | No public bucket, no egress fees within Cloudflare network |
| Latest run caching | Worker in-memory + KV (5 min TTL) | Eliminates cold-path R2 reads for `_LATEST_RUN` |
| Response caching | Cloudflare Cache API (1 h TTL) | Cache key = `{run}/{param}/{chunkLat}/{chunkLon}` |

---

## Prerequisites

- **Cloudflare account** with R2 enabled
- **GitHub repository** with Actions enabled
- **CDO ≥ 2.0** (installed automatically in CI via `apt-get`)
- **Python ≥ 3.11**
- **Terraform ≥ 1.5** with Cloudflare provider
- **Node.js ≥ 18** (for Worker build via Wrangler)

---

## Setup

### 1. Terraform — provision Cloudflare infrastructure

```bash
cd terraform/
cp terraform.tfvars.example terraform.tfvars
# Fill in account_id, r2_access_key_id, r2_secret_access_key

terraform init
terraform plan
terraform apply
```

`terraform.tfvars` (create this file — never commit it):

```hcl
account_id           = "your-cloudflare-account-id"
r2_access_key_id     = "your-r2-access-key"
r2_secret_access_key = "your-r2-secret-key"
```

### 2. GitHub Secrets

Add the following secrets in your repository settings (`Settings → Secrets → Actions`):

| Secret | Description |
|---|---|
| `CF_ACCOUNT_ID` | Cloudflare account ID |
| `R2_ACCESS_KEY_ID` | R2 S3-compatible access key |
| `R2_SECRET_ACCESS_KEY` | R2 S3-compatible secret key |
| `ALERT_WEBHOOK_URL` | Slack / Discord / PagerDuty webhook |

### 3. Build and deploy the Worker

```bash
cd worker/
npm install
npm run build          # tsc → dist/index.js
npx wrangler deploy    # or: terraform apply (updates Worker script)
```

`wrangler.toml` (create alongside `index.ts`):

```toml
name = "icon-api-worker"
main = "dist/index.js"
compatibility_date = "2024-01-01"

[[r2_buckets]]
binding = "DATABANK"
bucket_name = "icon-global-databank"

[[kv_namespaces]]
binding = "METADATA"
id = "<kv-namespace-id-from-terraform-output>"
```

### 4. Run ETL manually

```bash
# Install dependencies
pip install xarray cfgrib zarr s3fs blosc numpy requests netCDF4
sudo apt-get install cdo

# Export credentials
export CF_ACCOUNT_ID=...
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export ALERT_WEBHOOK_URL=...

# Run ETL for a single parameter
PARAMETER=t_2m MODEL_RUN=00 python etl.py

# Run validation + promotion (after all 8 parameters are staged)
MODEL_RUN=00 python etl.py --validate-only \
  --params t_2m tot_prec u_10m v_10m pmsl clct relhum_2m aswdir_s

# Run ensemble (40 members — ~40× more data)
PARAMETER=t_2m MODEL_RUN=00 ENABLE_ENSEMBLE=true python etl.py
```

---

## ETL Pipeline — 3-Phase Workflow

```
Phase 1 — Staging
  For each forecast step (0–78h hourly + 81–180h every 3h = 97 steps):
    1. Download .grib2.bz2 from DWD opendata (streaming)
    2. Decompress in-memory (bz2 stdlib)
    3. Write to temp file → CDO remapbil → 0.125° regular NetCDF
    4. Load into xarray, append step dimension
  Concatenate all steps → to_zarr() with Blosc encoding → R2 staging
  Write _SUCCESS marker

Phase 2 — Validation
  ✓ All 97 forecast steps present
  ✓ Data arrays non-empty, no all-NaN chunks
  ✓ Values within physical plausibility bounds
  ✓ Ensemble: exactly 40 members
  ✓ Zarr .zmetadata readable
  ✗ Any failure → retry (max 3, exponential backoff) → alert webhook

Phase 3 — Promotion + Janitor
  Copy /staging/{run}/ → /runs/{run}/
  Write _SUCCESS in /runs/{run}/
  Update /latest/_LATEST_RUN
  Write spatial_index.json
  Janitor: delete /runs/ older than KEEP_RUNS=2, orphaned /staging/ dirs
```

---

## Storage Layout

```
icon-global-databank/
├── staging/
│   └── 2026040300/
│       ├── t_2m/
│       │   ├── .zarray
│       │   ├── .zmetadata
│       │   ├── t_2m/0.0.0  ← chunk: step=all, lat=0..99, lon=0..99
│       │   └── _SUCCESS
│       └── tot_prec/ ...
├── runs/
│   └── 2026040300/
│       └── t_2m/ ...       ← validated, promoted copy
├── latest/
│   └── _LATEST_RUN         ← contains "2026040300"
└── spatial_index.json
```

---

## Worker API

### Endpoint

```
GET https://icon-api-worker.<your-subdomain>.workers.dev/?lat=&lon=&param=
```

### Query Parameters

| Parameter | Required | Example | Notes |
|---|---|---|---|
| `lat` | Yes | `52.52` | -90 to 90 |
| `lon` | Yes | `13.405` | -180 to 360 |
| `param` | Yes | `t_2m` | whitelist enforced |
| `ensemble` | No | `true` | default `false` |
| `run` | No | `2026040300` | default: latest |

### Example responses

**Deterministic:**
```json
{
  "lat": 52.52,
  "lon": 13.405,
  "param": "t_2m",
  "run": "2026040300",
  "unit": "K",
  "values": {
    "0": 283.15,
    "1": 283.45,
    "2": 284.10
  }
}
```

**Ensemble:**
```json
{
  "lat": 52.52,
  "lon": 13.405,
  "param": "t_2m",
  "run": "2026040300",
  "unit": "K",
  "members": 40,
  "values": {
    "0": [283.1, 283.5, 282.9],
    "1": [283.4, 284.0, 283.2]
  }
}
```

### Rate limiting

100 requests / minute per IP. Exceeding returns `429 Too Many Requests` with a `Retry-After` header.

---

## Extending Parameters

Add to `PARAM_BOUNDS` in [etl.py](etl.py) and `ALLOWED_PARAMS` + `PARAM_UNITS` in [worker/index.ts](worker/index.ts):

```python
# etl.py
PARAM_BOUNDS["new_param"] = (min_val, max_val)
```

```typescript
// worker/index.ts
ALLOWED_PARAMS.add("new_param");
PARAM_UNITS["new_param"] = "unit";
```

Then add `new_param` to the matrix in [.github/workflows/main.yml](.github/workflows/main.yml).

---

## Operational Runbook

### ETL pipeline failure

1. Check the failed GitHub Actions run — the parameter job logs show exactly which step/URL failed.
2. If a single parameter failed, re-trigger the workflow with `workflow_dispatch` and the same `run_date` + `model_run`. The idempotent design will re-stage only what's missing.
3. If all parameters failed, check DWD opendata availability — releases sometimes run late.
4. `_SUCCESS` markers confirm which parameters are already staged; ETL skips re-upload on retry.

### Validation failure

Alert webhook fires with diagnostic details (which parameter, which check failed, value range).
- **All-NaN chunk**: DWD occasionally publishes empty GRIB files for certain steps — re-run after ~30 min.
- **Missing steps**: Partial DWD release — wait for full release (check `https://opendata.dwd.de/weather/nwp/icon/grib/`).

### Worker returning 503

Usually means `/latest/_LATEST_RUN` is missing — the ETL has never completed successfully. Run a manual ETL to seed the first run.

### Blosc decompression in Worker

The Worker's `decompressChunk` function includes a structural placeholder for Blosc/zstd decompression. For production, choose one:

**Option A (recommended):** Package `numcodecs-wasm` or a Blosc WASM build alongside the Worker.

**Option B:** Change the ETL compressor to gzip, which is natively supported by Workers' `DecompressionStream`:
```python
# etl.py — replace compressor
compressor = zarr.GZip(level=5)
```

---

## Cost Estimates (approximate, deterministic only)

| Item | Volume/day | Cost |
|---|---|---|
| R2 storage (2 runs retained) | ~32 GB | ~$0.048/month/GB |
| R2 Class A ops (ETL writes) | ~4M ops/day | $4.50/million |
| R2 Class B ops (Worker reads) | Depends on traffic | $0.36/million |
| Worker requests | Depends on traffic | $0.30/million |
| GitHub Actions minutes | ~8 × 4 × 60 min | Free tier or ~$0.008/min |

Ensemble processing multiplies storage/compute by ~40×.

---

## Checklist

- [x] Fully serverless — no persistent VMs or containers
- [x] Fault-tolerant — 3 retries, exponential backoff, no data loss on failure
- [x] Cost-efficient — Blosc/zstd compression, 2-run retention, chunk-level reads
- [x] Globally scalable — Cloudflare edge network
- [x] Production-ready — observability, alerting, security, idempotency
- [x] Correct grid handling — icosahedral → 0.125° lat/lon via CDO remapbil
