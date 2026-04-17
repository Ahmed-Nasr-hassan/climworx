# Serverless Global Weather Data Bank

Fully automated, production-grade ETL pipeline for ICON Global / ICON-EPS numerical weather prediction data, served via a Cloudflare Workers edge API.

---

## Architecture Overview

```
DWD opendata (GRIB2/bz2)
        │
        ▼
GitHub Actions (cron × 4/day)
├── Matrix: one job per parameter (16 layers + optional ensemble)
│   └── etl.py: download-first → CDO remap → Zarr → R2 runs/{run}/{param}/
│
├── Promote: smoke-validate runs/… → runs/{run}/_PROMOTED (no byte copy)
├── derive_pet.py → runs/{run}/pet_pm/
└── Finalize: spatial_index + /latest/_LATEST_RUN + janitor
        │
        ▼
Cloudflare R2  (icon-global-databank)
├── staging/{YYYYMMDDHH}/            ← _RESOLVED_RUN redirect when CI run ≠ DWD run; legacy dirs optional
├── runs/{YYYYMMDDHH}/{param}/       ← canonical Zarr + _SUCCESS; runs/{run}/_PROMOTED after validate
├── latest/_LATEST_RUN               ← set in finalize only
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
| Regridding tool | CDO `remap` + precomputed weights (`gendis` / `genycon`) | ICON needs grid + weights; distance vs conservative per field type |
| Storage format | Zarr v2 + GZip(level=5) | Cloud-optimised chunks; Worker and dashboard decompress with gzip APIs |
| Chunking | `{latitude: 100, longitude: 100, step: -1}` | Spatial chunks; step unchunked for full time-series in one read |
| Idempotency | Matrix → validate + `_PROMOTED` → PET → finalize | `/latest` flips only after finalize; `_PROMOTED` gates finalize after smoke validation |
| Cleanup | Janitor in finalize (successful pipeline) | Failed matrix rows may leave partial `runs/` data until janitor; markers gate what users see |
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
pip install xarray cfgrib zarr s3fs numpy requests netCDF4 numcodecs dask
sudo apt-get install cdo

# Export credentials
export CF_ACCOUNT_ID=...
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export ALERT_WEBHOOK_URL=...

# Run ETL for a single parameter
PARAMETER=t_2m MODEL_RUN=00 python etl.py

# Smoke-validate runs/…, write runs/{run}/_PROMOTED (after matrix uploaded those params)
MODEL_RUN=00 python etl.py --validate-only \
  --params t_2m tot_prec u_10m v_10m pmsl clct relhum_2m aswdir_s

# After derive_pet.py: finalize (requires runs/{run}/_PROMOTED from validate step)
MODEL_RUN=00 python etl.py --finalize --params t_2m tot_prec u_10m v_10m pet_pm

# Run ensemble (40 members — ~40× more data)
PARAMETER=t_2m MODEL_RUN=00 ENABLE_ENSEMBLE=true python etl.py
```

---

## ETL Pipeline (summary)

```
Phase 1 — Matrix (etl.py per parameter)
  Download-first for all steps → CDO remap to 0.125° → concat → Zarr + GZip
  Upload to runs/{YYYYMMDDHH}/{param}/ + _SUCCESS
  If DWD run differs from CI clock: staging/{CI}/_RESOLVED_RUN → actual run id

Phase 2 — Promote (GitHub discover + etl.py --validate-only)
  Workflow lists params that have runs/…/_SUCCESS (honors _RESOLVED_RUN)
  etl.py smoke-reads Zarr (step-0 corner tile, primary field for normal stores)
  Write runs/{run}/_PROMOTED; best-effort delete legacy staging/{…}/{param}/ dirs
  ✗ Failure → retry (max 3) → alert; does not flip /latest

Phase 3 — derive_pet.py
  Parallel open of seven source Zarrs → PET → runs/{run}/pet_pm/_SUCCESS

Phase 4 — Finalize (etl.py --finalize)
  Require _PROMOTED + all param _SUCCESS (incl. pet_pm)
  Rebuild spatial_index.json, set latest/_LATEST_RUN, janitor (KEEP_RUNS)
```

---

## Storage Layout

```
icon-global-databank/
├── staging/
│   └── 2026040300/
│       └── _RESOLVED_RUN   ← optional: when matrix wrote a different model init under runs/
├── runs/
│   └── 2026040300/
│       ├── _PROMOTED       ← JSON marker after successful promote (validate) job
│       ├── t_2m/           ← Zarr group + _SUCCESS (written by matrix, not copied)
│       │   └── t_2m/.zarray …
│       ├── tot_prec/ …
│       └── pet_pm/ …       ← from derive_pet.py
├── latest/
│   └── _LATEST_RUN         ← contains "2026040300" (finalize only)
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
2. If a single parameter failed, re-trigger the workflow with `workflow_dispatch` and the same `run_date` + `model_run`. The idempotent design will re-upload only missing params under `runs/`.
3. If all parameters failed, check DWD opendata availability — releases sometimes run late.
4. `_SUCCESS` under `runs/{run}/{param}/` confirms which parameters finished; partial runs still need a successful promote (`_PROMOTED`) before finalize.

### Validation failure

Alert webhook fires with diagnostic details (which parameter, which check failed, value range).
- **All-NaN chunk**: DWD occasionally publishes empty GRIB files for certain steps — re-run after ~30 min.
- **Missing steps**: Partial DWD release — wait for full release (check `https://opendata.dwd.de/weather/nwp/icon/grib/`).

### Worker returning 503

Usually means `/latest/_LATEST_RUN` is missing — the ETL has never completed successfully. Run a manual ETL to seed the first run.

### Manual finalize

`etl.py --finalize` requires `runs/{run}/_PROMOTED` from a successful `etl.py --validate-only` on that same `run_dt` (CI promote job writes it). Run validate before finalize, or the finalize step will refuse.

### Compression

ETL uses **GZip level 5** (`numcodecs.GZip`); the Worker decompresses chunks with `DecompressionStream("gzip")`.

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
- [x] Cost-efficient — GZip chunk compression, configurable run retention, chunk-level reads
- [x] Globally scalable — Cloudflare edge network
- [x] Production-ready — observability, alerting, security, idempotency
- [x] Correct grid handling — icosahedral → 0.125° lat/lon via CDO remap + precomputed weights
