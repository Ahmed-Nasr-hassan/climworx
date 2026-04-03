# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ClimWorx is a serverless weather data pipeline that fetches ICON Global / ICON-EPS GRIB2 NWP data from DWD, regrids it to a 0.125° lat/lon grid, stores it as cloud-optimized Zarr in Cloudflare R2, and serves it via a Cloudflare Workers edge API.

## Running the ETL Pipeline

```bash
# Install Python dependencies
pip install xarray cfgrib zarr s3fs numpy requests netCDF4 numcodecs

# System dependency (required for regridding)
apt-get install cdo  # CDO >= 2.0

# Run single parameter (staging phase)
PARAMETER=t_2m MODEL_RUN=00 python etl.py

# Run validation + promotion (after all 8 params staged)
MODEL_RUN=00 python etl.py --validate-only --params t_2m tot_prec u_10m v_10m pmsl clct relhum_2m aswdir_s

# Run with ensemble members (40 members, ~40× more storage)
PARAMETER=t_2m MODEL_RUN=00 ENABLE_ENSEMBLE=true python etl.py
```

## Building and Deploying the Worker

```bash
cd worker/
npm install
npm run build          # tsc → dist/index.js
npx wrangler deploy    # Deploy to Cloudflare edge
```

## Infrastructure

```bash
cd terraform/
terraform init
terraform apply        # Creates R2 bucket, KV namespace, and Worker script
```

The `wrangler.toml` (not committed) must bind R2 as `DATABANK` and KV as `METADATA`. The KV namespace ID comes from `terraform output kv_namespace_id`.

## Required Environment Variables

| Variable | Required | Notes |
|---|---|---|
| `CF_ACCOUNT_ID` | Yes | Cloudflare account ID |
| `R2_ACCESS_KEY_ID` | Yes | S3-compatible key for R2 |
| `R2_SECRET_ACCESS_KEY` | Yes | S3-compatible secret for R2 |
| `ALERT_WEBHOOK_URL` | Recommended | Slack/Discord/PagerDuty webhook |
| `PARAMETER` | No | Defaults to `t_2m` |
| `MODEL_RUN` | No | `00`, `06`, `12`, or `18` |
| `RUN_DATE` | No | `YYYYMMDD`, defaults to today UTC |
| `ENABLE_ENSEMBLE` | No | `true` to process 40 ensemble members |
| `KEEP_RUNS` | No | Runs to retain, default `2` |

## Architecture

### 3-Phase ETL Workflow

1. **Staging** — Downloads GRIB2.bz2 from DWD for all 97 forecast steps, decompresses in-memory, regrids icosahedral → 0.125° lat/lon via `cdo remapbil`, concatenates, compresses with Blosc/zstd, and uploads to `s3://icon-global-databank/staging/{YYYYMMDDHH}/{param}/`. Writes a `_SUCCESS` marker for idempotency.

2. **Validation** — Checks all 97 steps present, arrays non-empty with no all-NaN chunks, values within physical bounds (`PARAM_BOUNDS` dict in `etl.py`), and Zarr metadata readable. Retries with exponential backoff (max 3 attempts).

3. **Promotion + Janitor** — Copies staging → `/runs/{YYYYMMDDHH}/`, updates `/latest/_LATEST_RUN`, writes `spatial_index.json` for the Worker, then deletes runs older than `KEEP_RUNS`.

### CI/CD

GitHub Actions (`.github/workflows/main.yml`) runs 4×/day at 02:15, 08:15, 14:15, 20:15 UTC (aligned with DWD release times). The `etl` job runs as a matrix across all 8 parameters in parallel. Promotion only runs if all 8 matrix jobs succeed.

### R2 Storage Layout

```
icon-global-databank/
├── staging/{YYYYMMDDHH}/{param}/   # Work-in-progress
├── runs/{YYYYMMDDHH}/{param}/      # Validated, promoted
├── latest/_LATEST_RUN              # Points to most recent YYYYMMDDHH
└── spatial_index.json              # Grid metadata for Worker
```

### Worker API (`worker/index.ts`)

`GET /?lat=&lon=&param=[&ensemble=true][&run=YYYYMMDDHH]`

Request flow: rate limit check (100 req/min per IP via KV) → validate params → resolve run ID → convert lat/lon to chunk index via `latLonToChunkIndex()` → check Cloudflare Cache API (1h TTL) → byte-range read from R2 → decompress Blosc chunk → extract time series → return JSON.

**Blosc decompression:** The Worker requires a WASM Blosc decoder (e.g. `numcodecs-wasm` or `blosc-wasm`). Register it at startup via `registerBloscDecoder()` exported from `index.ts`. Without it, chunk fetches fail with a descriptive error.

### Data Constants

- **Grid:** 2880×1441 (0.125° resolution, north→south, west→east; xfirst=-180.0, yfirst=-90.0 in CDO, flipped to N→S in Zarr)
- **Forecast steps:** 97 total (+0…+78h hourly, +81…+180h every 3h)
- **Parameters:** `t_2m`, `tot_prec`, `u_10m`, `v_10m`, `pmsl`, `clct`, `relhum_2m`, `aswdir_s`
- **Chunk size:** 100×100 lat/lon pixels, full step dimension (≈1.8 MB per chunk)
- **Compression:** Blosc + zstd + BITSHUFFLE, level 3
