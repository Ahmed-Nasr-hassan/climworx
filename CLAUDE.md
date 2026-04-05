# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ClimWorx is a serverless weather data pipeline that fetches ICON Global / ICON-EPS GRIB2 NWP data from DWD, regrids it to a 0.125° lat/lon grid, stores it as cloud-optimized Zarr in Cloudflare R2, and serves it via a Cloudflare Workers edge API. A single-file weather dashboard (`index.html`) visualizes the data on an interactive map.

## Running the ETL Pipeline

```bash
# Install Python dependencies
pip install xarray cfgrib zarr s3fs numpy requests netCDF4 numcodecs dask

# System dependency (required for regridding)
apt-get install cdo  # CDO >= 2.0

# Run single parameter (staging phase)
PARAMETER=tot_prec MODEL_RUN=00 python etl.py

# Run validation + promotion (after all staged params are done)
MODEL_RUN=00 python etl.py --validate-only --params tot_prec t_2m

# Run with ensemble members (40 members, ~40× more storage)
PARAMETER=tot_prec MODEL_RUN=00 ENABLE_ENSEMBLE=true python etl.py
```

## Local Smoke Test

```bash
# Exercises full ETL flow (download → decompress → CDO remap → Zarr → validate)
# without pushing to R2. Builds CDO remap weights the same way CI does.
python3 test_local.py
```

The test builds CDO weight files in `/tmp/climworx_test_remap/` (cached between runs). If local CDO cannot process ICON unstructured grids (common on macOS), it falls back to DWD's precomputed EASY tarball (~50 MB).

## Building and Deploying the Worker

```bash
cd worker/
npm install
npm run build          # esbuild → dist/index.js (ESM)
```

Worker is deployed via Terraform, not Wrangler directly.

## Infrastructure

```bash
cd terraform/
terraform init
terraform apply        # Creates R2 bucket, KV namespace, and Worker script
```

Infrastructure managed resources: R2 bucket `icon-global-databank`, KV namespace, Worker script `icon-api-worker`.

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
| `KEEP_RUNS` | No | Runs to retain, default `1` |

## Architecture

### 3-Phase ETL Workflow

1. **Staging** — `resolve_available_run()` probes DWD for the requested run and falls back to older runs (6h steps, up to 4 cycles back) if not yet published. Downloads GRIB2.bz2 for all forecast steps via `try_stream_download()` (returns `None` on 404, tolerating partially-published runs). Step-0 of accumulated fields (e.g. `tot_prec`) is a ~200-byte stub with no grid metadata — `regrid_grib_bytes()` detects files < 10,000 bytes and returns an all-zeros `_make_zeros_dataset()` instead. Full-size GRIBs are regridded from icosahedral → 0.125° lat/lon via `cdo remap,{target_grid},{weights}` using pre-computed weight files in `assets/`. Uploads concatenated Zarr to `s3://icon-global-databank/staging/{YYYYMMDDHH}/{param}/` with a `_SUCCESS` marker.

2. **Validation** — Checks all expected steps present (warns on missing, doesn't fail), arrays non-empty with no all-NaN slices, values within `PARAM_BOUNDS`, and Zarr metadata readable. Uses dask-lazy `.compute()` to avoid loading entire arrays into RAM (~1.6 GB per parameter). Retries with exponential backoff (max 3 attempts).

3. **Promotion + Janitor** — Copies staging → `/runs/{YYYYMMDDHH}/`, updates `/latest/_LATEST_RUN`, writes `spatial_index.json` (reads actual steps from promoted Zarr, not assumed catalog), then deletes runs and orphaned staging dirs older than `KEEP_RUNS`.

### CDO Regridding

ICON GRIB2 files reference their native icosahedral grid only by UUID — CDO cannot interpolate without actual grid coordinates. The pipeline uses pre-computed CDO weight files:

- `assets/target_grid_world_0125.txt` — target regular grid spec (xsize=2879, ysize=1441)
- `assets/weights_distance.nc` — distance-weighted for instantaneous fields (t_2m, u_10m, v_10m, pmsl, relhum_2m)
- `assets/weights_conservative.nc` — conservative for accumulated/flux fields (tot_prec, aswdir_s, clct)

**In CI (GitHub Actions):** Assets are built fresh each run by downloading `icon_grid_0026_R03B07_G.nc.bz2` + a sample GRIB2 file, then running `cdo gendis` + `cdo genycon`. See `.github/workflows/main.yml` "Build CDO remap assets" step.

**Locally:** `test_local.py::ensure_ci_style_remap_weights()` mirrors CI logic, with fallback to DWD's precomputed `ICON_GLOBAL2WORLD_0125_EASY.tar.bz2`.

**In production (etl.py):** `ASSETS_DIR = Path(__file__).resolve().parent / "assets"`. `WEIGHTS_MAP` controls which weight file is used per parameter.

### CI/CD

GitHub Actions (`.github/workflows/main.yml`) runs 4×/day at 06:15, 12:15, 18:15, 00:15 UTC (~4h after each model init). Currently processing `tot_prec` and `t_2m`. Promotion only runs if all matrix ETL jobs succeed and validation passes (fails hard if >15% of expected steps are missing — catches partial DWD uploads).

### R2 Storage Layout

```
icon-global-databank/
├── staging/{YYYYMMDDHH}/{param}/   # Work-in-progress (Zarr + _SUCCESS)
├── runs/{YYYYMMDDHH}/{param}/      # Validated, promoted
├── latest/_LATEST_RUN              # Points to most recent YYYYMMDDHH
└── spatial_index.json              # Grid + steps metadata for Worker
```

### Zarr Store Structure

Each parameter is a Zarr v2 group. The data variable is nested inside: `runs/{run}/{param}/{param}/.zarray`. Dimension shapes differ by parameter:

- `t_2m`: shape `[steps, 1, 1441, 2879]` — 4D with singleton `height` dimension, chunk key format `0.0.{ci}.{cj}`
- `tot_prec`: shape `[steps, 1441, 2879]` — 3D (no height), chunk key format `0.{ci}.{cj}`

Zarr v2, dimension separator `.`, dtype `<f4` (little-endian float32), C-order, GZip level 5 compression. The `_SUCCESS` marker is per-parameter at `runs/{run}/{param}/_SUCCESS`.

### Worker API (`worker/index.ts`)

The Worker exposes two routes:

**Per-pixel JSON API:** `GET /?lat=&lon=&param=[&ensemble=true][&run=YYYYMMDDHH]`

Request flow: rate limit (100 req/min per IP via KV) → validate params → resolve run ID (KV cache, 5-min TTL) → `latLonToChunkIndex()` → Cache API (1h TTL, per-pixel key) → fetch chunk from R2 → decompress GZip via native `DecompressionStream` → `extractTimeSeries()` using strides derived from `.zarray` chunk shape (handles any number of dimensions) → return JSON.

**Zarr proxy (for dashboard):** `GET /zarr/{param}/{...path}` — proxies R2 `runs/{latestRun}/{param}/{path}` with CORS headers (`Access-Control-Allow-Origin: *`). `GET /zarr/spatial_index.json` serves run metadata from bucket root. `OPTIONS` returns CORS preflight.

**Chunk decompression:** The Worker uses the native `DecompressionStream("gzip")` API — no WASM or external dependencies needed. ETL compresses with `numcodecs.GZip(level=5)`.

### Weather Dashboard (`index.html`)

Single-file interactive dashboard using MapLibre GL JS, Tailwind CSS, D3 color scales, and Pako (GZip decompression). Reads Zarr chunks directly via the `/zarr/*` proxy — no zarr.js library dependency.

- **Basemap toggle** — Imagery (default) ↔ Dark Carto (`switchBasemap()`). Dark style is prefetched/preloaded on page load to eliminate render delay. `MapboxDraw` is recreated and re-attached after each style switch via a `styledata` listener + poll guard. A sequence counter (`basemapSwitchSeq`) prevents stale callbacks from racing.
- Glassmorphism UI panels; responsive mobile layout (play/slider/speed on top row, step label below on mobile, inline on desktop)
- Draw a bounding box → fetches Zarr chunks in parallel (batches of 6) via `/zarr/*` proxy
- Handles both 4D (`t_2m`) and 3D (`tot_prec`) dimension layouts automatically
- Animated playback with non-uniform step awareness (hourly 0–78h, 3-hourly 81–120h)
- Color scales: `d3.interpolateTurbo` (temperature K→°C), `d3.interpolateYlGnBu` (precipitation, transparent at zero)
- Hover tooltip with lat/lon, value, and sparkline time series for all 93 steps
- 50,000-pixel area guard with toast notifications

### Data Constants

- **Grid:** 2879×1441 (0.125° resolution, north→south, west→east; `-180.0` to `179.875` longitude)
- **CDO target grid:** xfirst=-180.0, xinc=0.125, xsize=2879; yfirst=90.0, yinc=-0.125, ysize=1441 (N→S)
- **Forecast steps:** 97 total for 00Z/12Z (+0…+78h hourly, +81…+180h every 3h); 57 steps for 06Z/18Z (max +120h)
- **Active parameters (CI):** `tot_prec`, `t_2m` (others: `u_10m`, `v_10m`, `pmsl`, `clct`, `relhum_2m`, `aswdir_s`)
- **Chunk size:** 100×100 lat/lon pixels, full step dimension
- **Compression:** GZip level 5 (`from numcodecs import GZip`) — native `DecompressionStream` in Worker, `pako.ungzip` in dashboard
