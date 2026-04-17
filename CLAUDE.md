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

# Validation + promotion (copy staging→runs, cleanup staging). Does NOT flip /latest.
MODEL_RUN=00 python etl.py --validate-only --params tot_prec t_2m

# Derive PET into runs/{run}/pet_pm/ (requires t_2m, td_2m, u_10m, v_10m, asob_s, athb_s, ps already promoted)
python derive_pet.py --run-dt 2026041500

# Finalize: flip /latest, rebuild spatial_index, run janitor. Run AFTER derive_pet.py.
MODEL_RUN=00 python etl.py --finalize --params tot_prec t_2m pet_pm

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

### 5-Phase ETL Workflow

Each phase is a separate GitHub Actions job (`probe` → `etl` matrix → `promote` → `derive-pet` → `finalize`). The `/latest/_LATEST_RUN` pointer only flips at the very end, so the dashboard never sees a run with a partial set of layers — e.g. a run missing `pet_pm`.

1. **DWD Availability Probe** (`probe` job, `probe_dwd.py`) — Fast HEAD-request scan of all 16 params × all forecast steps (~1,500 URLs) with 50 concurrent workers (~30-60s). Outputs a `manifest.json` listing which steps exist on DWD for each parameter *before* any heavy downloading begins. Runs in parallel with `build-assets`. Matrix jobs use the manifest to skip known-missing steps without hitting DWD, and the pipeline gets an early-exit opportunity if too many steps are missing (>15% threshold).

2. **Staging** (`etl` matrix, one job per param) — `resolve_available_run()` probes DWD for the requested run and falls back to older runs (6h steps, up to 4 cycles back) if not yet published. Uses a **download-first** pattern: `download_all_steps()` fetches all GRIB2.bz2 files in parallel (8 workers) before any regridding begins, so mid-pipeline DWD file rotation cannot cause failures. When a probe manifest is available (`--manifest`), only known-available steps are downloaded. Step-0 of accumulated fields (e.g. `tot_prec`) is a ~200-byte stub with no grid metadata — `regrid_grib_bytes()` detects files < 10,000 bytes and returns an all-zeros `_make_zeros_dataset()` instead. Full-size GRIBs are regridded from icosahedral → 0.125° lat/lon via `cdo remap,{target_grid},{weights}` using pre-computed weight files in `assets/`. Uploads concatenated Zarr to `s3://icon-global-databank/staging/{YYYYMMDDHH}/{param}/` with a `_SUCCESS` marker. When DWD fallback kicks in, an `_RESOLVED_RUN` marker is written under the *original* CI run_dt so downstream jobs can discover which run was actually staged.

3. **Validation + Promotion** (`promote` job, `etl.py --validate-only`) — Validation checks all expected steps present (warns on missing, doesn't fail), arrays non-empty with no all-NaN slices, values within `PARAM_BOUNDS`, and Zarr metadata readable. Uses dask-lazy `.compute()` to avoid loading entire arrays into RAM (~1.6 GB per parameter). Retries with exponential backoff (max 3 attempts). After validation, `promote_run()` copies `staging/{run}/{param}/` → `runs/{run}/{param}/`, writes `_SUCCESS` under the promoted path, and deletes the staging copy per-param (kills the duplicate-storage window). Partial per-param cleanup failures warn-and-continue; only if *all* params fail cleanup does the job fail (systemic auth/R2 issue). The `_RESOLVED_RUN` marker is intentionally left in staging for the later finalize job. Does NOT flip `/latest` or run janitor.

4. **Derive PET** (`derive-pet` job, `derive_pet.py`) — Reads seven promoted params (`t_2m`, `td_2m`, `u_10m`, `v_10m`, `asob_s`, `athb_s`, `ps`) from `runs/{run}/`, computes FAO-56 Penman-Monteith PET (deaccumulating `asob_s`/`athb_s` to per-step fluxes first), and writes `runs/{run}/pet_pm/` with its own `_SUCCESS` marker. Honors `_RESOLVED_RUN` when `--run-dt` is passed. Does NOT touch `spatial_index.json` (finalize rebuilds it with `pet_pm` included).

5. **Finalize + Janitor** (`finalize` job, `etl.py --finalize`) — Verifies every advertised param has `runs/{run}/{param}/_SUCCESS` (hard guard against out-of-band invocations), rebuilds `spatial_index.json` with `pet_pm` in the params list (reads actual steps from promoted Zarr), flips `/latest/_LATEST_RUN` to the new run, then runs janitor to delete runs and orphaned staging dirs older than `KEEP_RUNS`. `--validate-only` and `--finalize` are mutually exclusive in argparse.

### CDO Regridding

ICON GRIB2 files reference their native icosahedral grid only by UUID — CDO cannot interpolate without actual grid coordinates. The pipeline uses pre-computed CDO weight files:

- `assets/target_grid_world_0125.txt` — target regular grid spec (xsize=2879, ysize=1441)
- `assets/weights_distance.nc` — distance-weighted for instantaneous fields (t_2m, u_10m, v_10m, pmsl, relhum_2m)
- `assets/weights_conservative.nc` — conservative for accumulated/flux fields (tot_prec, aswdir_s, clct)

**In CI (GitHub Actions):** Assets are built fresh each run by downloading `icon_grid_0026_R03B07_G.nc.bz2` + a sample GRIB2 file, then running `cdo gendis` + `cdo genycon`. See `.github/workflows/main.yml` "Build CDO remap assets" step.

**Locally:** `test_local.py::ensure_ci_style_remap_weights()` mirrors CI logic, with fallback to DWD's precomputed `ICON_GLOBAL2WORLD_0125_EASY.tar.bz2`.

**In production (etl.py):** `ASSETS_DIR = Path(__file__).resolve().parent / "assets"`. `WEIGHTS_MAP` controls which weight file is used per parameter.

### CI/CD

GitHub Actions (`.github/workflows/main.yml`) runs 4×/day at 07:15, 13:15, 19:15, 01:15 UTC (~5h after each model init). Matrix processes 16 params (see `.github/workflows/main.yml` `matrix.parameter`). Job dependency: `resolve-run` → [`build-assets` ∥ `probe`] → `etl` (matrix) → `promote` → `derive-pet` → `finalize`. The `probe` and `build-assets` jobs run in parallel since both only need `resolve-run`. Promote discovers successfully-staged params from R2 (_SUCCESS markers), so a single param failure doesn't block the rest. `derive-pet` and `finalize` run only if their predecessors succeeded — a PET failure keeps the dashboard pinned to the previous run instead of shipping a run without the PET layer.

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
- **Active parameters (CI):** `t_2m`, `tot_prec`, `u_10m`, `v_10m`, `pmsl`, `clct`, `relhum_2m`, `aswdir_s`, `td_2m`, `vmax_10m`, `asob_s`, `athb_s`, `alhfl_s`, `ps`, `runoff_s`, `runoff_g`, plus derived `pet_pm` (FAO-56 Penman-Monteith, computed by `derive_pet.py` from `t_2m`, `td_2m`, `u_10m`, `v_10m`, `asob_s`, `athb_s`, `ps`)
- **Chunk size:** 100×100 lat/lon pixels, full step dimension
- **Compression:** GZip level 5 (`from numcodecs import GZip`) — native `DecompressionStream` in Worker, `pako.ungzip` in dashboard
