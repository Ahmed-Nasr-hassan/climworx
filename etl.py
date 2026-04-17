#!/usr/bin/env python3
"""
ICON Global Weather ETL Pipeline
=================================
Fetches ICON Global / ICON-EPS GRIB2 data from DWD opendata,
regrids from the native icosahedral grid to a 0.125° regular lat/lon grid,
converts to Cloud-Optimized Zarr, and uploads to Cloudflare R2.

Workflow: matrix-write → validate → derive → finalize
  Each matrix job writes directly to runs/{run_dt}/{param}/ and writes a
  per-param _SUCCESS marker. The dashboard only sees the run after finalize
  flips /latest/_LATEST_RUN. Janitor sweeps old runs post-success.

Usage (environment variables):
  PARAMETER         = tot_prec
  ENABLE_ENSEMBLE   = false
  MODEL_RUN         = 00 | 06 | 12 | 18
  RUN_DATE          = 20260403          (optional, default: today UTC)
  R2_ACCESS_KEY_ID  = ...
  R2_SECRET_ACCESS_KEY = ...
  CF_ACCOUNT_ID     = ...
  ALERT_WEBHOOK_URL = https://...       (Slack/Discord/PagerDuty)
  KEEP_RUNS         = 2                 (Janitor: how many runs to retain)
"""

from __future__ import annotations

import argparse
import bz2
import io
import json
import logging
import os
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import requests
import s3fs
import xarray as xr
import zarr
from numcodecs import GZip

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("icon-etl")

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

DWD_BASE = "https://opendata.dwd.de/weather/nwp"
GRID_NX = 2879   # DWD official: -180 to 179.75 at 0.125° (avoids wrap-around duplicate)
GRID_NY = 1441   # 180 / 0.125 + 1  (includes both poles)
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0
CHUNK_LAT = 100
CHUNK_LON = 100
ENSEMBLE_MEMBERS = 40
KEEP_RUNS_DEFAULT = 1
MAX_RETRIES = 3
# Promotion is blocked if more than this fraction of expected steps are missing.
# Catches partial DWD uploads: e.g. 34/93 steps (63% missing) fails immediately.
MAX_MISSING_STEP_FRACTION = 0.15
RETRY_BASE_S = 30
RETRY_MAX_S = 300
BUCKET = "icon-global-databank"
# Written by ``--validate-only`` after smoke validation (matrix data already lives under runs/).
PROMOTED_RELATIVE = "_PROMOTED"

# Physical plausibility bounds per parameter (min, max)
PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "t_2m":      (180.0, 340.0),        # K
    "tot_prec":  (0.0, 4000.0),         # mm (global multi-day accumulation)
    "u_10m":     (-150.0, 150.0),       # m/s
    "v_10m":     (-150.0, 150.0),       # m/s
    "pmsl":      (85000.0, 108000.0),   # Pa
    "clct":      (0.0, 100.0),          # %
    "relhum_2m": (0.0, 110.0),          # % (allow slight super-saturation)
    "aswdir_s":  (0.0, 1500.0),         # W/m²
    "td_2m":     (180.0, 320.0),        # K (dew point, always ≤ t_2m)
    "vmax_10m":  (0.0, 120.0),          # m/s (wind gust)
    "asob_s":    (-200.0, 1400.0),      # W/m² (net shortwave; negative at night)
    "athb_s":    (-500.0, 300.0),       # W/m² (net longwave at surface; typically negative)
    "alhfl_s":   (-800.0, 800.0),       # W/m² (latent heat flux; strong evaporation can be < -600)
    "ps":        (40000.0, 110000.0),   # Pa (surface pressure; ICON terrain peaks ~7 km → ~40 kPa)
    "runoff_s":  (0.0, 500.0),          # kg/m² (surface runoff, accumulated)
    "runoff_g":  (0.0, 500.0),          # kg/m² (subsurface/groundwater runoff, accumulated)
}


def get_all_steps(model_run: str) -> list[int]:
    """
    Forecast step indices for ICON Global deterministic GRIB2 on DWD.

    00Z and 12Z runs extend to +180 h; 06Z and 18Z only to +120 h.
    Steps: +0..+78 hourly, then +81..+max every 3 h.
    """
    max_step = 120 if model_run in ("06", "18") else 180
    steps_short = list(range(0, 79))
    steps_extended = list(range(81, max_step + 1, 3))
    return steps_short + steps_extended


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        raise RuntimeError(f"Required environment variable '{key}' is not set")
    return val


def make_r2_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=env("R2_ACCESS_KEY_ID"),
        secret=env("R2_SECRET_ACCESS_KEY"),
        endpoint_url=f"https://{env('CF_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        client_kwargs={"region_name": "auto"},
        # Harden against transient R2 slow responses (notably bulk delete calls
        # issued during overwrite uploads).
        config_kwargs={
            "connect_timeout": 30,
            "read_timeout": 300,
            "retries": {"max_attempts": 10, "mode": "adaptive"},
        },
    )


def send_alert(message: str) -> None:
    webhook = os.environ.get("ALERT_WEBHOOK_URL")
    if not webhook:
        log.warning("ALERT_WEBHOOK_URL not set — skipping alert")
        return
    try:
        requests.post(webhook, json={"text": message}, timeout=10)
        log.info("Alert sent: %s", message)
    except Exception as exc:
        log.error("Failed to send alert: %s", exc)


def cleanup_failed_run_data(run_dt: str, params: list[str], fs: s3fs.S3FileSystem) -> None:
    """
    Best-effort cleanup for failed runs.
    Removes staged and promoted data for affected parameters so partial uploads
    do not linger after an ETL/validation/promotion failure.
    """
    log.warning("Cleaning up failed run data for run=%s", run_dt)
    for prefix in ("staging", "runs"):
        for param in params:
            path = f"{BUCKET}/{prefix}/{run_dt}/{param}"
            try:
                if fs.exists(path):
                    fs.rm(path, recursive=True)
                    log.warning("Removed failed data: s3://%s", path)
            except Exception as exc:
                log.error("Cleanup failed for s3://%s: %s", path, exc)

    # Remove run-level resolver marker if present for this run key.
    resolved_marker = f"{BUCKET}/staging/{run_dt}/_RESOLVED_RUN"
    try:
        if fs.exists(resolved_marker):
            fs.rm(resolved_marker)
            log.warning("Removed failed run marker: s3://%s", resolved_marker)
    except Exception as exc:
        log.error("Cleanup failed for marker s3://%s: %s", resolved_marker, exc)


def with_retry(fn, *args, label: str = "", **kwargs):
    """Call fn(*args, **kwargs) with up to MAX_RETRIES, exponential backoff.

    If the exception is a ``RateLimitError`` with a ``retry_after`` value,
    that value is used as the wait time instead of the default backoff.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            wait = min(RETRY_BASE_S * (2 ** (attempt - 1)), RETRY_MAX_S)
            # Respect Retry-After from DWD 429 responses
            if isinstance(exc, RateLimitError) and exc.retry_after is not None:
                wait = min(exc.retry_after, RETRY_MAX_S)
            if attempt == MAX_RETRIES:
                log.error("[%s] All %d attempts failed: %s", label, MAX_RETRIES, exc)
                raise
            log.warning(
                "[%s] Attempt %d/%d failed (%s). Retrying in %ds…",
                label, attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Run availability check & fallback
# ---------------------------------------------------------------------------

MODEL_RUNS = ["00", "06", "12", "18"]


def resolve_available_run(run_date: str, model_run: str, param: str, model: str) -> str:
    """
    Probe DWD for the requested run. If it 404s, fall back to earlier runs
    (up to 4 steps back, crossing into the previous day if needed).
    Returns the first available run_dt (YYYYMMDDHH).
    """
    # Build candidate runs: current, then progressively older
    candidates: list[str] = []
    dt = datetime.strptime(f"{run_date}{model_run}", "%Y%m%d%H")
    for _ in range(len(MODEL_RUNS)):
        candidates.append(dt.strftime("%Y%m%d%H"))
        # Step back 6 hours
        dt = dt - timedelta(hours=6)

    # Probe t_2m step-0 (always published for every ICON run) rather than the
    # target param — MAX/interval fields like vmax_10m have no step-0 file,
    # which would cause all candidates to 404 even when the run exists.
    for run_dt in candidates:
        hh = run_dt[8:10]
        fname_prefix = "icon_global" if model == "icon" else model.replace("-", "_")
        url = (
            f"{DWD_BASE}/{model}/grib/{hh}/t_2m/"
            f"{fname_prefix}_icosahedral_single-level"
            f"_{run_dt}_000_T_2M.grib2.bz2"
        )
        try:
            resp = requests.head(url, timeout=15)
            if resp.status_code == 200:
                if run_dt != candidates[0]:
                    log.warning(
                        "Run %s not yet available on DWD, falling back to %s",
                        candidates[0], run_dt,
                    )
                return run_dt
            log.debug("Run %s not available (HTTP %d), trying older…", run_dt, resp.status_code)
        except requests.RequestException as exc:
            log.debug("Run %s probe failed (%s), trying older…", run_dt, exc)

    raise RuntimeError(
        f"No available run found on DWD for {param} "
        f"(checked {candidates[0]} back to {candidates[-1]})"
    )


# ---------------------------------------------------------------------------
# DWD URL builders
# ---------------------------------------------------------------------------


def build_urls(model: str, run_dt: str, param: str, step: int) -> list[str]:
    """
    Return a list of URLs for a given parameter/step.
    For deterministic: one URL.
    For ensemble (model='icon-eps'): ENSEMBLE_MEMBERS URLs.
    """
    hh = run_dt[8:10]  # "00", "06", "12", "18"
    # DWD URL path uses "icon" or "icon-eps"
    base = f"{DWD_BASE}/{model}/grib/{hh}/{param}/"
    # DWD filenames use "icon_global" / "icon-eps" prefix
    fname_prefix = "icon_global" if model == "icon" else model.replace("-", "_")
    fname_template = (
        f"{fname_prefix}_icosahedral_single-level"
        f"_{run_dt}_{step:03d}_{param.upper()}.grib2.bz2"
    )
    if model == "icon-eps":
        urls = []
        for member in range(1, ENSEMBLE_MEMBERS + 1):
            fname = (
                f"{fname_prefix}_icosahedral_single-level"
                f"_{run_dt}_{step:03d}_{member:03d}_{param.upper()}.grib2.bz2"
            )
            urls.append(base + fname)
        return urls
    return [base + fname_template]


# ---------------------------------------------------------------------------
# Download & decompress
# ---------------------------------------------------------------------------


class RateLimitError(IOError):
    """DWD returned 429 — caller should back off and retry."""

    def __init__(self, url: str, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(
            f"DWD rate-limited (429) on {url}"
            + (f" — Retry-After: {retry_after}s" if retry_after else "")
        )


def _parse_retry_after_header(resp: requests.Response) -> float | None:
    val = resp.headers.get("Retry-After")
    if val is None:
        return None
    try:
        return max(float(val), 1.0)
    except (ValueError, TypeError):
        return None


def stream_download(url: str) -> bytes:
    """Stream-download a .bz2 file and return decompressed bytes.

    Raises ``RateLimitError`` on HTTP 429 so that ``with_retry`` honours
    the backoff instead of treating it as an opaque failure.
    """
    log.debug("Downloading %s", url)
    with requests.get(url, stream=True, timeout=120) as resp:
        if resp.status_code == 429:
            raise RateLimitError(url, _parse_retry_after_header(resp))
        resp.raise_for_status()
        compressed = b"".join(resp.iter_content(chunk_size=1 << 20))
    return bz2.decompress(compressed)


def try_stream_download(url: str) -> bytes | None:
    """
    Like stream_download, but return None on HTTP 404 (file not on DWD).
    Raises ``RateLimitError`` on 429, and other HTTP/network errors for use
    with ``with_retry``.
    """
    log.debug("Downloading %s", url)
    with requests.get(url, stream=True, timeout=120) as resp:
        if resp.status_code == 404:
            log.warning("DWD 404 — skipping step file: %s", url)
            return None
        if resp.status_code == 429:
            raise RateLimitError(url, _parse_retry_after_header(resp))
        resp.raise_for_status()
        compressed = b"".join(resp.iter_content(chunk_size=1 << 20))
    return bz2.decompress(compressed)


# ---------------------------------------------------------------------------
# Regridding: icosahedral → 0.125° regular lat/lon via CDO remapbil
# ---------------------------------------------------------------------------


ASSETS_DIR = Path(__file__).resolve().parent / "assets"
# Production: parameter-specific CDO weights (conservative for flux/accum fields).
WEIGHTS_MAP: dict[str, str] = {
    # Accumulated / flux fields → conservative remapping
    "tot_prec": "weights_conservative.nc",
    "aswdir_s": "weights_conservative.nc",
    "asob_s":   "weights_conservative.nc",
    "athb_s":   "weights_conservative.nc",
    "alhfl_s":  "weights_conservative.nc",
    "runoff_s": "weights_conservative.nc",
    "runoff_g": "weights_conservative.nc",
    "clct":     "weights_conservative.nc",
    # Instantaneous fields → distance-weighted (default):
    # t_2m, td_2m, u_10m, v_10m, vmax_10m, pmsl, relhum_2m
}


def _make_zeros_dataset(param: str) -> xr.Dataset:
    """Return an all-zeros Dataset on the target 0.125° grid.

    Used for step-0 of accumulated fields (e.g. tot_prec) where DWD publishes
    a near-empty GRIB without grid metadata that CDO cannot regrid.
    """
    lats = np.linspace(90.0, -90.0, GRID_NY)     # north → south
    lons = np.linspace(-180.0, 179.75, GRID_NX)   # west → east (2879 pts)
    data = np.zeros((GRID_NY, GRID_NX), dtype=np.float32)
    return xr.Dataset(
        {param: (["latitude", "longitude"], data)},
        coords={"latitude": lats, "longitude": lons},
    )


def regrid_grib_bytes(
    grib_bytes: bytes,
    param: str,
    *,
    remap_target_grid: Path | None = None,
    remap_weights: Path | None = None,
) -> xr.Dataset:
    """
    Write GRIB bytes to a temp file, regrid via CDO remap, return xarray on 0.125° grid.

    By default uses ``assets/`` (``target_grid_world_0125.txt`` + per-parameter weights).
    For tests, pass ``remap_target_grid`` and ``remap_weights`` explicitly (e.g. DWD EASY
    files downloaded outside this module).

    Falls back to an all-zeros grid if the GRIB is too small to contain
    grid metadata (e.g. step-0 of accumulated fields).
    """
    # Step-0 of accumulated fields like tot_prec is a near-empty GRIB
    # (~200 bytes) with no grid reference — return zeros instead.
    if len(grib_bytes) < 10_000:
        log.info("GRIB too small (%d bytes) — returning zeros grid for %s", len(grib_bytes), param)
        return _make_zeros_dataset(param)

    if remap_target_grid is not None or remap_weights is not None:
        assert remap_target_grid is not None and remap_weights is not None, (
            "remap_target_grid and remap_weights must be passed together"
        )
        target_grid, weights = remap_target_grid, remap_weights
        assert target_grid.is_file(), f"Missing target grid file: {target_grid}"
        assert weights.is_file(), f"Missing weights file: {weights}"
    else:
        assert ASSETS_DIR.is_dir(), (
            f"Production regrid requires {ASSETS_DIR} with remap assets "
            f"(target_grid_world_0125.txt, weights_distance.nc, weights_conservative.nc)"
        )
        target_grid = ASSETS_DIR / "target_grid_world_0125.txt"
        weights = ASSETS_DIR / WEIGHTS_MAP.get(param, "weights_distance.nc")
        assert target_grid.is_file(), f"Missing target grid file: {target_grid}"
        assert weights.is_file(), f"Missing weights file for {param}: {weights}"

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        src = tmp / "input.grib2"
        dst = tmp / "regridded.nc"

        src.write_bytes(grib_bytes)

        cmd = [
            "cdo",
            "-s",
            "-f", "nc4",
            f"remap,{target_grid},{weights}",
            str(src),
            str(dst),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"CDO regridding failed for {param}:\n{result.stderr}"
            )

        ds = xr.open_dataset(dst, engine="netcdf4").load()

    # Drop time dim if CDO added one (single-step files get time=1)
    if "time" in ds.dims and ds.sizes["time"] == 1:
        ds = ds.squeeze("time", drop=True)

    # Normalize dimension names to match Zarr/Worker conventions
    rename_map: dict[str, str] = {}
    if "lat" in ds.dims and "latitude" not in ds.dims:
        rename_map["lat"] = "latitude"
    if "lon" in ds.dims and "longitude" not in ds.dims:
        rename_map["lon"] = "longitude"
    if rename_map:
        ds = ds.rename(rename_map)

    # CDO uses GRIB short names (e.g. 'tp' for tot_prec) — rename to our param name
    data_vars = list(ds.data_vars)
    if len(data_vars) == 1 and data_vars[0] != param:
        ds = ds.rename({data_vars[0]: param})

    # Drop CF cell-bounds aux arrays (e.g. depth_bnds). They are not forecast grids,
    # bloat Zarr (extra compressed arrays + metadata), and are unused by the Worker.
    bnds_aux = [v for v in ds.data_vars if v.endswith("_bnds")]
    if bnds_aux:
        ds = ds.drop_vars(bnds_aux)

    # Ensure north-to-south latitude ordering (Worker expects index 0 = 90°N)
    if "latitude" in ds.coords and ds.latitude.values[0] < ds.latitude.values[-1]:
        ds = ds.reindex(latitude=ds.latitude[::-1])

    return ds


# ---------------------------------------------------------------------------
# Zarr encoding helpers
# ---------------------------------------------------------------------------


def make_encoding(ds: xr.Dataset) -> dict[str, Any]:
    compressor = GZip(level=5)
    return {var: {"compressor": compressor} for var in ds.data_vars}


def make_chunks(ensemble: bool) -> dict[str, int]:
    chunks: dict[str, int] = {
        "latitude": CHUNK_LAT,
        "longitude": CHUNK_LON,
        "step": -1,
    }
    if ensemble:
        chunks["realization"] = -1
    return chunks


# ---------------------------------------------------------------------------
# ETL core
# ---------------------------------------------------------------------------


# Parallel GRIB download threads per matrix job (override with ETL_DOWNLOAD_WORKERS).
DOWNLOAD_WORKERS = int(os.environ.get("ETL_DOWNLOAD_WORKERS", "6"))
DOWNLOAD_INTERVAL = 0.15   # seconds between request starts (throttle)


def download_all_steps(
    run_dt: str,
    param: str,
    steps: list[int],
    model: str = "icon",
) -> dict[int, bytes | None]:
    """
    Download + bz2-decompress all GRIB files for *param* in parallel.

    Returns {step: decompressed_bytes} for steps that exist on DWD, and
    {step: None} for 404s.  All bytes are held in memory (not disk) since a
    single param's GRIBs fit comfortably (~2-4 GB for 97 steps).

    This is the core of the "download-first" pattern: by the time regridding
    starts, every available file is already local and immune to DWD rotation.

    Rate-limiting: a shared lock + ``DOWNLOAD_INTERVAL`` sleep serializes
    request *starts* so we don't stampede DWD.  ``with_retry`` provides
    exponential backoff on failures, and honours ``Retry-After`` on 429s.
    """
    results: dict[int, bytes | None] = {}
    throttle = threading.Lock()

    def _download_one(step: int) -> tuple[int, bytes | None]:
        # Serialize the start of each HTTP request to stay polite to DWD
        with throttle:
            time.sleep(DOWNLOAD_INTERVAL)
        urls = build_urls(model, run_dt, param, step)
        url = urls[0]
        raw = with_retry(try_stream_download, url, label=f"dl/{param}/step={step}")
        return step, raw

    log.info("[download] %s: fetching %d steps with %d workers …", param, len(steps), DOWNLOAD_WORKERS)
    t0 = time.monotonic()

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
        futures = {pool.submit(_download_one, s): s for s in steps}
        done = 0
        for fut in as_completed(futures):
            step, raw = fut.result()
            results[step] = raw
            done += 1
            if done % 20 == 0:
                log.info("[download] %s: %d / %d steps fetched", param, done, len(steps))

    available = sum(1 for v in results.values() if v is not None)
    elapsed = time.monotonic() - t0
    log.info(
        "[download] %s: done in %.1fs — %d available, %d missing (404)",
        param, elapsed, available, len(results) - available,
    )
    return results


def process_deterministic(
    run_dt: str,
    param: str,
    fs: s3fs.S3FileSystem,
    all_steps: list[int],
    available_steps: list[int] | None = None,
) -> xr.Dataset:
    """
    Download + regrid all forecast steps for a deterministic (ICON Global) run.
    Returns a concatenated xr.Dataset with a 'step' dimension.

    Uses download-first pattern: all GRIBs are fetched in parallel before any
    regridding begins, so mid-pipeline DWD file rotation cannot cause failures.

    If *available_steps* is provided (from probe manifest), only those steps
    are downloaded — known-missing steps are skipped without hitting DWD.
    """
    steps_to_fetch = available_steps if available_steps is not None else all_steps

    # Phase 1: download all GRIBs in parallel
    downloaded = download_all_steps(run_dt, param, steps_to_fetch, model="icon")

    # Phase 2: regrid from downloaded bytes (sequential — CDO is CPU-bound)
    step_datasets: list[xr.Dataset] = []
    ordered_steps = sorted(s for s, raw in downloaded.items() if raw is not None)
    n_ord = len(ordered_steps)
    for i, step in enumerate(ordered_steps):
        raw = downloaded[step]

        def _regrid(r=raw, p=param, s=step):
            ds = regrid_grib_bytes(r, p)
            return ds.expand_dims({"step": [s]})

        ds_step = with_retry(_regrid, label=f"regrid/{param}/step={step}")
        step_datasets.append(ds_step)
        if i == 0 or i == n_ord - 1 or (i + 1) % 20 == 0:
            log.info("  [det] %s step=%d regridded (%d vars)", param, step, len(ds_step.data_vars))
        else:
            log.debug("  [det] %s step=%d regridded (%d vars)", param, step, len(ds_step.data_vars))

    if not step_datasets:
        raise RuntimeError(
            f"[det] {param} run={run_dt}: no forecast steps downloaded (all missing on DWD?)"
        )

    return xr.concat(step_datasets, dim="step")


def process_ensemble(
    run_dt: str,
    param: str,
    fs: s3fs.S3FileSystem,
    all_steps: list[int],
    available_steps: list[int] | None = None,
) -> xr.Dataset:
    """
    Download + regrid all 40 ensemble members across all forecast steps.
    Returns an xr.Dataset with dimensions (step, realization, latitude, longitude).

    Uses download-first pattern for member-0 to probe which steps exist, then
    downloads remaining members in parallel per step.
    """
    steps_to_fetch = available_steps if available_steps is not None else all_steps

    member_datasets: list[xr.Dataset] = []
    member_step_lists: list[list[xr.Dataset]] = [[] for _ in range(ENSEMBLE_MEMBERS)]

    # Phase 1: download member-0 for all steps in parallel (probes availability)
    member0_downloaded = download_all_steps(run_dt, param, steps_to_fetch, model="icon-eps")
    valid_steps = sorted(s for s, raw in member0_downloaded.items() if raw is not None)

    log.info("[eps] %s: %d / %d steps have member-0 on DWD", param, len(valid_steps), len(steps_to_fetch))

    for step in valid_steps:
        urls = build_urls("icon-eps", run_dt, param, step)
        first_raw = member0_downloaded[step]

        # Regrid member-0 from cached bytes
        ds0 = with_retry(
            lambda r=first_raw, p=param, s=step: regrid_grib_bytes(r, p).expand_dims({"step": [s]}),
            label=f"eps/{param}/mbr=0/step={step}",
        )
        member_step_lists[0].append(ds0)

        # Download + regrid remaining members
        for member_idx in range(1, ENSEMBLE_MEMBERS):
            url_m = urls[member_idx]
            mid = member_idx

            def _fetch(u=url_m, p=param, s=step, m=mid):
                raw = stream_download(u)
                return regrid_grib_bytes(raw, p).expand_dims({"step": [s]})

            ds_step = with_retry(
                _fetch, label=f"eps/{param}/mbr={member_idx}/step={step}"
            )
            member_step_lists[member_idx].append(ds_step)

    for member_idx, step_datasets in enumerate(member_step_lists):
        if not step_datasets:
            raise RuntimeError(
                f"[eps] {param} run={run_dt}: no steps for member {member_idx}"
            )
        ds_member = xr.concat(step_datasets, dim="step")
        ds_member = ds_member.expand_dims({"realization": [member_idx]})
        member_datasets.append(ds_member)
        log.debug("  [eps] %s member=%d complete (%d steps)", param, member_idx, len(step_datasets))

    out = xr.concat(member_datasets, dim="realization")
    log.info(
        "  [eps] %s: merged %d members × %d steps",
        param,
        len(member_datasets),
        len(member_step_lists[0]) if member_step_lists[0] else 0,
    )
    return out


def upload_to_run_store(
    ds: xr.Dataset,
    run_dt: str,
    param: str,
    ensemble: bool,
    fs: s3fs.S3FileSystem,
) -> str:
    """Chunk, compress, and upload Dataset directly to runs/{run_dt}/{param}/.

    The /latest/_LATEST_RUN pointer only flips in finalize, so writing straight
    under runs/ is safe: the Worker won't see this run until spatial_index +
    pointer flip, and a failed write is cleaned up by cleanup_failed_run_data.
    """
    chunks = make_chunks(ensemble)
    ds = ds.chunk(chunks)
    encoding = make_encoding(ds)

    store_path = f"{BUCKET}/runs/{run_dt}/{param}"
    store = s3fs.S3Map(root=store_path, s3=fs, check=False)

    log.info("Uploading %s → s3://%s …", param, store_path)

    def _write_zarr():
        ds.to_zarr(store, mode="w", consolidated=True, encoding=encoding, zarr_format=2)

    with_retry(_write_zarr, label=f"upload/{param}")

    marker_key = f"{store_path}/_SUCCESS"
    with fs.open(marker_key, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())
    log.info("_SUCCESS marker written: %s", marker_key)

    return store_path


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class ParamValidationError(ValueError):
    """Validation failed for one ETL parameter; ``.param`` names the failing key."""

    def __init__(self, param: str, message: str) -> None:
        super().__init__(message)
        self.param = param


def validate_one_param(
    run_dt: str,
    param: str,
    ensemble: bool,
    fs: s3fs.S3FileSystem,
) -> None:
    """
    Validate one staged parameter.
    Raises ParamValidationError with ``.param`` set so callers can retry from that index only.
    """
    store_path = f"{BUCKET}/runs/{run_dt}/{param}"

    # 1. _SUCCESS marker — fail fast if the matrix job's upload was interrupted
    marker = f"{store_path}/_SUCCESS"
    if not fs.exists(marker):
        raise ParamValidationError(
            param,
            f"[{param}] _SUCCESS marker missing at {marker} — "
            "matrix upload likely incomplete or ETL job failed",
        )

    store = s3fs.S3Map(root=store_path, s3=fs, check=False)

    # 2. Zarr store readable (consolidated metadata)
    try:
        ds = xr.open_zarr(store, consolidated=True)
    except Exception as exc:
        raise ParamValidationError(param, f"[{param}] Cannot open Zarr store: {exc}") from exc

    try:
        # 3. Single-chunk smoke read on the served forecast field only — fast
        #    (one small R2 read per param) and matches what the Worker exposes.
        #    Regrid drops *_bnds today; still skip them for older stores.
        if param in ds.data_vars:
            vars_to_check = [param]
        else:
            vars_to_check = [v for v in ds.data_vars if not v.endswith("_bnds")]
        if not vars_to_check:
            raise ParamValidationError(
                param,
                f"[{param}] No gridded data variables (only *_bnds aux or empty store?)",
            )
        for var in vars_to_check:
            if ds[var].size == 0:
                raise ParamValidationError(param, f"[{param}/{var}] Data array is empty")
            sample = ds[var].isel(step=0)
            while sample.ndim > 2:
                sample = sample.isel({sample.dims[0]: 0})
            sample = sample.isel(
                {sample.dims[0]: slice(0, 10), sample.dims[1]: slice(0, 10)}
            )
            if np.isnan(np.asarray(sample.values)).all():
                raise ParamValidationError(
                    param, f"[{param}/{var}] Step-0 sample chunk is all-NaN"
                )

        # 4. Ensemble member count
        if ensemble:
            n_members = ds.dims.get("realization", 0)
            if n_members != ENSEMBLE_MEMBERS:
                raise ParamValidationError(
                    param,
                    f"[{param}] Expected {ENSEMBLE_MEMBERS} ensemble members, got {n_members}",
                )

        n_steps = ds.sizes.get("step", 0)
        log.info(
            "  [OK] %s — %d steps in Zarr, %d data vars%s",
            param,
            n_steps,
            len(ds.data_vars),
            f", {ENSEMBLE_MEMBERS} members" if ensemble else "",
        )
    finally:
        ds.close()


def validate_run(
    run_dt: str,
    params: list[str],
    ensemble: bool,
    fs: s3fs.S3FileSystem,
) -> None:
    """
    Validate the complete staged run.
    Raises ParamValidationError (or ValueError after selective-retry exhaustion) on failure.
    """
    log.info("=== Validation: run=%s ===", run_dt)
    for param in params:
        validate_one_param(run_dt, param, ensemble, fs)
    log.info("=== Validation PASSED ===")


def validate_run_with_selective_retry(
    run_dt: str,
    params: list[str],
    ensemble: bool,
    fs: s3fs.S3FileSystem,
) -> None:
    """
    Like ``validate_run``, but on failure only re-validates from the failing parameter onward
    (same backoff as ``with_retry``), avoiding redundant work for parameters that already passed.
    """
    log.info("=== Validation: run=%s ===", run_dt)
    max_workers = int(os.environ.get("VALIDATE_CONCURRENCY", "3"))
    start_idx = 0
    attempt = 0
    while True:
        attempt += 1
        try:
            remaining = params[start_idx:]
            with ThreadPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(
                        validate_one_param, run_dt, p, ensemble, fs
                    ): p
                    for p in remaining
                }
                for fut in as_completed(futures):
                    fut.result()
            log.info("=== Validation PASSED ===")
            return
        except ParamValidationError as exc:
            try:
                failed_idx = params.index(exc.param)
            except ValueError as idx_err:
                raise ValueError(str(exc)) from idx_err
            start_idx = failed_idx
            if attempt >= MAX_RETRIES:
                log.error("[%s] All %d attempts failed: %s", "validation", MAX_RETRIES, exc)
                raise ValueError(str(exc)) from exc
            wait = min(RETRY_BASE_S * (2 ** (attempt - 1)), RETRY_MAX_S)
            log.warning(
                "[%s] Attempt %d/%d failed (%s). Retrying from %s in %ds…",
                "validation",
                attempt,
                MAX_RETRIES,
                exc,
                params[start_idx],
                wait,
            )
            time.sleep(wait)


def write_promoted_marker(
    run_dt: str,
    params: list[str],
    fs: s3fs.S3FileSystem,
) -> None:
    """Record that base-layer smoke validation passed (promote CI job)."""
    key = f"{BUCKET}/runs/{run_dt}/{PROMOTED_RELATIVE}"
    payload = json.dumps(
        {
            "run": run_dt,
            "params": sorted(params),
            "validated_at": datetime.now(timezone.utc).isoformat(),
        },
        separators=(",", ":"),
    )
    with fs.open(key, "w") as f:
        f.write(payload)
    log.info("Promoted marker written: s3://%s", key)


def cleanup_legacy_staging_dirs(
    ci_run_dt: str,
    resolved_run_dt: str,
    params: list[str],
    fs: s3fs.S3FileSystem,
) -> None:
    """
    Best-effort removal of pre-direct-write ``staging/{{run}}/{{param}}/`` trees.
    Parallelized because R2 latency dominates.
    """
    prefixes = {resolved_run_dt}
    if ci_run_dt != resolved_run_dt:
        prefixes.add(ci_run_dt)
    paths = [f"{BUCKET}/staging/{rid}/{param}" for rid in prefixes for param in params]
    if not paths:
        return

    def _rm_one(prefix: str) -> None:
        if not fs.exists(prefix):
            return
        fs.rm(prefix, recursive=True)
        log.info("Removed legacy staging: s3://%s", prefix)

    max_workers = min(8, len(paths))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_rm_one, pth) for pth in paths]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                log.warning("Legacy staging cleanup: %s", exc)


# ---------------------------------------------------------------------------
# Finalization (pointer flip + spatial_index + janitor)
# ---------------------------------------------------------------------------


def finalize_run(
    run_dt: str,
    params: list[str],
    keep_runs: int,
    fs: s3fs.S3FileSystem,
) -> None:
    """Flip /latest pointer, (re)build spatial_index, run janitor.

    Called after the promote (validate-only) job AND any downstream derivations
    (PET, etc.) have written under runs/{run_dt}/. ``params`` should include
    every layer the dashboard expects (pet_pm included).
    """
    log.info("=== Finalizing run=%s ===", run_dt)

    promoted_key = f"{BUCKET}/runs/{run_dt}/{PROMOTED_RELATIVE}"
    if not fs.exists(promoted_key):
        raise RuntimeError(
            f"Refusing to finalize run={run_dt}: missing {promoted_key} — "
            "run etl.py --validate-only (promote job) first so base layers are validated."
        )

    # Hard guard: every advertised param must have a _SUCCESS marker under
    # runs/{run_dt}/{param}/. Protects out-of-band invocations (manual reruns,
    # workflow_dispatch on finalize only, local `etl.py --finalize`) that
    # bypass the workflow's `needs:` graph.
    missing = [
        p for p in params
        if not fs.exists(f"{BUCKET}/runs/{run_dt}/{p}/_SUCCESS")
    ]
    if missing:
        raise RuntimeError(
            f"Refusing to finalize run={run_dt}: missing _SUCCESS for "
            f"{missing}. Promotion or derivation step did not complete."
        )

    # Build spatial_index first so it's in place the moment /latest flips.
    build_spatial_index(run_dt, params, fs)

    latest_key = f"{BUCKET}/latest/_LATEST_RUN"
    with fs.open(latest_key, "w") as f:
        f.write(run_dt)
    log.info("Updated latest pointer: %s → %s", latest_key, run_dt)

    run_janitor(run_dt, keep_runs, fs)
    log.info("=== Finalize COMPLETE ===")


# ---------------------------------------------------------------------------
# Spatial index generation
# ---------------------------------------------------------------------------


def build_spatial_index(run_dt: str, params: list[str], fs: s3fs.S3FileSystem) -> None:
    """
    Write spatial_index.json describing chunk layout for the Worker.
    This is a small JSON that the CF Worker loads to resolve lat/lon → chunk.
    ``steps`` are taken from the first readable promoted Zarr (matches skipped DWD steps).
    """
    # Invalidate the s3fs listing/content cache for the promoted paths so we
    # don't read stale consolidated metadata after matrix uploads / PET writes.
    fs.invalidate_cache(f"{BUCKET}/runs/{run_dt}")

    steps_in_store: list[int] = []
    for ref_param in params:
        try:
            store = s3fs.S3Map(
                root=f"{BUCKET}/runs/{run_dt}/{ref_param}", s3=fs, check=False
            )
            ds_ref = xr.open_zarr(store, consolidated=True)
            if "step" in ds_ref.coords:
                steps_in_store = [int(s) for s in ds_ref.coords["step"].values]
            ds_ref.close()
            if steps_in_store:
                break
        except Exception as exc:
            log.debug("spatial_index: skip ref %s: %s", ref_param, exc)
    if not steps_in_store:
        raise ValueError(
            f"Cannot build spatial_index: no step coordinate found under runs/{run_dt}/"
        )

    index = {
        "run": run_dt,
        "grid": {
            "lat_min": LAT_MIN, "lat_max": LAT_MAX,
            "lon_min": LON_MIN, "lon_max": LON_MAX,
            "nx": GRID_NX, "ny": GRID_NY,
            "resolution": 0.125,
        },
        "chunks": {"latitude": CHUNK_LAT, "longitude": CHUNK_LON},
        "params": params,
        "steps": steps_in_store,
    }
    key = f"{BUCKET}/spatial_index.json"
    with fs.open(key, "w") as f:
        json.dump(index, f)
    log.info("spatial_index.json written to s3://%s", key)


# ---------------------------------------------------------------------------
# Janitor
# ---------------------------------------------------------------------------


def run_janitor(
    current_run: str,
    keep_runs: int,
    fs: s3fs.S3FileSystem,
) -> None:
    """
    Delete old /runs/ and /staging/ directories, retaining the most recent `keep_runs`.
    MUST only be called after promotion succeeds.
    """
    log.info("=== Janitor: keep_runs=%d ===", keep_runs)

    # List all promoted runs
    try:
        run_dirs = fs.ls(f"{BUCKET}/runs/", detail=False)
    except FileNotFoundError:
        log.info("No runs directory found — nothing to clean.")
        return

    run_ids = sorted(
        [p.split("/")[-1] for p in run_dirs if p.split("/")[-1].isdigit()],
        reverse=True,
    )

    runs_to_delete = run_ids[keep_runs:]

    for old_run in runs_to_delete:
        for prefix in ("runs", "staging"):
            path = f"{BUCKET}/{prefix}/{old_run}"
            if fs.exists(path):
                log.info(
                    "[%s] Deleting %s (timestamp=%s)",
                    datetime.now(timezone.utc).isoformat(), path, old_run,
                )
                fs.rm(path, recursive=True)
            else:
                log.debug("[janitor] %s does not exist — skipping", path)

    # Clean orphaned staging dirs (failed runs not in /runs/)
    try:
        staging_dirs = fs.ls(f"{BUCKET}/staging/", detail=False)
    except FileNotFoundError:
        staging_dirs = []

    active_runs = set(run_ids[:keep_runs] + [current_run])
    for sdir in staging_dirs:
        run_id = sdir.split("/")[-1]
        if run_id not in active_runs:
            log.info("[janitor] Removing orphaned staging: %s", sdir)
            fs.rm(sdir, recursive=True)

    log.info("=== Janitor DONE ===")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ICON Global Weather ETL Pipeline")
    p.add_argument("--parameter", default=os.environ.get("PARAMETER", "t_2m"))
    p.add_argument(
        "--ensemble",
        action="store_true",
        default=os.environ.get("ENABLE_ENSEMBLE", "false").lower() == "true",
    )
    p.add_argument("--model-run", default=os.environ.get("MODEL_RUN", "00"),
                   choices=["00", "06", "12", "18"])
    p.add_argument("--run-date", default=os.environ.get("RUN_DATE"),
                   help="Override date as YYYYMMDD (default: today UTC)")
    p.add_argument("--keep-runs", type=int,
                   default=int(os.environ.get("KEEP_RUNS", KEEP_RUNS_DEFAULT)))
    p.add_argument(
        "--validate-only",
        action="store_true",
        help="Smoke-validate Zarr under runs/{run}/{param}/, write runs/{run}/_PROMOTED, "
             "best-effort remove legacy staging/{run}/{param}/ mirrors. "
             "Does NOT flip /latest or run janitor — use --finalize for that.",
    )
    p.add_argument(
        "--finalize",
        action="store_true",
        help="Flip /latest pointer, rebuild spatial_index, run janitor. "
             "Run after --validate-only and any derivations (e.g. derive_pet.py).",
    )
    p.add_argument(
        "--params",
        nargs="+",
        default=None,
        help="Override parameter list for validation/promotion/finalize steps",
    )
    p.add_argument(
        "--manifest",
        default=None,
        help="Path to probe manifest JSON (from probe_dwd.py). "
             "Tells the ETL which steps are available, skipping known-missing.",
    )
    args = p.parse_args()
    if args.validate_only and args.finalize:
        p.error(
            "--validate-only and --finalize are mutually exclusive: the workflow "
            "runs them as separate jobs with derive_pet.py in between."
        )
    return args


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()
    all_steps = get_all_steps(args.model_run)

    # Build run datetime string YYYYMMDDHH
    if args.run_date:
        run_date = args.run_date
    else:
        run_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    run_dt = f"{run_date}{args.model_run}"

    param = args.parameter
    ensemble = args.ensemble
    model = "icon-eps" if ensemble else "icon"

    log.info(
        "ICON ETL | run=%s | param=%s | ensemble=%s | model=%s",
        run_dt, param, ensemble, model,
    )

    fs = make_r2_fs()

    # ------------------------------------------------------------------
    # Phase 1 & 2: Download → Regrid → Stage
    # Skip when running --validate-only or --finalize.
    # ------------------------------------------------------------------
    if not args.validate_only and not args.finalize:
        # Probe DWD and fall back to an older run if the requested one isn't published yet
        original_run_dt = run_dt
        resolved_run_dt = resolve_available_run(run_date, args.model_run, param, model)
        if resolved_run_dt != run_dt:
            log.info("Resolved run_dt: %s → %s", run_dt, resolved_run_dt)
            run_dt = resolved_run_dt
            all_steps = get_all_steps(run_dt[8:10])

        # Load probe manifest (from probe_dwd.py) to skip known-missing steps
        available_steps: list[int] | None = None
        if args.manifest:
            with open(args.manifest) as mf:
                manifest = json.load(mf)
            param_info = manifest.get("params", {}).get(param)
            if param_info:
                available_steps = sorted(param_info["available"])
                log.info(
                    "Manifest: %s has %d / %d available steps (skipping %d known-missing)",
                    param, len(available_steps), len(all_steps),
                    len(param_info.get("missing", [])),
                )
            else:
                log.warning("Manifest has no entry for %s — downloading all steps", param)

        try:
            t0 = time.monotonic()
            if ensemble:
                ds = process_ensemble(run_dt, param, fs, all_steps, available_steps)
            else:
                ds = process_deterministic(run_dt, param, fs, all_steps, available_steps)

            store_path = upload_to_run_store(ds, run_dt, param, ensemble, fs)

            # Write _RESOLVED_RUN under the *original* CI run_dt path so
            # downstream jobs (which only know the CI run_dt) can discover
            # which run was actually written when DWD fallback occurred.
            if original_run_dt != run_dt:
                resolved_key = f"{BUCKET}/staging/{original_run_dt}/_RESOLVED_RUN"
                with fs.open(resolved_key, "w") as f:
                    f.write(run_dt)
                log.info("_RESOLVED_RUN redirect written: %s → %s", resolved_key, run_dt)

            elapsed = time.monotonic() - t0
            log.info(
                "Upload complete: %s  (%.1fs, %.1f MB in-memory)",
                store_path, elapsed,
                sum(ds[v].nbytes for v in ds.data_vars) / 1e6,
            )
        except Exception as exc:
            msg = f"ETL FAILED | run={run_dt} | param={param} | error={exc}"
            log.exception(msg)
            cleanup_failed_run_data(run_dt, [param], fs)
            send_alert(msg)
            sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 3: Validation  (triggered separately via --validate-only)
    # Data is already under runs/{run_dt}/{param}/; validation just reads
    # the _SUCCESS markers + Zarr metadata + a single smoke-test chunk.
    # ------------------------------------------------------------------
    if args.validate_only:
        all_params = args.params or list(PARAM_BOUNDS.keys())
        ci_run_dt = run_dt

        # The ETL matrix jobs may have fallen back to an older DWD run.
        # Read the resolved run_dt they actually wrote under.
        resolved_key = f"{BUCKET}/staging/{run_dt}/_RESOLVED_RUN"
        if fs.exists(resolved_key):
            with fs.open(resolved_key) as f:
                resolved = f.read().decode().strip()
            if resolved != run_dt:
                log.info("Validate: resolved run_dt %s → %s (from staging/_RESOLVED_RUN)", run_dt, resolved)
                run_dt = resolved
        else:
            log.warning("No _RESOLVED_RUN marker found; using CI run_dt %s", run_dt)

        try:
            validate_run_with_selective_retry(
                run_dt, all_params, ensemble, fs
            )
        except Exception as exc:
            msg = f"VALIDATION FAILED | run={run_dt} | error={exc}"
            log.exception(msg)
            cleanup_failed_run_data(run_dt, all_params, fs)
            send_alert(msg)
            sys.exit(1)

        try:
            write_promoted_marker(run_dt, all_params, fs)
        except Exception as exc:
            msg = f"PROMOTE MARKER FAILED | run={run_dt} | error={exc}"
            log.exception(msg)
            send_alert(msg)
            sys.exit(1)

        try:
            cleanup_legacy_staging_dirs(ci_run_dt, run_dt, all_params, fs)
        except Exception:
            log.warning("Legacy staging cleanup had errors (non-fatal)", exc_info=True)

        log.info("Validation complete for run=%s (finalize pending)", run_dt)

    # ------------------------------------------------------------------
    # Phase 4: Finalize (flip /latest, rebuild spatial_index, janitor)
    # Called as a separate invocation after derive_pet.py (and any other
    # derivation steps) have written layers under runs/{run_dt}/.
    # ------------------------------------------------------------------
    if args.finalize:
        all_params = args.params or list(PARAM_BOUNDS.keys())

        resolved_key = f"{BUCKET}/staging/{run_dt}/_RESOLVED_RUN"
        if fs.exists(resolved_key):
            with fs.open(resolved_key) as f:
                resolved = f.read().decode().strip()
            if resolved != run_dt:
                log.info("Finalize: resolved run_dt %s → %s", run_dt, resolved)
                run_dt = resolved

        try:
            finalize_run(run_dt, all_params, args.keep_runs, fs)
        except Exception as exc:
            msg = f"FINALIZE FAILED | run={run_dt} | error={exc}"
            log.exception(msg)
            send_alert(msg)
            sys.exit(1)

        log.info("All phases complete for run=%s", run_dt)


if __name__ == "__main__":
    main()
