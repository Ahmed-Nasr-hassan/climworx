#!/usr/bin/env python3
"""
ICON Global Weather ETL Pipeline
=================================
Fetches ICON Global / ICON-EPS GRIB2 data from DWD opendata,
regrids from the native icosahedral grid to a 0.125° regular lat/lon grid,
converts to Cloud-Optimized Zarr, and uploads to Cloudflare R2.

3-Phase Workflow: staging → validation → promotion
Janitor: safe cleanup of old runs post-success

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
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import requests
import s3fs
import xarray as xr
import zarr
from numcodecs import Blosc

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
GRID_NX = 2880   # 360 / 0.125
GRID_NY = 1441   # 180 / 0.125 + 1  (includes both poles)
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0
CHUNK_LAT = 100
CHUNK_LON = 100
ENSEMBLE_MEMBERS = 40
KEEP_RUNS_DEFAULT = 2
MAX_RETRIES = 3
RETRY_BASE_S = 30
RETRY_MAX_S = 300
BUCKET = "icon-global-databank"

# Deterministic forecast steps: +0..+78 hourly, +81..+180 every 3h
STEPS_SHORT = list(range(0, 79))           # 0–78 inclusive
STEPS_EXTENDED = list(range(81, 181, 3))   # 81, 84, ..., 180
ALL_STEPS = STEPS_SHORT + STEPS_EXTENDED   # 97 steps total

# Physical plausibility bounds per parameter (min, max)
PARAM_BOUNDS: dict[str, tuple[float, float]] = {
    "t_2m":      (180.0, 340.0),   # K
    "tot_prec":  (0.0, 500.0),     # mm
    "u_10m":     (-150.0, 150.0),  # m/s
    "v_10m":     (-150.0, 150.0),  # m/s
    "pmsl":      (85000.0, 108000.0),  # Pa
    "clct":      (0.0, 100.0),     # %
    "relhum_2m": (0.0, 110.0),     # % (allow slight super-saturation)
    "aswdir_s":  (0.0, 1500.0),    # W/m²
}

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


def with_retry(fn, *args, label: str = "", **kwargs):
    """Call fn(*args, **kwargs) with up to MAX_RETRIES, exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            wait = min(RETRY_BASE_S * (2 ** (attempt - 1)), RETRY_MAX_S)
            if attempt == MAX_RETRIES:
                log.error("[%s] All %d attempts failed: %s", label, MAX_RETRIES, exc)
                raise
            log.warning(
                "[%s] Attempt %d/%d failed (%s). Retrying in %ds…",
                label, attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)


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
    base = f"{DWD_BASE}/{model}/grib/{hh}/{param}/"
    fname_template = (
        f"{model.replace('-', '_')}_icosahedral_single-level"
        f"_{run_dt}_{step:03d}_{param.upper()}.grib2.bz2"
    )
    if model == "icon-eps":
        urls = []
        for member in range(1, ENSEMBLE_MEMBERS + 1):
            fname = (
                f"{model.replace('-', '_')}_icosahedral_single-level"
                f"_{run_dt}_{step:03d}_{member:03d}_{param.upper()}.grib2.bz2"
            )
            urls.append(base + fname)
        return urls
    return [base + fname_template]


# ---------------------------------------------------------------------------
# Download & decompress
# ---------------------------------------------------------------------------


def stream_download(url: str) -> bytes:
    """Stream-download a .bz2 file and return decompressed bytes."""
    log.debug("Downloading %s", url)
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        compressed = b"".join(resp.iter_content(chunk_size=1 << 20))
    return bz2.decompress(compressed)


# ---------------------------------------------------------------------------
# Regridding: icosahedral → 0.125° regular lat/lon via CDO remapbil
# ---------------------------------------------------------------------------


def _write_target_grid(path: Path) -> None:
    """Write a CDO target grid description for 0.125° global lat/lon."""
    content = (
        "gridtype  = lonlat\n"
        f"xsize     = {GRID_NX}\n"
        f"ysize     = {GRID_NY}\n"
        "xfirst    = -180.0\n"
        "xinc      = 0.125\n"
        "yfirst    = -90.0\n"
        "yinc      = 0.125\n"
    )
    path.write_text(content)


def regrid_grib_bytes(grib_bytes: bytes, param: str) -> xr.Dataset:
    """
    Write GRIB bytes to a temp file, regrid via CDO remapbil,
    and return an xarray Dataset on the regular 0.125° grid.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        src = tmp / "input.grib2"
        dst = tmp / "regridded.nc"
        grid_file = tmp / "target_grid.txt"

        src.write_bytes(grib_bytes)
        _write_target_grid(grid_file)

        cmd = [
            "cdo",
            "-f", "nc4",
            f"remapbil,{grid_file}",
            str(src),
            str(dst),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"CDO regridding failed for {param}:\n{result.stderr}"
            )

        ds = xr.open_dataset(dst, engine="netcdf4").load()

    # Normalize dimension names to match Zarr/Worker conventions
    rename_map: dict[str, str] = {}
    if "lat" in ds.dims and "latitude" not in ds.dims:
        rename_map["lat"] = "latitude"
    if "lon" in ds.dims and "longitude" not in ds.dims:
        rename_map["lon"] = "longitude"
    if rename_map:
        ds = ds.rename(rename_map)

    # Ensure north-to-south latitude ordering (Worker expects index 0 = 90°N)
    if "latitude" in ds.coords and ds.latitude.values[0] < ds.latitude.values[-1]:
        ds = ds.reindex(latitude=ds.latitude[::-1])

    return ds


# ---------------------------------------------------------------------------
# Zarr encoding helpers
# ---------------------------------------------------------------------------


def make_encoding(ds: xr.Dataset) -> dict[str, Any]:
    compressor = Blosc(cname="zstd", clevel=3, shuffle=Blosc.BITSHUFFLE)
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


def process_deterministic(
    run_dt: str,
    param: str,
    fs: s3fs.S3FileSystem,
) -> xr.Dataset:
    """
    Download + regrid all forecast steps for a deterministic (ICON Global) run.
    Returns a concatenated xr.Dataset with a 'step' dimension.
    """
    step_datasets: list[xr.Dataset] = []

    for step in ALL_STEPS:
        urls = build_urls("icon", run_dt, param, step)
        url = urls[0]

        def _fetch_and_regrid(u=url, p=param, s=step):
            raw = stream_download(u)
            ds = regrid_grib_bytes(raw, p)
            ds = ds.expand_dims({"step": [s]})
            return ds

        ds_step = with_retry(_fetch_and_regrid, label=f"{param}/step={step}")
        step_datasets.append(ds_step)
        log.info("  [det] %s step=%d regridded (%d vars)", param, step, len(ds_step.data_vars))

    return xr.concat(step_datasets, dim="step")


def process_ensemble(
    run_dt: str,
    param: str,
    fs: s3fs.S3FileSystem,
) -> xr.Dataset:
    """
    Download + regrid all 40 ensemble members across all forecast steps.
    Returns an xr.Dataset with dimensions (step, realization, latitude, longitude).
    """
    member_datasets: list[xr.Dataset] = []

    for member_idx in range(ENSEMBLE_MEMBERS):
        step_datasets: list[xr.Dataset] = []
        for step in ALL_STEPS:
            urls = build_urls("icon-eps", run_dt, param, step)
            url = urls[member_idx]

            def _fetch(u=url, p=param, s=step):
                raw = stream_download(u)
                ds = regrid_grib_bytes(raw, p)
                return ds.expand_dims({"step": [s]})

            ds_step = with_retry(_fetch, label=f"eps/{param}/mbr={member_idx}/step={step}")
            step_datasets.append(ds_step)

        ds_member = xr.concat(step_datasets, dim="step")
        ds_member = ds_member.expand_dims({"realization": [member_idx]})
        member_datasets.append(ds_member)
        log.info("  [eps] %s member=%d complete", param, member_idx)

    return xr.concat(member_datasets, dim="realization")


def upload_to_staging(
    ds: xr.Dataset,
    run_dt: str,
    param: str,
    ensemble: bool,
    fs: s3fs.S3FileSystem,
) -> str:
    """Chunk, compress, and upload Dataset to staging path. Returns the staging path."""
    chunks = make_chunks(ensemble)
    ds = ds.chunk(chunks)
    encoding = make_encoding(ds)

    staging_path = f"{BUCKET}/staging/{run_dt}/{param}"
    store = s3fs.S3Map(root=staging_path, s3=fs, check=False)

    log.info("Uploading %s → s3://%s …", param, staging_path)
    ds.to_zarr(store, mode="w", consolidated=True, encoding=encoding)

    # Write _SUCCESS marker for this parameter
    marker_key = f"{staging_path}/_SUCCESS"
    with fs.open(marker_key, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())
    log.info("_SUCCESS marker written: %s", marker_key)

    return staging_path


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_run(run_dt: str, params: list[str], ensemble: bool, fs: s3fs.S3FileSystem) -> None:
    """
    Validate the complete staged run.
    Raises ValueError with a descriptive message on any failure.
    """
    log.info("=== Validation: run=%s ===", run_dt)

    for param in params:
        staging_path = f"{BUCKET}/staging/{run_dt}/{param}"
        store = s3fs.S3Map(root=staging_path, s3=fs, check=False)

        # 1. Zarr metadata readable
        try:
            ds = xr.open_zarr(store, consolidated=True)
        except Exception as exc:
            raise ValueError(f"[{param}] Cannot open Zarr store: {exc}") from exc

        # 2. Step completeness
        if "step" not in ds.coords:
            raise ValueError(f"[{param}] 'step' coordinate not found in Zarr store")
        actual_steps = set(int(s) for s in ds.coords["step"].values)
        expected_steps = set(ALL_STEPS)
        missing = expected_steps - actual_steps
        if missing:
            raise ValueError(
                f"[{param}] Missing {len(missing)} forecast steps: "
                f"{sorted(missing)[:10]}{'…' if len(missing) > 10 else ''}"
            )

        # 3. Non-empty arrays + plausibility bounds
        #    Use dask-aware operations to avoid loading entire arrays into RAM
        #    (a single parameter can be ~1.6 GB as float32).
        bounds = PARAM_BOUNDS.get(param)
        for var in ds.data_vars:
            if ds[var].size == 0:
                raise ValueError(f"[{param}/{var}] Data array is empty")
            if bool(ds[var].isnull().all().compute()):
                raise ValueError(f"[{param}/{var}] All-NaN array")
            if bounds is not None:
                vmin = float(ds[var].min().compute())
                vmax = float(ds[var].max().compute())
                if vmin < bounds[0] or vmax > bounds[1]:
                    raise ValueError(
                        f"[{param}/{var}] Values out of plausible range "
                        f"[{bounds[0]}, {bounds[1]}]: got [{vmin:.2f}, {vmax:.2f}]"
                    )

        # 4. Ensemble member count
        if ensemble:
            n_members = ds.dims.get("realization", 0)
            if n_members != ENSEMBLE_MEMBERS:
                raise ValueError(
                    f"[{param}] Expected {ENSEMBLE_MEMBERS} ensemble members, got {n_members}"
                )

        # 5. _SUCCESS marker exists
        marker = f"{staging_path}/_SUCCESS"
        if not fs.exists(marker):
            raise ValueError(f"[{param}] _SUCCESS marker missing at {marker}")

        log.info("  [OK] %s — %d steps, %d data vars%s",
                 param, len(ALL_STEPS), len(ds.data_vars),
                 f", {ENSEMBLE_MEMBERS} members" if ensemble else "")

    log.info("=== Validation PASSED ===")


# ---------------------------------------------------------------------------
# Promotion
# ---------------------------------------------------------------------------


def promote_run(
    run_dt: str,
    params: list[str],
    fs: s3fs.S3FileSystem,
) -> None:
    """Copy validated staging data to /runs/{run_dt}/ and update /latest/ pointer."""
    log.info("=== Promoting run=%s ===", run_dt)

    for param in params:
        src = f"{BUCKET}/staging/{run_dt}/{param}"
        dst = f"{BUCKET}/runs/{run_dt}/{param}"
        log.info("  Copying %s → %s", src, dst)
        fs.copy(src, dst, recursive=True)

        # Write _SUCCESS in promoted path
        with fs.open(f"{dst}/_SUCCESS", "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())

    # Update /latest/_LATEST_RUN pointer
    latest_key = f"{BUCKET}/latest/_LATEST_RUN"
    with fs.open(latest_key, "w") as f:
        f.write(run_dt)
    log.info("Updated latest pointer: %s → %s", latest_key, run_dt)

    log.info("=== Promotion COMPLETE ===")


# ---------------------------------------------------------------------------
# Spatial index generation
# ---------------------------------------------------------------------------


def build_spatial_index(run_dt: str, params: list[str], fs: s3fs.S3FileSystem) -> None:
    """
    Write spatial_index.json describing chunk layout for the Worker.
    This is a small JSON that the CF Worker loads to resolve lat/lon → chunk.
    """
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
        "steps": ALL_STEPS,
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
        help="Run validation + promotion only (skip download/regrid)",
    )
    p.add_argument(
        "--params",
        nargs="+",
        default=None,
        help="Override parameter list for validation/promotion steps",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    args = parse_args()

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
    # Phase 1 & 2: Download → Regrid → Stage  (skip if --validate-only)
    # ------------------------------------------------------------------
    if not args.validate_only:
        try:
            t0 = time.monotonic()
            if ensemble:
                ds = process_ensemble(run_dt, param, fs)
            else:
                ds = process_deterministic(run_dt, param, fs)

            staging_path = upload_to_staging(ds, run_dt, param, ensemble, fs)
            elapsed = time.monotonic() - t0
            log.info(
                "Staging complete: %s  (%.1fs, %.1f MB in-memory)",
                staging_path, elapsed,
                sum(ds[v].nbytes for v in ds.data_vars) / 1e6,
            )
        except Exception as exc:
            msg = f"ETL FAILED | run={run_dt} | param={param} | error={exc}"
            log.exception(msg)
            send_alert(msg)
            sys.exit(1)

    # ------------------------------------------------------------------
    # Phase 3: Validation + Promotion  (triggered separately via --validate-only)
    # ------------------------------------------------------------------
    if args.validate_only:
        all_params = args.params or list(PARAM_BOUNDS.keys())
        try:
            with_retry(validate_run, run_dt, all_params, ensemble, fs,
                       label="validation")
        except Exception as exc:
            msg = f"VALIDATION FAILED | run={run_dt} | error={exc}"
            log.exception(msg)
            send_alert(msg)
            sys.exit(1)

        try:
            promote_run(run_dt, all_params, fs)
            build_spatial_index(run_dt, all_params, fs)
            run_janitor(run_dt, args.keep_runs, fs)
        except Exception as exc:
            msg = f"PROMOTION/JANITOR FAILED | run={run_dt} | error={exc}"
            log.exception(msg)
            send_alert(msg)
            sys.exit(1)

        log.info("All phases complete for run=%s", run_dt)


if __name__ == "__main__":
    main()
