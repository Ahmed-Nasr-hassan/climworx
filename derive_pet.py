#!/usr/bin/env python3
"""
Derive PET (FAO-56 Penman-Monteith) from promoted ICON layers in R2.

Reads required parameters from:
  runs/{run_dt}/{param}/

Writes:
  runs/{run_dt}/pet_pm/
  runs/{run_dt}/pet_pm/_SUCCESS
  spatial_index.json (append pet_pm to params)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import numpy as np
import s3fs
import xarray as xr
from numcodecs import GZip

BUCKET = "icon-global-databank"
CHUNK_LAT = 100
CHUNK_LON = 100

REQUIRED_PARAMS = ["t_2m", "td_2m", "u_10m", "v_10m", "asob_s", "athb_s", "ps"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("derive-pet")


def env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return val


def make_r2_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=env("R2_ACCESS_KEY_ID"),
        secret=env("R2_SECRET_ACCESS_KEY"),
        endpoint_url=f"https://{env('CF_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        client_kwargs={"region_name": "auto"},
    )


def deaccumulate_since_init(da: xr.DataArray) -> xr.DataArray:
    # ICON asob_s/athb_s are time-averaged since model start. Convert to the
    # period-mean flux over (t_prev, t] so downstream PET is a per-step rate.
    # Step can be encoded either as timedelta64 or numeric forecast hour.
    step_coord = da["step"]
    if np.issubdtype(step_coord.dtype, np.timedelta64):
        steps = (step_coord / np.timedelta64(1, "h")).astype("float32")
    else:
        steps = step_coord.astype("float32")
    prev_steps = steps.shift(step=1).fillna(0.0)
    dt = steps - prev_steps
    prev = da.shift(step=1).fillna(0.0)
    dt_safe = xr.where(dt > 0, dt, 1.0)
    inst = (da * steps - prev * prev_steps) / dt_safe
    inst = xr.where(dt > 0, inst, 0.0)
    return inst.astype(da.dtype)


def squeeze_height(da: xr.DataArray) -> xr.DataArray:
    for dim in da.dims:
        if "height" in str(dim).lower():
            return da.isel({dim: 0}, drop=True)
    return da


def load_param(fs: s3fs.S3FileSystem, run_dt: str, param: str) -> xr.DataArray:
    root = f"{BUCKET}/runs/{run_dt}/{param}"
    if not fs.exists(root):
        raise FileNotFoundError(f"Missing parameter store: s3://{root}")
    store = s3fs.S3Map(root=root, s3=fs, check=False)
    ds = xr.open_zarr(store, consolidated=True, chunks="auto")
    if param not in ds.data_vars:
        if len(ds.data_vars) == 1:
            only = list(ds.data_vars)[0]
            da = ds[only]
        else:
            raise KeyError(f"Parameter variable '{param}' not found in {root}")
    else:
        da = ds[param]
    da = squeeze_height(da)

    chunk_map: dict[str, int] = {}
    if "step" in da.dims:
        chunk_map["step"] = -1
    for lat_name in ("lat", "latitude"):
        if lat_name in da.dims:
            chunk_map[lat_name] = CHUNK_LAT
            break
    for lon_name in ("lon", "longitude"):
        if lon_name in da.dims:
            chunk_map[lon_name] = CHUNK_LON
            break
    if chunk_map:
        da = da.chunk(chunk_map)
    return da


def resolve_run_dt(fs: s3fs.S3FileSystem, run_dt: str | None) -> str:
    if run_dt:
        return run_dt
    latest_key = f"{BUCKET}/latest/_LATEST_RUN"
    with fs.open(latest_key, "r") as f:
        return f.read().strip()


def compute_pet_pm(
    t_k: xr.DataArray,
    td_k: xr.DataArray,
    u10: xr.DataArray,
    v10: xr.DataArray,
    asob: xr.DataArray,
    athb: xr.DataArray,
    ps_pa: xr.DataArray,
) -> xr.DataArray:
    # Air temperature / dew point (degC)
    t_c = t_k - 273.15
    td_c = td_k - 273.15

    # Vapor pressure terms (kPa)
    es = 0.6108 * np.exp((17.27 * t_c) / (t_c + 237.3))
    ea = 0.6108 * np.exp((17.27 * td_c) / (td_c + 237.3))
    vpd = xr.where(es - ea < 0, 0, es - ea)

    # Wind speed at 2m from 10m components
    u10_abs = np.hypot(u10, v10)
    wind_factor = 4.87 / np.log(67.8 * 10.0 - 5.42)  # FAO-56 z=10m -> u2
    u2 = u10_abs * wind_factor

    # Net radiation (W m^-2) -> MJ m^-2 h^-1
    rn_wm2 = asob + athb
    rn_mj_h = rn_wm2 * 0.0036

    # Psychrometric and slope terms
    p_kpa = ps_pa / 1000.0
    gamma = 0.000665 * p_kpa
    delta = (4098.0 * es) / ((t_c + 237.3) ** 2)

    # Soil heat flux G ~ 0 for forecast-scale operational product
    g = 0.0

    # FAO-56 Penman-Monteith form (hourly-friendly using local flux timestep)
    num = 0.408 * delta * (rn_mj_h - g) + gamma * (37.0 / (t_c + 273.15)) * u2 * vpd
    den = delta + gamma * (1.0 + 0.34 * u2)
    et0 = num / den
    et0 = xr.where(np.isfinite(et0), et0, np.nan)
    et0 = xr.where(et0 < 0, 0, et0)
    return et0.astype("float32").rename("pet_pm")


def write_pet(fs: s3fs.S3FileSystem, run_dt: str, pet: xr.DataArray) -> None:
    ds = xr.Dataset({"pet_pm": pet})
    zarr_path = f"{BUCKET}/runs/{run_dt}/pet_pm"
    store = s3fs.S3Map(root=zarr_path, s3=fs, check=False)

    pet_dims = ds["pet_pm"].dims
    chunks_by_dim: list[int] = []
    for dim in pet_dims:
        if dim == "step":
            chunks_by_dim.append(ds["pet_pm"].sizes[dim])
        elif dim in ("lat", "latitude"):
            chunks_by_dim.append(CHUNK_LAT)
        elif dim in ("lon", "longitude"):
            chunks_by_dim.append(CHUNK_LON)
        else:
            chunks_by_dim.append(1)
    chunks = tuple(chunks_by_dim)
    ds["pet_pm"].encoding = {
        "compressor": GZip(level=5),
        "dtype": "float32",
        "chunks": chunks,
        "dimension_separator": ".",
        "_FillValue": None,
    }
    ds.to_zarr(store=store, mode="w", consolidated=True, zarr_version=2)
    with fs.open(f"{zarr_path}/_SUCCESS", "w") as f:
        f.write("ok\n")
    log.info("PET written: s3://%s", zarr_path)


def update_spatial_index(fs: s3fs.S3FileSystem, run_dt: str) -> None:
    key = f"{BUCKET}/spatial_index.json"
    if fs.exists(key):
        with fs.open(key, "r") as f:
            idx = json.load(f)
    else:
        idx = {"run": run_dt, "params": []}
        log.info("spatial_index.json missing; creating a new one for run=%s", run_dt)

    if idx.get("run") != run_dt:
        log.warning("spatial_index run=%s differs from target run=%s; leaving unchanged", idx.get("run"), run_dt)
        return

    params = idx.get("params", [])
    if "pet_pm" not in params:
        params.append("pet_pm")
        idx["params"] = params
        with fs.open(key, "w") as f:
            json.dump(idx, f)
        log.info("spatial_index.json updated with pet_pm")
    else:
        log.info("spatial_index.json already includes pet_pm")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Derive PET from promoted ICON run")
    p.add_argument("--run-dt", default=os.environ.get("RUN_DT"), help="YYYYMMDDHH (default: latest)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    fs = make_r2_fs()
    run_dt = resolve_run_dt(fs, args.run_dt)
    log.info("Derive PET for run=%s", run_dt)

    loaded: dict[str, xr.DataArray] = {}
    for p in REQUIRED_PARAMS:
        loaded[p] = load_param(fs, run_dt, p)
        log.info("Loaded %s", p)

    asob_inst = deaccumulate_since_init(loaded["asob_s"])
    athb_inst = deaccumulate_since_init(loaded["athb_s"])
    log.info("De-accumulated asob_s/athb_s to per-step mean fluxes")

    log.info("Computing PET from chunked source arrays")
    pet = compute_pet_pm(
        loaded["t_2m"],
        loaded["td_2m"],
        loaded["u_10m"],
        loaded["v_10m"],
        asob_inst,
        athb_inst,
        loaded["ps"],
    )
    log.info("PET expression built; writing Zarr output")
    write_pet(fs, run_dt, pet)
    update_spatial_index(fs, run_dt)
    log.info("Done.")


if __name__ == "__main__":
    main()
