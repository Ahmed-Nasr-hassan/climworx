#!/usr/bin/env python3
"""
Local smoke test: exercises the full ETL flow (download → decompress → regrid → Zarr)
without pushing to R2. Tests 2 forecast steps to keep it fast.
"""
import bz2
import shutil
import tarfile
import tempfile
from pathlib import Path

import numpy as np
import requests
import xarray as xr
from numcodecs import Blosc

# Import ETL functions
from etl import (
    regrid_grib_bytes,
    build_urls,
    resolve_available_run,
    make_encoding,
    make_chunks,
    PARAM_BOUNDS,
)

PARAM = "tot_prec"
MODEL = "icon"
TEST_STEPS = [0, 1]  # Only test first 2 steps for speed

# DWD EASY tarball (not used by production etl.py — only for this smoke test)
DWD_EASY_WEIGHTS_URL = (
    "https://opendata.dwd.de/weather/lib/cdo/ICON_GLOBAL2WORLD_0125_EASY.tar.bz2"
)
_EASY_CACHE = Path(tempfile.gettempdir()) / "icon_weights"
_EASY_DIR = _EASY_CACHE / "ICON_GLOBAL2WORLD_0125_EASY"
_EASY_TARGET_GRID = _EASY_DIR / "target_grid_world_0125.txt"
_EASY_WEIGHTS_NC = _EASY_DIR / "weights_icogl2world_0125.nc"


def ensure_easy_weights() -> tuple[Path, Path]:
    """Download/cache DWD ICON_GLOBAL2WORLD_0125_EASY remap files (~50 MB once)."""
    if _EASY_TARGET_GRID.is_file() and _EASY_WEIGHTS_NC.is_file():
        return _EASY_TARGET_GRID, _EASY_WEIGHTS_NC

    print("Downloading ICON remap weights from DWD (~50 MB)…")
    _EASY_CACHE.mkdir(parents=True, exist_ok=True)
    resp = requests.get(DWD_EASY_WEIGHTS_URL, stream=True, timeout=120)
    resp.raise_for_status()
    archive = _EASY_CACHE / "weights.tar.bz2"
    with open(archive, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
    with tarfile.open(archive, "r:bz2") as tar:
        tar.extractall(path=_EASY_CACHE)
    archive.unlink()
    if not _EASY_WEIGHTS_NC.is_file():
        raise RuntimeError(f"Weights missing after extract: {_EASY_WEIGHTS_NC}")
    print(f"  Cached at {_EASY_CACHE}\n")
    return _EASY_TARGET_GRID, _EASY_WEIGHTS_NC


def encoding_with_chunks(ds: xr.Dataset, chunk_spec: dict[str, int]) -> dict:
    """Zarr encoding with per-variable chunk tuples (avoids xarray .chunk() → dask)."""
    encoding = make_encoding(ds)
    for var in ds.data_vars:
        da = ds[var]
        enc_chunks = []
        for dim in da.dims:
            c = chunk_spec[dim]
            enc_chunks.append(int(ds.sizes[dim]) if c == -1 else int(c))
        encoding[var] = {**encoding[var], "chunks": tuple(enc_chunks)}
    return encoding


def main():
    # 1. Resolve available run
    print("=" * 60)
    print("STEP 1: Resolve available run")
    print("=" * 60)
    run_dt = resolve_available_run("20260403", "18", PARAM, MODEL)
    print(f"  Resolved run: {run_dt}\n")

    # 2. Download + decompress + regrid
    print("=" * 60)
    print("STEP 2: Download → Decompress → Regrid")
    print("=" * 60)
    easy_tg, easy_w = ensure_easy_weights()
    step_datasets = []
    for step in TEST_STEPS:
        urls = build_urls(MODEL, run_dt, PARAM, step)
        url = urls[0]
        print(f"  Downloading step {step}: {url}")
        resp = requests.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        compressed = b"".join(resp.iter_content(chunk_size=1 << 20))
        raw = bz2.decompress(compressed)
        print(f"    compressed={len(compressed):,}B → decompressed={len(raw):,}B")

        print(f"  Regridding step {step} via CDO remapdis...")
        ds = regrid_grib_bytes(
            raw, PARAM, remap_target_grid=easy_tg, remap_weights=easy_w
        )
        print(f"    Dimensions: {dict(ds.sizes)}")
        print(f"    Variables: {list(ds.data_vars)}")
        print(f"    Coords: {list(ds.coords)}")

        # Check latitude ordering (should be north→south after our fix)
        if "latitude" in ds.coords:
            lats = ds.latitude.values
            print(f"    Lat range: {lats[0]:.2f} → {lats[-1]:.2f} (expecting 90 → -90)")
            assert lats[0] > lats[-1], "ERROR: Latitude should be north→south!"
            print(f"    ✓ Latitude is north→south")
        else:
            print(f"    WARNING: 'latitude' not in coords, got: {list(ds.coords)}")

        if "longitude" in ds.coords:
            lons = ds.longitude.values
            print(f"    Lon range: {lons[0]:.2f} → {lons[-1]:.2f} (expecting -180 → 179.875)")
        else:
            print(f"    WARNING: 'longitude' not in coords, got: {list(ds.coords)}")

        ds = ds.expand_dims({"step": [step]})
        step_datasets.append(ds)
        print()

    # 3. Concatenate along step dimension
    print("=" * 60)
    print("STEP 3: Concatenate steps")
    print("=" * 60)
    combined = xr.concat(step_datasets, dim="step")
    print(f"  Combined shape: {dict(combined.sizes)}")
    print(f"  Data vars: {list(combined.data_vars)}")
    print()

    # 4. Chunk + compress → local Zarr
    print("=" * 60)
    print("STEP 4: Chunk + Compress → local Zarr")
    print("=" * 60)
    chunks = make_chunks(ensemble=False)
    encoding = encoding_with_chunks(combined, chunks)
    print(f"  Chunks: {chunks}")
    print(f"  Encoding: {encoding}")

    out_dir = Path.cwd() / "outputs"
    out_dir.mkdir(parents=True, exist_ok=True)
    zarr_path = out_dir / "test_output.zarr"
    if zarr_path.exists():
        shutil.rmtree(zarr_path)
    combined.to_zarr(
        str(zarr_path),
        mode="w",
        consolidated=True,
        encoding=encoding,
        zarr_format=2,
    )
    print(f"  Written to: {zarr_path}")

    # List Zarr contents
    zarr_files = sorted(zarr_path.rglob("*"))
    print(f"  Zarr store contains {len(zarr_files)} files/dirs")
    for f in zarr_files[:20]:
        if f.is_file():
            print(f"    {f.relative_to(zarr_path)} ({f.stat().st_size:,}B)")

    # 5. Read back and validate
    print()
    print("=" * 60)
    print("STEP 5: Read back + Validate")
    print("=" * 60)
    ds_check = xr.open_zarr(str(zarr_path), consolidated=True)
    print(f"  Dims: {dict(ds_check.sizes)}")
    print(f"  Steps: {ds_check.coords['step'].values.tolist()}")

    bounds = PARAM_BOUNDS.get(PARAM)
    for var in ds_check.data_vars:
        arr = ds_check[var].values
        vmin, vmax = float(np.nanmin(arr)), float(np.nanmax(arr))
        all_nan = bool(np.all(np.isnan(arr)))
        print(f"  {var}: min={vmin:.4f}, max={vmax:.4f}, all_nan={all_nan}, shape={arr.shape}")
        if bounds:
            in_bounds = vmin >= bounds[0] and vmax <= bounds[1]
            print(f"    Bounds check [{bounds[0]}, {bounds[1]}]: {'✓ PASS' if in_bounds else '✗ FAIL'}")

    print()
    print("=" * 60)
    print("ALL CHECKS PASSED — pipeline is ready to push to R2")
    print("=" * 60)


if __name__ == "__main__":
    main()
