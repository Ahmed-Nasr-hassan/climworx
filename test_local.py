#!/usr/bin/env python3
"""
Local smoke test: exercises the full ETL flow (download → decompress → regrid → Zarr)
without pushing to R2. Tests 2 forecast steps to keep it fast.

Remap weights are built like GitHub Actions (gendis/genycon + sample GRIB2). If your local
CDO cannot read ICON unstructured GRIB (common on macOS), the script falls back to DWD’s
ICON_GLOBAL2WORLD_0125_EASY precomputed weights so the smoke test still runs.
"""
import bz2
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timedelta, timezone
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
    WEIGHTS_MAP,
)

PARAM = "tot_prec"
MODEL = "icon"
TEST_STEPS = [0, 1]  # Only test first 2 steps for speed

_REMAP_CACHE = Path(tempfile.gettempdir()) / "climworx_test_remap"
_ICON_GRID_URL = "https://opendata.dwd.de/weather/lib/cdo/icon_grid_0047_R19B07_L.nc.bz2"
_EASY_TARBALL_URL = (
    "https://opendata.dwd.de/weather/lib/cdo/ICON_GLOBAL2WORLD_0125_EASY.tar.bz2"
)
_EASY_SUBDIR = "ICON_GLOBAL2WORLD_0125_EASY"

# Must match etl.py GRID_NX / GRID_NY and CI workflow target_grid_world_0125.txt
_TARGET_GRID_SPEC = """gridtype  = lonlat
xsize     = 2879
ysize     = 1441
xfirst    = -180.0
xinc      = 0.125
yfirst    = 90.0
yinc      = -0.125
"""


def _stream_download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=120) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)


def _download_sample_t2m_grib_bz2(run_date_yyyymmdd: str) -> bytes:
    """
    Same discovery order as .github/workflows (RUN_DATE × 4 cycles, then 15 days × 4).
    Returns compressed .bz2 bytes.
    """
    urls: list[str] = []
    for hh in ("18", "12", "06", "00"):
        urls.append(
            f"https://opendata.dwd.de/weather/nwp/icon/grib/{hh}/t_2m/"
            f"icon_global_icosahedral_single-level_{run_date_yyyymmdd}{hh}_000_T_2M.grib2.bz2"
        )
    for days_ago in range(15):
        d = (datetime.now(timezone.utc) - timedelta(days=days_ago)).strftime("%Y%m%d")
        for hh in ("18", "12", "06", "00"):
            u = (
                f"https://opendata.dwd.de/weather/nwp/icon/grib/{hh}/t_2m/"
                f"icon_global_icosahedral_single-level_{d}{hh}_000_T_2M.grib2.bz2"
            )
            if u not in urls:
                urls.append(u)

    last_exc: Exception | None = None
    for url in urls:
        try:
            with requests.get(url, stream=True, timeout=120) as resp:
                if resp.status_code != 200:
                    continue
                data = b"".join(resp.iter_content(chunk_size=1 << 20))
            print(f"    Sample GRIB2: {url}")
            return data
        except requests.RequestException as exc:
            last_exc = exc
    raise RuntimeError(
        "Could not download t_2m step-0 sample GRIB2 (.github/workflows search exhausted)"
    ) from last_exc


def _run_cdo(argv: list[str]) -> None:
    r = subprocess.run(argv, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"CDO failed: {' '.join(argv)}\n{r.stderr}")


def _run_cdo_shell(cmd: str) -> None:
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"CDO failed: {cmd}\n{r.stderr}")


def _populate_cache_from_dwd_easy(
    cache: Path,
    target: Path,
    w_dist: Path,
    w_con: Path,
) -> None:
    """Precomputed EASY weights + target grid (single .nc copied to both weight names)."""
    easy_root = cache / "easy_extract"
    if easy_root.is_dir():
        shutil.rmtree(easy_root, ignore_errors=True)
    easy_root.mkdir(parents=True, exist_ok=True)
    arc = easy_root / "easy.tar.bz2"
    _stream_download(_EASY_TARBALL_URL, arc)
    with tarfile.open(arc, "r:bz2") as tar:
        tar.extractall(path=easy_root)
    arc.unlink(missing_ok=True)
    sub = easy_root / _EASY_SUBDIR
    src_tg = sub / "target_grid_world_0125.txt"
    src_w = sub / "weights_icogl2world_0125.nc"
    if not src_tg.is_file() or not src_w.is_file():
        raise RuntimeError(f"EASY tarball layout unexpected: {sub}")
    shutil.copy2(src_tg, target)
    shutil.copy2(src_w, w_dist)
    shutil.copy2(src_w, w_con)
    shutil.rmtree(easy_root, ignore_errors=True)


def ensure_ci_style_remap_weights(param: str, run_dt: str) -> tuple[Path, Path]:
    """
    Build or reuse cached weights_distance.nc + weights_conservative.nc (same logic as CI).
    ``run_dt`` is YYYYMMDDHH from resolve_available_run; sample GRIB2 uses the same
    cycle as the test data (grib/{hh}/…_{run_dt}_000_…) so the file exists when the run does.

    Returns (target_grid_world_0125.txt path, weights file path for ``param``).
    """
    cache = _REMAP_CACHE
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / "target_grid_world_0125.txt"
    icon_nc = cache / "icon_grid_R19B07_L.nc"
    sample_grib = cache / "sample.grib2"
    w_dist = cache / "weights_distance.nc"
    w_con = cache / "weights_conservative.nc"
    weights_for_param = cache / WEIGHTS_MAP.get(param, "weights_distance.nc")

    if target.is_file() and w_dist.is_file() and w_con.is_file():
        print(f"  Using cached remap weights under {cache}\n")
        return target, weights_for_param

    print("=" * 40)
    print("Remap assets (mirror CI workflow)")
    print("=" * 40)

    print("  [1/4] Download native grid …")
    bz_path = cache / "icon_grid.nc.bz2"
    _stream_download(_ICON_GRID_URL, bz_path)
    raw = bz2.decompress(bz_path.read_bytes())
    icon_nc.write_bytes(raw)
    bz_path.unlink(missing_ok=True)

    run_date = run_dt[:8]
    print(f"  [2/4] Download sample GRIB2 template (t_2m step 0; try {run_date}×4h then 15d×4h) …")
    sample_bz_bytes = _download_sample_t2m_grib_bz2(run_date)
    sample_grib.write_bytes(bz2.decompress(sample_bz_bytes))

    print("  [3/4] Write target_grid_world_0125.txt …")
    target.write_text(_TARGET_GRID_SPEC)

    print("  [4/4] cdo gendis + genycon …")
    tg, ic, sg, wd, wc = (str(p) for p in (target, icon_nc, sample_grib, w_dist, w_con))
    try:
        try:
            _run_cdo_shell(f'cdo -O gendis,"{tg}" -setgrid,"{ic}" "{sg}" "{wd}"')
            _run_cdo_shell(f'cdo -O genycon,"{tg}" -setgrid,"{ic}" "{sg}" "{wc}"')
        except RuntimeError as exc:
            print("  ! -setgrid + icon NC failed. Retrying with GRIB2 only.")
            print(f"    ({exc})")
            try:
                _run_cdo_shell(f'cdo -O gendis,"{tg}" "{sg}" "{wd}"')
                _run_cdo_shell(f'cdo -O genycon,"{tg}" "{sg}" "{wc}"')
            except RuntimeError as exc2:
                print("  ! Local CDO cannot build ICON weights; using DWD EASY tarball (~50 MB).")
                print(f"    ({exc2})")
                w_dist.unlink(missing_ok=True)
                w_con.unlink(missing_ok=True)
                _populate_cache_from_dwd_easy(cache, target, w_dist, w_con)
    finally:
        icon_nc.unlink(missing_ok=True)
        sample_grib.unlink(missing_ok=True)

    print(f"  Done. Cache: {cache}\n")

    return target, weights_for_param


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
    target_grid, weights_nc = ensure_ci_style_remap_weights(PARAM, run_dt)
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
            raw, PARAM, remap_target_grid=target_grid, remap_weights=weights_nc
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
