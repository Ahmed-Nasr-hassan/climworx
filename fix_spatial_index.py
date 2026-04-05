"""
One-shot script to regenerate spatial_index.json from the actual data in R2.
Use when spatial_index.json is stale (e.g. after manually updating Zarr in R2).

Usage:
    RUN_DATE=2026040500 python fix_spatial_index.py
    # or omit RUN_DATE to read from latest/_LATEST_RUN
"""
import json
import os
import sys
import s3fs
import xarray as xr

BUCKET = "icon-global-databank"
PARAMS = ["tot_prec", "t_2m", "u_10m", "v_10m", "pmsl", "clct", "relhum_2m", "aswdir_s"]

GRID = {
    "lat_min": -90.0, "lat_max": 90.0,
    "lon_min": -180.0, "lon_max": 180.0,
    "nx": 2879, "ny": 1441,
    "resolution": 0.125,
}
CHUNKS = {"latitude": 100, "longitude": 100}


def make_fs() -> s3fs.S3FileSystem:
    def env(k):
        v = os.environ.get(k)
        if not v:
            sys.exit(f"Missing env var: {k}")
        return v
    return s3fs.S3FileSystem(
        key=env("R2_ACCESS_KEY_ID"),
        secret=env("R2_SECRET_ACCESS_KEY"),
        endpoint_url=f"https://{env('CF_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        client_kwargs={"region_name": "auto"},
    )


def resolve_run(fs: s3fs.S3FileSystem) -> str:
    run_dt = os.environ.get("RUN_DATE")
    if run_dt:
        print(f"Using RUN_DATE from env: {run_dt}")
        return run_dt
    key = f"{BUCKET}/latest/_LATEST_RUN"
    with fs.open(key) as f:
        run_dt = f.read().decode().strip()
    print(f"Latest run from R2: {run_dt}")
    return run_dt


def read_steps(fs: s3fs.S3FileSystem, run_dt: str) -> list[int]:
    for param in PARAMS:
        try:
            store = s3fs.S3Map(root=f"{BUCKET}/runs/{run_dt}/{param}", s3=fs, check=False)
            ds = xr.open_zarr(store, consolidated=True)
            if "step" in ds.coords:
                steps = sorted(int(s) for s in ds.coords["step"].values)
                ds.close()
                print(f"  Read {len(steps)} steps from {param}: {steps[:5]}...{steps[-3:]}")
                return steps
            ds.close()
        except Exception as e:
            print(f"  Skipping {param}: {e}")
    sys.exit("Could not read steps from any parameter store.")


def get_present_params(fs: s3fs.S3FileSystem, run_dt: str) -> list[str]:
    present = []
    for param in PARAMS:
        success_key = f"{BUCKET}/runs/{run_dt}/{param}/_SUCCESS"
        if fs.exists(success_key):
            present.append(param)
    print(f"Params with _SUCCESS marker: {present}")
    return present


def main():
    fs = make_fs()
    run_dt = resolve_run(fs)
    params = get_present_params(fs, run_dt)
    if not params:
        sys.exit(f"No promoted params found under runs/{run_dt}/. Wrong run ID?")

    steps = read_steps(fs, run_dt)

    index = {
        "run": run_dt,
        "grid": GRID,
        "chunks": CHUNKS,
        "params": params,
        "steps": steps,
    }

    key = f"{BUCKET}/spatial_index.json"
    with fs.open(key, "w") as f:
        json.dump(index, f)

    print(f"\nspatial_index.json written → run={run_dt}, {len(steps)} steps, params={params}")


if __name__ == "__main__":
    main()
