#!/usr/bin/env python3
"""
DWD Availability Probe
======================
Fast HEAD-request scan of all parameter × forecast-step combinations on DWD
opendata.  Outputs a JSON manifest so downstream ETL matrix jobs know exactly
which files exist *before* any heavy downloading begins.

Typical runtime: ~30-60 s for ~1 500 URLs with 50 concurrent workers.

Usage:
    python probe_dwd.py --run-dt 2026041500 \
        --params t_2m tot_prec u_10m v_10m pmsl clct relhum_2m aswdir_s \
                 td_2m vmax_10m asob_s athb_s alhfl_s ps runoff_s runoff_g \
        --output manifest.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from etl import get_all_steps, build_urls, MAX_MISSING_STEP_FRACTION

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("probe-dwd")

# ---------------------------------------------------------------------------
# Rate-limiting / retry constants
# ---------------------------------------------------------------------------

MAX_WORKERS = 12          # concurrent HEAD requests (keep polite to DWD)
HEAD_TIMEOUT = 15         # seconds per request
HEAD_RETRIES = 4          # retry transient failures (not 404s)
HEAD_BACKOFF_BASE = 1.0   # first retry wait (seconds); doubles each attempt
HEAD_BACKOFF_MAX = 15.0   # cap per-retry wait
REQUEST_INTERVAL = 0.05   # seconds between request starts per worker (throttle)


# ---------------------------------------------------------------------------
# Probe logic
# ---------------------------------------------------------------------------


def _probe_one(url: str, throttle: threading.Lock) -> bool:
    """Return True if DWD responds 200 to a HEAD request.

    Retries on transient errors (429, 5xx, network) with exponential backoff.
    Respects ``Retry-After`` header when the server sends 429.
    """
    for attempt in range(1, HEAD_RETRIES + 1):
        # Throttle: serialize the *start* of each request briefly so we don't
        # stampede DWD with MAX_WORKERS simultaneous connections at once.
        with throttle:
            time.sleep(REQUEST_INTERVAL)

        try:
            resp = requests.head(url, timeout=HEAD_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                return False
            # 429 — respect Retry-After if present
            if resp.status_code == 429:
                retry_after = _parse_retry_after(resp)
                wait = retry_after if retry_after else _backoff(attempt)
                log.debug("429 on %s — waiting %.1fs (attempt %d)", url, wait, attempt)
                time.sleep(wait)
                continue
            # Other server errors (5xx) — exponential backoff
            if attempt < HEAD_RETRIES:
                wait = _backoff(attempt)
                log.debug("HTTP %d on %s — retrying in %.1fs", resp.status_code, url, wait)
                time.sleep(wait)
        except requests.RequestException:
            if attempt < HEAD_RETRIES:
                wait = _backoff(attempt)
                time.sleep(wait)
    return False


def _backoff(attempt: int) -> float:
    """Exponential backoff clamped to HEAD_BACKOFF_MAX."""
    return min(HEAD_BACKOFF_BASE * (2 ** (attempt - 1)), HEAD_BACKOFF_MAX)


def _parse_retry_after(resp: requests.Response) -> float | None:
    """Parse ``Retry-After`` header (seconds).  Returns None if absent/invalid."""
    val = resp.headers.get("Retry-After")
    if val is None:
        return None
    try:
        return max(float(val), 1.0)
    except (ValueError, TypeError):
        return None


def probe_all(
    run_dt: str,
    params: list[str],
    model: str = "icon",
) -> dict:
    """
    Probe DWD for every param × step combination.

    Uses *MAX_WORKERS* concurrent HEAD requests with per-request throttling
    (``REQUEST_INTERVAL``) and exponential-backoff retries to stay within
    DWD's rate limits.

    Returns a manifest dict:
    {
      "run_dt": "2026041500",
      "model_run": "00",
      "total_steps": 97,
      "params": {
        "t_2m":  {"available": [0,1,...], "missing": [42,43]},
        ...
      }
    }
    """
    model_run = run_dt[8:10]
    all_steps = get_all_steps(model_run)

    # Build (param, step, url) work items
    work: list[tuple[str, int, str]] = []
    for param in params:
        for step in all_steps:
            urls = build_urls(model, run_dt, param, step)
            work.append((param, step, urls[0]))

    log.info(
        "Probing %d URLs (%d params × %d steps) with %d workers …",
        len(work), len(params), len(all_steps), MAX_WORKERS,
    )
    t0 = time.monotonic()

    results: dict[str, dict[str, list[int]]] = {
        p: {"available": [], "missing": []} for p in params
    }

    # Shared lock used by workers to serialize request starts (throttle)
    throttle = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        future_to_info = {
            pool.submit(_probe_one, url, throttle): (param, step)
            for param, step, url in work
        }
        done = 0
        for fut in as_completed(future_to_info):
            param, step = future_to_info[fut]
            try:
                exists = fut.result()
            except Exception:
                exists = False
            bucket = "available" if exists else "missing"
            results[param][bucket].append(step)
            done += 1
            if done % 200 == 0:
                log.info("  … %d / %d probed", done, len(work))

    elapsed = time.monotonic() - t0
    log.info("Probe finished in %.1fs", elapsed)

    # Sort step lists for deterministic output
    for param in results:
        results[param]["available"].sort()
        results[param]["missing"].sort()

    manifest = {
        "run_dt": run_dt,
        "model_run": model_run,
        "total_steps": len(all_steps),
        "params": results,
    }
    return manifest


# ---------------------------------------------------------------------------
# Reporting + early-exit logic
# ---------------------------------------------------------------------------


def report_and_check(manifest: dict) -> list[str]:
    """
    Log a summary table and return a list of params that are too incomplete
    (> MAX_MISSING_STEP_FRACTION missing).  Caller decides whether to abort.
    """
    total = manifest["total_steps"]
    blocked: list[str] = []

    log.info("─── DWD Availability Summary (run %s) ───", manifest["run_dt"])
    for param, info in manifest["params"].items():
        avail = len(info["available"])
        miss = len(info["missing"])
        frac = miss / total if total else 0
        status = "OK" if frac <= MAX_MISSING_STEP_FRACTION else "BLOCKED"
        if status == "BLOCKED":
            blocked.append(param)
        log.info(
            "  %-12s  %3d / %d available  (%2d missing, %.0f%%)  [%s]",
            param, avail, total, miss, frac * 100, status,
        )
    log.info("────────────────────────────────────────────")

    if blocked:
        log.warning(
            "%d param(s) exceed %.0f%% missing threshold: %s",
            len(blocked), MAX_MISSING_STEP_FRACTION * 100, blocked,
        )
    return blocked


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    p = argparse.ArgumentParser(
        description="Probe DWD for ICON GRIB2 file availability",
    )
    p.add_argument("--run-dt", required=True, help="Run datetime YYYYMMDDHH")
    p.add_argument("--params", nargs="+", required=True, help="Parameters to probe")
    p.add_argument("--output", default="manifest.json", help="Output manifest path")
    p.add_argument(
        "--fail-on-blocked",
        action="store_true",
        help="Exit non-zero if any param exceeds the missing-step threshold",
    )
    args = p.parse_args()

    manifest = probe_all(args.run_dt, args.params)
    blocked = report_and_check(manifest)

    with open(args.output, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info("Manifest written → %s", args.output)

    if args.fail_on_blocked and blocked:
        log.error(
            "Aborting: %d param(s) too incomplete to proceed: %s", len(blocked), blocked,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
