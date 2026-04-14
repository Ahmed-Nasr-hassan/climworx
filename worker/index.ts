/**
 * ICON Global Weather API — Cloudflare Worker
 * ============================================
 * GET /?lat=52.52&lon=13.405&param=t_2m[&ensemble=true][&run=2026040300]
 *
 * Data flow:
 *   1. Resolve run ID (KV cache of _LATEST_RUN, refreshed every 5 min)
 *   2. Compute Zarr chunk indices from lat/lon
 *   3. Byte-range read of the required chunk from R2
 *   4. Decompress GZip chunk in-memory
 *   5. Extract single grid-point time series
 *   6. Return JSON; cache response for 1 hour via Cache API
 */

// ---------------------------------------------------------------------------
// Cloudflare bindings (declared in wrangler.toml / Terraform)
// ---------------------------------------------------------------------------
export interface Env {
  DATABANK: R2Bucket;   // R2 bucket binding — never public
  METADATA: KVNamespace; // KV for sub-ms metadata reads
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------
const GRID_NX = 2879;      // DWD official: -180 to 179.875 at 0.125° (no wrap-around duplicate)
const GRID_NY = 1441;      // 180 / 0.125 + 1
const CHUNK_LAT = 100;
const CHUNK_LON = 100;
const LATEST_RUN_KEY = "latest_run";
const LATEST_RUN_TTL_MS = 5 * 60 * 1000; // refresh every 5 minutes
const CACHE_TTL_SECONDS = 3600;           // 1 hour

const ALLOWED_PARAMS = new Set([
  "t_2m", "tot_prec", "u_10m", "v_10m",
  "pmsl", "clct", "relhum_2m", "aswdir_s",
]);

const PARAM_UNITS: Record<string, string> = {
  t_2m:      "K",
  tot_prec:  "mm",
  u_10m:     "m/s",
  v_10m:     "m/s",
  pmsl:      "Pa",
  clct:      "%",
  relhum_2m: "%",
  aswdir_s:  "W/m²",
};

// Rate-limiting: simple per-IP counter stored in KV (best-effort, not atomic)
const RATE_LIMIT_RPM = 100;
const RATE_LIMIT_WINDOW_S = 60;

// ---------------------------------------------------------------------------
// In-memory cache for latest run ID (avoids KV on every request)
// ---------------------------------------------------------------------------
let cachedLatestRun: { runId: string; fetchedAt: number } | null = null;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ZarrArrayMeta {
  chunks: number[];
  dtype: string;
  compressor: { id: string; cname: string; clevel: number } | null;
  shape: number[];
  order: string;
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------
function jsonError(status: number, message: string): Response {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// ---------------------------------------------------------------------------
// Input validation
// ---------------------------------------------------------------------------
interface ParsedQuery {
  lat: number;
  lon: number;
  param: string;
  ensemble: boolean;
  runOverride: string | null;
}

function parseAndValidate(url: URL): ParsedQuery | Response {
  const latStr = url.searchParams.get("lat");
  const lonStr = url.searchParams.get("lon");
  const param  = url.searchParams.get("param");

  if (!latStr || !lonStr || !param) {
    return jsonError(400, "Missing required parameters: lat, lon, param");
  }

  const lat = parseFloat(latStr);
  const lon = parseFloat(lonStr);

  if (isNaN(lat) || lat < -90 || lat > 90) {
    return jsonError(400, "lat must be a number in [-90, 90]");
  }
  if (isNaN(lon) || lon < -180 || lon > 360) {
    return jsonError(400, "lon must be a number in [-180, 360]");
  }
  if (!ALLOWED_PARAMS.has(param)) {
    return jsonError(400, `param must be one of: ${[...ALLOWED_PARAMS].join(", ")}`);
  }

  const runRaw = url.searchParams.get("run");
  if (runRaw && !/^\d{10}$/.test(runRaw)) {
    return jsonError(400, "run must be in YYYYMMDDHH format (10 digits)");
  }

  const ensemble = url.searchParams.get("ensemble") === "true";

  return { lat, lon, param, ensemble, runOverride: runRaw };
}

// ---------------------------------------------------------------------------
// Latest run resolution
// ---------------------------------------------------------------------------
async function resolveRunId(
  env: Env,
  runOverride: string | null,
  forceRefresh = false,
): Promise<string> {
  if (runOverride) return runOverride;

  const now = Date.now();
  if (!forceRefresh && cachedLatestRun && now - cachedLatestRun.fetchedAt < LATEST_RUN_TTL_MS) {
    return cachedLatestRun.runId;
  }

  // On forced refresh, skip KV (it may also be stale) and read R2 directly.
  let runId: string | null = forceRefresh ? null : await env.METADATA.get(LATEST_RUN_KEY);

  if (!runId) {
    const obj = await env.DATABANK.get("latest/_LATEST_RUN");
    if (!obj) throw new Error("No latest run found in R2");
    runId = (await obj.text()).trim();
    await env.METADATA.put(LATEST_RUN_KEY, runId, { expirationTtl: 400 });
  }

  runId = runId.trim();
  cachedLatestRun = { runId, fetchedAt: now };
  return runId;
}

// ---------------------------------------------------------------------------
// Spatial chunk index computation
// ---------------------------------------------------------------------------
function latLonToChunkIndex(
  lat: number,
  lon: number,
): { chunkLat: number; chunkLon: number; localLat: number; localLon: number } {
  // Clamp lon to [-180, 180)
  const normLon = lon > 180 ? lon - 360 : lon;

  // Pixel indices in the full grid (y from north→south, x from west→east)
  const pixelLat = Math.round((90 - lat) / 0.125);   // 0-indexed from north
  const pixelLon = Math.round((normLon + 180) / 0.125);

  const clampedPixelLat = Math.min(Math.max(pixelLat, 0), GRID_NY - 1);
  const clampedPixelLon = Math.min(Math.max(pixelLon, 0), GRID_NX - 1);

  const chunkLat = Math.floor(clampedPixelLat / CHUNK_LAT);
  const chunkLon = Math.floor(clampedPixelLon / CHUNK_LON);
  const localLat = clampedPixelLat % CHUNK_LAT;
  const localLon = clampedPixelLon % CHUNK_LON;

  return { chunkLat, chunkLon, localLat, localLon };
}

// ---------------------------------------------------------------------------
// R2 + Zarr helpers
// ---------------------------------------------------------------------------
async function fetchZarrMeta(
  env: Env,
  runId: string,
  param: string,
): Promise<ZarrArrayMeta> {
  // Variable array is nested: runs/{runId}/{param}/{param}/.zarray
  const cacheKey = `${runId}/${param}/${param}/.zarray`;

  // Try KV metadata cache first
  const cached = await env.METADATA.get(cacheKey);
  if (cached) return JSON.parse(cached) as ZarrArrayMeta;

  const obj = await env.DATABANK.get(`runs/${runId}/${param}/${param}/.zarray`);
  if (!obj) throw new Error(`Zarr metadata not found for ${param} in run ${runId}`);
  const text = await obj.text();
  await env.METADATA.put(cacheKey, text, { expirationTtl: 300 });
  return JSON.parse(text) as ZarrArrayMeta;
}

/**
 * Build the R2 key for a Zarr chunk.
 * Zarr v2 chunk key format: {c0}.{c1}.{c2}... (one index per dimension)
 * Leading dimensions (step, optional height/realization) always use chunk index 0.
 * nDims is derived from the .zarray shape length.
 */
function buildChunkKey(
  runId: string,
  param: string,
  varName: string,
  chunkLat: number,
  chunkLon: number,
  nDims: number,
): string {
  // Leading dims (all before lat/lon) are always chunk 0
  const leadingZeros = Array(nDims - 2).fill(0).join(".");
  const prefix = leadingZeros ? `${leadingZeros}.` : "";
  return `runs/${runId}/${param}/${varName}/${prefix}${chunkLat}.${chunkLon}`;
}

/**
 * Decompress a GZip-compressed Zarr chunk using the native DecompressionStream API.
 * ETL uses numcodecs.GZip(level=5); Workers support gzip natively — no WASM needed.
 */
async function decompressChunk(compressed: ArrayBuffer): Promise<Float32Array> {
  const stream = new DecompressionStream("gzip");
  const writer = stream.writable.getWriter();
  const reader = stream.readable.getReader();

  writer.write(new Uint8Array(compressed));
  writer.close();

  const parts: Uint8Array[] = [];
  let totalBytes = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    parts.push(value);
    totalBytes += value.byteLength;
  }

  const out = new Uint8Array(totalBytes);
  let offset = 0;
  for (const part of parts) { out.set(part, offset); offset += part.byteLength; }

  return new Float32Array(out.buffer, out.byteOffset, totalBytes / 4);
}

/**
 * Extract a single lat/lon pixel's time series from a flat Float32Array.
 * Zarr stores data in C-order. Strides are derived from the actual chunk shape
 * so this works regardless of how many leading dimensions exist (e.g. [step, lat, lon]
 * vs [step, height, lat, lon]).
 *
 * For ensemble data the second-to-last-minus-2 dimension is treated as the
 * realization axis and each member value is returned per step.
 */
function extractTimeSeries(
  data: Float32Array,
  localLat: number,
  localLon: number,
  chunkShape: number[],
  ensemble: boolean,
  nMembers = 1,
): Record<number, number | number[]> {
  const nDims = chunkShape.length;
  const nSteps = chunkShape[0];
  const chunkLatSize = chunkShape[nDims - 2];
  const chunkLonSize = chunkShape[nDims - 1];

  // Compute C-order strides from the chunk shape
  const strides = new Array(nDims);
  strides[nDims - 1] = 1;
  for (let i = nDims - 2; i >= 0; i--) {
    strides[i] = strides[i + 1] * chunkShape[i + 1];
  }

  // Base offset for the target pixel within the lat/lon plane
  const pixelOffset = localLat * chunkLonSize + localLon;

  const result: Record<number, number | number[]> = {};

  for (let s = 0; s < nSteps; s++) {
    const stepBase = s * strides[0];

    if (!ensemble) {
      // Sum strides for all middle dims (e.g. height=0) — they're all index 0
      // so they contribute 0 to the offset, leaving just stepBase + pixelOffset.
      result[s] = round4(data[stepBase + pixelOffset]);
    } else {
      const memberValues: number[] = [];
      // Realization is the second dimension for ensemble data
      const memberStride = strides[1];
      for (let m = 0; m < nMembers; m++) {
        const idx = stepBase + m * memberStride + pixelOffset;
        memberValues.push(round4(data[idx]));
      }
      result[s] = memberValues;
    }
  }

  return result;
}

function round4(v: number): number {
  return Math.round(v * 10000) / 10000;
}

// ---------------------------------------------------------------------------
// Rate limiting (best-effort, KV-backed)
// ---------------------------------------------------------------------------
async function checkRateLimit(env: Env, ip: string): Promise<boolean> {
  const key = `rl:${ip}`;
  const raw = await env.METADATA.get(key);
  const count = raw ? parseInt(raw, 10) : 0;
  if (count >= RATE_LIMIT_RPM) return false;
  // Fire-and-forget increment; not atomic but sufficient for soft rate limiting
  await env.METADATA.put(key, String(count + 1), {
    expirationTtl: RATE_LIMIT_WINDOW_S,
  });
  return true;
}

// ---------------------------------------------------------------------------
// Core fetch-from-R2 logic
// ---------------------------------------------------------------------------
async function fetchFromR2(
  env: Env,
  runId: string,
  param: string,
  lat: number,
  lon: number,
  chunkLat: number,
  chunkLon: number,
  localLat: number,
  localLon: number,
  ensemble: boolean,
): Promise<Record<string, unknown>> {
  // By convention the Zarr variable name matches the parameter name.
  const varName = param;

  const meta = await fetchZarrMeta(env, runId, param);
  const chunkKey = buildChunkKey(runId, param, varName, chunkLat, chunkLon, meta.shape.length);

  const obj = await env.DATABANK.get(chunkKey);
  if (!obj) {
    throw new Error(`Chunk not found in R2: ${chunkKey}`);
  }

  const compressed = await obj.arrayBuffer();
  const data = await decompressChunk(compressed);

  const nMembers = ensemble ? (meta.shape[1] ?? 40) : 1;
  const values = extractTimeSeries(
    data, localLat, localLon,
    meta.chunks, ensemble, nMembers,
  );

  return {
    lat,
    lon,
    param,
    run: runId,
    unit: PARAM_UNITS[param] ?? "unknown",
    ...(ensemble ? { members: nMembers } : {}),
    values,
  };
}

// ---------------------------------------------------------------------------
// CORS helpers
// ---------------------------------------------------------------------------
const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, Range",
  "Access-Control-Expose-Headers": "Content-Length, Content-Range",
};

function withCors(response: Response): Response {
  const headers = new Headers(response.headers);
  for (const [k, v] of Object.entries(CORS_HEADERS)) headers.set(k, v);
  return new Response(response.body, { status: response.status, headers });
}

// ---------------------------------------------------------------------------
// Zarr proxy: GET /zarr/{param}/{...path} → R2 runs/{latestRun}/{param}/{path}
// ---------------------------------------------------------------------------
async function handleZarrProxy(
  env: Env,
  pathname: string,
): Promise<Response> {
  // /zarr/spatial_index.json — special case, lives at bucket root
  if (pathname === "/zarr/spatial_index.json") {
    const obj = await env.DATABANK.get("spatial_index.json");
    if (!obj) return jsonError(404, "spatial_index.json not found");
    return new Response(obj.body, {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=60",
        ...CORS_HEADERS,
      },
    });
  }

  // /zarr/{param}/{...path}
  const match = pathname.match(/^\/zarr\/([^/]+)\/(.+)$/);
  if (!match) return jsonError(400, "Invalid zarr path. Use /zarr/{param}/{path}");

  const [, param, objPath] = match;

  let runId: string;
  try {
    runId = await resolveRunId(env, null);
  } catch {
    return jsonError(503, "No latest run available");
  }

  let r2Key = `runs/${runId}/${param}/${objPath}`;
  let obj = await env.DATABANK.get(r2Key);
  // If the cached runId points to a run that was janitored after promotion,
  // bust caches and retry once against the freshly-resolved runId.
  if (!obj) {
    try {
      const freshRunId = await resolveRunId(env, null, true);
      if (freshRunId !== runId) {
        runId = freshRunId;
        r2Key = `runs/${runId}/${param}/${objPath}`;
        obj = await env.DATABANK.get(r2Key);
      }
    } catch {
      // fall through to 404 below
    }
  }
  if (!obj) return jsonError(404, `Not found: ${r2Key}`);

  const isMetadata = /\.(zarray|zattrs|zmetadata|zgroup)$/.test(objPath);
  const contentType = isMetadata ? "application/json" : "application/octet-stream";
  // Metadata (e.g. .zarray) uses a short TTL: the proxy URL has no run-version so
  // a long cache would serve a stale shape after a run change. 60s is well under
  // the 6-hour run interval while still benefiting from CDN for concurrent users.
  const cacheControl = isMetadata ? "public, max-age=60" : "public, max-age=3600";

  return new Response(obj.body, {
    headers: {
      "Content-Type": contentType,
      "Cache-Control": cacheControl,
      ...CORS_HEADERS,
    },
  });
}

// ---------------------------------------------------------------------------
// Main Worker handler
// ---------------------------------------------------------------------------
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // Zarr proxy route
    if (url.pathname.startsWith("/zarr/")) {
      if (request.method !== "GET") return withCors(jsonError(405, "Method not allowed"));
      return handleZarrProxy(env, url.pathname);
    }

    // Existing per-pixel API (GET /)
    if (url.pathname !== "/") {
      return jsonError(404, "Not found. Use GET /?lat=&lon=&param= or GET /zarr/{param}/{path}");
    }
    if (request.method !== "GET") {
      return jsonError(405, "Method not allowed");
    }

    // --- Rate limiting ---
    const clientIp = request.headers.get("CF-Connecting-IP") ?? "unknown";
    const allowed = await checkRateLimit(env, clientIp);
    if (!allowed) {
      return new Response(JSON.stringify({ error: "Too many requests" }), {
        status: 429,
        headers: {
          "Content-Type": "application/json",
          "Retry-After": String(RATE_LIMIT_WINDOW_S),
        },
      });
    }

    // --- Parse & validate ---
    const parsed = parseAndValidate(url);
    if (parsed instanceof Response) return parsed;
    const { lat, lon, param, ensemble, runOverride } = parsed;

    // --- Resolve run ID ---
    let runId: string;
    try {
      runId = await resolveRunId(env, runOverride);
    } catch (err) {
      return jsonError(503, `Cannot resolve run: ${(err as Error).message}`);
    }

    // --- Validate run ID exists (only for explicit overrides) ---
    if (runOverride) {
      const check = await env.DATABANK.head(`runs/${runId}/${param}/_SUCCESS`);
      if (!check) {
        return jsonError(404, `Run ${runId} not found or not yet promoted for ${param}`);
      }
    }

    // --- Cache lookup (keyed per pixel to avoid serving wrong data) ---
    const { chunkLat, chunkLon, localLat, localLon } = latLonToChunkIndex(lat, lon);
    const cacheKey = new Request(
      `https://icon-cache/${runId}/${param}/${chunkLat}/${chunkLon}/${localLat}/${localLon}${ensemble ? "/ens" : ""}`,
    );
    const cache = caches.default;
    const cached = await cache.match(cacheKey);
    if (cached) {
      const body = await cached.json<Record<string, unknown>>();
      body.lat = lat;
      body.lon = lon;
      return new Response(JSON.stringify(body), {
        headers: {
          "Content-Type": "application/json",
          "X-Cache": "HIT",
          "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
        },
      });
    }

    // --- Fetch from R2 ---
    let body: Record<string, unknown>;
    try {
      body = await fetchFromR2(
        env, runId, param, lat, lon,
        chunkLat, chunkLon, localLat, localLon,
        ensemble,
      );
    } catch (err) {
      // If cached runId was janitored after promotion, refresh and retry once.
      if (!runOverride) {
        try {
          const freshRunId = await resolveRunId(env, null, true);
          if (freshRunId !== runId) {
            runId = freshRunId;
            body = await fetchFromR2(
              env, runId, param, lat, lon,
              chunkLat, chunkLon, localLat, localLon,
              ensemble,
            );
          } else {
            throw err;
          }
        } catch (retryErr) {
          console.error("R2 fetch error:", retryErr);
          return jsonError(500, `Failed to retrieve data: ${(retryErr as Error).message}`);
        }
      } else {
        console.error("R2 fetch error:", err);
        return jsonError(500, `Failed to retrieve data: ${(err as Error).message}`);
      }
    }

    const finalResponse = new Response(JSON.stringify(body), {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
        "X-Cache": "MISS",
        "X-Run": runId,
        "X-Chunk": `${chunkLat}/${chunkLon}`,
      },
    });

    // Cache the per-pixel response
    ctx.waitUntil(
      cache.put(
        cacheKey,
        new Response(JSON.stringify(body), {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
          },
        }),
      ),
    );

    return finalResponse;
  },
} satisfies ExportedHandler<Env>;
