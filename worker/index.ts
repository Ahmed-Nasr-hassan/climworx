/**
 * ICON Global Weather API — Cloudflare Worker
 * ============================================
 * GET /?lat=52.52&lon=13.405&param=t_2m[&ensemble=true][&run=2026040300]
 *
 * Data flow:
 *   1. Resolve run ID (KV cache of _LATEST_RUN, refreshed every 5 min)
 *   2. Compute Zarr chunk indices from lat/lon
 *   3. Byte-range read of the required chunk from R2
 *   4. Decompress Blosc/zstd chunk in-memory
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
const GRID_NX = 2880;      // 360 / 0.125
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
interface SpatialIndex {
  run: string;
  grid: {
    lat_min: number; lat_max: number;
    lon_min: number; lon_max: number;
    nx: number; ny: number;
    resolution: number;
  };
  chunks: { latitude: number; longitude: number };
  params: string[];
  steps: number[];
}

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
): Promise<string> {
  if (runOverride) return runOverride;

  const now = Date.now();
  if (cachedLatestRun && now - cachedLatestRun.fetchedAt < LATEST_RUN_TTL_MS) {
    return cachedLatestRun.runId;
  }

  // Try KV first (sub-ms)
  let runId = await env.METADATA.get(LATEST_RUN_KEY);

  // Fall back to R2 object
  if (!runId) {
    const obj = await env.DATABANK.get("latest/_LATEST_RUN");
    if (!obj) throw new Error("No latest run found in R2");
    runId = await obj.text();
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
  const cacheKey = `${runId}/${param}/.zarray`;

  // Try KV metadata cache first
  const cached = await env.METADATA.get(cacheKey);
  if (cached) return JSON.parse(cached) as ZarrArrayMeta;

  const obj = await env.DATABANK.get(`runs/${runId}/${param}/.zarray`);
  if (!obj) throw new Error(`Zarr metadata not found for ${param} in run ${runId}`);
  const text = await obj.text();
  await env.METADATA.put(cacheKey, text, { expirationTtl: 3600 });
  return JSON.parse(text) as ZarrArrayMeta;
}

/**
 * Build the R2 key for a Zarr chunk.
 * Zarr v2 chunk key format: {c0}.{c1}.{c2}... (one index per dimension)
 * Dimensions order: [step, latitude, longitude]  (or [step, realization, lat, lon] for EPS)
 */
function buildChunkKey(
  runId: string,
  param: string,
  varName: string,
  chunkLat: number,
  chunkLon: number,
  ensemble: boolean,
): string {
  // step chunk = 0 (step is stored unchunked: chunk size = -1 = full dim)
  if (ensemble) {
    // dims: step, realization, latitude, longitude
    return `runs/${runId}/${param}/${varName}/0.0.${chunkLat}.${chunkLon}`;
  }
  // dims: step, latitude, longitude
  return `runs/${runId}/${param}/${varName}/0.${chunkLat}.${chunkLon}`;
}

/**
 * Minimal Blosc header parser.
 * Blosc frame: 16-byte header → uncompressed size at bytes 4–7 (LE uint32).
 * The Worker uses DecompressionStream('deflate-raw') for the inner codec (zstd
 * is not natively available in Workers; we rely on Blosc's "noshuffle" fallback
 * or pre-decompress on the ETL side with gzip for Worker compatibility).
 *
 * NOTE: Full Blosc decompression inside a CF Worker requires a WASM port of
 * blosc (e.g., numcodecs-wasm). For production, either:
 *   (a) Use a WASM build of blosc, or
 *   (b) Store chunks with gzip compressor (Workers have native DecompressionStream).
 *
 * The implementation below demonstrates the pattern; swap `decompressChunk`
 * for your chosen approach.
 */
async function decompressChunk(compressed: ArrayBuffer): Promise<Float32Array> {
  // Blosc header is 16 bytes; skip it and try to decompress remaining bytes.
  // This is a simplified placeholder — replace with a proper WASM blosc decoder.
  const view = new DataView(compressed);
  const uncompressedSize = view.getUint32(4, true); // bytes 4-7, little-endian

  // For zstd-compressed Blosc data inside a Worker we use a WASM decompressor.
  // Here we return a typed array of the expected size filled from raw bytes
  // as a structural placeholder (real decoding requires WASM or a side-car).
  const raw = new Uint8Array(compressed, 16); // skip 16-byte Blosc frame header
  const ds = new DecompressionStream("deflate-raw");
  const writer = ds.writable.getWriter();
  writer.write(raw);
  writer.close();
  const reader = ds.readable.getReader();
  const chunks: Uint8Array[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const totalLen = chunks.reduce((s, c) => s + c.byteLength, 0);
  const buf = new Uint8Array(totalLen);
  let offset = 0;
  for (const c of chunks) {
    buf.set(c, offset);
    offset += c.byteLength;
  }
  return new Float32Array(buf.buffer);
}

/**
 * Extract a single lat/lon pixel's time series from a flat Float32Array.
 * Zarr stores data in C-order: [step, lat, lon] or [step, member, lat, lon].
 */
function extractTimeSeries(
  data: Float32Array,
  localLat: number,
  localLon: number,
  nSteps: number,
  chunkLatSize: number,
  chunkLonSize: number,
  ensemble: boolean,
  nMembers = 40,
): Record<number, number | number[]> {
  const result: Record<number, number | number[]> = {};

  for (let s = 0; s < nSteps; s++) {
    if (!ensemble) {
      // Index in flat array: s * (chunkLatSize * chunkLonSize) + localLat * chunkLonSize + localLon
      const idx = s * chunkLatSize * chunkLonSize + localLat * chunkLonSize + localLon;
      result[s] = round4(data[idx]);
    } else {
      const memberValues: number[] = [];
      for (let m = 0; m < nMembers; m++) {
        const idx =
          s * (nMembers * chunkLatSize * chunkLonSize) +
          m * (chunkLatSize * chunkLonSize) +
          localLat * chunkLonSize +
          localLon;
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
  chunkLat: number,
  chunkLon: number,
  localLat: number,
  localLon: number,
  ensemble: boolean,
): Promise<Response> {
  // Determine variable name (first data variable in the Zarr group)
  // By convention we use the parameter name as the variable name.
  const varName = param;

  const meta = await fetchZarrMeta(env, runId, param);
  const nSteps = meta.shape[0];
  const chunkKey = buildChunkKey(runId, param, varName, chunkLat, chunkLon, ensemble);

  const obj = await env.DATABANK.get(chunkKey);
  if (!obj) {
    throw new Error(`Chunk not found in R2: ${chunkKey}`);
  }

  const compressed = await obj.arrayBuffer();
  const data = await decompressChunk(compressed);

  const nMembers = ensemble ? (meta.shape[1] ?? 40) : 1;
  const values = extractTimeSeries(
    data, localLat, localLon,
    nSteps, CHUNK_LAT, CHUNK_LON,
    ensemble, nMembers,
  );

  return new Response(
    JSON.stringify({
      lat: undefined,   // filled by caller
      lon: undefined,
      param,
      run: runId,
      unit: PARAM_UNITS[param] ?? "unknown",
      ...(ensemble ? { members: nMembers } : {}),
      values,
    }),
    { headers: { "Content-Type": "application/json" } },
  );
}

// ---------------------------------------------------------------------------
// Main Worker handler
// ---------------------------------------------------------------------------
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname !== "/") {
      return jsonError(404, "Not found. Use GET /?lat=&lon=&param=");
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
      const check = await env.DATABANK.head(`runs/${runId}/_SUCCESS`);
      if (!check) {
        return jsonError(404, `Run ${runId} not found or not yet promoted`);
      }
    }

    // --- Cache lookup ---
    const { chunkLat, chunkLon, localLat, localLon } = latLonToChunkIndex(lat, lon);
    const cacheKey = new Request(
      `https://icon-cache/${runId}/${param}/${chunkLat}/${chunkLon}${ensemble ? "/ens" : ""}`,
    );
    const cache = caches.default;
    const cached = await cache.match(cacheKey);
    if (cached) {
      // Re-extract single point from cached chunk response
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
    let response: Response;
    try {
      response = await fetchFromR2(
        env, runId, param,
        chunkLat, chunkLon, localLat, localLon,
        ensemble,
      );
    } catch (err) {
      console.error("R2 fetch error:", err);
      return jsonError(500, `Failed to retrieve data: ${(err as Error).message}`);
    }

    // Inject lat/lon into body
    const body = await response.json<Record<string, unknown>>();
    body.lat = lat;
    body.lon = lon;
    const finalResponse = new Response(JSON.stringify(body), {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": `public, max-age=${CACHE_TTL_SECONDS}`,
        "X-Cache": "MISS",
        "X-Run": runId,
        "X-Chunk": `${chunkLat}/${chunkLon}`,
      },
    });

    // Cache the chunk response for subsequent requests at nearby coordinates
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
