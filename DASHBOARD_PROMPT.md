# ClimWorx Weather Dashboard — Build Prompt

## Role & Goal

Act as an Expert Geospatial Frontend Developer and UX/UI Designer. Build a single-file `index.html` (Vanilla JS + Tailwind CSS CDN) that renders an interactive weather dashboard inspired by Windy.com and Apple Weather. The dashboard visualizes ICON Global NWP forecast data served from Cloudflare R2 via a Cloudflare Worker.

---

## Architecture Overview

The data lives in a **private** Cloudflare R2 bucket. The browser **cannot** make direct HTTP range requests to R2. Instead, a Cloudflare Worker acts as the gateway. You will need to:

1. **Extend the existing Worker** (`worker/index.ts`) with a new `/zarr/*` proxy route that serves raw Zarr chunk bytes from R2 with CORS headers. This lets `zarr.js` in the browser read the Zarr store transparently.
2. **Build the frontend** (`index.html`) that uses `zarr.js` pointed at the Worker's `/zarr/` endpoint as its Zarr HTTP store base URL.

### Worker Proxy Route Spec

Add to the existing Worker's `fetch` handler:

```
GET /zarr/{param}/{...path}
```

- Maps to R2 key: `runs/{latestRunId}/{param}/{path}`
- Returns the raw R2 object bytes with headers:
  - `Access-Control-Allow-Origin: *`
  - `Content-Type: application/octet-stream` (for chunks) or `application/json` (for `.zarray`/`.zattrs`/`.zmetadata`/`.zgroup`)
- Supports `OPTIONS` preflight for CORS.
- Resolves `{latestRunId}` the same way the existing API does (KV cache of `_LATEST_RUN`).
- Returns `404` if the R2 object doesn't exist, `503` if no latest run is available.
- Add `GET /zarr/spatial_index.json` that returns `spatial_index.json` from the bucket root.

### Frontend Base URL

```js
const WORKER_BASE = 'https://icon-api-worker.ahmednasrhassan123.workers.dev';
const ZARR_BASE   = `${WORKER_BASE}/zarr`;
// zarr.js HTTPStore pointed at: `${ZARR_BASE}/t_2m/` or `${ZARR_BASE}/tot_prec/`
```

---

## Actual Data Structure (Verified from Live R2)

### Parameters

| Parameter   | Zarr Path (relative to run)  | Shape                   | Chunks               | Dimensions                          |
|-------------|------------------------------|-------------------------|-----------------------|-------------------------------------|
| `t_2m`      | `t_2m/t_2m/`                 | `[93, 1, 1441, 2879]`  | `[93, 1, 100, 100]`  | `[step, height, latitude, longitude]` |
| `tot_prec`  | `tot_prec/tot_prec/`         | `[93, 1441, 2879]`     | `[93, 100, 100]`     | `[step, latitude, longitude]`        |

**Critical:** `t_2m` has 4 dimensions (extra singleton `height` axis at index 1), `tot_prec` has 3 dimensions. The code must handle both shapes — after fetching, squeeze/ignore the height dimension for `t_2m`.

### Compression

- **Compressor:** GZip level 5 (`"compressor": {"id": "gzip", "level": 5}`)
- **NOT Blosc** — zarr.js handles GZip natively, no custom codec registration needed.

### Grid Geometry

```
Resolution:  0.125°
Latitude:    90.0 (index 0) → -90.0 (index 1440)    — NORTH to SOUTH, 1441 points
Longitude:  -180.0 (index 0) → 179.875 (index 2878)  — WEST to EAST, 2879 points
```

**Lat/Lon → Array Index Math:**

```js
// lat:  90.0 to -90.0 (north→south), 1441 points
// lon: -180.0 to 179.875 (west→east), 2879 points
const RESOLUTION = 0.125;
const LAT_MAX = 90.0;
const LON_MIN = -180.0;
const NY = 1441;
const NX = 2879;

function latToIndex(lat) {
  return Math.round((LAT_MAX - lat) / RESOLUTION);  // 90→0, -90→1440
}

function lonToIndex(lon) {
  return Math.round((lon - LON_MIN) / RESOLUTION);   // -180→0, 179.875→2878
}

function indexToLat(i) { return LAT_MAX - i * RESOLUTION; }
function indexToLon(j) { return LON_MIN + j * RESOLUTION; }
```

### Zarr v2 Specifics

- `zarr_format: 2`
- `dimension_separator: "."` — chunk keys are `0.0.5.12` not `0/0/5/12`
- `dtype: "<f4"` — little-endian float32
- `fill_value: "NaN"`
- `order: "C"` — row-major (C-order)
- Coordinate arrays (`latitude/`, `longitude/`, `step/`) are at the Zarr group level alongside the data variable.

### Forecast Steps (Non-Uniform!)

93 total steps. **NOT all hourly.** The step values are actual forecast hours:

```
Hourly:       0, 1, 2, 3, ..., 78       (79 steps)
Every 3 hours: 81, 84, 87, ..., 120     (14 steps)
```

The `step` coordinate array contains these exact values. **Read it from Zarr** — do not hardcode. Use the step values for the clock display (e.g., step index 80 = forecast hour 84, displayed as "T+84h" or the actual valid datetime).

### Run Metadata

Fetch `${ZARR_BASE}/spatial_index.json` on startup to get:
```json
{
  "run": "2026040406",          // YYYYMMDDHH — model init time
  "grid": { "lat_min": -90, "lat_max": 90, "lon_min": -180, "lon_max": 180,
            "nx": 2879, "ny": 1441, "resolution": 0.125 },
  "chunks": { "latitude": 100, "longitude": 100 },
  "params": ["tot_prec", "t_2m"],
  "steps": [0, 1, 2, ..., 78, 81, 84, ..., 120]
}
```

Parse the run ID to compute valid times: `validTime = initTime + stepHours`.

---

## Tech Stack (All via CDN)

| Library                  | Purpose                                    |
|--------------------------|--------------------------------------------|
| Tailwind CSS (script)    | Styling                                    |
| MapLibre GL JS           | Base map rendering                         |
| `@mapbox/mapbox-gl-draw` | Bounding box drawing on map                |
| `zarr.js` (zarrita/zarr) | Read Zarr v2 stores via HTTP range requests|
| `d3-scale-chromatic`     | Color scales for weather data              |
| `d3-scale`               | Linear/sequential scale mapping            |
| `pako`                   | GZip decompression (if zarr.js needs it)   |

> **Note on zarr.js:** Use `zarrita` or `@zarrita/core` + `@zarrita/storage` (the modern JS Zarr library). If CDN availability is limited, `zarr-js` from npm via unpkg/esm.sh is acceptable. Configure the HTTP store with the Worker base URL and ensure `dimension_separator: "."` is respected.

---

## Core Features

### 1. UI Design — Dark Glassmorphism

- **Base map:** Carto Dark Matter (`https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json`)
- **Panels:** Frosted glass aesthetic — `bg-black/40 backdrop-blur-xl border border-white/10 rounded-2xl shadow-2xl`
- **Typography:** Clean, modern. Use Tailwind's font-mono for data values. White text with muted (`text-white/60`) labels.
- **Animations:** Smooth CSS transitions on panel open/close. Fade-in for data layers.
- **No scrollbars visible.** Overflow hidden or styled.

### 2. Controls Panel (Top-Left Floating)

- **Parameter Toggle:** Segmented control switching between "Temperature" and "Precipitation". Show the active one with a colored accent (warm orange for temp, cool blue for precip).
- **"Draw Area" Button:** Activates MapLibre Draw in `draw_rectangle` mode. Styled with a subtle pulse animation when active.
- **Run Info Badge:** Show current run datetime (e.g., "ICON 2026-04-04 06Z") parsed from `spatial_index.json`.
- **Area size indicator:** When drawing, show estimated area in km^2 and pixel count. Warn in real-time if too large.

### 3. Fetch Logic (On Bounding Box Draw)

When the user completes a rectangle:

1. Extract `[south, west, north, east]` from the drawn rectangle.
2. Convert to array indices using the math above. Clamp to grid bounds.
3. **Validate size:** If `latPixels * lonPixels > 50,000` pixels, show a toast error: *"Selected area is too large (X pixels). Please draw a smaller region (max ~220x220)."* Delete the drawn rectangle and abort.
4. **Fetch with zarr.js:** Use the Zarr `get()` / `slice()` API to request the subarray:
   - For `t_2m`: `slice([null, 0, latStart:latEnd, lonStart:lonEnd])` — select all steps, height=0, lat/lon range.
   - For `tot_prec`: `slice([null, latStart:latEnd, lonStart:lonEnd])` — select all steps, lat/lon range.
5. **Loading state:** Show a full-screen semi-transparent overlay with an animated spinner and "Loading forecast data..." text. Show a progress indicator if possible (chunk count fetched / total chunks needed).
6. **Result:** A Float32Array of shape `[93, latPixels, lonPixels]` (after squeezing height for t_2m).

### 4. Visualization & Animation

Once data is loaded:

#### Canvas Overlay
- Create an HTML `<canvas>` element.
- Position it as a MapLibre custom layer or as an absolutely-positioned overlay synced to the map's bounding box.
- On map move/zoom, reproject the canvas to stay aligned with the geographic bounds. Use `map.project()` / `map.unproject()` for coordinate transforms.
- The canvas should have `image-rendering: pixelated` for the crisp grid-cell look (no blurring between pixels).

#### Color Mapping
- **Temperature:** Convert K → C (`value - 273.15`). Use `d3.interpolateTurbo` mapped to a dynamic range `[dataMin, dataMax]` (compute from the fetched slice). Show the color bar with C values.
- **Precipitation:** Use `d3.interpolateBlues` or `d3.interpolateYlGnBu`. Map `[0, dataMax]`. **Crucially:** pixels with value `0.0` or `NaN` must be **fully transparent** so the map shows through. Only show the precipitation layer where it's actually raining.
- **Color Legend:** A vertical gradient bar on the right side of the screen with labeled tick marks. Update when switching parameters or when data range changes.

#### Animation Playback
- **Bottom bar:** A sleek playback bar spanning the viewport width. Contains:
  - Play/Pause button (toggle icon).
  - A timeline slider spanning all 93 steps. Mark the transition from hourly to 3-hourly with a subtle visual break or tick-mark change.
  - Current step label: show both the step index and the valid datetime (e.g., "Step 45 — Sat Apr 05, 03:00 UTC").
  - Playback speed control (0.5x, 1x, 2x, 4x).
- Use `requestAnimationFrame` for smooth playback. At each frame, render the current step's 2D slice to the canvas.
- **Interpolation (optional but nice):** For the 3-hourly steps (81+), optionally interpolate between frames for smoother visual transitions.

### 5. Clock / Timestamp Overlay

- A prominent, always-visible element (top-right corner).
- Shows the **valid forecast time** for the currently displayed step: e.g., `"Sat Apr 5, 2026 — 03:00 UTC"`.
- Below it in smaller text: `"T+45h | ICON 06Z Run"`.
- Animate the time change smoothly (fade or counter roll effect).

### 6. Info Tooltip (On Hover)

When the user hovers over the rendered canvas area:
- Show a floating tooltip near the cursor with:
  - Lat/Lon of the hovered pixel.
  - Value at that pixel (e.g., "18.4 C" or "2.3 mm").
  - Small sparkline or mini-chart showing the full time series for that pixel (all 93 steps).

---

## Constraints & Error Handling

1. **Single file.** All HTML, CSS, JS in one `index.html`. Organize with clear comment sections. Inline `<style>` for custom CSS, `<script type="module">` for JS.
2. **Max area guard.** Enforce the 50,000-pixel limit. Show a styled toast notification (top-center, auto-dismiss after 5s, with a close button).
3. **Graceful degradation.** If the Zarr fetch fails (network error, 404, etc.), show an error toast with the specific reason. Don't crash.
4. **Memory management.** After fetching, store the Float32Array. When the user draws a new box, release the old data (`data = null`) before fetching the new slice.
5. **Map sync.** The canvas overlay must stay perfectly aligned during pan/zoom. Re-render on `map.on('move')`. Debounce expensive redraws.
6. **Mobile responsive.** Controls stack vertically on small screens. The playback bar stays full-width at the bottom.

---

## File Structure

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <!-- Meta, title, CDN links for Tailwind, MapLibre, d3, zarr.js, pako -->
</head>
<body class="bg-gray-950 text-white overflow-hidden h-screen w-screen">

  <!-- Map container (full viewport) -->
  <!-- Controls panel (top-left, glassmorphism) -->
  <!-- Clock overlay (top-right) -->
  <!-- Color legend (right side) -->
  <!-- Playback bar (bottom) -->
  <!-- Canvas overlay (synced to map) -->
  <!-- Toast container (top-center) -->
  <!-- Loading overlay -->

  <script type="module">
    // === Constants & Config ===
    // === Zarr Store Setup ===
    // === Map Initialization ===
    // === Drawing Tool ===
    // === Data Fetching ===
    // === Canvas Rendering ===
    // === Animation Loop ===
    // === UI Controllers ===
    // === Event Wiring ===
  </script>
</body>
</html>
```

---

## Deliverables

1. **`worker/index.ts`** — Updated Worker with the `/zarr/*` proxy route added to the existing `fetch` handler. Keep all existing functionality intact.
2. **`index.html`** — The complete single-file dashboard.

Prioritize correctness over flashiness. The data must be accurately mapped to geographic coordinates. Test mentally with Berlin (52.5N, 13.4E) — temperature should be ~10-15 C in April, precipitation near zero.
