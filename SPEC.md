HexaTiles — Spec

HexaTiles is open source software.

Parquet → H3 polygons → NDJSON → PMTiles in one command.
No map server. Drop the .pmtiles on a CDN and use MapLibre.

⸻

Who it’s for
	•	Anyone with a column of H3 cells (metrics, coverage, model outputs).
	•	Wants a single-file tileset, static hosting, no PostGIS/TileServer nonsense.

⸻

Non-Goals (on purpose)
	•	No routing, no network analysis, no dynamic API.
	•	No geometry other than H3 cell polygons (we’re not a general vector tiler).

⸻

Input contract

Required Parquet columns (any one of):
	•	h3 (string, e.g. "8928308280fffff") or
	•	h3_id (uint64 packed integer) or
	•	h3 as int64 hex-parsed tolerated if users screw up (warn and coerce).

Optional columns (copied to feature properties):
	•	Numerics: int, float (will be quantized/rounded per rules below).
	•	Categoricals: string (short keys encouraged).
	•	You can include/exclude columns via CLI.

Assumptions
	•	All rows represent full H3 cells (not partial polygons).
	•	Mixed resolutions allowed; we derive polygon per cell’s own res.

Validation
	•	Reject non-H3 strings (isValidCell) with line numbers/samples.
	•	Reject rows whose res < --min-res or > --max-res (optional).
	•	Warn on property bloat (>15 props or >20 KB feature JSON pre-tiling).

⸻

Output artifacts
	•	out/xyz.ndjson (optionally kept for debugging)
	•	out/tiles.pmtiles (final artifact)
	•	out/report.html (build log + stats: features in/out, res histogram, min/max zooms, size per zoom, property bytes, quantization loss)

⸻

CLI design

hexatiles build \
  --in data/metrics.parquet \
  --out out/tiles.pmtiles \
  --keep-ndjson \
  --minzoom 0 --maxzoom 12 \
  --min-res 5 --max-res 10 \
  --props "score,category" \
  --props-drop "debug_*" \
  --quantize float=0.01 int=1 \
  --simplify false \
  --threads 8

Other commands:
	•	hexatiles validate --in data/*.parquet
	•	hexatiles inspect --in out/tiles.pmtiles (prints meta, layer list, zoom histogram)
	•	hexatiles preview --pmtiles out/tiles.pmtiles --open (spawns a static file preview on localhost; no tileserver)
	•	hexatiles schema --in data/file.parquet (shows detected columns/res distribution)

⸻

Flags that matter
	•	--props: whitelist properties to carry through. Default: none.
	•	--props-drop: glob drop list.
	•	--quantize: numeric rounding (float=0.01 → 2 decimals; int=1 no change). Shrinks PMTiles dramatically.
	•	--simplify: default false. H3 boundaries are already minimal; simplification can create slivers.
	•	--minzoom/--maxzoom: defaults derived from H3 res histogram (z_min=0, z_max=max(12, res+2) with clamp).

⸻

Implementation plan

Language: Go.

Pipeline:
	1.	Read Parquet (Arrow/Parquet reader). Stream in row groups.
	2.	Parse H3 cells → polygon rings (GeoJSON) using official H3 bindings.
	3.	Attach properties with quantization + drop rules.
	4.	Write line-delimited GeoJSON (NDJSON).
	5.	Tile generation:
	•	Option A (initial): shell out to tippecanoe → .mbtiles → convert to .pmtiles (using pmtiles CLI).
	•	Option B (roadmap): integrate a vector tiler + PMTiles writer directly.
	6.	Emit report.html with counts, byte sizes, res→zoom mapping, per-zoom feature counts.

Concurrency: chunk by Parquet row group; polygonize in worker pool.
Determinism: stable feature ordering (sort by h3); tippecanoe flags set for deterministic output.

⸻

Tippecanoe sane defaults
	•	--drop-densest-as-needed
	•	--extend-zooms-if-still-dropping
	•	--coalesce-densest-as-needed
	•	--force
	•	--layer=h3
	•	--no-feature-limit --no-tile-size-limit
	•	--minimum-zoom / --maximum-zoom from CLI
	•	Do not use --simplify-* for H3
	•	--attributes= from --props

MBTiles→PMTiles: use pmtiles convert with preserved metadata:
	•	name, description, attribution, vector_layers (fields + types)
	•	tileset_version (semver), generator=hexatiles

⸻

Demo (MapLibre, zero server)

Ship examples/demo/index.html wired to a relative .pmtiles file. Keep it tiny.

(see your snippet — unchanged)

⸻

Repo skeleton

hexatiles/
  README.md
  LICENSE
  cmd/hexatiles/main.go
  internal/
    parquet/reader.go
    h3/geom.go
    props/quantize.go
    ndjson/writer.go
    tiler/tippecanoe.go
    tiler/pmtiles.go
    report/report.go
    validate/validate.go
  examples/demo/index.html
  examples/sample.parquet
  .github/workflows/ci.yml
  Makefile


⸻

CI
	•	Cross-compile binaries (linux/amd64, linux/arm64, macOS).
	•	Run unit tests on quantization + validator.
	•	Lint.
	•	Build demo tiles from examples/sample.parquet and assert size bounds.

⸻

README outline (for GitHub stars)
	1.	60-sec quickstart (download binary + hexatiles build + open demo).
	2.	Why HexaTiles (no server, one file, CDN-friendly).
	3.	Input contract (3 bullets).
	4.	Common recipes:
	•	Only keep score & category
	•	Round floats to 2 decimals
	•	Clamp to z=0–12
	5.	Performance notes.
	6.	Limitations (H3 only, not a general tiler).
	7.	Contributing (good first issues, tests).

⸻

Performance targets
	•	5–10M H3 cells: <8 min on a 16-core dev box.
	•	PMTiles conversion: <2 min.
	•	Output tileset (nationwide r7–r8, 1–2 props): 300–800 MB.
	•	Memory: ≤4–6 GB at 10M rows.

⸻

Differentiators
	•	H3-first: no geometry headaches.
	•	Single binary: no GDAL/Node soup.
	•	Opinionated defaults: shareable tiles with one command.
	•	Report page: transparent, not magical.

⸻

Edge cases & guardrails
	•	Mixed resolutions allowed; warn if mixing r5 + r12.
	•	Hard cap on property bytes per feature (default 2 KB).
	•	Fail fast on invalid H3 (print samples).
	•	Use official H3 polygonizer; don’t roll your own.

⸻

Roadmap
	•	v0.1: Parquet→NDJSON→Tippecanoe→MBTiles→PMTiles, validate, report, demo.
	•	v0.2: Direct PMTiles writer (skip MBTiles), parallelized tiling.
	•	v0.3: Optional quantile color ramps baked into metadata.
	•	Nice-to-have: --join to merge props from CSV by H3.

⸻

License & governance
	•	Apache-2.0.
	•	Close scope-creep PRs: “We’re not a general vector tiler.”

⸻

Brutal reality check
	•	If you overreach into generic tiling, you’ll drown.
	•	If you require PostGIS, you’ll lose 80% of stars.
	•	If it isn’t instantly viewable via copy-paste demo, you’ll lose 15%.
	•	Keep it H3-only, one binary, one command, one demo.

⸻

The Problem HexaTiles Solves

Lots of people have data already tied to H3 hexagons — crime counts, air quality, delivery coverage, cell service, flood risk, retail reach, etc. They want to put it on a map and share it.

Right now, that means: PostGIS → SQL → GeoJSON → Tippecanoe → server hosting. It’s painful.

HexaTiles skips all that. One command → one .pmtiles → drop it on GitHub Pages, S3, or any static host → open demo.html. Done.