# HexaTiles

HexaTiles converts Parquet files with H3 cells into ready-to-serve PMTiles vector tiles. One command, one artifact, no servers.

## Quickstart (60 seconds)

```bash
# 1. Download or build the binary
make build

# 2. Generate the sample tileset
make sample

# 3. Preview locally
./hexatiles preview --pmtiles dist/sample.pmtiles --open
```

The preview opens a MapLibre page backed by your PMTiles file. Drop the same `sample.pmtiles` onto any static host to share it.

## Why HexaTiles

- **No servers** – Produce a PMTiles archive and host it anywhere (S3, GitHub Pages, Netlify).
- **H3-first** – Polygon generation, resolution handling, and validation tailored for H3 cell datasets.
- **Deterministic** – Stable defaults, quantization, and a detailed build report so you know exactly what shipped.
- **One binary** – Go, H3, Tippecanoe, PMTiles orchestration without PostGIS or Node.js stacks.

## Input Contract

1. Include one of `h3` (string) or `h3_id` (uint64). Mixed resolutions are allowed.
2. Additional columns become feature properties (numbers and strings recommended).
3. Invalid H3 cells or out-of-range resolutions fail validation before tiling.

## Common Recipes

```bash
# Only keep score & category, quantize floats to 2 decimal places, clamp to resolutions 5-10
hexatiles build \
  --in data/metrics.parquet \
  --out dist/metrics.pmtiles \
  --props score,category \
  --quantize float=0.01 \
  --min-res 5 --max-res 10

# Inspect a PMTiles archive
hexatiles inspect --in dist/metrics.pmtiles

# Validate a folder of Parquet files without building tiles
hexatiles validate --in data/metrics.parquet --sample 10000
```

## Performance Notes

- Parquet rows stream in row-group batches to keep memory bounded.
- Polygonization runs in a worker pool sized to CPU cores (tune with `--threads`).
- Tippecanoe is invoked with deterministic flags (`--sort-by=h3`, no simplification) for reproducible tiles.
- Property quantization and filtering happen before tiling; see `hexatiles build --help` for sizing options.

## Limitations

- Only H3 hexagon cells are supported (no arbitrary geometry inputs).
- Tippecanoe and the pmtiles CLI must be installed and reachable on `PATH`.
- PMTiles metadata is derived from the dataset; customise styling in your MapLibre client.

## Contributing

Issues and pull requests are welcome! Please:

1. Run `make test` before submitting.
2. Keep changes focused—HexaTiles is intentionally scoped to H3 polygon tiling.
3. Add tests for new quantization or validation behaviours.
