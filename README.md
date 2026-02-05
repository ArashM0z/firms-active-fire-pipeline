# FIRMS Active Fire Pipeline

Daily ETL for NASA FIRMS active-fire detections (MODIS C6.1 + VIIRS NRT).
Downloads the public 24-hour CSV feeds, validates every row against a
Pydantic schema, deduplicates on (lat, lon, date, time, satellite), and
writes a single Parquet file ready for downstream geospatial joins.

## What's in the box

- `FireDetection` Pydantic schema with bounds checks on lat / lon /
  confidence / FRP and a `daynight` enum constraint.
- `fetch_csv(url)` + `parse_csv(text)` for the FIRMS public feeds; the
  HHMM-integer `acq_time` column is normalised to a `datetime.time`.
- `validate(df)` drops rows that don't match the schema (logged).
- `deduplicate(df)` collapses point duplicates emitted by overlapping
  satellite passes.
- `run_pipeline([urls], out)` + `firms-pipeline` CLI for the full run.

## Quickstart

```bash
pip install -e ".[dev]"
firms-pipeline --sources MODIS_C6_1_global_24h VIIRS_SNPP_NRT_global_24h \
               --out data/firms.parquet
```

Output:

```json
{ "fetched": 13412, "valid": 13312, "deduplicated": 11920,
  "written_to": "data/firms.parquet" }
```

The Parquet file plays nicely with `geopandas.read_parquet`,
`duckdb.read_parquet`, or any Arrow-aware downstream.

## Layout

```
src/firms/
├── schema.py      # FireDetection Pydantic model
├── pipeline.py    # fetch / parse / validate / dedupe / run_pipeline
└── cli.py         # firms-pipeline entrypoint
tests/             # parse, schema rejection, dedupe, run_pipeline e2e
```

## References

- NASA FIRMS: https://firms.modaps.eosdis.nasa.gov/
- MODIS C6.1 active fire user guide
- VIIRS SNPP I-band active fire product
