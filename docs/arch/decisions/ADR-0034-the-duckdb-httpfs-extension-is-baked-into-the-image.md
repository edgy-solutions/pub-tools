# ADR-0034 The DuckDB httpfs extension is baked into the image

## Status

`built`. Taken in `f02dc26`.

## Decision

The image build downloads the httpfs extension and bakes it in. At run time DuckDB loads it from a fixed extension directory and does not fetch it. `scripts/bake_duckdb_extensions.py` does the bake.

## Why

The `f02dc26` message says a pod with no outbound route to the extension repository cannot download the extension. Baking it makes cold start independent of the network. The extension directory logic is at `pub_tools/lake.py:62-69`.

## Consequences

The extension version must match the DuckDB wheel. A DuckDB upgrade needs a rebuild of the image. The commit did not touch the agent docs (ADR-0040).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The build bakes the extension | `.github/workflows/build-container.yml:73-87`; `scripts/bake_duckdb_extensions.py:1-18` | no test | `built` |
| The connection uses a fixed extension directory | `pub_tools/lake.py:50-96` | `test_lake.py:58` and `test_lake.py:72` baked extension tests | `built` |
