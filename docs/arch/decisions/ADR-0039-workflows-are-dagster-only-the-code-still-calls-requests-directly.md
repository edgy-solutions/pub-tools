# ADR-0039 Workflows are Dagster-only; the code still calls requests directly

## Status

`accepted`. Written in `.cursorrules:18-20`; the code departs from it in one place.

## Governs

CO-PT-PIPELINE.

## Decision

Pipelines are written as Dagster assets and components. The rules say not to add parallel runners. DuckDB is the engine, and `.cursorrules:22-25` says not to add dlt.

## Why

`e5612fa` carried the original dlt rule in `.cursorrules`, `llms.txt` and `README.md`. `5bfc98e` rewrote all three for DuckDB, and the rules now say DuckDB and no dlt. The README note at `README.md:29-37` records the dlt-shape of the env table.

## Consequences

The code departs from the rule in places. `pub_tools/assets.py` calls `requests` directly rather than through a Dagster resource. `dlt` is still declared (ADR-0037), and docstrings still say "dlt-style".

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The rules prescribe Dagster assets and DuckDB, and forbid dlt | `.cursorrules:18-25` | no test | `accepted` |
| The agent docs were rewritten for DuckDB | commit `5bfc98e` | no test | `built` |
| Downloads use requests directly | `pub_tools/assets.py:180-183` | no test | `built` |
