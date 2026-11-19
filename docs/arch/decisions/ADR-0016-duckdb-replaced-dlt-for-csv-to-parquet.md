# ADR-0016 DuckDB replaced dlt for CSV to Parquet

## Status

`built`. Taken in `28ff07b`.

## Decision

CSV to Parquet conversion is done by DuckDB. The row-by-row dlt loader is gone, and dlt is not imported anywhere in the package.

## Why

The `28ff07b` message says the old path pushed every row through a Python generator into dlt's normalizer at about 22-27k rows per second. It says that was nearly all the runtime, and that P_CAGE (403 MiB, 4.16M rows) took 150 seconds. It says nothing dlt offered was in use: every column is text, and each month wrote its own dataset, so there was no schema evolution and no merge.

## Consequences

The message gives the dlt numbers only. It records no DuckDB timing, so the speed-up is not established.

`pyproject.toml:11` still declares `dlt[parquet,s3]`, and docstrings still say "dlt-style" (`pub_tools/lake.py:35,60,75`; `pub_tools/assets.py:161,170`). The agent docs were rewritten for DuckDB in `5bfc98e`; see ADR-0040. The dependency has no recorded reason (ADR-0037).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Conversion is DuckDB SQL over object storage | `pub_tools/lake.py:609-626` | `test_publog_io_manager.py:73` test_table_lands_where_the_io_manager_owns | `built` |
| dlt ran at 22-27k rows/s and P_CAGE (4.16M rows) took 150 s | commit `28ff07b` message | no test | `built` (note: the run is reported only in the source cited; no re-readable result records it) |
| No dlt feature was in use | commit `28ff07b` message | no test | `built` |
| dlt is not imported in the package or scripts | searched `pub_tools/`, `scripts/` and `example/` at the pin | no test | `built` |
| The dlt dependency is still declared | `pyproject.toml:11` | no test | `built` |
