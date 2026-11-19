# ADR-0019 Tables are written by the dag-tools DuckDB IO manager from a lazy relation

## Status

`built`. Taken in `42cb35d`.

## Governs

CO-PT-PIPELINE, CO-PT-LAKE, FL-PT-PUBLISH.

## Decision

A table asset returns a lazy DuckDB relation. dag-tools' `ConfigurableDuckDBIOManager` executes it and writes the Parquet. The IO manager owns the lake layout through `asset_uri`. The component does not spell the layout out, and `table_parquet_url` asks the IO manager for the path.

## Why

The `42cb35d` message says the layout used to live in the component and the tables were invisible to the mesh. Now the layout is owned in one place, and the broker can advertise the tables (ADR-0030). A skip still works because Dagster calls `handle_output` only for a real output value.

The message says the freshness check had to be reworked, because on S3 a directory has no LastModified. `dataset_mtime` stats the newest part instead. It says a test pins `table_parquet_url` to the IO manager's own path, because a check against a path the writer does not use either rebuilds forever or never rebuilds. It says this was verified against an S3-compatible store, not only the local suite.

## Consequences

The component depends on the `asset_uri` and `split_endpoint_instance` functions in dag-tools (`dag_tools/io_managers/duckdb.py:75,94` at `a99779f`). The layout is a dag-tools matter.

The verification against an object store is reported in the commit. Its record is not in the repo.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The table asset returns csv_relation and the IO manager writes it | `pub_tools/components/publog_pipeline/component.py:283-297,571-595` | `test_publog_io_manager.py:73` test_table_lands_where_the_io_manager_owns | `built` |
| table_parquet_url delegates to asset_uri | `pub_tools/lake.py:132-155` | `test_lake.py:173` test_table_parquet_url_tracks_the_io_manager | `built` |
| dataset_mtime stats the newest part file | `pub_tools/lake.py:204-240` | `test_publog_io_manager.py:143` test_rerun_skips_without_rewriting | `built` |
| The IO manager class and asset_uri exist in dag-tools | dag-tools `a99779f`, `dag_tools/io_managers/duckdb.py:94,384` | no test | `built` |
