# ADR-0021 Parquet uses zstd with 128 MB part files, one directory per table

## Status

`built`. Taken in `42cb35d`; the defaults are at `pub_tools/components/publog_pipeline/component.py:372-385`.

## Governs

CO-PT-PIPELINE, FL-PT-PUBLISH.

## Decision

The Parquet codec defaults to zstd. Part files target 128 MB. Any `part_file_size` value makes each table a directory of `data_N.parquet` parts, and null gives a single file.

## Why

The docstring says a large table then splits instead of producing one unwieldy object. No reason for zstd is recorded.

## Consequences

Consumers read a directory of parts, not one file. The freshness check in ADR-0010 accounts for this. No test pins the two defaults.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| zstd and 128MB are the defaults | `pub_tools/components/publog_pipeline/component.py:372-385` | no test | `built` |
| The settings are passed to the IO manager | `pub_tools/components/publog_pipeline/component.py:593-594` | no test | `built` |
| A run produces a directory of parquet parts | `pub_tools/components/publog_pipeline/component.py:314-315` | `test_publog_io_manager.py:73` test_table_lands_where_the_io_manager_owns (asserts a directory containing .parquet files) | `built` |
