# ADR-0013 Tables are replaced in place each month, with no per-month Parquet snapshots

## Status

`built`. Taken in `42cb35d`.

## Governs

CO-PT-PIPELINE.

## Decision

A table's Parquet lives at one path and a monthly run replaces it. The earlier layout wrote a dated directory per month. That is gone. The dated history survives only as the staged CSVs under `_raw/`.

## Why

The commit message says nothing read the dated path. It says the dated CSVs are still there, so any past month can be rebuilt.

## Consequences

This is a behaviour change. Per-month Parquet snapshots no longer exist. A past month needs a rebuild from its staged CSV. ADR-0014 records the idea to bring snapshots back.

`dataset_name` was removed with the dated path.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| A run replaces the table at one path | `pub_tools/components/publog_pipeline/component.py:317-322` | `test_publog_io_manager.py:73` test_table_lands_where_the_io_manager_owns | `built` |
| Dated history survives as staged CSVs | `pub_tools/lake.py:99-118` | `test_lake.py:157` test_url_builders_are_stable | `built` |
| The old layout and its removal | commit `42cb35d` message (BEHAVIOUR CHANGE paragraph) | no test | `built` |
