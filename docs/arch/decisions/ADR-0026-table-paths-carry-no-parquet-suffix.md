# ADR-0026 Table paths carry no .parquet suffix

## Status

`built`. Taken in `74445b8`.

## Decision

A table's Parquet path is `<root>/<table>/`, a directory with no `.parquet` suffix. The suffix was dropped so the object path and the asset key match.

Scope: this rule applies across the mesh; its owning record belongs in dag-tools, which has not written one.

## Why

The commit message gives the match with the asset key as the reason. A catalog dataset named for the path would otherwise carry the suffix.

## Consequences

The `74445b8` message marks the change BREAKING, with consumers' old paths orphaned. Two places still say `.parquet/`: `llms.txt:15` and `pub_tools/components/publog_pipeline/component.py:314-315`.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The table path has no suffix | `pub_tools/lake.py:132-155` | `test_lake.py:173` test_table_parquet_url_tracks_the_io_manager | `built` |
| The docstrings still say .parquet | `pub_tools/components/publog_pipeline/component.py:314-315`; `llms.txt:15` | no test | `built` |
| The change is breaking for old paths | commit `74445b8` message | no test | `built` |
