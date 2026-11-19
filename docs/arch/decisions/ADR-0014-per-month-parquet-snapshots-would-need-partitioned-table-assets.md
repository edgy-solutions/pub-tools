# ADR-0014 Per-month Parquet snapshots would need partitioned table assets

## Status

`proposed`. Written as intent in the component docstring and in `42cb35d`; nobody has accepted or built it.

## Decision

To bring per-month Parquet snapshots back, the table assets would be partitioned. Nothing implements this.

## Why

The docstring and the commit message both call partitioning "the right way to model it", and the commit adds that it is a larger change than the one it made.

## Consequences

Until someone builds it, ADR-0013 stands.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Partitioning is named as the way to restore snapshots | `pub_tools/components/publog_pipeline/component.py:321-322`; commit `42cb35d` message | no test | `proposed` |
| No table asset is partitioned | `pub_tools/components/publog_pipeline/component.py:480-490` | no test | `built` |
