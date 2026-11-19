# ADR-0008 Ingest has two stages: one staging asset per archive, then one asset per table

## Status

`built`. Taken in `28ff07b`, after production bugs the commit describes.

## Decision

Each source archive has one staging asset, which downloads it and writes its CSV members to the lake. Each CSV member then has its own table asset, which depends on the staging asset and converts that one CSV to Parquet.

This replaced a per-archive `@multi_asset` (`fcdc305`), which had itself replaced a single asset that ran everything (the first paragraph of the `fcdc305` message).

## Why

The `28ff07b` message says the table assets were bookkeeping. The archive step did all the work, and two bugs followed. With `can_subset=True`, materializing the archive alone made Dagster suppress the table results, leaving them unmaterialized. Re-materializing a table took the skip path and did nothing. The message says both were hit in production.

Splitting the work lets conversions run in parallel, retry one by one, and run alone without re-downloading a large archive.

## Consequences

There are many more steps, and each pays its own start-up cost. The memory limits in ADR-0035 follow from that.

The component docstring still describes the staging key as `<key_prefix>/source/<slug>` and the staging path as `_raw/<as_of>/<slug>/`. Both were changed later; see ADR-0025.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| A staging asset is built per source URL and a table asset per manifest member, with a dependency between them | `pub_tools/components/publog_pipeline/component.py:430-490` | no test | `built` |
| The two-stage design is described in the docstrings | `pub_tools/components/publog_pipeline/component.py:303-333`; `pub_tools/lake.py:1-13` | `test_publog_io_manager.py:73` test_table_lands_where_the_io_manager_owns (table asset only) | `built` |
| Two production bugs drove the split | commit `28ff07b` message | no test | `built` |
| It replaced a per-archive multi_asset, which replaced a single asset | commits `fcdc305`, `28ff07b` | no test | `built` |
