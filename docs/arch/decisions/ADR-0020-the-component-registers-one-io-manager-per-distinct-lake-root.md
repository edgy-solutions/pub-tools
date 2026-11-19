# ADR-0020 The component registers one IO manager per distinct lake root

## Status

`built`. Taken in `42cb35d`; the per-root rule is at `pub_tools/components/publog_pipeline/component.py:499-514`.

## Governs

CO-PT-PIPELINE.

## Decision

The component registers its own DuckDB IO manager under `io_manager_key` (default `publog_duckdb_io_manager`). It registers one manager per distinct lake root. The monthly and quarterly sets share one manager when they share a root, and get a second, suffixed key when they do not. Overriding the key points the tables at an IO manager defined elsewhere.

## Why

The code comment says sharing one key across two roots would silently write quarterly tables into the monthly lake. The docstring says registering the manager in the component keeps it self-contained and visible to the broker.

## Consequences

In the deployed case both sets share a root, so there is one manager. The two-root case arises only with the built-in local defaults. No test builds two roots.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| One manager per distinct root, with a suffixed key for the quarterly set | `pub_tools/components/publog_pipeline/component.py:499-514,546-548` | no test | `built` |
| io_manager_key is configurable | `pub_tools/components/publog_pipeline/component.py:375-380` | no test | `built` |
| A manager is registered in Definitions resources | `pub_tools/components/publog_pipeline/component.py:567-569` | `test_publog_io_manager.py:173` test_io_manager_is_registered_for_the_broker | `built` |
