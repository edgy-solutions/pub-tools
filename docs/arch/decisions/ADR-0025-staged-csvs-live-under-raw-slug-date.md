# ADR-0025 Staged CSVs live under _raw/<slug>/<date>

## Status

`built`. Taken in `b128017`.

## Decision

The raw layout is `_raw/<slug>/<date>/<member>.csv`, with the staging marker `_source.json` under the same dated prefix. The earlier layout put the date first.

## Why

The `b128017` message says the date-first layout broke the catalog's path-spec. A pattern with a single wildcard for the slug works only when the slug comes before the date. The message says it checked four shapes against the catalog's own path-spec parser. The record of that check is not in the repo.

## Consequences

`b128017` is marked BREAKING. Staged CSVs under the old layout are not found, so the first run after the change downloads again.

`llms.txt:15` and the component docstring at `pub_tools/components/publog_pipeline/component.py:314-315` still describe the Parquet as `<table>.parquet/`. They were not updated. That text also pre-dates ADR-0026.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The raw prefix is _raw/slug/date | `pub_tools/lake.py:99-118` | `test_lake.py:157` test_url_builders_are_stable | `built` |
| The marker URL is under the dated prefix | `pub_tools/lake.py:127-129` | `test_lake.py:259` test_marker_roundtrip_and_missing | `built` |
| Staged CSVs re-download after the change | commit `b128017` message | no test | `built` |
| The layout was checked against the catalog's own path-spec parser | commit `b128017` message; no run record in the repo | no test | `built` |
