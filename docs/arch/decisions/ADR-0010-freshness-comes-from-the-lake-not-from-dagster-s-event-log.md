# ADR-0010 Freshness comes from the lake, not from Dagster's event log

## Status

`built`. Taken in `28ff07b`.

## Decision

Whether to skip work is decided from the lake. Staging compares the source's Last-Modified with a marker object and checks that the staged CSVs still exist. Conversion rebuilds when its Parquet is older than its input CSV. Dagster's materialization history is not consulted.

## Why

The `28ff07b` message says `instance.get_latest_materialization_event` returns wiped events when called from inside a run. It says this was verified on dagster 1.13.7: it yields the pre-wipe value in the run and returns None outside. A skip built on it can skip work that was never done. A comment in `pub_tools/lake.py` repeats this.

The message also says deleting an object from the bucket rebuilds exactly that object.

## Consequences

`load_previous_last_modified` in `pub_tools/assets.py:231-267` still reads the event log. No caller was found in the package. Whether it is dead code is not established.

The marker sits under the dated raw prefix (`pub_tools/lake.py:127-129`). So a marker only matches within one as-of period. A new month starts with no marker.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Staging decides from a marker and the staged objects | `pub_tools/components/publog_pipeline/component.py:143-171` | `test_lake.py:259` test_marker_roundtrip_and_missing; `test_lake.py:266` test_read_marker_tolerates_corruption | `built` |
| Conversion decides from object times | `pub_tools/components/publog_pipeline/component.py:255-262` | `test_publog_io_manager.py:143` test_rerun_skips_without_rewriting | `built` |
| The event-log call returns wiped events inside a run (dagster 1.13.7) | commit `28ff07b` message; comment `pub_tools/lake.py:158-163`; no run record in the repo | no test | `built` (note: the run is reported only in the source cited; no re-readable result records it) |
