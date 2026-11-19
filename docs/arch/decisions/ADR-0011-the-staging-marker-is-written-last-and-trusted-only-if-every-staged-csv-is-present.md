# ADR-0011 The staging marker is written last and trusted only if every staged CSV is present

## Status

`built`. Taken in `28ff07b`.

## Decision

Staging writes the `_source.json` marker only after all CSVs are staged. A later run skips staging only if the marker's Last-Modified matches the source and every declared CSV is still in the lake. If a CSV is missing, staging logs a warning and re-stages.

## Why

The code comment says an interrupted staging then leaves no marker, so the next run re-stages and does not trust a half-written prefix.

## Consequences

The check calls `object_exists` once per member, per run.

The presence check and the write-last ordering are in the component, and no test runs them. The tests cover only the marker read and write helpers.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The marker is written after all members are staged | `pub_tools/components/publog_pipeline/component.py:197-208` | no test | `built` |
| The skip needs a matching Last-Modified and every CSV present | `pub_tools/components/publog_pipeline/component.py:146-171` | no test | `built` |
| The marker round-trips and a corrupt marker reads as unknown | `pub_tools/lake.py:242-256` | `test_lake.py:259` test_marker_roundtrip_and_missing; `test_lake.py:266` test_read_marker_tolerates_corruption | `built` |
