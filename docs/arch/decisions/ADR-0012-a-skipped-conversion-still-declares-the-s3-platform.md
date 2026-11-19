# ADR-0012 A skipped conversion still declares the s3 platform

## Status

`built`. Taken in `25dabf4`.

## Governs

CO-PT-PIPELINE, CO-PT-CATALOG, FL-PT-PUBLISH.

## Decision

When a table conversion skips, it returns a `MaterializeResult` and the IO manager never runs. The result still carries `destination_name` set to the IO manager's source type, so the catalog sensor can resolve the s3 platform.

## Why

The `25dabf4` message says a skipped table re-registered in the catalog under the `unknown` platform. That made a second dataset, separate from the one the write produced. It says this was confirmed in a deployment, where one table appeared under both s3 and unknown.

## Consequences

The same commit picks up a dag-tools change that stops emitting an unknown physical dataset for assets with no platform. The fix is built here. The commit records the fork but no run showing it gone, so the effect of the fix is not established.

`test_rerun_skips_without_rewriting` checks the skip. It does not check the platform field.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The skip result carries destination_name | `pub_tools/components/publog_pipeline/component.py:266-281` | `test_publog_io_manager.py:143` test_rerun_skips_without_rewriting (checks skipped_reason only) | `built` |
| Before the fix, one table appeared under both s3 and unknown in a deployment | commit `25dabf4` message | no test | `built` (note: the run is reported only in the source cited; no re-readable result records it) |
