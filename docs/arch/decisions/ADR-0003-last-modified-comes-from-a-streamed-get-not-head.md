# ADR-0003 Last-Modified comes from a streamed GET, not HEAD

## Status

`built`. Taken in `8fed6c8`; the function body is at `pub_tools/assets.py:202-211`.

## Decision

To read a source file's Last-Modified header, the code issues a GET with `stream=True` and closes it once the headers arrive. It never sends HEAD. The body is not downloaded.

## Why

The docstring says the CDN blocks HEAD on these URLs. The commit message calls the probe "HEAD-like". No run record of the HEAD failure is in the repo, so this is not measured here.

## Consequences

The probe still opens a full GET to the source for every archive, every run. The header is the only thing read. A source with no Last-Modified makes staging raise (`pub_tools/components/publog_pipeline/component.py:139-141`).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The probe is a streamed GET that reads headers only | `pub_tools/assets.py:202-211` | `test_assets.py:146` test_last_modified_passes_through_for_real_file (reads the header from a GET response) | `built` |
| A missing Last-Modified raises | `pub_tools/components/publog_pipeline/component.py:139-141` | no test | `built` |
| The CDN blocks HEAD | `pub_tools/assets.py:205`, docstring only | no test | `accepted` |
