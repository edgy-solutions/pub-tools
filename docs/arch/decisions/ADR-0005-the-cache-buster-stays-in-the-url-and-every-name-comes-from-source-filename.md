# ADR-0005 The cache-buster stays in the URL, and every name comes from source_filename

## Status

`built`. Taken in `fcdc305`.

## Governs

CO-PT-PIPELINE, FL-PT-PUBLISH.

## Decision

One quarterly zip link carries a `?ver=` query parameter. The parameter stays in the URL, because the source needs it. Every place that needs a filename, a manifest key or a slug calls `source_filename()`, which drops the query and fragment. Nothing splits the URL on `/` by hand.

## Why

The code comment says that without the parameter the portal returns its 404 page. The commit message says that splitting on `/` made the zip-suffix check, the manifest lookup and the asset slug all fail at once.

## Consequences

The parameter value is a literal in the URL list. If the source rotates it, the file will return the HTML page and ADR-0004 will catch it. Nothing in the repo says how often the value changes. That is not established.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The quarterly zip URL carries a required cache-buster | `pub_tools/assets.py:50-54` | no test | `built` |
| source_filename strips query and fragment | `pub_tools/assets.py:148-157` | `test_assets.py:63` test_source_filename_ignores_query_and_fragment | `built` |
| The slug is derived through source_filename | `pub_tools/components/publog_pipeline/component.py:56-59` | `test_assets.py:76` test_source_slug | `built` |
