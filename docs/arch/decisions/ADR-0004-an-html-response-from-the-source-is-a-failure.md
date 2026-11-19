# ADR-0004 An HTML response from the source is a failure

## Status

`built`. Taken in `fcdc305`.

## Decision

Any response with a `text/html` content type is treated as an error, for both the Last-Modified probe and the download. The code raises a RuntimeError that names the URL and says the file was probably moved or withdrawn.

## Why

The source serves its 404 page as HTTP 200 with `text/html`, so `raise_for_status()` sees nothing wrong. Before this check the failure appeared late, as a bad-zip error or a CSV of markup. The commit message gives the example of a dead quarterly URL that failed as "No Last-Modified header".

## Consequences

A legitimate response with an HTML content type would also be rejected. None is expected from these URLs.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Both the probe and the download reject an HTML response | `pub_tools/assets.py:186-199,210,218` | `test_assets.py:132` test_html_error_page_rejected_by_last_modified; `test_assets.py:140` test_html_error_page_rejected_by_download | `built` |
| The source returns its 404 page as HTTP 200 | commit `fcdc305` message; docstring `pub_tools/assets.py:189-192` | no test | `built` |
