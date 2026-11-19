# ADR-0002 Requests carry a recent browser User-Agent

## Status

`built`. Taken in `8fed6c8`.

## Decision

Every request to the source sends a pinned, recent desktop-browser User-Agent. The source's CDN bot manager is reported to refuse curl, wget and Schannel TLS fingerprints. The code comment says the string must not be "cleaned up" without testing against the live endpoint, or ingest will fail with a 403.

## Why

The module docstring and the commit message give the same reason: the CDN rejects those clients, and Python `requests` with a browser User-Agent gets through. The census called this measured against the live endpoint. No record of such a run is in the repo, so this record says `built`.

## Consequences

The string pins a browser major version, and it will age. Nothing in the repo says how old a string the CDN accepts. That is not established.

The commit reports a check whose record is not in the repo. No test covers the header value.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| One session sets the User-Agent header on every request | `pub_tools/assets.py:19-23,180-183` | no test | `built` |
| The docstring forbids changing the string without a live test | `pub_tools/assets.py:8-11` | no test | `accepted` |
| The CDN rejects curl, wget and Schannel | commit `8fed6c8` message; no run record in the repo | no test | `built` |
