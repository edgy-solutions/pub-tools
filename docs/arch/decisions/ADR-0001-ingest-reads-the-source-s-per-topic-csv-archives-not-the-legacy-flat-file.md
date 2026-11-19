# ADR-0001 Ingest reads the source's per-topic CSV archives, not the legacy flat file

## Status

`built`. Taken in `8fed6c8`.

## Decision

Ingest reads the public PUB LOG release as the source's per-topic CSV archives. There are nine monthly zip archives. The quarterly set is one zip (the FLISV topic) and five bare CSV files (the MRD files). Each zip holds one CSV per logical table.

The legacy single flat-file endpoint is not used.

## Why

The commit says the legacy flat-file endpoint is dead and replaces it with the per-topic archives. The module docstring says each topic ships as standard RFC 4180 CSV. Nothing in the repo records when or how the legacy endpoint died. That is not established here.

## Consequences

Two kinds of source exist: zips with several members, and bare CSV files with one. Staging has a separate branch for the bare files (`pub_tools/lake.py:292-301`).

The commit describes two assets, a monthly one and a quarterly one. That shape was replaced later; see ADR-0008.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Nine monthly archive URLs are declared | `pub_tools/assets.py:35-48` | no test | `built` |
| The quarterly set is one zip plus five bare CSV files | `pub_tools/assets.py:50-60` | no test | `built` |
| The commit replaces the legacy endpoint with the per-topic archives | commit `8fed6c8` | no test | `built` |
| The legacy endpoint is dead | commit `8fed6c8` message only | no test | `accepted` |
