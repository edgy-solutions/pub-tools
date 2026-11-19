# ADR-0009 Staging streams archive members straight into the object store

## Status

`built`. Taken in `28ff07b`.

## Decision

Staging downloads the archive to a temporary directory. It then copies each CSV member out of the zip directly to the object store, without writing the extracted file to local disk. Local disk holds the compressed archive only.

## Why

The commit message says the expanded CSVs are far larger than the archive, so expanding them locally would cost disk. The module docstring repeats the point.

## Consequences

The rule does not hold for the bare CSV sources. There the downloaded file is the CSV itself, so it sits on local disk while it is copied (`pub_tools/lake.py:292-301`).

The copy passes through the UTF-8 check described in ADR-0022.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Zip members are copied from the archive to the object store without a local expansion | `pub_tools/lake.py:283-330` | `test_lake.py:200` test_stage_zip_streams_declared_members_only (checks which members land; does not check local disk use) | `built` |
| Bare CSV sources are copied from the downloaded local file | `pub_tools/lake.py:292-301,454-459` | `test_lake.py:239` test_stage_bare_csv_source | `built` |
| The docstring states the intent | `pub_tools/lake.py:269-273` | no test | `built` |
