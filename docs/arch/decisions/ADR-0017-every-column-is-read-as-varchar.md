# ADR-0017 Every column is read as VARCHAR

## Status

`built`. Taken in `28ff07b`.

## Governs

CO-PT-LAKE, FL-PT-PUBLISH.

## Decision

CSV reads use `all_varchar=true, header=true, null_padding=true`. Every column lands in Parquet as text. Types are not inferred at ingest.

## Why

PUB LOG identifiers (NSN, NIIN, FSC, CAGE) are zero-padded, and inference would turn `01234` into `1234`. The code comment says that silently corrupts them. The commit message adds that the option states the type rule directly.

## Consequences

Consumers get text and must cast it themselves. The option also applies to the relation path used in production, which a test pins.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The read options are fixed in one constant | `pub_tools/lake.py:28-31` | `test_lake.py:283` test_csv_to_parquet_preserves_leading_zeros_and_types | `built` |
| Leading zeros survive the relation path used by the table assets | `pub_tools/lake.py:609-626` | `test_publog_io_manager.py:90` test_leading_zeros_survive_the_relation_path | `built` |
