# ADR-0018 Headers are cleaned, colliding names are renamed, and short rows are padded

## Status

`built`. Taken in `28ff07b`.

## Governs

CO-PT-LAKE, FL-PT-PUBLISH.

## Decision

Column names are cleaned: stripped, lower-cased, and with spaces, dots, hyphens and slashes turned into underscores. If two cleaned names collide, the later ones get a numeric suffix. Short rows are padded, and the padding is an empty string, not NULL.

## Why

The `28ff07b` message says these behaviours were preserved from the old loader and tested against the old behaviour. The code comment says downstream consumers depend on the cleaned names. Parquet cannot hold two columns with one name.

## Consequences

The cleaned names are a contract with consumers. Changing the rules renames columns. The census named `clean_header` for this; the rules live at the cited lines.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| clean_header rules | `pub_tools/lake.py:462-470` | `test_lake.py:193` test_clean_header | `built` |
| Collisions get a numeric suffix | `pub_tools/lake.py:508-520` | `test_lake.py:322` test_csv_to_parquet_deduplicates_colliding_column_names | `built` |
| Short rows become empty strings | `pub_tools/lake.py:584-606` | `test_lake.py:310` test_csv_to_parquet_pads_short_rows_with_empty_string | `built` |
| Cleaned headers survive the IO manager path | `pub_tools/components/publog_pipeline/component.py:297` | `test_publog_io_manager.py:109` test_headers_are_cleaned | `built` |
