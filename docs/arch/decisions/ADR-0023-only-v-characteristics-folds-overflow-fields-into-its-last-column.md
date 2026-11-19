# ADR-0023 Only V_CHARACTERISTICS folds overflow fields into its last column

## Status

`built`. Taken in `e5612fa`. Not deployed; see ADR-0036.

## Decision

For tables in `MERGE_OVERFLOW_TABLES`, which holds only `v_characteristics`, the read declares 16 extra VARCHAR columns (`OVERFLOW_COLUMNS = 16`). The last real column, `CLEAR_TEXT_REPLY`, is rebuilt by joining it and the overflow columns with newlines. Every other table still fails loudly on a ragged row.

## Why

The commit says the source's export turned line breaks inside the free-text column into field separators. It says 12 of 44.6M rows had 5-7 fields. The code comment says the known rows have at most 3 extra fields, and that 16 is headroom that still fails a truly corrupt row. For other tables a column-count mismatch is more likely real corruption, and folding would hide it.

## Consequences

The rejoin uses a newline, which matches what the export removed, but the original separator is not recorded anywhere. That is not established.

The test fixtures hold 3 ragged rows, not 12. Per the architect's ruling the commit is not deployed (ADR-0036); a board item carries it forward.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The overflow merge applies to one table only | `pub_tools/lake.py:477-491` | `test_lake.py:471` test_csv_to_parquet_merge_overflow_folds_ragged_rows | `built` |
| Other tables still raise on a ragged row | `pub_tools/lake.py:553-570` | `test_lake.py:522` test_csv_to_parquet_without_merge_overflow_still_raises_on_ragged_rows | `built` |
| The relation path used in production agrees with the COPY path | `pub_tools/lake.py:609-626` | `test_lake.py:535` test_csv_relation_merge_overflow_matches_csv_to_parquet | `built` |
| 12 of 44.6M rows were ragged | commit `e5612fa` message | no test | `built` (note: the run is reported only in the source cited; no re-readable result records it) |
