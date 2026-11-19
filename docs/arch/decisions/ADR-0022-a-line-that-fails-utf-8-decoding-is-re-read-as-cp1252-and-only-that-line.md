# ADR-0022 A line that fails UTF-8 decoding is re-read as CP1252, and only that line

## Status

`built`. Taken in `e5612fa`. Not deployed; see ADR-0036.

## Governs

CO-PT-LAKE, FL-PT-PUBLISH.

## Decision

During staging, bytes are checked as UTF-8 in chunks cut at the last newline. A valid chunk is written byte for byte. For a chunk that fails, each line is checked, and only a failing line is decoded as CP1252 and re-encoded as UTF-8. The repair is logged with the member name and line number.

## Why

The commit says the case is one stray CP1252 byte (0x96, an en dash) in one file, at line 22624 of the P_HELP member of the identification archive. DuckDB 1.5.5 cannot read it under any option. The docstring says other files hold legitimate multi-byte UTF-8, so decoding everything as CP1252 would corrupt them.

## Consequences

The 0x96 byte and its line number are reported in the commit and the docstring. No run record is in the repo.

Byte counts staged for a repaired file can differ from the input, since one byte becomes three. The staged size is the written size.

Per the architect's ruling the commit is not deployed (ADR-0036).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Valid chunks pass through unchanged; failing lines are re-read as CP1252 | `pub_tools/lake.py:345-451` | `test_lake.py:381` test_stage_zip_repairs_stray_cp1252_byte_leaves_valid_utf8_untouched; `test_lake.py:408` test_stage_zip_leaves_already_valid_utf8_byte_identical; `test_lake.py:427` test_stream_utf8_normalized_handles_chunk_boundary | `built` |
| The input is a single 0x96 byte at line 22624 of one member | commit `e5612fa` message; `pub_tools/lake.py:351-353` | no test | `built` |
| The commit is not in the deployed build | invincible-agent `6b3e8efd`, `helm/invincible-agent/values.yaml:53-56` | no test | `accepted` |
