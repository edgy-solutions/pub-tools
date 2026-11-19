# ADR-0040 The agent-facing docs describe data movement and must match the code

## Status

`accepted`. Written in `AGENTS.md:22-27`; the docs were brought up to date in `5bfc98e`.

## Governs

No component or flow. It is a rule for work on the repository.

## Decision

`README.md`, `llms.txt` and `.cursorrules` state how data moves. They say DuckDB, an object store, and Parquet. They are kept in step with the code by hand.

## Why

`28ff07b` and `f02dc26` changed the pipeline and did not touch these docs. `5bfc98e` brought them up to date later. The rule at `AGENTS.md:22-27` (ADR-0044) is the reason such a drift matters.

## Consequences

`llms.txt:15` still says `<table>.parquet/` (ADR-0026). Some function parameters carry no type hint: `log` and `session` at `pub_tools/lake.py:260,267`, and `as_of_fn` at `pub_tools/components/publog_pipeline/component.py:414`. The style rule is in ADR-0041.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The three docs describe DuckDB | `README.md`, `llms.txt:8-12`, `.cursorrules:22-25` | no test | `accepted` |
| The earlier commits left the docs untouched | commits `28ff07b`, `f02dc26` (`--stat`) | no test | `built` |
| llms.txt still gives the old suffix | `llms.txt:15` | no test | `built` |
