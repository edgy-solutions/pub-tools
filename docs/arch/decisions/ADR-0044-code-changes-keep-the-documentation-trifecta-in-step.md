# ADR-0044 Code changes keep the documentation trifecta in step

## Status

`accepted`. Written in `AGENTS.md:22-27`.

## Governs

No component or flow. It is a rule for work on the repository.

## Decision

When a core technology changes, `llms.txt`, `.cursorrules` and `README.md` are updated in the same change. The rule is aimed at agents.

## Why

The rule gives self-maintenance of the guardrails as the reason. `5bfc98e` is a late catch-up after the DuckDB switch missed it.

## Consequences

Nothing checks it. The DuckDB switch (`28ff07b`) and the extension bake (`f02dc26`) missed it.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The rule is written | `AGENTS.md:22-27` | no test | `accepted` |
| The two commits missed the docs | commits `28ff07b`, `f02dc26` (`--stat`) | no test | `built` |
| The catch-up commit | commit `5bfc98e` | no test | `built` |
