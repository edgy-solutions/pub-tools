# ADR-0041 Public functions carry type hints

## Status

`accepted`. Written in `.cursorrules:28-31`; compliance is partial.

## Governs

CO-PT-PIPELINE, CO-PT-LAKE.

## Decision

Python code uses type hints and short docstrings. No type checker runs in CI.

## Why

No reason is recorded.

## Consequences

Three parameters lack hints: `log` and `session` in `pub_tools/lake.py` (lines 260 and 267), and `as_of_fn` in the component (line 414).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The style rule is written | `.cursorrules:28-31` | no test | `accepted` |
| Some parameters are untyped | `pub_tools/lake.py:260,267`; `pub_tools/components/publog_pipeline/component.py:414` | no test | `built` |
| No type checker runs in CI | `.github/workflows/build-container.yml` | no test | `built` |
