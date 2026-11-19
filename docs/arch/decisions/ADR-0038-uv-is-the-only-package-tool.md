# ADR-0038 uv is the only package tool

## Status

`built`. The lock and the CI build use uv. The rule is written in `.cursorrules:9-16` and `README.md:44`, and no check enforces it.

## Decision

Dependencies are managed with `uv`. The rules say not to use pip, poetry or conda directly. The CI build also uses `uv export` and `uv pip`.

## Why

No reason is recorded. The rule appears in the agent rules and in the README.

## Consequences

Nothing blocks a pip install. The rule is held by review.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The rule is written | `.cursorrules:9-16`; `README.md:44` | no test | `accepted` |
| CI uses uv | `.github/workflows/build-container.yml:29,71` | no test | `built` |
