# ADR-0031 The pub-tools name is reserved for real pipelines

## Status

`accepted`. Taken in `93b6982`.

## Decision

The name `pub-tools` is kept for code that runs real data pipelines. Demos and mesh scaffolding live elsewhere.

## Why

The `93b6982` message gives the rule and names two dag-tools commits (`9055f09` and `3c1524e`) where the scaffolding went. It names a third dag-tools commit, which was not found in the local dag-tools repo, and is not cited here.

## Consequences

Nothing enforces it. It is a naming rule held by review.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The rule is stated in the commit message | commit `93b6982` message | no test | `accepted` |
| Two named dag-tools commits exist | dag-tools `9055f09`, `3c1524e` | no test | `built` |
