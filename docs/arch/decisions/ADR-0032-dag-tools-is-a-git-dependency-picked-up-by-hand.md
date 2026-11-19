# ADR-0032 dag-tools is a git dependency, picked up by hand

## Status

`built`. Taken in `b4bcd42`; the pin is at `uv.lock:1421-1423`.

## Governs

CO-PT-IMAGE.

## Decision

pub-tools depends on `edgy-dag-tools` from the dag-tools repo's default branch, through `[tool.uv.sources]`. `uv.lock` pins the exact commit, which is `a99779f`. Direct references are allowed in the build settings. New dag-tools code is picked up by re-locking.

## Why

The `b4bcd42` message gives the package rename. The pick-up commits (`fe0adff`, `d5042fe`, `506397e`, `e8fe19f`, `1aa2cbe`) each re-locked. `1aa2cbe` moves to `a99779f`, nine dag-tools commits including the broker URN fix, and says "50 tests pass". No reason for a git dependency over a published package is recorded.

## Consequences

The lock holds the commit. A move to dag-tools needs a commit in this repo. The pin follows a branch, not a tag, and a re-lock can pick up a surprise. No process is written for who re-locks, or when.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The dependency is a git source on the default branch | `pyproject.toml:57-71` | no test | `built` |
| Direct references are allowed | `pyproject.toml:88-89` | no test | `built` |
| The lock pins a99779f | `uv.lock:1421-1423` | no test | `built` |
| Pick-up commits exist | commits `fe0adff`, `d5042fe`, `506397e`, `e8fe19f`, `1aa2cbe` | no test | `built` |
