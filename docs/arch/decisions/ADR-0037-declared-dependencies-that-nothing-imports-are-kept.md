# ADR-0037 Declared dependencies that nothing imports are kept

## Status

`built`. The declarations are at `pyproject.toml:7-37`. No ruling keeps them, and nothing removes them.

## Decision

Several packages in `pyproject.toml` are declared and not imported by package code. `dlt[parquet,s3]` is declared at line 11. `s3fs`, `pandas` and `dagster-aws` are declared with comments that say why. Nothing removes them.

## Why

The `pyproject.toml` comments give reasons for some. The DuckDB comment is at lines 13-18, `s3fs` at 19-22, `pandas` at 23-27 (added for arm64, `38696a0`), and `dagster-aws` at 28-37 (`ea2c1fa`). No reason is recorded for `dlt`, `dagster-cloud` or `dagster-postgres`.

## Consequences

Unused packages enlarge the image and the lock. A reader assumes dlt is in use, since docstrings say "dlt-style". A cleanup would need its own commit and its own check.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| dlt is declared and not imported | `pyproject.toml:11`; searched `pub_tools/`, `scripts/`, `example/` | no test | `built` |
| Reasons are given for DuckDB, s3fs, pandas and dagster-aws | `pyproject.toml:13-37` | no test | `built` |
| No reason is given for dlt, dagster-cloud or dagster-postgres | commits `e9d15fb`, `a6504fa` | no test | `built` |
