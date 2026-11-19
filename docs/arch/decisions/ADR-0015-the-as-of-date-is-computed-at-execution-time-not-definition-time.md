# ADR-0015 The as-of date is computed at execution time, not definition time

## Status

`built`. Taken in `fcdc305`.

## Decision

Each asset receives an `as_of_fn` and calls it inside its compute function. The date is not fixed when definitions load. The monthly job uses the first of the current month. The quarterly job uses the first day of the current quarter.

## Why

A code server can stay up across a month boundary. The docstring says every derived name must come from one call so the names cannot disagree. The commit message says a date fixed at definition time would freeze the month.

## Consequences

The staging step and each table step call `as_of_fn` separately, so a run that crosses midnight at a month boundary could in principle see two dates. Whether that has happened is not established.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| as_of_fn is called inside each compute function | `pub_tools/components/publog_pipeline/component.py:411-424,440-448,465-476` | no test | `built` |
| Monthly and quarterly as-of rules | `pub_tools/components/publog_pipeline/component.py:50-53,518,544` | no test | `built` |
