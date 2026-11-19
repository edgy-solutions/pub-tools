# ADR-0042 Destructive actions need explicit permission

## Status

`accepted`. Written in `AGENTS.md:13-15`.

## Governs

No component or flow. It is a rule for work on the repository.

## Decision

An agent may not delete, drop or overwrite lake paths, local database files or raw archives without explicit, verified permission from the user. It may not overwrite the core Dagster definitions or the IO manager wiring without a requested refactor.

## Why

The text gives safety as the reason.

## Consequences

The rule is read by the agent and is not enforced by code. For local tests the rule says to default to a temporary path.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The rule is written | `AGENTS.md:13-15` | no test | `accepted` |
| Nothing in the repo enforces it | searched `pub_tools/`, `scripts/` at the pin | no test | `built` |
