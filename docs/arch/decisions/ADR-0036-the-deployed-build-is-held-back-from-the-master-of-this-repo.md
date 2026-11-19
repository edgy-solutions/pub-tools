# ADR-0036 The deployed build is held back from the master of this repo

## Status

`accepted`. Ruled 2026-11-05 by the architect; recorded in `docs/cluster-build.md` (PR #3, `263d62b`).

## Governs

CO-PT-IMAGE.

## Decision

The deployment runs a pub-tools build that is not the current master. It is held until the two changes in `e5612fa` (ADR-0022 and ADR-0023) are carried forward on a reviewed build. A board item in invincible-agent tracks that carry-forward.

## Why

The page `docs/cluster-build.md:1-23` records which build runs and why it is not master. The ruling is recorded in invincible-agent commit `44a89b21` (2026-11-05, "chart: cut as 0.4.80"). The digest pin and its comment are at `helm/invincible-agent/values.yaml:53-56`.

## Consequences

ADR-0022 and ADR-0023 are built here and not live. `docs/cluster-build.md:25-36` says how to re-check the running build.

Three dependencies are declared with no recorded reason: dlt, dagster-cloud and dagster-postgres. Their commits are `e9d15fb` (Initial release, which added dlt and dagster-cloud) and `a6504fa` (dagster-postgres, "Fix missing package and bump dagster"). Removing `dagster-aws` is `proposed` only; its reason (`ea2c1fa`) is not recorded here.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The running build is recorded and is not master | `docs/cluster-build.md:1-23` | no test | `accepted` |
| The architect ruling is in the chart history | invincible-agent `6b3e8efd`, commit `44a89b21` | no test | `accepted` |
| The digest pin | invincible-agent `6b3e8efd`, `helm/invincible-agent/values.yaml:53-56` | no test | `built` |
| A reviewed carry-forward is planned | invincible-agent `6b3e8efd`, board item `01M6P5ZAJ382FBHEQDA23941EA` | no test | `proposed` |
| Removing dagster-aws | no record at the pin | no test | `proposed` |
