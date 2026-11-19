# ADR-0028 The lineage sensor defaults to RUNNING

## Status

`built`. Taken in `bf34f5c`.

## Decision

The catalog sensor is created RUNNING, not STOPPED. Deployments need no manual start.

Scope: this rule applies across the mesh; its owning record belongs in dag-tools, which has not written one.

## Why

The component comment says a sensor left stopped silently registers nothing. No further reason is recorded.

## Consequences

A fresh deployment starts registering at once. The default is set by the dag-tools component (`dag_tools/components/datahub_lineage/component.py:437`), and this repo's deployed file restates it (`pub_tools/components/datahub_lineage/component.yaml:25-28`).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The deployed component sets the default status to RUNNING | `pub_tools/components/datahub_lineage/component.yaml:25-28` | no test | `built` |
| dag-tools exposes a default_status field | dag-tools `a99779f`, `dag_tools/components/datahub_lineage/component.py:437` | no test | `built` |
