# ADR-0027 Catalog registration happens through the sensor only

## Status

`built`. Taken in `bf34f5c`, after `033500d` added the sensor and `16c8cfa` replaced it.

## Decision

Assets do not emit catalog metadata themselves. The `DatahubLineageComponent` sensor, from dag-tools, registers datasets and lineage. The pub-tools instance only has to expose the right asset keys and physical coordinates.

## Why

The history is mixed. `033500d` (2026-06-24) added the sensor. `16c8cfa` moved registration inline into the IO manager. `bf34f5c` (2026-07-31) went back to the sensor. The reasoning for each swing is in the commit messages and is not repeated here. The component file keeps a header comment stating the rule: sensor only, with a token read from the environment (`pub_tools/components/datahub_lineage/component.yaml:1-19`).

## Consequences

The sensor depends on dag-tools being deployed and on its token. An asset that never materializes is not registered. A skipped conversion needs the platform declared (ADR-0012).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The deployed lineage component states the sensor-only rule | `pub_tools/components/datahub_lineage/component.yaml:1-19` | no test | `accepted` |
| The token comes from the environment | `pub_tools/components/datahub_lineage/component.yaml:14-17` | no test | `built` |
| The sensor class exists in dag-tools | dag-tools `a99779f`, `dag_tools/components/datahub_lineage/component.py:372` | no test | `built` |
| Registration went sensor, inline, then sensor again | commits `033500d`, `16c8cfa`, `bf34f5c` | no test | `built` |
