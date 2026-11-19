# ADR-0035 Concurrency is bounded by memory: four runs, a DuckDB memory limit, a pod limit

## Status

`accepted`. Taken in `eaa1522` and `19b778b`; the limits themselves live in the deployment chart.

## Decision

Three limits work together. The component runs at most four steps at once (`max_concurrent` defaults to 4). Each DuckDB connection caps its own memory by `DUCKDB_MEMORY_LIMIT`. The pod has its own limit in the deployment chart.

The sizing is written intent. Nothing in this repo checks that four runs fit in the pod limit.

## Why

`eaa1522` cut concurrency from 6 to 4 after an out-of-memory kill at a 1 Gi limit, which left a run stuck in STARTED for about 8 hours. `19b778b` added `DUCKDB_MEMORY_LIMIT`. Its message records a measured case on 5M rows: a 256MB cap cut peak resident memory from 434MB to 136MB for about 6% more wall time. The same numbers are in the code comment at `pub_tools/lake.py:78-84` and in the chart.

## Consequences

The chart sizing is outside this repo (`helm/invincible-agent/values.yaml:1235-1257`, with a 3Gi pod limit). A larger CSV or a higher concurrency can break the fit. The chart comment records a 251 MB CSV that killed a pod at 1 Gi.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| max_concurrent defaults to 4 | `pub_tools/components/publog_pipeline/component.py:387-400`; `pub_tools/components/publog_instance/component.yaml:17` | no test | `built` |
| The memory limit comes from the environment | `pub_tools/components/publog_pipeline/component.py:589`; `pub_tools/lake.py:78-96` | `test_lake.py:125` and `test_lake.py:138` memory limit tests | `built` |
| A 256MB cap cut peak resident memory from 434MB to 136MB at about 6% more wall time | commit `19b778b` message; `pub_tools/lake.py:78-84` | no test | `measured` |
| Concurrency was cut from 6 to 4 after an out-of-memory kill | commit `eaa1522` message | no test | `built` |
| The pod limit and sizing | invincible-agent `6b3e8efd`, `helm/invincible-agent/values.yaml:1235-1257` | no test | `accepted` |
