# ADR-0030 The broker reads physical coordinates from the IO manager

## Status

`accepted`. Taken in `42cb35d`; the code to honor it is in dag-tools.

## Decision

The domain broker advertises each table by asking the registered IO manager for its physical coordinates. The component therefore registers the IO manager under a key the broker can find, and takes no part in advertising.

## Why

The `42cb35d` message says registering the IO manager makes the tables visible to the broker. The README describes the broker.

## Consequences

The rule is carried out by dag-tools (`physical_coordinates`, `dag_tools/io_managers/duckdb.py:428` at `a99779f`). This repo depends on it but cannot enforce it. The test below checks only that the manager is registered.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The IO manager is registered where the broker looks | `pub_tools/components/publog_pipeline/component.py:567-569` | `test_publog_io_manager.py:173` test_io_manager_is_registered_for_the_broker | `built` |
| An s3 table advertises a ticket | `pub_tools/components/publog_pipeline/component.py:571-595` | `test_publog_io_manager.py:185` test_s3_advertises_a_ticket | `built` |
| physical_coordinates exists on the dag-tools IO manager | dag-tools `a99779f`, `dag_tools/io_managers/duckdb.py:428` | no test | `built` |
| The broker advertises what the IO manager reports | `README.md:20` | no test | `accepted` |
