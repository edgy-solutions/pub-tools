# Catalog wiring (CO-PT-CATALOG)

pub-tools registers what it produces in the catalog through one declaration, `datahub_lineage/component.yaml`, which instantiates the dag-tools `DatahubLineageComponent`. That component defines one sensor. The sensor watches this code location's asset materializations and registers each asset, its job and run, and its lineage with the catalog. pub-tools writes no catalog code of its own. This record covers the declaration and what it sets; the component itself is a dag-tools record.

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **The sensor is the only path from pub-tools to the catalog.** The declaration's comment gives the reason: an IO manager is bound per asset and may be bound to assets another deployment owns, but a materialization event means this deployment produced the asset. Neither the IO manager nor the pipeline emits to the catalog. `built`
2. **The sensor is on by default.** The dag-tools component passes the declared status to the sensor, and this declaration defaults it to running. Without that, the plugin's own default leaves the sensor defined but stopped, so nothing reaches the catalog and nothing reports an error. `DATAHUB_SENSOR_STATUS=STOPPED` turns it off without a new build. `built`
3. **The catalog name of an asset is spelled from its asset key.** The [pipeline component](publog-pipeline.md) puts the store's instance and bucket in every key (finding 4 there). A skipped step declares its platform (finding 6 there). Between them, a table keeps one catalog entry whether its last run converted it or skipped it. `built`
4. **The catalog token is optional.** The sensor reads `DATAHUB_TOKEN` from the environment and sends it when it is set. When it is not set, the sensor sends no credential, which works only against a catalog service with authentication off. The deployment chart mounts the token from a secret marked optional, so a missing secret does not stop the pod. `built`
5. **The catalog address comes from `DATAHUB_GMS_URL`.** It is the metadata service's base address, not its GraphQL endpoint. The declaration's default is a development in-cluster address. `built`

## Settings and defaults

| Setting (environment) | Default | What it does |
|---|---|---|
| `DATAHUB_GMS_URL` | a development in-cluster address | the catalog's metadata service |
| `DATAHUB_SENSOR_STATUS` | `RUNNING` | whether the sensor starts on |
| `DATAHUB_TOKEN` | unset: no credential sent | the catalog access token |

## Gaps

- **Nothing fails when the token is missing and the catalog requires one.** The sensor's calls fail at run time instead of at start-up. `built` (the optional token); a check at start-up is `not built`.
- **No test in this repo covers the declaration.** Its behaviour is tested, if at all, in dag-tools. `not built`

## Not established from code

- Whether the catalog service in a given deployment runs with authentication on, so whether the token is required there.
- Whether the token secret exists in a given deployment.

## Sources

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`. dag-tools paths are at `a99779fa7075d45a21879c8a057e4cb36a58caf8`. invincible-agent paths are at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`.

| Claim | Source | Test | Status |
|---|---|---|---|
| One sensor, the only catalog path, and why | `pub_tools/components/datahub_lineage/component.yaml:1-18` | no test in this repo | `built` |
| On by default; the plugin's default is stopped | `pub_tools/components/datahub_lineage/component.yaml:25-28`; dag-tools `dag_tools/components/datahub_lineage/component.py:437`, `:743-755` | no test in this repo | `built` |
| Catalog name from the asset key; one entry per table | `pub_tools/components/publog_pipeline/component.py:62-117`, `:263-279` | `pub_tools_tests/test_publog_io_manager.py:211`, `:235` | `built` |
| Optional token; no credential when unset | dag-tools `dag_tools/components/datahub_lineage/component.py:721-728`; invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:133-143` | no test in this repo | `built` |
| Address from `DATAHUB_GMS_URL` | `pub_tools/components/datahub_lineage/component.yaml:21-23`; invincible-agent `helm/invincible-agent/values.yaml:1197-1265` | no test in this repo | `built` |
