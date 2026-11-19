# Catalog wiring (CO-PT-CATALOG)

pub-tools registers what it produces in the catalog through one declaration, `datahub_lineage/component.yaml`, which instantiates the dag-tools `DatahubLineageComponent` (dag-tools CO-DATAHUB-LINEAGE). That component defines one sensor. The sensor watches this code location's asset materializations and registers each asset, its job and run, and its lineage with the catalog (dag-tools CO-DATAHUB). pub-tools writes no catalog code of its own. This record covers the declaration and what it sets; the component itself is a dag-tools record.

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **The sensor is the only path from pub-tools to the catalog.** The declaration's comment gives the reason: an IO manager is bound per asset and may be bound to assets another deployment owns, but a materialization event means this deployment produced the asset. Neither the IO manager nor the pipeline emits to the catalog. `built`
2. **The sensor is on by default.** The dag-tools component passes the declared status to the sensor, and this declaration defaults it to running. Without that, the plugin's own default leaves the sensor defined but stopped, so nothing reaches the catalog and nothing reports an error. `DATAHUB_SENSOR_STATUS=STOPPED` turns it off without a new build. `built`
3. **The catalog name of an asset is spelled from its asset key.** [CO-PT-PIPELINE](publog-pipeline.md) puts the store's instance and bucket in every key (finding 4 there). A skipped step declares its platform (finding 6 there). Between them, a table keeps one catalog entry whether its last run converted it or skipped it. `built`
4. **The catalog token is optional.** The sensor reads `DATAHUB_TOKEN` from the environment and sends it when it is set. When it is not set, the sensor sends no credential, which works only against a catalog service with authentication off. The deployment chart mounts the token from a secret marked optional, so a missing secret does not stop the pod. `built`
5. **The catalog address comes from `DATAHUB_GMS_URL`.** It is the metadata service's base address, not its GraphQL endpoint. The declaration's default is a development in-cluster address. `built`

## Connections

One row per connection, read from this component's side. Direction is one of calls, is called by, reads or writes. An id that starts `CO-PT-` is a record here; any other `CO-` id is a dag-tools arch lane record (dag-tools `docs/arch/components/`). Exposures are written out under Gaps; this table only marks them.

| # | Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|---|
| 1 | CO-DAGSTER | reads | Dagster's instance API, in the sensor's evaluation | the instance's own storage connection; not established here | this code location's materializations: asset key, declared platform, job and run | dag-tools `dag_tools/components/datahub_lineage/component.py:256-290`, `:467` | `built` |
| 2 | CO-DATAHUB | writes | HTTP, to the metadata service at `DATAHUB_GMS_URL` | `DATAHUB_TOKEN` when set; none when unset (present exposure, Gaps) | datasets, the job and run, and lineage from each staging asset to its tables | `pub_tools/components/datahub_lineage/component.yaml:21-23`; dag-tools `dag_tools/components/datahub_lineage/component.py:235`, `:657`, `:721-728` | `built` |

## Settings and defaults

| Setting (environment) | Default | What it does |
|---|---|---|
| `DATAHUB_GMS_URL` | a development in-cluster address | the catalog's metadata service |
| `DATAHUB_SENSOR_STATUS` | `RUNNING` | whether the sensor starts on |
| `DATAHUB_TOKEN` | unset: no credential sent | the catalog access token |

## Gaps

- **Present exposure: the sensor writes to the catalog with no credential when the token is absent** (finding 4). That succeeds only against a catalog that accepts unauthenticated writes, and nothing in pub-tools refuses to start without the token. If the catalog requires one, the sensor's calls fail at run time instead. Requiring the token is `not built`.
- **No test in this repo covers the declaration.** Its behaviour is tested, if at all, in dag-tools. `not built`

## Not established from code

- Whether the catalog service in a given deployment runs with authentication on, so whether the token is required there.
- Whether the token secret exists in a given deployment.

## Place in the mesh

The plane is taken from the mesh's plane list, which is held in the dag-tools records README under "Planes" (dag-tools PR #111, `16da96502281c5496b1553e3ed560c2a10e77ed0`, not yet merged). This member defines no plane of its own. Each component sits in exactly one plane. The placement is `proposed` until the list is merged and the placement is accepted.

| Aspect | What this record shows | Alignment row | Status |
|---|---|---|---|
| Plane | `data-management`: it is this member's declaration of CO-DATAHUB-LINEAGE, and it writes lineage to CO-DATAHUB (Connections 2). It reads only this code location's own materializations | AL-DCRA L3 | `proposed` |

## Sources

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`. dag-tools paths are at `a99779fa7075d45a21879c8a057e4cb36a58caf8`. invincible-agent paths are at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`.

| Claim | Source | Test | Status |
|---|---|---|---|
| One sensor, the only catalog path, and why | `pub_tools/components/datahub_lineage/component.yaml:1-18` | no test in this repo | `built` |
| On by default; the plugin's default is stopped | `pub_tools/components/datahub_lineage/component.yaml:25-28`; dag-tools `dag_tools/components/datahub_lineage/component.py:437`, `:743-755` | no test in this repo | `built` |
| Catalog name from the asset key; one entry per table | `pub_tools/components/publog_pipeline/component.py:62-117`, `:263-279` | `pub_tools_tests/test_publog_io_manager.py:211`, `:235` | `built` |
| Optional token; no credential when unset | dag-tools `dag_tools/components/datahub_lineage/component.py:721-728`; invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:133-143` | no test in this repo | `built` |
| Address from `DATAHUB_GMS_URL` | `pub_tools/components/datahub_lineage/component.yaml:21-23`; invincible-agent `helm/invincible-agent/values.yaml:1197-1265` | no test in this repo | `built` |
