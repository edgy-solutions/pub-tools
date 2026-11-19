# PUB LOG pipeline component (CO-PT-PIPELINE)

`PublogPipelineComponent` is the Dagster component that turns the public PUB LOG release into lake tables. For each source archive it defines one staging asset, which downloads the archive and stages its CSVs in the object store. For each CSV it defines one table asset, which converts it to Parquet through the dag-tools DuckDB IO manager. It also defines the jobs and schedules that run them, and it registers the IO manager the domain broker reads to advertise the tables to the mesh.

This record covers what the component and its declaration in this repo provide, and what a deployment may set. The staging and conversion code it calls is [the lake library](lake-library.md). The image that runs it is [the pub-tools image](pub-tools-image.md).

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **One component declaration defines the whole pipeline.** `definitions.py` builds the code location's definitions from the `components/` directory. The `publog_instance` declaration instantiates this component with settings read from the environment. `built`
2. **There are 15 sources and 51 tables, so the code location defines 66 assets.** 9 sources run monthly and 6 quarterly. A checked-in manifest lists each archive's CSV members, because Dagster needs every asset key at definition time and an archive's members are known only after download. A test checks the manifest against observed extraction counts. `built`
3. **Each CSV is its own asset and its own step.** A table can be rebuilt alone without downloading its archive again, and conversions run in parallel up to `max_concurrent` (default 4). No retry policy is set, so a failed step is re-run by hand or by the next scheduled run. `built`
4. **Asset keys carry the store's identity.** A table key is `<platform_instance>/<bucket>/<key_prefix>/<table>`, and a staging key is `<platform_instance>/<bucket>/_raw/<slug>`. The instance is derived from the object-store endpoint by a dag-tools helper, and the bucket comes from the lake root. The Dagster key, the catalog URN and the object path are therefore three spellings of one string. A local `file://` lake falls back to `<key_prefix>/<table>` and `<key_prefix>/source/<slug>`. `built`
5. **Both stages are incremental, and freshness is read from the lake.** Staging skips when the source's `Last-Modified` equals the one recorded in a marker beside the staged files, and every declared CSV is still present. A conversion skips when its Parquet is newer than its staged CSV. Neither stage reads Dagster's event log to decide. `built`
6. **A skipped step still declares its platform.** A skipped conversion records the IO manager's source type, and a staging step records the object-store platform and its undated prefix. Without this, the catalog sensor would register a skipped run's asset under a different platform, and the catalog would hold two entries for one table. `built`
7. **The component registers its own IO manager.** It builds a `ConfigurableDuckDBIOManager` from the destination settings and puts it in `Definitions(resources=)`. The table assets write through it. The domain broker finds the tables by reading `physical_coordinates` off that IO manager ([image, finding 3](pub-tools-image.md)). Monthly and quarterly sources share one IO manager when they share a lake root. `built`
8. **Tables are replaced in place each run.** A table's path is not dated, so each monthly run overwrites the previous month's Parquet. Dated history survives only as the staged CSVs under `_raw/<slug>/<date>/`. Monthly snapshots would need partitioned assets. The component's docstring calls that "the right way to model it", but no decision records it. `built` (replacement); `proposed` (partitioning)
9. **Two schedules are defined, and the component sets no default status for them.** The monthly schedule runs at 14:00 UTC on day 2 of each month. The quarterly schedule runs on day 2 of January, April, July and October. Whether they are switched on is left to Dagster's default and to the deployment. `built`
10. **Concurrency is bounded by memory, not disk.** The component's own sizing note gives each concurrent step about 256 MB of DuckDB plus about 250 MB of Python, with about 300 MB for the parent. If `max_concurrent` is raised without raising the pod's memory limit, the pod is killed and the run is left in a started state with no failure event. `built`. Note: the kill at a 1 Gi limit is reported only in commit `eaa1522`, and the DuckDB cap's saving only in commit `19b778b`; no re-readable result records either.
11. **The source is read anonymously over HTTPS.** The client sends a fixed user agent. It rejects an HTML page served with status 200, because the source serves its not-found page that way. It reads `Last-Modified` with a streamed GET, because the source's web server refuses HEAD. Tests cover the HTML rejection. `built`

## Settings and defaults

| Setting (environment) | Default | What it does |
|---|---|---|
| `PUBLOG_S3_BUCKET_URL` | a local-development bucket URL | the lake root; its bucket becomes part of every asset key |
| `AWS_ENDPOINT_URL` | a local-development endpoint | the object store; its host becomes the platform instance in every asset key |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | a well-known development default pair | the object-store identity for staging, conversion and the IO manager |
| `PUBLOG_MONTHLY_CRON`, `PUBLOG_QUARTERLY_CRON`, `PUBLOG_SCHEDULE_TZ` | day 2 at 14:00, monthly and quarterly; UTC | the two schedules |
| `PUBLOG_MAX_CONCURRENT` | 4 | steps per run |
| `DUCKDB_MEMORY_LIMIT` | unset: DuckDB's own default | per-step DuckDB memory cap |

The deployment chart sets the lake root, endpoint, key pair, region and memory cap for this component. Its test requires every name it sets to have a reader here or in dag-tools (invincible-agent `5ae27397`).

## Gaps

- **Unset credentials fall back to a well-known development pair** rather than failing. A deployment that forgets them writes, or fails to write, as that identity. `built`
- **No retry policy** (finding 3). `not built`
- **No monthly snapshots** (finding 8). `proposed`
- **The component's docstring still describes the old staging layout** `_raw/<as_of>/<slug>/` and a `.parquet` table suffix. The code does neither. `built` (the code); the docstring is stale.

## Not established from code

- Whether the two schedules are running in a given deployment.
- Where this component sits in the outside reference architectures. The dag-tools arch lane proposes rows L2, P1, S7 and S4 from its alignment records (dag-tools PR #75), read from this repo's description, not its code. S4 applies because the [catalog wiring](catalog-wiring.md) publishes lineage. No record here claims asynchronous publishing to subscribers (S9). `proposed`

## Sources

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`. Component paths are under `pub_tools/components/`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Definitions built from the components directory; one declaration | `pub_tools/definitions.py:1-7`; `publog_instance/component.yaml:1-26` | no test | `built` |
| 15 sources, 51 tables, 66 assets; manifest | `pub_tools/assets.py:35-58`, `:70-146` | `pub_tools_tests/test_assets.py:88`, `:98`, `:108` | `built` |
| One asset per table; `max_concurrent` 4; no retry policy | `publog_pipeline/component.py:411-492`, `:387-400`, `:494-497` | no test | `built` |
| Asset keys carry instance and bucket; local fallback | `publog_pipeline/component.py:62-79`, `:101-117`; dag-tools `dag_tools/io_managers/duckdb.py` `split_endpoint_instance` | `pub_tools_tests/test_publog_io_manager.py:211`, `:235` | `built` |
| Incremental staging and conversion; freshness from the lake | `publog_pipeline/component.py:124-222`, `:225-297`; `pub_tools/lake.py:158-257` | `pub_tools_tests/test_publog_io_manager.py:143`; `pub_tools_tests/test_lake.py:259`, `:266`, `:276` | `built` |
| Skipped steps declare their platform | `publog_pipeline/component.py:82-98`, `:263-279` | no test | `built` |
| The component registers its IO manager; shared per lake root | `publog_pipeline/component.py:499-508`, `:566-595` | `pub_tools_tests/test_publog_io_manager.py:173` | `built` |
| Tables replaced in place; partitioning named as the right model | `publog_pipeline/component.py:316-322` | no test | `built`; `proposed` |
| Two schedules, no default status set | `publog_pipeline/component.py:402-410`, `:531-538`, `:558-565` | no test | `built` |
| Memory sizing and the incident | `publog_pipeline/component.py:387-400`; invincible-agent `helm/invincible-agent/values.yaml:1237-1257`; commits `eaa1522`, `19b778b` | no test | `built`; the incident is reported in commits only |
| Anonymous HTTPS; error page rejected; GET for `Last-Modified` | `pub_tools/assets.py:19-23`, `:180-212` | `pub_tools_tests/test_assets.py:132`, `:140`, `:146` | `built` |
| Settings and defaults | `publog_instance/component.yaml:7-26` | no test | `built` |
| The chart sets only names the code reads | invincible-agent `helm/invincible-agent/values.yaml:1197-1265`; invincible-agent `tests/test_pub_tools_chart_env_is_read_by_its_code.py` (`5ae27397`) | that test | `built` |
| Stale docstring | `publog_pipeline/component.py:305-306`, `:315`, `:320` | — | `built` (the code) |
