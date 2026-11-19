# Publishing a data product (FL-PT-PUBLISH)

This flow follows one PUB LOG table from the public source to its entry in the catalog and its route in the mesh. It asks what starts the work, where the bytes land, how the table gets its one identity, and what a reader is handed when it asks for the table. It crosses four components: the [pipeline](../components/publog-pipeline.md), the [lake library](../components/lake-library.md), the [catalog wiring](../components/catalog-wiring.md) and the [image](../components/pub-tools-image.md), which runs the broker.

The flow has two halves that do not wait for each other. The run and the catalog sensor publish the table's data and its catalog entry. The broker advertises a route to the table when its pod starts, whether or not the table has been written. The gateway's half of a read, and the access decision, are dag-tools records (dag-tools `docs/arch/flows/need-to-know.md`) and are not repeated here.

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **A run starts from a schedule or by hand, and nothing else starts one.** There is no sensor on the source and no event from it. The source's `Last-Modified` is read only inside a run, by the staging step. `built`
2. **The bytes pass through the pod twice and are held on its disk once.** The archive is downloaded to a temporary directory, and its members are streamed to the store in chunks; Python handles those bytes, line by line where it repairs an encoding fault. The conversion then reads the staged CSV from the store and writes Parquet back to it inside DuckDB, so no rows pass through Python there. `built`
3. **The table has one identity, spelled the same in four places.** The asset key holds the store's instance, the bucket and the path. The IO manager writes to the path that key names. The catalog sensor names the dataset from the key and the platform the step declared. The broker names its route from the key and the platform on its ticket. A dag-tools test pins the agreement between the sensor's name and the broker's. `built`
4. **The catalog entry follows the run; the route does not.** The sensor registers a table after a run materializes it. The broker registers routes for every asset the code location can materialize, when its pod starts and every two minutes after, so a route can exist for a table that has never been written. A read of such a table gets a ticket that points at nothing. `built`
5. **The staging step has a catalog entry too, and lineage joins it to its tables.** The staging step declares the object-store platform and its undated prefix, so its entry is the staged dataset rather than a Dagster-only entity. Each table asset depends on its staging asset, and the sensor records that dependency as lineage. `built`
6. **A reader is handed the producer's write credentials.** At the pinned dag-tools, the ticket the broker returns is the one the IO manager built, and it contains the key pair pub-tools writes with ([image, finding 5](../components/pub-tools-image.md)). `built` at the pin; dag-tools v0.16.0 changes it, and pub-tools has not moved.
7. **A skipped run publishes again, without new data.** When nothing changed, both steps skip, and each still emits a materialization carrying its declared platform. The sensor sees a materialization and re-registers the same entry under the same name; the catalog gains a run but not a second dataset. `built`

## The flow, step by step

| # | Step | Component | Status | Evidence |
|---|---|---|---|---|
| 1 | The monthly or quarterly schedule, or an operator, starts the job for one cadence's assets. | pipeline | `built` | `publog_pipeline/component.py:524-538`, `:551-565` |
| 2 | The staging step reads the source's `Last-Modified` with a streamed GET and compares it with the marker beside the last staging. If they match and every declared CSV is present, it skips. | pipeline, lake library | `built` | `publog_pipeline/component.py:124-172`; `pub_tools/assets.py:202-212`; `pub_tools/lake.py:242-258` |
| 3 | Otherwise it downloads the archive and streams each declared member to `_raw/<slug>/<date>/`, repairing a stray legacy-encoded byte, and writes the marker last. | lake library | `built` | `pub_tools/lake.py:259-344`, `:345-453`; `publog_pipeline/component.py:173-222` |
| 4 | The staging step's materialization declares the object-store platform and the undated prefix. | pipeline | `built` | `publog_pipeline/component.py:82-98` |
| 5 | Each table step checks whether its Parquet is newer than its staged CSV. If so it skips, and still declares the IO manager's platform. | pipeline, lake library | `built` | `publog_pipeline/component.py:225-279`; `pub_tools/lake.py:132-156`, `:204-241` |
| 6 | Otherwise it returns a lazy DuckDB query over the staged CSV. | pipeline, lake library | `built` | `publog_pipeline/component.py:280-297`; `pub_tools/lake.py:609-628` |
| 7 | The DuckDB IO manager runs the query and writes Parquet to the path the asset key names, with zstd compression and files of up to 128 MB. It records the row count and its platform. | dag-tools IO manager, configured by the pipeline | `built` | dag-tools `dag_tools/io_managers/duckdb.py:94`, `:200-239`, `:380`; `publog_pipeline/component.py:571-595` |
| 8 | The catalog sensor, on its next tick, reads the materialization, names the dataset from the asset key and the declared platform, and registers the dataset, the job and run, and the lineage from the staging asset. | catalog wiring (dag-tools sensor) | `built` | dag-tools `dag_tools/components/datahub_lineage/component.py:256-290`, `:467`, `:537-538`, `:568` |
| 9 | Separately, when the broker's pod starts, it loads the code location's definitions, keeps the assets it can materialize, and names each one's route the way the sensor names its dataset. | image (dag-tools broker) | `built` | dag-tools `dag_tools/domain_broker/main.py:96-130`, `:201-224`, `:278`, `:300-320` |
| 10 | For each table it stores the IO manager's coordinates as the routing ticket. | image (dag-tools broker) | `built` | dag-tools `dag_tools/domain_broker/main.py:41-80`; `dag_tools/io_managers/duckdb.py:428-476` |
| 11 | It registers its list of names with the gateway, and again every 120 seconds. | image (dag-tools broker) | `built` | dag-tools `dag_tools/domain_broker/main.py:327-375`, `:376-418` |
| 12 | When the gateway resolves a name, the broker returns the stored ticket unchanged. | image (dag-tools broker) | `built` | dag-tools `dag_tools/domain_broker/main.py:451-468` |

## Gaps

- **Readers receive write credentials** (finding 6). `not built` at the pin.
- **A route can point at a table that does not exist yet** (finding 4). Nothing checks that a route's table has been written. `not built`
- **The sensor-and-broker agreement is tested in dag-tools, not here.** No pub-tools test builds a pub-tools asset's name both ways. `not built`
- **A failed step is not retried** ([pipeline, finding 3](../components/publog-pipeline.md)), so the catalog entry and the data stay at the last good run until the next run. `not built`

## Not established from code

- Whether the schedules are running in a given deployment, so whether step 1 happens without an operator.
- Whether the catalog service requires a token in a given deployment, so whether step 8 succeeds there.
- Whether the broker's registration with the gateway succeeds in a given deployment (step 11).
- How long after a run the sensor registers it; that depends on the sensor's interval and the deployment's daemon, and no run was recorded.

## Sources

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`; `publog_pipeline/` is under `pub_tools/components/`. dag-tools paths are at `a99779fa7075d45a21879c8a057e4cb36a58caf8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Runs start only from a schedule or by hand | `publog_pipeline/component.py:494-569` | no test | `built` |
| Bytes through the pod; disk holds the archive only; no rows through Python | `pub_tools/lake.py:259-344`, `:609-628` | `pub_tools_tests/test_lake.py:200`; `pub_tools_tests/test_publog_io_manager.py:73` | `built` |
| One identity in key, path, catalog name and route | `publog_pipeline/component.py:62-117`; `pub_tools/lake.py:132-156`; dag-tools `dag_tools/domain_broker/main.py:96-130`; dag-tools `dag_tools/components/datahub_lineage/component.py:256-290` | dag-tools `dag_tools_tests/test_broker_physical_urn_identity.py:67`, `:104`; `pub_tools_tests/test_publog_io_manager.py:211` | `built` |
| Routes for every materializable asset, at start and every 120 seconds | dag-tools `dag_tools/domain_broker/main.py:201-224`, `:278`, `:346-375` | no test in this repo | `built` |
| Staging entry and lineage to tables | `publog_pipeline/component.py:82-98`, `:483` | no test | `built` |
| Reader gets the producer's key pair | dag-tools `dag_tools/io_managers/duckdb.py:465-476`; dag-tools `dag_tools/domain_broker/main.py:451-468` | `pub_tools_tests/test_publog_io_manager.py:185` | `built` |
| A skipped run still declares its platform | `publog_pipeline/component.py:263-279` | `pub_tools_tests/test_publog_io_manager.py:143` | `built` |
