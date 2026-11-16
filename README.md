# pub-tools

Ingests the Defense Logistics Agency (DLA) PUB LOG, published as CSV archives in the FLIS Electronic Reading Room, into an S3 Parquet lake.

[Dagster](https://dagster.io/) orchestrates it. [DuckDB](https://duckdb.org/) does the conversion, reading and writing object storage directly.

## How it works

One Dagster component (`pub_tools/components/publog_pipeline`, configured by `pub_tools/components/publog_instance/component.yaml`) builds two kinds of asset.

1. **Staging, one asset per source archive** (`<instance>/<bucket>/_raw/<slug>`). It downloads the archive and streams each CSV member into the lake at `<lake_root>/_raw/<slug>/<as_of_date>/`, with the slug before the date. Nothing is extracted to local disk. Stray CP1252 bytes are repaired to UTF-8 on the way. The asset skips when the source's `Last-Modified` has not changed.
2. **Conversion, one asset per table** (`<instance>/<bucket>/<key_prefix>/<table>`, for example `.../publog/v_cage_address`). DuckDB reads the staged CSV over `httpfs` with `all_varchar=true` (`DUCKDB_READ_CSV_OPTIONS` in `pub_tools/lake.py`), and the asset returns a lazy relation. dag-tools' `ConfigurableDuckDBIOManager` writes it as a directory of Parquet parts at `<lake_root>/<key_prefix>/<table>.parquet/`. The asset skips when its Parquet is newer than the staged CSV.

Table Parquet is not date-versioned: each run replaces the table in place. Dated history is kept only as the staged CSVs under `_raw/`.

Table asset keys come from `PUBLOG_SOURCE_MANIFEST` (`pub_tools/assets.py`), because Dagster needs keys before any archive has been downloaded. Staging compares the manifest with the archive and warns when they differ.

Monthly and quarterly schedules run the pipeline. A second component (`pub_tools/components/datahub_lineage`, from dag-tools) sends each materialization's lineage to DataHub.

The same image also runs the dag-tools domain broker (`hypercorn dag_tools.domain_broker.main:app` with `DAGSTER_DEFS_MODULE=pub_tools.definitions:defs`). The broker registers the tables with the mesh's central gateway, using the IO manager's physical coordinates.

## Configuration

The code reads these environment variables:

| Variable | Read by | Default |
|---|---|---|
| `PUBLOG_S3_BUCKET_URL` | `publog_instance/component.yaml` | `s3://publog-lake` |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | `publog_instance/component.yaml` | `minioadmin` |
| `AWS_ENDPOINT_URL` | `publog_instance/component.yaml` | `http://minio:9000` |
| `PUBLOG_MONTHLY_CRON`, `PUBLOG_QUARTERLY_CRON`, `PUBLOG_SCHEDULE_TZ` | `publog_instance/component.yaml` | `0 14 2 * *`, `0 14 2 1,4,7,10 *`, `UTC` |
| `PUBLOG_MAX_CONCURRENT` | `publog_instance/component.yaml` | `4` |
| `DUCKDB_MEMORY_LIMIT` | `pub_tools/lake.py`, `publog_pipeline/component.py` | DuckDB's own |
| `DATAHUB_GMS_URL` | `datahub_lineage/component.yaml` | `http://datahub-datahub-gms:8080` |
| `DATAHUB_SENSOR_STATUS` | `datahub_lineage/component.yaml` | `RUNNING` |
| `DATAHUB_TOKEN` | dag-tools `DatahubLineageComponent` | none |

`PUBLOG_MAX_CONCURRENT` and `DUCKDB_MEMORY_LIMIT` together set the pod's peak memory, because each concurrent step is a subprocess with its own DuckDB budget. Raise them only together with the pod's memory limit.

The component's `dest_config` keeps the shape of a dlt filesystem destination (`drivername`, `destination.bucket_url`, `credentials`). That is only a config shape. dlt itself is not used.

## Getting started locally

The project uses `uv` for dependency management. Do not use `pip` directly.

```bash
uv sync
uv run --frozen pytest -q pub_tools_tests
uv run dagster dev
```

Open http://localhost:3000 to see the assets. A run needs an S3 endpoint. `example/` has a Docker Compose setup with MinIO.

## Important AI context files

If you are an AI assistant or agent working in this repository, read these before changing code:

1.  [`llms.txt`](llms.txt): domain context (DLA PUB LOG to an S3 Parquet lake).
2.  [`.cursorrules`](.cursorrules): the enforced stack (`uv`, Dagster components, DuckDB through the dag-tools IO manager).
3.  [`AGENTS.md`](AGENTS.md): safety constraints and operating procedures for agents (destructive changes, S3 keys, session files).
