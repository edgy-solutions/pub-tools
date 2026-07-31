# End-to-End Example

This directory contains a full end-to-end example of running the `pub_tools` pipeline using Docker Compose.

It sets up:
1. **Dagster**: The orchestration engine, running both the webserver and the daemon.
2. **MinIO**: A local S3-compatible object storage server to act as the Data Lake destination.
3. **MinIO Setup**: A short-lived container that creates the `publog-lake` bucket automatically.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

## Running the Example

1. Navigate to the `example` directory:
   ```bash
   cd example
   ```

2. Start the services:
   ```bash
   # You may need to authenticate with GitHub Container Registry first:
   # echo $CR_PAT | docker login ghcr.io -u USERNAME --password-stdin
   
   docker-compose up
   ```

3. Access the UIs:
   - **Dagster Webserver**: [http://localhost:3000](http://localhost:3000)
   - **MinIO Console**: [http://localhost:9001](http://localhost:9001)
     - **Username**: `minioadmin`
     - **Password**: `minioadmin`

## Running the Pipeline

1. Open the Dagster UI at [http://localhost:3000](http://localhost:3000).
2. Navigate to the **Assets** tab.
3. Select the `publog_ingestion` asset group. Ingest runs in two stages:
   - `publog/source/<name>` — one per PUB LOG source archive
     (`publog/source/cage`, ...). Downloads the archive and streams each CSV
     member into MinIO under `_raw/<as-of-date>/<name>/`, without ever
     expanding it onto local disk.
   - `publog/<table>` — one per CSV (`publog/v_cage_address`, ...). Reads its
     one staged CSV and converts it to Parquet with DuckDB, writing
     `<dataset>/<table>/data.parquet`. These are what downstream assets
     should depend on.
4. Materialize the whole group for a full snapshot, or any single asset.
5. Every table converts as its own step, so conversions run in parallel (6 at a
   time by default; see `max_concurrent`) and retry individually — a failed
   table re-converts without re-downloading its archive.

Both stages are incremental, and both decide from the lake rather than from
Dagster's run history:

- Staging skips when the source's `Last-Modified` is unchanged *and* every
  declared CSV is still present in the bucket.
- A conversion skips when its Parquet is already newer than its staged CSV.

So deleting an object from MinIO causes exactly that object to be rebuilt on
the next run.

Every column is written as text. PUB LOG identifiers (NSN, NIIN, FSC, CAGE
codes) are zero-padded, and type inference would silently turn `01234` into
`1234`.

To depend on a PUB LOG table from your own asset:

```python
from dagster import AssetKey, asset

@asset(deps=[AssetKey(["publog", "v_flis_identification"])])
def my_downstream_model(): ...
```
6. Once completed, you can view the resulting Parquet files in the MinIO Console at [http://localhost:9001](http://localhost:9001) under the `publog-lake` bucket.

## Verifying the Data

To ensure the pipeline successfully wrote the Parquet files with the correct schema and data, you can run the provided verification script. This script connects to MinIO, reads the Parquet file, and prints its schema and row count.

Run the following command:
```bash
docker-compose exec dagster uv run python /app/verify_parquet.py
```

## Stopping the Example

To stop the services and remove the containers, run:
```bash
docker-compose down
```

If you want to remove the downloaded data (volumes) as well, run:
```bash
docker-compose down -v
```
