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
3. Click on the `publog_lake_export` asset.
4. Click **Materialize** to start the pipeline.
5. The pipeline will download the PUB LOG dataset, extract it, and use `dlt` to stream the Parquet files into the local MinIO bucket.
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
