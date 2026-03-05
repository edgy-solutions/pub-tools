# pub-tools

High-performance data ingestion pipelines for the Defense Logistics Agency (DLA) PUB LOG and other public datasets.

This project is orchestrated by [Dagster](https://dagster.io/) and uses [dlt (data load tool)](https://dlthub.com/) to build streaming destinations directly into an S3-backed Parquet Data Lake.

## Architecture & Tooling

*   **Orchestration**: Dagster (`@asset`)
*   **Data Movement**: `dlt` (`@dlt.resource` generators)
*   **Destination**: Parquet formatting on local filesystem (`/tmp`) or AWS S3 (`s3://...`).
*   **Package Management**: `uv`

## Getting Started Locally

This project strictly uses `uv` for dependency management. Do not use `pip` directly.

To install dependencies and start the local Dagster UI web server:

```bash
uv run dagster dev
```

Open http://localhost:3000 with your browser to see the project assets and execute the Data Lake materialization runs.

## Important AI Context Files

If you are an AI assistant or Agent navigating this repository, please review the following core files before making code changes:

1.  [`llms.txt`](llms.txt): Core domain semantic context (DLA PUB LOG to S3).
2.  [`.cursorrules`](.cursorrules): Enforced strict coding style and acceptable use of `uv` and `dlt`.
3.  [`AGENTS.md`](AGENTS.md): Critical safety constraints and operating procedures for agents (e.g., handling destructive changes and S3 keys).
