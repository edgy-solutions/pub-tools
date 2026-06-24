# pub-tools

High-performance data ingestion pipelines for the Defense Logistics Agency (DLA) PUB LOG and other public datasets.

This project is orchestrated by [Dagster](https://dagster.io/) and uses [dlt (data load tool)](https://dlthub.com/) to build streaming destinations directly into an S3-backed Parquet Data Lake.

## Architecture & Tooling

*   **Orchestration**: Dagster (`@asset` and declarative Components)
*   **Data Movement**: `dlt` (`@dlt.resource` generators)
*   **Configuration**: Dagster declarative components (`component.yaml`)
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

## Integration with iagent — Canonical Sidecar Example

`pub-tools` is the **first real (non-dummy) example** of a Dagster user-deployment that publishes its asset metadata to the iagent `domain-broker` registry on startup. Any other Dagster user-deployment that wants its assets to be queryable through the iagent mesh can copy this pattern verbatim.

### What the sidecar does

At code-location boot, `dag_tools.sidecar.publish_to_registry_at_startup` reads this deployment's own `Definitions`, derives a URN per asset, and POSTs each one to the iagent `domain-broker` so iagent's data clients (`CortexDataClient`, `central-gateway`) can resolve those URNs to physical data paths.

The sidecar is **non-fatal by design**: when `MESH_REGISTRY_URL` is unset or the broker is unreachable, the code-location boots normally. Registration is observability-of-iagent, not a code-location-startup blocker.

### Where it lives in this repo

[`pub_tools/definitions.py`](pub_tools/definitions.py) — the bottom of the file shows the integration:

```python
from dag_tools.sidecar import publish_to_registry_at_startup
publish_to_registry_at_startup(defs, location="pub-tools")
```

That's the whole integration. Three lines (one import, one call, one location-name string).

### Copying the pattern for another user-deployment

1. Add `dag-tools` to your project's dependencies.
2. After `defs = ...` in your `definitions.py`, add the two lines above with your own `location=` string (the human-readable code-location name).
3. Configure `MESH_REGISTRY_URL` in your deployment's env — point at the iagent `domain-broker` (e.g. `http://iagent-domain-broker:8000` in-cluster). When unset, the sidecar no-ops, so local dev and tests stay silent.

### URN derivation order

The sidecar derives an asset's URN with this priority:

1. `asset.tags["datahub/urn"]` if explicitly set on the asset.
2. `record.urn` populated by the [`dag-tools` `datahub-lineage` component sidecar](https://github.com/edgy-solutions/dag-tools).
3. Fallback: `urn:li:dataset:(urn:li:dataPlatform:dagster,<asset_key_dotted>,PROD)`.

This means `publog_lake_export` (without an explicit URN tag) registers as `urn:li:dataset:(urn:li:dataPlatform:dagster,publog_lake_export,PROD)`. Setting `tags={"datahub/urn": "urn:li:dataset:..."}` lets you align with an externally-tracked DataHub URN.

### Operational verification

Once deployed:

```bash
# In-cluster: list what the registry currently knows about
curl -sS http://iagent-domain-broker:8000/api/v1/admin/registry
```

That endpoint returns the URN list (plus count). `pub-tools`' assets should appear after the code-location boots.

### Why this matters

The architectural principle: every Dagster user-deployment is the source-of-truth for the assets it produces. The registry stays passive — it holds whatever user-deployments tell it about — and avoids a central configuration that gets out of sync with reality. New asset? It registers itself on the next boot. Schema change? Same. No central registry team to coordinate with.

For iagent specifically, this is what lets the analytical query path (`mesh:analyzeDataset` → Engine DA → `CortexDataClient.get_dataframe(urn)`) reach a real data backend without any sandbox-specific mock code, satisfying the [`feedback-synthetic-data-no-mock-leak`](https://github.com/edgy-solutions/invincible-agent) discipline.
