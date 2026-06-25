"""Mesh-aware demo component.

Ships one Dagster asset bound to ``CortexPolarsIOManager`` from
dag-tools, so the asset is automatically mesh-publishable through the
domain broker. The asset materializes a small synthetic-real Polars
DataFrame to S3 (MinIO in the sandbox) as parquet — concrete data the
cortex data client can read end-to-end through the central gateway.

The demo is intentionally small: a handful of synthetic customer rows
that exercise the full read path (broker → central gateway → cortex
data client → MinIO → polars.scan_parquet) without depending on any
external network fetch. Larger / production assets continue to use
the publog_pipeline component's dlt-based flow; this one is purely a
mesh-publishing demonstrator that any consumer (JupyterHub notebook,
iagent agent, downstream Dagster job) can read by URN.
"""

from datetime import datetime
from typing import List

import polars as pl
from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from dag_tools.io_managers.cortex_io_manager import CortexPolarsIOManager


class MeshDemoComponent(Component, Resolvable, Model):
    """Materializes a small synthetic-real dataset using ``CortexPolarsIOManager``.

    The IO manager handles the actual S3 write (and the URN + physical_uri
    metadata emission) so the asset's compute function only needs to
    return the Polars DataFrame. The same IO manager exposes
    ``physical_coordinates`` so the dag-tools domain broker can
    advertise this asset's URN through the central gateway with real
    routing coordinates — no placeholder bucket, no STS minting.
    """

    s3_bucket: str
    """S3 bucket where the asset's parquet is written. Same value the
    broker pod's IO manager instance sees, so ``physical_coordinates``
    points at exactly the bucket ``handle_output`` writes to."""

    prefix: str = "mesh_demo"
    """Path prefix under the bucket. Asset parquet lands at
    ``s3://{s3_bucket}/{prefix}/{asset_key}.parquet``."""

    broker_url: str = "http://iagent-central-gateway:8090"
    """Central gateway URL. Required by the IO manager's load_input path
    (when an asset of this kind reads an upstream); the demo asset
    here doesn't read upstreams, but the field is required by the
    underlying ConfigurableIOManager schema."""

    client_id: str = ""
    """M2M OAuth2 client ID for upstream reads via the central gateway.
    Empty when this asset only produces (no upstream reads)."""

    client_secret: str = ""
    """M2M OAuth2 client secret. Empty when only producing."""

    keycloak_url: str = ""
    """Keycloak token URL — leave empty to use the cortex data client's
    default (in-cluster Keycloak service)."""

    asset_name: str = "mesh_demo_customers"
    """Dagster asset name. Also drives the URN
    (``urn:li:dataset:(urn:li:dataPlatform:dagster,{asset_name},PROD)``)
    and the S3 file name."""

    asset_group: str = "mesh_demo"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        asset_group = self.asset_group

        @asset(
            name=asset_name,
            group_name=asset_group,
            io_manager_key="mesh_demo_io",
        )
        def mesh_demo_customers(context: AssetExecutionContext) -> pl.DataFrame:
            """Synthetic customer dataset.

            Real-shaped rows (id, name, region, signup_date, plan) but
            generated locally so the materialization has no external
            dependencies — fast, deterministic, repeatable. The IO
            manager handles the actual S3 write.
            """
            df = pl.DataFrame(
                {
                    "id": list(range(1, 11)),
                    "name": [
                        "Avery Stone", "Blair Tate", "Casey Quinn", "Drew Pine",
                        "Emery Lane", "Finley Cross", "Gray Marsh", "Hollis Reed",
                        "Indigo Vale", "Jules Banner",
                    ],
                    "region": [
                        "US-East", "US-West", "EU-North", "US-East", "EU-South",
                        "APAC", "US-West", "EU-North", "APAC", "US-East",
                    ],
                    "signup_date": [
                        "2024-01-15", "2024-02-03", "2024-02-19", "2024-03-04",
                        "2024-03-22", "2024-04-08", "2024-04-29", "2024-05-11",
                        "2024-05-28", "2024-06-12",
                    ],
                    "plan": [
                        "pro", "starter", "enterprise", "pro", "starter",
                        "pro", "enterprise", "starter", "pro", "enterprise",
                    ],
                }
            )
            context.log.info(
                "mesh_demo_customers materialized: %d rows, %d columns",
                df.height,
                df.width,
            )
            # Return the DataFrame — the IO manager writes it to S3 and
            # attaches the URN + physical_uri metadata for the broker
            # and downstream consumers to pick up.
            return df

        io_manager = CortexPolarsIOManager(
            s3_bucket=self.s3_bucket,
            prefix=self.prefix,
            broker_url=self.broker_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
            keycloak_url=self.keycloak_url or None,
        )

        return Definitions(
            assets=[mesh_demo_customers],
            resources={"mesh_demo_io": io_manager},
        )
