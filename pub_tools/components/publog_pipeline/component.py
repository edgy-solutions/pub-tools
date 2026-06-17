import os
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

import dlt
from dagster import (
    AssetExecutionContext,
    AssetSelection,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
)
from dagster.components import Component, ComponentLoadContext
from dagster.components.resolved.base import Resolvable
from dagster.components.resolved.model import Model

from pub_tools.assets import (
    PUBLOG_MONTHLY_URLS,
    PUBLOG_QUARTERLY_URLS,
    fetch_last_modified,
    fetch_url_to_dir,
    load_previous_last_modified,
    publog_csv_resource,
    publog_session,
)


def _build_destination(dest_config: Dict[str, Any], default_bucket_url: str):
    bucket_url = dest_config.get("destination", {}).get("bucket_url") or default_bucket_url
    drivername = dest_config.get("drivername", "filesystem")
    if drivername == "filesystem":
        from dlt.destinations import filesystem

        return filesystem(
            bucket_url=bucket_url,
            credentials=dest_config.get("credentials", {}),
        ), bucket_url
    return drivername, bucket_url


def _current_quarter_start() -> str:
    now = datetime.now()
    q_month = ((now.month - 1) // 3) * 3 + 1
    return now.replace(month=q_month, day=1).strftime("%Y-%m-01")


def _materialize_publog_snapshot(
    context: AssetExecutionContext,
    urls: List[str],
    as_of_date: str,
    dataset_name_prefix: str,
    pipeline_name_prefix: str,
    dest_config: Dict[str, Any],
    default_bucket_url: str,
) -> MaterializeResult:
    """Common materialization: HEAD all URLs, skip if no source changed since
    last run, otherwise download every URL and load via dlt as a full snapshot."""
    session = publog_session()

    previous_lm = load_previous_last_modified(context)
    current_lm: Dict[str, str] = {}
    for url in urls:
        lm = fetch_last_modified(session, url)
        if lm is None:
            raise RuntimeError(f"No Last-Modified header for {url}")
        current_lm[url] = lm

    if previous_lm and current_lm == previous_lm:
        context.log.info(
            "All %d PUB LOG source files unchanged since last load; skipping.",
            len(urls),
        )
        return MaterializeResult(
            metadata={
                "skipped_reason": "no source changes",
                "source_last_modified": current_lm,
                "as_of_date": as_of_date,
                "urls": urls,
            }
        )

    dataset_name = f"{dataset_name_prefix}_{as_of_date.replace('-', '_')}"
    destination_obj, bucket_url = _build_destination(
        dest_config,
        default_bucket_url=f"file:///tmp/publog-lake/{as_of_date}",
    )

    with tempfile.TemporaryDirectory() as temp_root:
        all_csvs: List[str] = []
        for url in urls:
            file_dir = os.path.join(temp_root, url.rsplit("/", 1)[-1].rsplit(".", 1)[0])
            context.log.info("Downloading %s", url)
            csvs = fetch_url_to_dir(session, url, file_dir)
            context.log.info("  -> %d CSV(s) extracted", len(csvs))
            all_csvs.extend(csvs)

        if not all_csvs:
            raise RuntimeError("No CSV files were downloaded; aborting load.")

        context.log.info(
            "Loading %d CSV files via dlt -> %s (dataset %s)",
            len(all_csvs),
            bucket_url,
            dataset_name,
        )
        pipeline = dlt.pipeline(
            pipeline_name=f"{pipeline_name_prefix}_{as_of_date}",
            destination=destination_obj,
            dataset_name=dataset_name,
        )
        load_info = pipeline.run(
            publog_csv_resource(all_csvs),
            loader_file_format="parquet",
        )
        context.log.info(str(load_info))

    return MaterializeResult(
        metadata={
            "destination_bucket_url": bucket_url,
            "as_of_date": as_of_date,
            "dlt_pipeline_name": pipeline.pipeline_name,
            "dlt_dataset_name": pipeline.dataset_name,
            "urls": urls,
            "csv_count": len(all_csvs),
            "source_last_modified": current_lm,
        }
    )


class PublogPipelineComponent(Component, Resolvable, Model):
    """Ingests DLA PUB LOG CSV data from the FLIS Electronic Reading Room.

    Creates two assets:
      * `<asset_name>` - monthly snapshot of the 9 PUB LOG topical zips
      * `<asset_name>_quarterly` - quarterly snapshot of FLISV + MRD files

    Each asset is incremental at the run level: if every source URL's
    Last-Modified header is unchanged since the previous successful
    materialization, the run skips downloading and dlt loading and records a
    `skipped_reason` in the materialization metadata.
    """

    monthly_urls: List[str] = PUBLOG_MONTHLY_URLS
    """Full URLs of monthly zip files."""

    quarterly_urls: List[str] = PUBLOG_QUARTERLY_URLS
    """Full URLs of quarterly files (set to [] to disable the quarterly asset)."""

    dest_config: Dict[str, Any]
    """Destination credentials and bucket configuration (dlt filesystem-style)."""

    dataset_name: Optional[str] = None
    """Prefix for dlt dataset names; defaulted to 'publog'."""

    asset_name: str = "publog_lake_export"
    """Name of the monthly asset; the quarterly asset is suffixed `_quarterly`."""

    asset_group: str = "publog_ingestion"

    monthly_cron_schedule: str = "0 14 2 * *"
    """Cron for monthly ingest. Default: 14:00 UTC on day 2 of each month
    (DLA publishes on the 1st business day; day 2 gives a buffer)."""

    quarterly_cron_schedule: str = "0 14 2 1,4,7,10 *"
    """Cron for quarterly ingest. Default: 14:00 UTC on day 2 of Jan/Apr/Jul/Oct."""

    schedule_timezone: str = "UTC"

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        monthly_urls = list(self.monthly_urls)
        quarterly_urls = list(self.quarterly_urls)
        dest_config = self.dest_config
        dataset_name_prefix = self.dataset_name or "publog"
        asset_name = self.asset_name
        quarterly_asset_name = f"{asset_name}_quarterly"
        asset_group = self.asset_group
        monthly_cron = self.monthly_cron_schedule
        quarterly_cron = self.quarterly_cron_schedule
        schedule_tz = self.schedule_timezone

        @asset(name=asset_name, group_name=asset_group)
        def publog_lake_export(context: AssetExecutionContext) -> MaterializeResult:
            return _materialize_publog_snapshot(
                context=context,
                urls=monthly_urls,
                as_of_date=datetime.now().strftime("%Y-%m-01"),
                dataset_name_prefix=dataset_name_prefix,
                pipeline_name_prefix="publog_lake_pipeline",
                dest_config=dest_config,
                default_bucket_url=f"file:///tmp/publog-lake/{datetime.now().strftime('%Y-%m-01')}",
            )

        assets = [publog_lake_export]

        monthly_job = define_asset_job(
            name=f"{asset_name}_monthly_job",
            selection=AssetSelection.assets(publog_lake_export),
        )
        schedules = [
            ScheduleDefinition(
                name=f"{asset_name}_monthly_schedule",
                cron_schedule=monthly_cron,
                execution_timezone=schedule_tz,
                job=monthly_job,
            )
        ]
        jobs = [monthly_job]

        if quarterly_urls:
            @asset(name=quarterly_asset_name, group_name=asset_group)
            def publog_lake_export_quarterly(
                context: AssetExecutionContext,
            ) -> MaterializeResult:
                return _materialize_publog_snapshot(
                    context=context,
                    urls=quarterly_urls,
                    as_of_date=_current_quarter_start(),
                    dataset_name_prefix=f"{dataset_name_prefix}_quarterly",
                    pipeline_name_prefix="publog_quarterly_pipeline",
                    dest_config=dest_config,
                    default_bucket_url=f"file:///tmp/publog-lake-quarterly/{_current_quarter_start()}",
                )

            assets.append(publog_lake_export_quarterly)

            quarterly_job = define_asset_job(
                name=f"{quarterly_asset_name}_job",
                selection=AssetSelection.assets(publog_lake_export_quarterly),
            )
            jobs.append(quarterly_job)
            schedules.append(
                ScheduleDefinition(
                    name=f"{quarterly_asset_name}_schedule",
                    cron_schedule=quarterly_cron,
                    execution_timezone=schedule_tz,
                    job=quarterly_job,
                )
            )

        return Definitions(assets=assets, jobs=jobs, schedules=schedules)
