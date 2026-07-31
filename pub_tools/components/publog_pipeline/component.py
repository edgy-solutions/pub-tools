import os
import re
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional

import dlt
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    define_asset_job,
    multi_asset,
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
    manifest_tables,
    publog_csv_resource,
    publog_session,
    source_filename,
    table_name_for,
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


def source_slug(url: str) -> str:
    """Asset-name-safe slug for a source URL: `.../H-SERIES.zip` -> `h_series`."""
    stem = source_filename(url).rsplit(".", 1)[0]
    return re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_")


def _ingest_source(
    context: AssetExecutionContext,
    url: str,
    bundle_key: AssetKey,
    table_keys: Dict[str, AssetKey],
    as_of_date: str,
    dataset_name: str,
    pipeline_name: str,
    dest_config: Dict[str, Any],
    default_bucket_url: str,
):
    """Download one PUB LOG source file and load its CSVs via dlt, emitting one
    MaterializeResult for the source bundle and one per declared table.

    Skips download and load entirely when the source's Last-Modified header is
    unchanged since the bundle's previous materialization.
    """
    selected = context.selected_asset_keys
    session = publog_session()

    lm = fetch_last_modified(session, url)
    if lm is None:
        raise RuntimeError(f"No Last-Modified header for {url}")
    current_lm = {url: lm}

    previous_lm = load_previous_last_modified(context, asset_key=bundle_key)
    if previous_lm == current_lm:
        context.log.info("%s unchanged since last load (%s); skipping.", url, lm)
        skipped = {
            "skipped_reason": "no source changes",
            "source_last_modified": current_lm,
            "as_of_date": as_of_date,
            "url": url,
        }
        for key in [bundle_key, *table_keys.values()]:
            if key in selected:
                yield MaterializeResult(asset_key=key, metadata=skipped)
        return

    destination_obj, bucket_url = _build_destination(dest_config, default_bucket_url)

    with tempfile.TemporaryDirectory() as temp_root:
        context.log.info("Downloading %s", url)
        csvs = fetch_url_to_dir(session, url, temp_root)
        if not csvs:
            raise RuntimeError(f"No CSV files extracted from {url}; aborting load.")
        extracted_mb = sum(os.path.getsize(p) for p in csvs) / (1024 * 1024)
        context.log.info("  -> %d CSV(s) extracted, %.1f MiB total", len(csvs), extracted_mb)

        # Reconcile the checked-in manifest against what the archive actually
        # holds. Undeclared tables still load -- dropping data silently would be
        # worse than an un-keyed table -- but they have no asset to attach to,
        # so say so loudly enough that someone regenerates the manifest.
        found = {table_name_for(p) for p in csvs}
        undeclared = sorted(found - set(table_keys))
        missing = sorted(set(table_keys) - found)
        if undeclared:
            context.log.warning(
                "%s contains %d table(s) absent from PUBLOG_SOURCE_MANIFEST: %s. "
                "They will load into the lake but have no asset key; regenerate "
                "the manifest to make them addressable downstream.",
                url, len(undeclared), ", ".join(undeclared),
            )
        if missing:
            context.log.warning(
                "%s is missing %d table(s) declared in PUBLOG_SOURCE_MANIFEST: %s. "
                "Their assets will not be materialized this run.",
                url, len(missing), ", ".join(missing),
            )

        context.log.info(
            "Loading %d CSV file(s) via dlt -> %s (dataset %s). This is the long "
            "phase; per-file progress follows.",
            len(csvs), bucket_url, dataset_name,
        )
        row_counts: Dict[str, int] = {}
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=destination_obj,
            dataset_name=dataset_name,
        )
        load_info = pipeline.run(
            publog_csv_resource(csvs, log=context.log, row_counts=row_counts),
            loader_file_format="parquet",
        )
        context.log.info(
            "Loaded %s rows across %d table(s).",
            f"{sum(row_counts.values()):,}", len(row_counts),
        )
        context.log.info(str(load_info))

    if bundle_key in selected:
        yield MaterializeResult(
            asset_key=bundle_key,
            metadata={
                "destination_bucket_url": bucket_url,
                "as_of_date": as_of_date,
                "dlt_pipeline_name": pipeline.pipeline_name,
                "dlt_dataset_name": dataset_name,
                "url": url,
                "csv_count": len(csvs),
                "row_count": sum(row_counts.values()),
                "rows_per_table": row_counts,
                "undeclared_tables": undeclared,
                "missing_tables": missing,
                "source_last_modified": current_lm,
            },
        )

    for table, key in table_keys.items():
        if key not in selected or table in missing:
            continue
        yield MaterializeResult(
            asset_key=key,
            metadata={
                "dagster/row_count": row_counts.get(table, 0),
                "as_of_date": as_of_date,
                "dlt_table": table,
                "dlt_dataset_name": dataset_name,
                "table_path": f"{bucket_url.rstrip('/')}/{dataset_name}/{table}",
                "source_url": url,
                "source_last_modified": lm,
            },
        )


class PublogPipelineComponent(Component, Resolvable, Model):
    """Ingests DLA PUB LOG CSV data from the FLIS Electronic Reading Room.

    Creates one `@multi_asset` per source file. Each emits:

      * `<key_prefix>/source/<slug>` -- the source bundle, carrying download and
        load metadata for the whole zip
      * `<key_prefix>/<table>` -- one asset per CSV in that zip, e.g.
        `publog/v_cage_address`, each depending on its bundle

    So downstream code depends on individual tables
    (`deps=[AssetKey(["publog", "v_flis_identification"])]`) while the zip
    remains the unit of execution: one source file, one step, one subprocess
    under the multiprocess executor. That gives parallelism, independent retry,
    per-source skip granularity, and bounds peak disk to `max_concurrent`
    sources' extracted CSVs rather than all of them at once.

    Table asset keys come from `PUBLOG_SOURCE_MANIFEST`, since Dagster needs
    keys at definition time but a zip's members are only knowable after
    download. Ingest reconciles manifest against archive at runtime and warns
    on drift.

    Each source is incremental at the run level: if its URL's Last-Modified
    header is unchanged since the bundle's previous successful materialization,
    the run skips downloading and dlt loading and records a `skipped_reason`.
    """

    monthly_urls: List[str] = PUBLOG_MONTHLY_URLS
    """Full URLs of monthly zip files; one multi_asset is created per entry."""

    quarterly_urls: List[str] = PUBLOG_QUARTERLY_URLS
    """Full URLs of quarterly files; one per entry (set [] to disable)."""

    dest_config: Dict[str, Any]
    """Destination credentials and bucket configuration (dlt filesystem-style)."""

    dataset_name: Optional[str] = None
    """Prefix for dlt dataset names; defaulted to 'publog'."""

    key_prefix: str = "publog"
    """Leading component of every generated asset key."""

    asset_name: str = "publog_lake_export"
    """Prefix for generated op, job, and schedule names."""

    asset_group: str = "publog_ingestion"

    max_concurrent: int = 3
    """Steps run in parallel per job. Each concurrent step holds one source's
    extracted CSVs plus dlt's staged parquet on the run worker's ephemeral
    disk, so raise this only alongside the pod's storage limit."""

    monthly_cron_schedule: str = "0 14 2 * *"
    """Cron for monthly ingest. Default: 14:00 UTC on day 2 of each month
    (DLA publishes on the 1st business day; day 2 gives a buffer)."""

    quarterly_cron_schedule: str = "0 14 2 1,4,7,10 *"
    """Cron for quarterly ingest. Default: 14:00 UTC on day 2 of Jan/Apr/Jul/Oct."""

    schedule_timezone: str = "UTC"

    def _build_source_assets(
        self,
        urls: List[str],
        as_of_fn,
        dataset_name_prefix: str,
        pipeline_name_prefix: str,
        bucket_url_prefix: str,
    ):
        """One multi_asset per URL. Loop variables are bound as closure defaults
        so each asset captures its own source rather than the last one.

        `as_of_fn` is evaluated at execution time, not definition time -- a code
        server can stay up across a month boundary, and every derived name must
        come from the one call so they cannot disagree.
        """
        dest_config = self.dest_config
        assets = []

        for url in urls:
            slug = source_slug(url)
            bundle_key = AssetKey([self.key_prefix, "source", slug])
            table_keys = {
                table: AssetKey([self.key_prefix, table]) for table in manifest_tables(url)
            }

            filename = source_filename(url)
            specs = [
                AssetSpec(
                    key=bundle_key,
                    group_name=self.asset_group,
                    skippable=True,
                    kinds={"file"},
                    description=f"PUB LOG source archive {filename}.",
                    metadata={"source_url": url, "declared_tables": sorted(table_keys)},
                )
            ]
            specs += [
                AssetSpec(
                    key=key,
                    deps=[bundle_key],
                    group_name=self.asset_group,
                    skippable=True,
                    kinds={"parquet", "dlt"},
                    description=f"PUB LOG table `{table}`, from {filename}.",
                    metadata={"source_url": url, "dlt_table": table},
                )
                for table, key in table_keys.items()
            ]

            # multi_asset reads the compute function's signature as asset INPUTS,
            # so extra parameters cannot be used to bind loop variables the way
            # they can with @asset. Build each closure in its own scope instead.
            def make_compute(url=url, slug=slug, bundle_key=bundle_key, table_keys=table_keys):
                def _compute(context: AssetExecutionContext):
                    as_of_date = as_of_fn()
                    yield from _ingest_source(
                        context=context,
                        url=url,
                        bundle_key=bundle_key,
                        table_keys=table_keys,
                        as_of_date=as_of_date,
                        dataset_name=f"{dataset_name_prefix}_{as_of_date.replace('-', '_')}",
                        pipeline_name=f"{pipeline_name_prefix}_{slug}_{as_of_date}",
                        dest_config=dest_config,
                        default_bucket_url=f"{bucket_url_prefix}/{as_of_date}",
                    )

                return _compute

            assets.append(
                multi_asset(
                    name=f"{self.asset_name}_{slug}",
                    specs=specs,
                    can_subset=True,
                )(make_compute())
            )

        return assets

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        dataset_name_prefix = self.dataset_name or "publog"
        exec_config = {
            "execution": {"config": {"multiprocess": {"max_concurrent": self.max_concurrent}}}
        }

        monthly_assets = self._build_source_assets(
            urls=list(self.monthly_urls),
            as_of_fn=lambda: datetime.now().strftime("%Y-%m-01"),
            dataset_name_prefix=dataset_name_prefix,
            pipeline_name_prefix="publog_lake_pipeline",
            bucket_url_prefix="file:///tmp/publog-lake",
        )

        monthly_job = define_asset_job(
            name=f"{self.asset_name}_monthly_job",
            selection=AssetSelection.assets(*monthly_assets),
            config=exec_config,
        )
        assets = list(monthly_assets)
        jobs = [monthly_job]
        schedules = [
            ScheduleDefinition(
                name=f"{self.asset_name}_monthly_schedule",
                cron_schedule=self.monthly_cron_schedule,
                execution_timezone=self.schedule_timezone,
                job=monthly_job,
            )
        ]

        quarterly_urls = list(self.quarterly_urls)
        if quarterly_urls:
            quarterly_assets = self._build_source_assets(
                urls=quarterly_urls,
                as_of_fn=_current_quarter_start,
                dataset_name_prefix=f"{dataset_name_prefix}_quarterly",
                pipeline_name_prefix="publog_quarterly_pipeline",
                bucket_url_prefix="file:///tmp/publog-lake-quarterly",
            )
            quarterly_job = define_asset_job(
                name=f"{self.asset_name}_quarterly_job",
                selection=AssetSelection.assets(*quarterly_assets),
                config=exec_config,
            )
            assets.extend(quarterly_assets)
            jobs.append(quarterly_job)
            schedules.append(
                ScheduleDefinition(
                    name=f"{self.asset_name}_quarterly_schedule",
                    cron_schedule=self.quarterly_cron_schedule,
                    execution_timezone=self.schedule_timezone,
                    job=quarterly_job,
                )
            )

        return Definitions(assets=assets, jobs=jobs, schedules=schedules)
