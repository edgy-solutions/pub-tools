import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
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
    PUBLOG_SOURCE_MANIFEST,
    fetch_last_modified,
    publog_session,
    source_filename,
    table_name_for,
)
from pub_tools.lake import (
    csv_to_parquet,
    duckdb_connection,
    marker_url,
    object_exists,
    object_mtime,
    raw_csv_url,
    raw_prefix,
    read_marker,
    stage_zip_to_lake,
    storage_options,
    table_parquet_url,
    write_marker,
)


def _current_quarter_start() -> str:
    now = datetime.now()
    q_month = ((now.month - 1) // 3) * 3 + 1
    return now.replace(month=q_month, day=1).strftime("%Y-%m-01")


def source_slug(url: str) -> str:
    """Asset-name-safe slug for a source URL: `.../H-SERIES.zip` -> `h_series`."""
    stem = source_filename(url).rsplit(".", 1)[0]
    return re.sub(r"[^a-z0-9]+", "_", stem.lower()).strip("_")


def _lake_root(dest_config: Dict[str, Any], default_bucket_url: str) -> str:
    return (dest_config.get("destination", {}) or {}).get("bucket_url") or default_bucket_url


def _stage_source(
    context: AssetExecutionContext,
    url: str,
    members: List[str],
    as_of_date: str,
    lake_root: str,
    dest_config: Dict[str, Any],
) -> MaterializeResult:
    """Stage one source archive's CSVs into the lake under `_raw/`."""
    session = publog_session()
    slug = source_slug(url)
    options = storage_options(dest_config)
    prefix = raw_prefix(lake_root, as_of_date, slug)
    marker = marker_url(lake_root, as_of_date, slug)

    lm = fetch_last_modified(session, url)
    if lm is None:
        raise RuntimeError(f"No Last-Modified header for {url}")

    # Freshness comes from the lake, not from Dagster's event log -- see the
    # note in pub_tools.lake. The marker records which source version produced
    # the staged files; it is only trusted if those files are still present.
    previous = read_marker(marker, options)
    if previous and previous.get("source_last_modified") == lm:
        absent = [
            m for m in members
            if not object_exists(raw_csv_url(lake_root, as_of_date, slug, m), options)
        ]
        if not absent:
            context.log.info(
                "%s unchanged since last staging (%s) and all %d CSV(s) still "
                "present; leaving staged copies in place.",
                url, lm, len(members),
            )
            return MaterializeResult(
                metadata={
                    "skipped_reason": "no source changes",
                    "source_last_modified": lm,
                    "as_of_date": as_of_date,
                    "url": url,
                    "raw_prefix": prefix,
                }
            )
        context.log.warning(
            "%s is unchanged, but %d staged CSV(s) are missing from the lake "
            "(%s); re-staging.", url, len(absent), ", ".join(absent),
        )

    staged = stage_zip_to_lake(
        session=session,
        url=url,
        lake_root=lake_root,
        as_of_date=as_of_date,
        slug=slug,
        members=members,
        dest_config=dest_config,
        log=context.log,
    )
    if not staged:
        raise RuntimeError(f"No CSV members were staged from {url}; aborting.")

    missing = sorted({m for m in members} - set(staged))
    if missing:
        context.log.warning(
            "%s is missing %d member(s) declared in PUBLOG_SOURCE_MANIFEST: %s. "
            "Their table assets will fail until the manifest is regenerated.",
            url, len(missing), ", ".join(missing),
        )

    total_mb = sum(v["size_bytes"] for v in staged.values()) / (1024 * 1024)
    context.log.info("Staged %d CSV(s), %.1f MiB total.", len(staged), total_mb)

    # Written last, so an interrupted staging leaves no marker and the next run
    # re-stages rather than trusting a half-written prefix.
    write_marker(
        marker,
        {
            "url": url,
            "source_last_modified": lm,
            "as_of_date": as_of_date,
            "members": sorted(staged),
        },
        options,
    )

    return MaterializeResult(
        metadata={
            "source_last_modified": lm,
            "as_of_date": as_of_date,
            "url": url,
            "raw_prefix": prefix,
            "staged_csv_count": len(staged),
            "staged_bytes": sum(v["size_bytes"] for v in staged.values()),
            "staged_members": sorted(staged),
            "missing_members": missing,
        }
    )


def _convert_table(
    context: AssetExecutionContext,
    url: str,
    member: str,
    table: str,
    as_of_date: str,
    dataset_name: str,
    lake_root: str,
    dest_config: Dict[str, Any],
    compression: str,
) -> MaterializeResult:
    """Convert one staged CSV into one Parquet table."""
    slug = source_slug(url)
    options = storage_options(dest_config)
    csv_url = raw_csv_url(lake_root, as_of_date, slug, member)
    parquet_url = table_parquet_url(lake_root, dataset_name, table)

    csv_mtime = object_mtime(csv_url, options)
    if csv_mtime is None:
        raise RuntimeError(
            f"Staged CSV {csv_url} does not exist. Materialize "
            f"{AssetKey([context.asset_key.path[0], 'source', slug]).to_user_string()} "
            f"first, or re-run it if the raw staging area was cleared."
        )

    # Rebuild only when the input is newer than the output. Both timestamps come
    # from the same store, so they are comparable, and a deleted or truncated
    # Parquet simply rebuilds rather than being reported as fresh.
    parquet_mtime = object_mtime(parquet_url, options)
    if parquet_mtime is not None and parquet_mtime >= csv_mtime:
        context.log.info(
            "%s is already newer than its staged CSV; skipping conversion.", table
        )
        return MaterializeResult(
            metadata={
                "skipped_reason": "parquet already newer than staged CSV",
                "as_of_date": as_of_date,
                "table": table,
                "table_path": parquet_url,
                "source_csv": csv_url,
            }
        )

    con = duckdb_connection(dest_config)
    try:
        rows = csv_to_parquet(
            con, csv_url, parquet_url, compression=compression, log=context.log
        )
    finally:
        con.close()

    return MaterializeResult(
        metadata={
            "dagster/row_count": rows,
            "as_of_date": as_of_date,
            "table": table,
            "table_path": parquet_url,
            "source_csv": csv_url,
            "source_url": url,
        }
    )


class PublogPipelineComponent(Component, Resolvable, Model):
    """Ingests DLA PUB LOG CSV data from the FLIS Electronic Reading Room.

    Two stages, one Dagster asset per unit of work:

      * `<key_prefix>/source/<slug>` -- downloads a source archive and streams
        each CSV member into the lake under `_raw/<as_of>/<slug>/`.
      * `<key_prefix>/<table>` -- converts one staged CSV into one Parquet
        table with DuckDB, e.g. `publog/v_cage_address`.

    Every table is its own step, so conversions run in parallel, retry
    individually, and can be materialized on their own without re-downloading
    a several-hundred-megabyte archive. Downstream code depends on the table
    it actually needs:

        @asset(deps=[AssetKey(["publog", "v_flis_identification"])])

    Table asset keys come from `PUBLOG_SOURCE_MANIFEST`, since Dagster needs
    keys at definition time but an archive's members are only knowable after
    download. Staging reconciles manifest against archive and warns on drift.

    Both stages are incremental. Staging skips when the source's Last-Modified
    is unchanged; a conversion skips when the table was already built from that
    same source version.
    """

    monthly_urls: List[str] = PUBLOG_MONTHLY_URLS
    """Full URLs of monthly source files."""

    quarterly_urls: List[str] = PUBLOG_QUARTERLY_URLS
    """Full URLs of quarterly source files (set [] to disable)."""

    dest_config: Dict[str, Any]
    """Destination credentials and bucket configuration (dlt filesystem-style)."""

    dataset_name: Optional[str] = None
    """Prefix for lake dataset names; defaulted to 'publog'."""

    key_prefix: str = "publog"
    """Leading component of every generated asset key."""

    asset_name: str = "publog_lake_export"
    """Prefix for generated op, job, and schedule names."""

    asset_group: str = "publog_ingestion"

    parquet_compression: str = "zstd"
    """DuckDB Parquet codec: zstd, snappy, gzip, or uncompressed."""

    max_concurrent: int = 6
    """Steps run in parallel per job. Staging holds one compressed archive on
    the run worker's disk; conversions stream object storage and hold neither
    input nor output locally, so this is bounded by memory and network rather
    than disk."""

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
        default_bucket_url: str,
    ):
        """Build the staging asset and per-table conversion assets for each URL.

        Loop variables are bound as closure defaults so every asset captures its
        own source. `as_of_fn` is evaluated at execution time, not definition
        time -- a code server can stay up across a month boundary, and every
        derived name must come from the one call so they cannot disagree.
        """
        dest_config = self.dest_config
        compression = self.parquet_compression
        lake_root = _lake_root(dest_config, default_bucket_url)
        staging_assets, table_assets = [], []

        for url in urls:
            slug = source_slug(url)
            filename = source_filename(url)
            members = PUBLOG_SOURCE_MANIFEST[filename]
            bundle_key = AssetKey([self.key_prefix, "source", slug])

            # A Dagster asset function's parameters are INPUTS, not a place to
            # bind loop variables -- doing that silently invents upstream assets
            # named after the parameters. Build each closure in its own scope.
            def make_staging(url=url, members=members):
                def _compute(context: AssetExecutionContext) -> MaterializeResult:
                    return _stage_source(
                        context=context,
                        url=url,
                        members=members,
                        as_of_date=as_of_fn(),
                        lake_root=lake_root,
                        dest_config=dest_config,
                    )

                return _compute

            staging_assets.append(
                asset(
                    key=bundle_key,
                    group_name=self.asset_group,
                    kinds={"file"},
                    description=f"Downloads {filename} and stages its CSVs in the lake.",
                    op_tags={"publog/source": slug},
                )(make_staging())
            )

            for member in members:
                table = table_name_for(member)

                def make_table(url=url, member=member, table=table):
                    def _compute(context: AssetExecutionContext) -> MaterializeResult:
                        as_of_date = as_of_fn()
                        return _convert_table(
                            context=context,
                            url=url,
                            member=member,
                            table=table,
                            as_of_date=as_of_date,
                            dataset_name=(
                                f"{dataset_name_prefix}_{as_of_date.replace('-', '_')}"
                            ),
                            lake_root=lake_root,
                            dest_config=dest_config,
                            compression=compression,
                        )

                    return _compute

                table_assets.append(
                    asset(
                        key=AssetKey([self.key_prefix, table]),
                        deps=[bundle_key],
                        group_name=self.asset_group,
                        kinds={"parquet", "duckdb"},
                        description=f"PUB LOG table `{table}`, converted from {member}.",
                        op_tags={"publog/source": slug},
                    )(make_table())
                )

        return staging_assets, table_assets

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        dataset_name_prefix = self.dataset_name or "publog"
        exec_config = {
            "execution": {"config": {"multiprocess": {"max_concurrent": self.max_concurrent}}}
        }

        monthly_staging, monthly_tables = self._build_source_assets(
            urls=list(self.monthly_urls),
            as_of_fn=lambda: datetime.now().strftime("%Y-%m-01"),
            dataset_name_prefix=dataset_name_prefix,
            default_bucket_url="file:///tmp/publog-lake",
        )
        monthly_assets = monthly_staging + monthly_tables

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
            quarterly_staging, quarterly_tables = self._build_source_assets(
                urls=quarterly_urls,
                as_of_fn=_current_quarter_start,
                dataset_name_prefix=f"{dataset_name_prefix}_quarterly",
                default_bucket_url="file:///tmp/publog-lake-quarterly",
            )
            quarterly_assets = quarterly_staging + quarterly_tables
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
