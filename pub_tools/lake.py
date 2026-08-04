"""Object-store staging and CSV -> Parquet conversion for the PUB LOG lake.

Ingest runs in two stages, one asset each:

  1. `stage_zip_to_lake` downloads a source archive and streams each CSV member
     straight into the lake under `_raw/`, never materialising the extracted
     file on local disk.
  2. `csv_to_parquet` converts one staged CSV into one Parquet table with
     DuckDB, reading and writing object storage directly.

Splitting them is what lets every table convert as its own Dagster step: the
conversions are independent, so they run in parallel and retry individually
without re-downloading a multi-hundred-megabyte archive.
"""
import os
import zipfile
from typing import Any, Dict, List, Optional

import fsspec

# The DuckDB connection setup this module used to carry by hand -- httpfs
# loading, endpoint reshaping, the S3 SET statements -- now lives in
# dag-tools, which needs it for its own DuckDB IO manager. `duckdb_path` is
# re-exported because callers here and in the tests import it from this
# module.
from dag_tools.resources.duckdb import DuckDBResource, duckdb_path  # noqa: F401

# Everything is read and written as text. PUB LOG is full of identifiers whose
# leading zeros carry meaning -- NSNs, NIINs, FSCs, CAGE codes -- and letting a
# type inferencer turn "01234" into the integer 1234 silently corrupts them.
DUCKDB_READ_CSV_OPTIONS = "all_varchar=true, header=true, null_padding=true"


def storage_options(dest_config: Dict[str, Any]) -> Dict[str, Any]:
    """fsspec storage options from a dlt-style destination config."""
    creds = dest_config.get("credentials") or {}
    key = creds.get("aws_access_key_id")
    secret = creds.get("aws_secret_access_key")
    endpoint = creds.get("endpoint_url")
    options: Dict[str, Any] = {}
    if key:
        options["key"] = key
    if secret:
        options["secret"] = secret
    if endpoint:
        options["client_kwargs"] = {"endpoint_url": endpoint}
    return options


def duckdb_connection(dest_config: Dict[str, Any]):
    """A DuckDB connection able to read and write the lake's object storage.

    httpfs is what lets DuckDB address `s3://` directly, so a multi-GiB CSV
    converts without ever landing on the pod's disk.

    The wiring itself -- loading httpfs, reshaping the endpoint URL into
    DuckDB's `s3_endpoint` + `s3_use_ssl` pair, path-style URLs for MinIO --
    is `dag_tools.resources.duckdb.DuckDBResource`, which grew out of this
    function and is now shared with dag-tools' DuckDB IO manager. What stays
    here is the translation from a dlt-style destination config, and the
    pointer to this repo's own extension-baking script.

    In the container image the extension is baked in at build time (see
    scripts/bake_duckdb_extensions.py) and DUCKDB_EXTENSION_DIRECTORY points at
    it. When that variable is set the image is meant to be self-contained, so a
    failure to load is raised immediately rather than silently reaching for
    duckdb.org -- which in a restricted cluster means a long hang followed by a
    confusing error, once per asset. Outside the image the variable is unset
    and a download is a reasonable convenience.
    """
    creds = dest_config.get("credentials") or {}
    resource = DuckDBResource(
        aws_access_key_id=creds.get("aws_access_key_id"),
        aws_secret_access_key=creds.get("aws_secret_access_key"),
        # dlt names this `region_name`; DuckDB and boto call it region.
        aws_region=creds.get("region_name") or "us-east-1",
        endpoint_url=creds.get("endpoint_url"),
        # Conversions run in a pod with a cgroup memory limit, but DuckDB
        # sizes its default budget from detected system RAM, so on a large
        # CSV it can exceed the pod's limit and be OOMKilled instead of
        # spilling to disk. Capping is close to free -- measured elsewhere at
        # 5M rows, a 256MB cap cut peak RSS from 434MB to 136MB for ~6% more
        # wall time. Unset leaves DuckDB's own default in place.
        memory_limit=os.environ.get("DUCKDB_MEMORY_LIMIT"),
    )
    try:
        return resource.connect()
    except RuntimeError as e:
        # The shared resource cannot know how THIS image provides the
        # extension, so name the script an operator here would actually run.
        raise RuntimeError(
            f"{e} In this image httpfs is provided by "
            f"scripts/bake_duckdb_extensions.py -- set "
            f"DUCKDB_EXTENSION_DIRECTORY and run it to supply the extension "
            f"offline."
        ) from e


def raw_prefix(lake_root: str, as_of_date: str, slug: str) -> str:
    """`<lake_root>/_raw/<slug>/<as_of_date>` -- slug BEFORE the date.

    The order matters for the catalog. DataHub's s3 source names a dataset
    from the path up to and including the `{table}` capture, so with the
    date first (`_raw/<date>/<slug>`) every month produces a NEW dataset:

        minio-svc.publog-lake/_raw/2026-08-01/cage

    One stable Dagster asset would map to N DataHub datasets, and the
    lineage into the tables would fan out with it. Putting the slug first
    makes the date a partition BELOW the table, so the identity is stable
    and the crawler agrees with what Dagster publishes:

        path_spec  s3://publog-lake/_raw/{table}/{partition[0]}/*
        urn        minio-svc.publog-lake/_raw/cage

    Verified against DataHub's own PathSpec, not assumed.
    """
    return f"{lake_root.rstrip('/')}/_raw/{slug}/{as_of_date}"


def raw_csv_url(lake_root: str, as_of_date: str, slug: str, member: str) -> str:
    """Where a staged CSV lives. Deterministic, so the conversion step can find
    its input without threading state through the upstream asset."""
    return f"{raw_prefix(lake_root, as_of_date, slug)}/{os.path.basename(member)}"


def marker_url(lake_root: str, as_of_date: str, slug: str) -> str:
    """Marker recording which source version produced the staged CSVs."""
    return f"{raw_prefix(lake_root, as_of_date, slug)}/_source.json"


def table_parquet_url(
    lake_root: str, table: str, key_prefix: str = "publog"
) -> str:
    """Where a converted table lands.

    The DuckDB IO manager owns this location now, so the layout comes from
    `dag_tools.io_managers.duckdb.asset_uri` rather than being spelled out
    here -- a second copy would drift from the writer, and a freshness check
    against the wrong path silently either rebuilds forever or never
    rebuilds.

    Returned without the trailing slash: this is used to *stat* the output,
    not to hand to a parquet reader.
    """
    from dag_tools.io_managers.duckdb import asset_uri

    # key_prefix may be a multi-segment path ("minio-svc/publog-lake/publog"),
    # because the asset key encodes <platform_instance>/<bucket>/<path> so the
    # Dagster key, the DataHub URN and the S3 path all derive from one string.
    key = [*key_prefix.split("/"), table]
    return asset_uri(
        lake_root, key, directory=False,
        key_encodes_location=len(key) >= 3,
    )


# Freshness is decided from the lake, never from Dagster's event log.
# `instance.get_latest_materialization_event` returns wiped events when called
# from inside a run (verified on dagster 1.13.7: it yields the pre-wipe value
# in-run while correctly returning None outside), so anything built on it skips
# work that was never actually done. Object metadata cannot drift from reality
# the same way, and it self-heals if someone deletes a file from the bucket.


def _fs_and_path(url: str, options: Dict[str, Any]):
    fs, _, paths = fsspec.get_fs_token_paths(url, storage_options=options or None)
    return fs, paths[0]


def object_exists(url: str, options: Dict[str, Any]) -> bool:
    fs, path = _fs_and_path(url, options)
    try:
        return bool(fs.exists(path))
    except Exception:
        return False


def object_mtime(url: str, options: Dict[str, Any]) -> Optional[float]:
    """Last-modified of an object as an epoch float, or None if absent.

    fsspec reports `mtime` for local files and `LastModified` for S3, so both
    shapes have to be handled to compare an input against its output.
    """
    fs, path = _fs_and_path(url, options)
    try:
        return _mtime_from_info(fs.info(path))
    except Exception:
        return None


def _mtime_from_info(info: Dict[str, Any]) -> Optional[float]:
    for field in ("mtime", "LastModified", "last_modified", "created"):
        value = info.get(field)
        if value is None:
            continue
        if hasattr(value, "timestamp"):
            return value.timestamp()
        if isinstance(value, (int, float)):
            return float(value)
    return None


def dataset_mtime(url: str, options: Dict[str, Any]) -> Optional[float]:
    """Newest part file under a dataset directory, or None if it has none.

    The IO manager writes a directory of `data_N.parquet` parts, and on S3 a
    directory is only a key prefix -- `info()` on it reports no
    LastModified, so `object_mtime` returns None and the table would rebuild
    on every run. Statting the parts is what actually answers "when was this
    table last written".

    Newest rather than oldest: a rebuild replaces every part, so the newest
    is when the table was last produced. An interrupted write leaves a
    partial directory whose newest part is still older than a subsequently
    re-staged CSV, so it rebuilds -- which is the safe direction.
    """
    fs, path = _fs_and_path(url.rstrip("/"), options)
    try:
        if not fs.exists(path):
            return None
        # A single file is still a valid shape (file_size_bytes disabled).
        if fs.isfile(path):
            return _mtime_from_info(fs.info(path))
        entries = fs.find(path)
    except Exception:
        return None

    times = []
    for entry in entries:
        if not str(entry).endswith(".parquet"):
            continue
        try:
            value = _mtime_from_info(fs.info(entry))
        except Exception:
            continue
        if value is not None:
            times.append(value)
    return max(times) if times else None


def read_marker(url: str, options: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    import json

    try:
        with fsspec.open(url, "rb", **options) as f:
            return json.loads(f.read().decode("utf-8"))
    except Exception:
        return None


def write_marker(url: str, payload: Dict[str, Any], options: Dict[str, Any]) -> None:
    import json

    with fsspec.open(url, "wb", **options) as f:
        f.write(json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"))


def stage_zip_to_lake(
    session,
    url: str,
    lake_root: str,
    as_of_date: str,
    slug: str,
    members: List[str],
    dest_config: Dict[str, Any],
    log=None,
) -> Dict[str, Dict[str, Any]]:
    """Download an archive and stream each CSV member into the lake.

    Members are copied out of the zip straight to object storage, so local disk
    only ever holds the compressed archive -- not the far larger expansion.
    Returns {member: {"url": ..., "size_bytes": ...}}.
    """
    import tempfile

    from pub_tools.assets import download_url, source_filename

    options = storage_options(dest_config)
    staged: Dict[str, Dict[str, Any]] = {}
    filename = source_filename(url)

    with tempfile.TemporaryDirectory() as tmp:
        local = os.path.join(tmp, filename)
        if log:
            log.info("Downloading %s", url)
        download_url(session, url, local)
        size_mb = os.path.getsize(local) / (1024 * 1024)
        if log:
            log.info("  -> %.1f MiB downloaded", size_mb)

        if not filename.lower().endswith(".zip"):
            target = raw_csv_url(lake_root, as_of_date, slug, filename)
            _copy_local_to_url(local, target, options)
            staged[filename] = {
                "url": target,
                "size_bytes": os.path.getsize(local),
            }
            if log:
                log.info("  staged %s -> %s", filename, target)
            return staged

        with zipfile.ZipFile(local) as z:
            present = {
                os.path.basename(n): n
                for n in z.namelist()
                if n.lower().endswith(".csv") and not n.endswith("/")
            }
            for i, member in enumerate(members, start=1):
                name = present.get(os.path.basename(member))
                if name is None:
                    continue
                target = raw_csv_url(lake_root, as_of_date, slug, member)
                info = z.getinfo(name)
                if log:
                    log.info(
                        "[%d/%d] staging %s (%.1f MiB) -> %s",
                        i, len(members), member,
                        info.file_size / (1024 * 1024), target,
                    )
                with z.open(name) as src:
                    with fsspec.open(target, "wb", **options) as dst:
                        _stream(src, dst)
                staged[os.path.basename(member)] = {
                    "url": target,
                    "size_bytes": info.file_size,
                }

        # Anything in the archive that the manifest does not declare has no
        # asset to convert it, so it would vanish without a word.
        undeclared = sorted(set(present) - {os.path.basename(m) for m in members})
        if undeclared and log:
            log.warning(
                "%s contains %d CSV(s) absent from PUBLOG_SOURCE_MANIFEST: %s. "
                "They are NOT staged and have no table asset; regenerate the "
                "manifest with scripts/discover_manifest.py to pick them up.",
                filename, len(undeclared), ", ".join(undeclared),
            )
    return staged


def _stream(src, dst, chunk_size: int = 8 * 1024 * 1024) -> int:
    total = 0
    while True:
        chunk = src.read(chunk_size)
        if not chunk:
            return total
        dst.write(chunk)
        total += len(chunk)


def _copy_local_to_url(local_path: str, target: str, options: Dict[str, Any]) -> None:
    with open(local_path, "rb") as src:
        with fsspec.open(target, "wb", **options) as dst:
            _stream(src, dst)


def clean_header(s: str) -> str:
    return (
        s.strip()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
        .replace("/", "_")
        .lower()
    )


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _projection(con, csv_path: str) -> str:
    """Build the SELECT that renames CSV headers to lake column names.

    DuckDB keeps the raw header (`CAGE Code`); the lake has always used cleaned
    names (`cage_code`), and downstream consumers depend on those. Short rows
    are null-padded on read, so COALESCE restores the empty string the previous
    row-by-row loader produced rather than introducing NULLs.
    """
    described = con.execute(
        f"SELECT * FROM read_csv('{csv_path}', {DUCKDB_READ_CSV_OPTIONS}) LIMIT 0"
    ).description
    raw_names = [d[0] for d in described]

    projected: List[str] = []
    used: Dict[str, int] = {}
    for raw in raw_names:
        name = clean_header(raw)
        if name in used:
            used[name] += 1
            name = f"{name}_{used[name]}"
        else:
            used[name] = 1
        projected.append(
            f"COALESCE({_quote(raw)}, '') AS {_quote(name)}"
        )
    return ", ".join(projected)


def csv_relation(con, csv_url: str):
    """The staged CSV as a lazy DuckDB relation, headers cleaned.

    Nothing is read yet -- the relation is a query. The DuckDB IO manager
    executes it during `handle_output`, streaming CSV straight to Parquet in
    vectorised C++ without the rows passing through Python.

    The connection must outlive the asset that builds this, since the query
    only runs later; see `DuckDBResource.connect` versus `get_connection`.
    """
    csv_path = duckdb_path(csv_url)
    return con.sql(
        f"SELECT {_projection(con, csv_path)} FROM "
        f"read_csv('{csv_path}', {DUCKDB_READ_CSV_OPTIONS})"
    )


def csv_to_parquet(
    con,
    csv_url: str,
    parquet_url: str,
    compression: str = "zstd",
    log=None,
) -> int:
    """Convert one staged CSV to one Parquet file. Returns the row count.

    DuckDB streams this in vectorised C++ rather than a per-row Python loop,
    which is where the previous loader spent nearly all of its time.
    """
    csv_path = duckdb_path(csv_url)
    parquet_path = duckdb_path(parquet_url)

    parent = os.path.dirname(parquet_path)
    if not parquet_path.startswith(("s3://", "gs://", "az://", "http")):
        os.makedirs(parent, exist_ok=True)

    projection = _projection(con, csv_path)
    if log:
        log.info("Converting %s -> %s", csv_url, parquet_url)
    con.execute(
        f"COPY (SELECT {projection} FROM read_csv('{csv_path}', "
        f"{DUCKDB_READ_CSV_OPTIONS})) TO '{parquet_path}' "
        f"(FORMAT PARQUET, COMPRESSION {compression})"
    )
    # Parquet stores its row count in the footer, so this does not rescan.
    (rows,) = con.execute(
        f"SELECT count(*) FROM read_parquet('{parquet_path}')"
    ).fetchone()
    if log:
        log.info("  -> %s rows", f"{rows:,}")
    return int(rows)
