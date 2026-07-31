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
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import fsspec

# Everything is read and written as text. PUB LOG is full of identifiers whose
# leading zeros carry meaning -- NSNs, NIINs, FSCs, CAGE codes -- and letting a
# type inferencer turn "01234" into the integer 1234 silently corrupts them.
DUCKDB_READ_CSV_OPTIONS = "all_varchar=true, header=true, null_padding=true"


def _split_endpoint(endpoint_url: Optional[str]) -> Tuple[Optional[str], bool]:
    """Split `http://minio:9000` into ("minio:9000", use_ssl=False).

    DuckDB wants the bare host:port in `s3_endpoint` plus a separate
    `s3_use_ssl` flag, where fsspec/boto want the full URL.
    """
    if not endpoint_url:
        return None, True
    parsed = urlparse(endpoint_url)
    if not parsed.netloc:
        return endpoint_url, True
    return parsed.netloc, parsed.scheme != "http"


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

    In the container image the extension is baked in at build time (see
    scripts/bake_duckdb_extensions.py) and DUCKDB_EXTENSION_DIRECTORY points at
    it. When that variable is set the image is meant to be self-contained, so a
    failure to load is raised immediately rather than silently reaching for
    duckdb.org -- which in a restricted cluster means a long hang followed by a
    confusing error, once per asset. Outside the image the variable is unset
    and a download is a reasonable convenience.
    """
    import duckdb

    con = duckdb.connect()
    directory = os.environ.get("DUCKDB_EXTENSION_DIRECTORY")
    if directory:
        con.execute("SET extension_directory=?", [directory])
        con.execute("SET autoinstall_known_extensions=false")
        try:
            con.execute("LOAD httpfs")
        except Exception as e:
            raise RuntimeError(
                f"DuckDB {duckdb.__version__} could not load the httpfs "
                f"extension from {directory}, which is required to read and "
                f"write s3:// paths. The image is supposed to ship it -- "
                f"rebuild so scripts/bake_duckdb_extensions.py runs against "
                f"this DuckDB version, since extensions are version- and "
                f"architecture-specific and an upgraded duckdb will not find "
                f"an extension baked for the previous one. "
                f"Underlying error: {e}"
            ) from e
    else:
        try:
            con.execute("LOAD httpfs")
        except Exception:
            try:
                con.execute("INSTALL httpfs")
                con.execute("LOAD httpfs")
            except Exception as e:
                raise RuntimeError(
                    "DuckDB could not load or install the httpfs extension, "
                    "which is required to read and write s3:// paths. Set "
                    "DUCKDB_EXTENSION_DIRECTORY and run "
                    "scripts/bake_duckdb_extensions.py to provide it offline. "
                    f"Underlying error: {e}"
                ) from e

    creds = dest_config.get("credentials") or {}
    endpoint, use_ssl = _split_endpoint(creds.get("endpoint_url"))
    if creds.get("aws_access_key_id"):
        con.execute("SET s3_access_key_id=?", [creds["aws_access_key_id"]])
    if creds.get("aws_secret_access_key"):
        con.execute("SET s3_secret_access_key=?", [creds["aws_secret_access_key"]])
    con.execute("SET s3_region=?", [creds.get("region_name") or "us-east-1"])
    if endpoint:
        con.execute("SET s3_endpoint=?", [endpoint])
        con.execute("SET s3_use_ssl=?", [use_ssl])
        # MinIO serves path-style buckets; virtual-host style resolves to a
        # hostname that does not exist there.
        con.execute("SET s3_url_style='path'")
    return con


def duckdb_path(url: str) -> str:
    """DuckDB addresses local files by plain path, but object stores by URL."""
    if url.startswith("file://"):
        parsed = urlparse(url)
        path = (parsed.netloc or "") + parsed.path
        # file:///C:/x on Windows arrives as /C:/x
        if os.name == "nt" and len(path) > 2 and path[0] == "/" and path[2] == ":":
            path = path[1:]
        return path
    return url


def raw_prefix(lake_root: str, as_of_date: str, slug: str) -> str:
    return f"{lake_root.rstrip('/')}/_raw/{as_of_date}/{slug}"


def raw_csv_url(lake_root: str, as_of_date: str, slug: str, member: str) -> str:
    """Where a staged CSV lives. Deterministic, so the conversion step can find
    its input without threading state through the upstream asset."""
    return f"{raw_prefix(lake_root, as_of_date, slug)}/{os.path.basename(member)}"


def marker_url(lake_root: str, as_of_date: str, slug: str) -> str:
    """Marker recording which source version produced the staged CSVs."""
    return f"{raw_prefix(lake_root, as_of_date, slug)}/_source.json"


def table_parquet_url(lake_root: str, dataset: str, table: str) -> str:
    return f"{lake_root.rstrip('/')}/{dataset}/{table}/data.parquet"


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
        info = fs.info(path)
    except Exception:
        return None
    for field in ("mtime", "LastModified", "last_modified", "created"):
        value = info.get(field)
        if value is None:
            continue
        if hasattr(value, "timestamp"):
            return value.timestamp()
        if isinstance(value, (int, float)):
            return float(value)
    return None


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
