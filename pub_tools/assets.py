"""DLA PUB LOG ingestion via the FLIS Electronic Reading Room CSV feeds.

The Reading Room replaced the legacy flat-file feed in April 2023. Each topic
ships as a zip of standard RFC 4180 CSVs, one CSV per logical table. Monthly
files are republished on the first business day of each month; quarterly
files (FLISV, MRD) are republished quarterly.

DLA's CDN (Akamai Bot Manager) rejects curl/wget/Schannel TLS fingerprints
outright. Python `requests` happens to slip through with a recent Chrome
User-Agent; do NOT "clean up" the UA below without testing against the live
endpoint, or ingest will silently 403.
"""
import csv
import os
import zipfile
from typing import Dict, Iterator, List, Optional

import dlt
import requests

PUBLOG_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)

PUBLOG_BASE_URL = (
    "https://www.dla.mil/Portals/104/Documents/"
    "InformationOperations/LogisticsInformationServices/FOIA/PUBLOG/"
)

PUBLOG_FOIA_BASE_URL = (
    "https://www.dla.mil/Portals/104/Documents/"
    "InformationOperations/LogisticsInformationServices/FOIA/"
)

PUBLOG_MONTHLY_URLS: List[str] = [
    PUBLOG_BASE_URL + name
    for name in (
        "CAGE.zip",
        "CHARACTERISTICS.zip",
        "FREIGHT_PACKAGING.zip",
        "HISTORY.zip",
        "H-SERIES.zip",
        "IDENTIFICATION.zip",
        "MANAGEMENT.zip",
        "REFERENCE.zip",
        "MOE_RULE.zip",
    )
]

PUBLOG_QUARTERLY_URLS: List[str] = [
    PUBLOG_BASE_URL + "FLISV_1.zip",
    PUBLOG_FOIA_BASE_URL + "MRD0107.CSV",
    PUBLOG_FOIA_BASE_URL + "MRD0300.CSV",
    PUBLOG_FOIA_BASE_URL + "MRD0500.CSV",
    PUBLOG_FOIA_BASE_URL + "MRD06P1.CSV",
    PUBLOG_FOIA_BASE_URL + "MRD06P2.CSV",
]


def publog_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": PUBLOG_USER_AGENT})
    return s


def fetch_last_modified(session: requests.Session, url: str) -> Optional[str]:
    """Return the Last-Modified header for url, or None if absent.

    Akamai blocks HEAD on these URLs, so we issue a GET with stream=True and
    close immediately after reading the headers.
    """
    with session.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        return r.headers.get("Last-Modified")


def download_url(session: requests.Session, url: str, dest_path: str) -> None:
    """Stream a URL to dest_path."""
    with session.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def fetch_url_to_dir(session: requests.Session, url: str, dest_dir: str) -> List[str]:
    """Download url into dest_dir. If it's a zip, extract and return the list
    of contained CSV paths. If it's a bare CSV, return [downloaded path].
    """
    os.makedirs(dest_dir, exist_ok=True)
    filename = url.rsplit("/", 1)[-1]
    local_path = os.path.join(dest_dir, filename)
    download_url(session, url, local_path)

    if filename.lower().endswith(".zip"):
        with zipfile.ZipFile(local_path) as z:
            z.extractall(dest_dir)
            csvs = [
                os.path.join(dest_dir, n)
                for n in z.namelist()
                if n.lower().endswith(".csv") and not n.endswith("/")
            ]
        os.remove(local_path)
        return csvs

    if filename.lower().endswith(".csv"):
        return [local_path]

    raise ValueError(f"Unsupported PUB LOG file type for url: {url}")


def _clean_header(s: str) -> str:
    return (
        s.strip()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
        .replace("/", "_")
        .lower()
    )


@dlt.resource(name="publog_table", write_disposition="replace")
def publog_csv_resource(csv_paths: List[str]) -> Iterator[dict]:
    """Stream rows from each CSV, routing to a dlt table named after the file."""
    for csv_path in csv_paths:
        table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()
        with open(csv_path, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            try:
                raw_headers = next(reader)
            except StopIteration:
                continue
            headers = [_clean_header(h) for h in raw_headers]
            for row in reader:
                if len(row) < len(headers):
                    row = row + [""] * (len(headers) - len(row))
                elif len(row) > len(headers):
                    row = row[: len(headers)]
                record = dict(zip(headers, row))
                yield dlt.mark.with_table_name(record, table_name)


def load_previous_last_modified(context, metadata_key: str = "source_last_modified") -> Dict[str, str]:
    """Read the per-url Last-Modified map from the latest materialization of
    the running asset. Returns {} if none recorded or unparseable."""
    try:
        event = context.instance.get_latest_materialization_event(context.asset_key)
    except Exception:
        return {}
    if event is None or event.asset_materialization is None:
        return {}
    meta = event.asset_materialization.metadata.get(metadata_key)
    if meta is None:
        return {}
    for attr in ("data", "value"):
        if hasattr(meta, attr):
            try:
                return dict(getattr(meta, attr))
            except (TypeError, ValueError):
                pass
    if hasattr(meta, "text"):
        import json
        try:
            return json.loads(meta.text)
        except (TypeError, ValueError):
            return {}
    try:
        return dict(meta)
    except (TypeError, ValueError):
        return {}
