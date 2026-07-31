import pytest
import requests

from pub_tools import assets
from pub_tools.assets import (
    PUBLOG_MONTHLY_URLS,
    PUBLOG_QUARTERLY_URLS,
    PUBLOG_SOURCE_MANIFEST,
    manifest_tables,
    source_filename,
    table_name_for,
)
from pub_tools.components.publog_pipeline.component import source_slug


class FakeResponse:
    def __init__(self, body=b"", headers=None, status=200):
        self.content = body
        self.headers = headers or {}
        self.status_code = status
        self._body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))

    def iter_content(self, chunk_size=1):
        yield self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.headers = {}

    def get(self, url, **kwargs):
        return self.response


# --- URL parsing -----------------------------------------------------------
# DLA appends a required `?ver=` cache-buster to some links. Splitting the URL
# on "/" then sees "FLISV.zip?ver=..." as the filename, which silently breaks
# the .zip suffix check, the manifest lookup, and the asset slug at once.

VER = "?ver=8lwvL-2fQoC8fQloktcenw%3d%3d"


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://x/PUBLOG/CAGE.zip", "CAGE.zip"),
        ("https://x/PUBLOG/FLISV.zip" + VER, "FLISV.zip"),
        ("https://x/FOIA/MRD0107.CSV", "MRD0107.CSV"),
        ("https://x/PUBLOG/H-SERIES.zip#frag", "H-SERIES.zip"),
    ],
)
def test_source_filename_ignores_query_and_fragment(url, expected):
    assert source_filename(url) == expected


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://x/PUBLOG/H-SERIES.zip", "h_series"),
        ("https://x/PUBLOG/FREIGHT_PACKAGING.zip", "freight_packaging"),
        ("https://x/PUBLOG/FLISV.zip" + VER, "flisv"),
        ("https://x/FOIA/MRD06P1.CSV", "mrd06p1"),
    ],
)
def test_source_slug(url, expected):
    assert source_slug(url) == expected


def test_slugs_are_unique_across_all_sources():
    slugs = [source_slug(u) for u in PUBLOG_MONTHLY_URLS + PUBLOG_QUARTERLY_URLS]
    assert len(slugs) == len(set(slugs))


# --- manifest --------------------------------------------------------------


def test_every_configured_url_has_a_manifest_entry():
    for url in PUBLOG_MONTHLY_URLS + PUBLOG_QUARTERLY_URLS:
        assert manifest_tables(url), f"no manifest tables for {url}"


def test_manifest_tables_unknown_source_names_the_fix():
    with pytest.raises(KeyError, match="PUBLOG_SOURCE_MANIFEST"):
        manifest_tables("https://x/PUBLOG/NOT_A_REAL_FILE.zip")


def test_table_names_are_globally_unique():
    """Table assets are flat keys under one prefix, so a name colliding across
    two archives would silently make two sources write the same asset."""
    seen = {}
    for url in PUBLOG_MONTHLY_URLS + PUBLOG_QUARTERLY_URLS:
        for table in manifest_tables(url):
            assert table not in seen, f"{table} claimed by {seen.get(table)} and {url}"
            seen[table] = url


def test_manifest_matches_observed_extraction_counts():
    """Counts observed in a real production run; a mismatch means the manifest
    drifted from what DLA actually ships."""
    observed = {
        "CAGE.zip": 3,
        "CHARACTERISTICS.zip": 1,
        "FREIGHT_PACKAGING.zip": 5,
        "HISTORY.zip": 4,
        "H-SERIES.zip": 15,
        "IDENTIFICATION.zip": 6,
        "MANAGEMENT.zip": 9,
        "REFERENCE.zip": 1,
        "MOE_RULE.zip": 1,
        "FLISV.zip": 1,
    }
    for filename, count in observed.items():
        assert len(PUBLOG_SOURCE_MANIFEST[filename]) == count, filename


# --- HTML-404 detection ----------------------------------------------------
# DLA serves its 404 page as HTTP 200 text/html, so raise_for_status sees
# nothing wrong and the failure surfaces much later as BadZipFile.


def test_html_error_page_rejected_by_last_modified():
    session = FakeSession(
        FakeResponse(b"<!DOCTYPE html>", {"Content-Type": "text/html; charset=utf-8"})
    )
    with pytest.raises(RuntimeError, match="HTML page"):
        assets.fetch_last_modified(session, "https://x/PUBLOG/GONE.zip")


def test_html_error_page_rejected_by_download(tmp_path):
    session = FakeSession(FakeResponse(b"<!DOCTYPE html>", {"Content-Type": "text/html"}))
    with pytest.raises(RuntimeError, match="HTML page"):
        assets.download_url(session, "https://x/PUBLOG/GONE.zip", str(tmp_path / "f.zip"))


def test_last_modified_passes_through_for_real_file():
    session = FakeSession(
        FakeResponse(b"PK\x03\x04", {
            "Content-Type": "application/x-zip-compressed",
            "Last-Modified": "Mon, 27 Jul 2026 18:10:09 GMT",
        })
    )
    assert assets.fetch_last_modified(session, "https://x/f.zip") == (
        "Mon, 27 Jul 2026 18:10:09 GMT"
    )
