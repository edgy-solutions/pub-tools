import io
import json
import pathlib
import zipfile

import duckdb
import pytest

from pub_tools.lake import (
    clean_header,
    csv_to_parquet,
    duckdb_connection,
    duckdb_path,
    marker_url,
    object_exists,
    object_mtime,
    raw_csv_url,
    read_marker,
    stage_zip_to_lake,
    storage_options,
    table_parquet_url,
    write_marker,
)


@pytest.fixture
def lake(tmp_path):
    return (tmp_path / "lake").as_uri()


def _write_csv(tmp_path, name, body):
    p = tmp_path / name
    p.write_text(body, newline="")
    return p


# --- config plumbing -------------------------------------------------------


def test_storage_options_maps_dlt_credentials():
    opts = storage_options({
        "credentials": {
            "aws_access_key_id": "user",
            "aws_secret_access_key": "pass",
            "endpoint_url": "http://minio:9000",
        }
    })
    assert opts["key"] == "user"
    assert opts["secret"] == "pass"
    assert opts["client_kwargs"] == {"endpoint_url": "http://minio:9000"}


def test_storage_options_empty_when_unconfigured():
    assert storage_options({}) == {}


def test_duckdb_connection_fails_fast_when_baked_extension_missing(tmp_path, monkeypatch):
    """The container image ships httpfs (scripts/bake_duckdb_extensions.py).
    When DUCKDB_EXTENSION_DIRECTORY says so but the extension is not there,
    the connection must fail immediately with an actionable message rather
    than quietly downloading from duckdb.org -- in a restricted cluster that
    is a long hang and a confusing error, repeated for every table."""
    monkeypatch.setenv("DUCKDB_EXTENSION_DIRECTORY", str(tmp_path / "empty"))
    with pytest.raises(RuntimeError) as excinfo:
        duckdb_connection({})
    message = str(excinfo.value)
    assert "bake_duckdb_extensions" in message
    assert "architecture-specific" in message


def test_duckdb_connection_uses_baked_extension_directory(tmp_path, monkeypatch):
    """Bake into a scratch directory the same way the image build does, then
    prove a connection loads httpfs from it with auto-install disabled."""
    import duckdb

    directory = tmp_path / "ext"
    directory.mkdir()
    seed = duckdb.connect()
    seed.execute("SET extension_directory=?", [str(directory)])
    try:
        seed.execute("INSTALL httpfs")
    except Exception:
        pytest.skip("no network to fetch httpfs for the offline-load check")
    finally:
        seed.close()

    monkeypatch.setenv("DUCKDB_EXTENSION_DIRECTORY", str(directory))
    con = duckdb_connection({})
    try:
        loaded = con.execute(
            "SELECT loaded FROM duckdb_extensions() WHERE extension_name='httpfs'"
        ).fetchall()
        assert loaded and loaded[0][0] is True
    finally:
        con.close()


def _setting(con, name):
    return con.execute(f"SELECT current_setting('{name}')").fetchone()[0]


def test_duckdb_connection_applies_dlt_credentials():
    """dlt names the region `region_name`; DuckDB wants a bare host:port in
    s3_endpoint plus a separate use_ssl flag, and path-style URLs because
    MinIO does not serve virtual-host buckets."""
    con = duckdb_connection({
        "credentials": {
            "aws_access_key_id": "user",
            "aws_secret_access_key": "pass",
            "endpoint_url": "http://minio:9000",
            "region_name": "eu-west-1",
        }
    })
    try:
        assert _setting(con, "s3_endpoint") == "minio:9000"
        assert _setting(con, "s3_use_ssl") is False
        assert _setting(con, "s3_region") == "eu-west-1"
        assert _setting(con, "s3_url_style") == "path"
        assert _setting(con, "s3_access_key_id") == "user"
    finally:
        con.close()


def test_duckdb_connection_honors_memory_limit_env(monkeypatch):
    """Conversions run in a pod with a cgroup limit, but DuckDB sizes its
    default budget from detected system RAM -- so a large CSV can get the pod
    OOMKilled instead of spilling. DUCKDB_MEMORY_LIMIT caps it."""
    monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "256MB")
    con = duckdb_connection({})
    try:
        # DuckDB reads 256MB as 256e6 bytes and reports it back in MiB.
        assert _setting(con, "memory_limit") == "244.1 MiB"
    finally:
        con.close()


def test_duckdb_connection_leaves_memory_default_when_env_unset(monkeypatch):
    monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
    con = duckdb_connection({})
    try:
        assert _setting(con, "memory_limit") != "244.1 MiB"
    finally:
        con.close()


def test_duckdb_path_strips_file_scheme_but_not_s3(tmp_path):
    assert duckdb_path("s3://bucket/a/b.csv") == "s3://bucket/a/b.csv"
    # tmp_path, not a hardcoded POSIX path: file URIs require an absolute path,
    # and "/tmp/..." is not absolute on Windows.
    target = tmp_path / "x" / "y.csv"
    local = duckdb_path(target.as_uri())
    assert "://" not in local
    assert pathlib.Path(local) == target


def test_url_builders_are_stable():
    assert raw_csv_url("s3://lake/", "2026-07-01", "cage", "sub/dir/P_CAGE.CSV") == (
        "s3://lake/_raw/cage/2026-07-01/P_CAGE.CSV"
    )
    assert marker_url("s3://lake", "2026-07-01", "cage") == (
        "s3://lake/_raw/cage/2026-07-01/_source.json"
    )
    # The DuckDB IO manager owns the table layout now, so this must agree
    # with where the manager actually writes -- a freshness check against a
    # path the writer does not use silently rebuilds forever or never.
    assert table_parquet_url("s3://lake", "p_cage") == "s3://lake/publog/p_cage"
    assert table_parquet_url("s3://lake", "p_cage", key_prefix="other") == (
        "s3://lake/other/p_cage"
    )


def test_table_parquet_url_tracks_the_io_manager():
    """Pinned against the IO manager's own path function rather than a
    literal, so the two cannot drift apart."""
    from dag_tools.io_managers.duckdb import asset_uri

    assert table_parquet_url("s3://lake", "p_cage") == asset_uri(
        "s3://lake", ["publog", "p_cage"], directory=False
    )


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CAGE Code", "cage_code"),
        ("NIIN No.", "niin_no_"),
        ("Ref-No", "ref_no"),
        ("A/B", "a_b"),
        ("  Padded  ", "padded"),
    ],
)
def test_clean_header(raw, expected):
    assert clean_header(raw) == expected


# --- staging ---------------------------------------------------------------


def test_stage_zip_streams_declared_members_only(tmp_path, lake, monkeypatch):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("P_CAGE.CSV", "a,b\n1,2\n")
        z.writestr("V_CAGE_ADDRESS.CSV", "a,b\n3,4\n")
        z.writestr("SURPRISE.CSV", "a,b\n5,6\n")
        z.writestr("readme.txt", "ignore")

    monkeypatch.setattr(
        "pub_tools.assets.download_url",
        lambda session, url, dest: pathlib.Path(dest).write_bytes(buf.getvalue()),
    )
    warnings = []

    class Log:
        def info(self, msg, *a):
            pass

        def warning(self, msg, *a):
            warnings.append(msg % a if a else msg)

    staged = stage_zip_to_lake(
        session=None,
        url="https://x/PUBLOG/CAGE.zip",
        lake_root=lake,
        as_of_date="2026-07-01",
        slug="cage",
        members=["P_CAGE.CSV", "V_CAGE_ADDRESS.CSV"],
        dest_config={},
        log=Log(),
    )

    assert sorted(staged) == ["P_CAGE.CSV", "V_CAGE_ADDRESS.CSV"]
    assert object_exists(raw_csv_url(lake, "2026-07-01", "cage", "P_CAGE.CSV"), {})
    # an undeclared member has no asset to convert it, so it must be reported
    assert any("SURPRISE.CSV" in w for w in warnings), warnings
    assert not object_exists(raw_csv_url(lake, "2026-07-01", "cage", "SURPRISE.CSV"), {})


def test_stage_bare_csv_source(tmp_path, lake, monkeypatch):
    monkeypatch.setattr(
        "pub_tools.assets.download_url",
        lambda session, url, dest: pathlib.Path(dest).write_text("a,b\n1,2\n"),
    )
    staged = stage_zip_to_lake(
        session=None,
        url="https://x/FOIA/MRD0107.CSV",
        lake_root=lake,
        as_of_date="2026-07-01",
        slug="mrd0107",
        members=["MRD0107.CSV"],
        dest_config={},
    )
    assert list(staged) == ["MRD0107.CSV"]


# --- markers ---------------------------------------------------------------


def test_marker_roundtrip_and_missing(lake):
    url = marker_url(lake, "2026-07-01", "cage")
    assert read_marker(url, {}) is None
    write_marker(url, {"source_last_modified": "LM1", "members": ["A.CSV"]}, {})
    assert read_marker(url, {})["source_last_modified"] == "LM1"


def test_read_marker_tolerates_corruption(lake, tmp_path):
    url = marker_url(lake, "2026-07-01", "cage")
    write_marker(url, {"a": 1}, {})
    target = pathlib.Path(duckdb_path(url))
    target.write_text("{not json")
    # A corrupt marker must read as "unknown" so staging redoes the work,
    # rather than raising and wedging the asset.
    assert read_marker(url, {}) is None


def test_object_mtime_absent_is_none(lake):
    assert object_mtime(lake + "/nope.csv", {}) is None


# --- conversion ------------------------------------------------------------


def test_csv_to_parquet_preserves_leading_zeros_and_types(tmp_path, lake):
    """PUB LOG identifiers are zero-padded; any type inference corrupts them."""
    csv = _write_csv(
        tmp_path,
        "P_CAGE.CSV",
        'CAGE Code,NIIN No.,Company-Name\n01234,0001,"ACME, INC."\n0000A,0002,"Q ""X"""\n',
    )
    out = table_parquet_url(lake, "publog_2026_07_01", "p_cage")
    con = duckdb.connect()
    rows = csv_to_parquet(con, csv.as_uri(), out)
    assert rows == 2

    described = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [duckdb_path(out)]
    ).fetchall()
    assert [d[0] for d in described] == ["cage_code", "niin_no_", "company_name"]
    assert {d[1] for d in described} == {"VARCHAR"}

    values = con.execute(
        "SELECT * FROM read_parquet(?) ORDER BY niin_no_", [duckdb_path(out)]
    ).fetchall()
    assert values == [
        ("01234", "0001", "ACME, INC."),
        ("0000A", "0002", 'Q "X"'),
    ]


def test_csv_to_parquet_pads_short_rows_with_empty_string(tmp_path, lake):
    """The previous loader padded missing trailing fields with "", not NULL.
    Downstream consumers depend on that, so parity is asserted explicitly."""
    csv = _write_csv(tmp_path, "T.CSV", "a,b,c\n1,2\n")
    out = table_parquet_url(lake, "ds", "t")
    con = duckdb.connect()
    assert csv_to_parquet(con, csv.as_uri(), out) == 1
    assert con.execute(
        "SELECT * FROM read_parquet(?)", [duckdb_path(out)]
    ).fetchall() == [("1", "2", "")]


def test_csv_to_parquet_deduplicates_colliding_column_names(tmp_path, lake):
    """"A.B" and "A-B" both clean to "a_b"; Parquet cannot hold two columns of
    the same name, so the second must be suffixed rather than silently lost."""
    csv = _write_csv(tmp_path, "T.CSV", "A.B,A-B\n1,2\n")
    out = table_parquet_url(lake, "ds", "t")
    con = duckdb.connect()
    csv_to_parquet(con, csv.as_uri(), out)
    described = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [duckdb_path(out)]
    ).fetchall()
    assert [d[0] for d in described] == ["a_b", "a_b_2"]


def test_csv_to_parquet_row_count_matches_input(tmp_path, lake):
    body = "a\n" + "".join("%d\n" % i for i in range(5000))
    csv = _write_csv(tmp_path, "BIG.CSV", body)
    out = table_parquet_url(lake, "ds", "big")
    con = duckdb.connect()
    assert csv_to_parquet(con, csv.as_uri(), out) == 5000
