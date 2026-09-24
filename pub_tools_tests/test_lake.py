import io
import json
import pathlib
import zipfile

import duckdb
import pytest

from pub_tools.lake import (
    clean_header,
    csv_relation,
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


# --- staging: encoding repair -----------------------------------------------


class _RecordingLog:
    """Minimal stand-in for a Dagster step's logger."""

    def __init__(self):
        self.warnings = []

    def info(self, msg, *a):
        pass

    def warning(self, msg, *a):
        self.warnings.append(msg % a if a else msg)


def _stage_one_member(tmp_path, lake, monkeypatch, member_name, member_bytes, log=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(member_name, member_bytes)
    monkeypatch.setattr(
        "pub_tools.assets.download_url",
        lambda session, url, dest: pathlib.Path(dest).write_bytes(buf.getvalue()),
    )
    staged = stage_zip_to_lake(
        session=None,
        url="https://x/PUBLOG/IDENTIFICATION.zip",
        lake_root=lake,
        as_of_date="2026-07-01",
        slug="identification",
        members=[member_name],
        dest_config={},
        log=log,
    )
    target = raw_csv_url(lake, "2026-07-01", "identification", member_name)
    return staged, pathlib.Path(duckdb_path(target)).read_bytes()


def test_stage_zip_repairs_stray_cp1252_byte_leaves_valid_utf8_untouched(
    tmp_path, lake, monkeypatch
):
    """P_HELP.CSV line 22624 carries one stray CP1252 byte (0x96, en dash) in
    an otherwise UTF-8 file. DuckDB 1.5.5 cannot read that byte under any
    read_csv option, so staging must repair it to its UTF-8 equivalent
    (U+2013) -- while a DIFFERENT line's legitimate UTF-8 en dash
    ("\xe2\x80\x93") must pass through byte-for-byte, since blanket
    CP1252-decoding would corrupt it."""
    bad_line = "WEIGHING MORE THAN 25 ".encode("utf-8") + b"\x96" + " 70 G/M2\n".encode("utf-8")
    good_line = ("A LEGITIMATE EN DASH – HERE\n").encode("utf-8")
    member_bytes = b"HDR_A,HDR_B\n" + bad_line + good_line

    log = _RecordingLog()
    staged, out_bytes = _stage_one_member(
        tmp_path, lake, monkeypatch, "P_HELP.CSV", member_bytes, log=log
    )

    text = out_bytes.decode("utf-8")  # must not raise
    assert "WEIGHING MORE THAN 25 – 70 G/M2" in text
    assert good_line in out_bytes
    assert b"\x96" not in out_bytes
    assert staged["P_HELP.CSV"]["size_bytes"] == len(out_bytes)
    # header is line 1, the repaired line is line 2
    assert any(w.startswith("P_HELP.CSV:2:") for w in log.warnings), log.warnings


def test_stage_zip_leaves_already_valid_utf8_byte_identical(tmp_path, lake, monkeypatch):
    """A file with legitimate multi-byte UTF-8 (en dash, degree sign, curly
    quotes) but no encoding problem must pass through unchanged, not just
    unchanged in content -- byte-for-byte, so size_bytes stays meaningful."""
    member_bytes = (
        "CAGE,NAME\n"
        "1,° DEGREE SIGN\n"
        "2,“CURLY QUOTES”\n"
        "3,EN DASH – HERE\n"
    ).encode("utf-8")

    staged, out_bytes = _stage_one_member(
        tmp_path, lake, monkeypatch, "P_CAGE.CSV", member_bytes
    )

    assert out_bytes == member_bytes
    assert staged["P_CAGE.CSV"]["size_bytes"] == len(member_bytes)


def test_stream_utf8_normalized_handles_chunk_boundary(tmp_path):
    """A small chunk_size forces the stray byte and a legitimate multi-byte
    character to land near (or across) a chunk boundary. The carry-to-next-
    -chunk logic must still repair only the bad line and leave the good line
    untouched, regardless of where the boundary falls."""
    from pub_tools.lake import _stream_utf8_normalized

    bad_line = "AAAA ".encode("utf-8") + b"\x96" + " BBBB\n".encode("utf-8")
    good_line = ("CCCC " + "–" + " DDDD\n").encode("utf-8")
    data = bad_line + good_line

    src = io.BytesIO(data)
    dst = io.BytesIO()
    written = _stream_utf8_normalized(src, dst, "MEMBER.CSV", chunk_size=5)
    out = dst.getvalue()

    assert out == "AAAA – BBBB\n".encode("utf-8") + good_line
    assert written == len(out)
    out.decode("utf-8")  # must not raise


# --- conversion: overflow merge for ragged rows -----------------------------


# The 12 known ragged V_CHARACTERISTICS rows only show up past row ~25,000 in
# production, which is past DuckDB's default CSV sniffer sample
# (sample_size=20480 rows) -- exactly the scenario that makes the bug hard to
# hit in a small hand test file, so the fixture reproduces it deliberately.
_CHARACTERISTICS_HEADER = '"NIIN","MRC","REQUIREMENTS_STATEMENT","CLEAR_TEXT_REPLY"\n'
_CHARACTERISTICS_RAGGED_ROWS = (
    '"016727034","FEAT","SPECIAL FEATURES","NEW CAGE WILL BE SAFE FLEET",'
    '"319 ROSKE DRIVE","ELKHART,IN 46516","PHONE;(574)266-3700"\n'
    '"011514249","ZZZT","NONDEFINITIVE SPEC/STD DATA","TYPE  ",""\n'
    '"016873317","AGAV","END ITEM IDENTIFICATION","","1B1TVR AMK2"\n'
)


def _characteristics_ragged_body(n_normal: int) -> str:
    normal = "".join(
        f'"{i:09d}","AAAA","NORMAL","JUST FINE {i}"\n' for i in range(n_normal)
    )
    return _CHARACTERISTICS_HEADER + normal + _CHARACTERISTICS_RAGGED_ROWS


def test_csv_to_parquet_merge_overflow_folds_ragged_rows(tmp_path, lake):
    n_normal = 25005
    csv = _write_csv(
        tmp_path, "V_CHARACTERISTICS.CSV", _characteristics_ragged_body(n_normal)
    )
    out = table_parquet_url(lake, "ds", "v_characteristics")
    con = duckdb.connect()
    rows = csv_to_parquet(con, csv.as_uri(), out, merge_overflow=True)
    assert rows == n_normal + 3

    described = con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [duckdb_path(out)]
    ).fetchall()
    assert [d[0] for d in described] == [
        "niin", "mrc", "requirements_statement", "clear_text_reply",
    ]

    result = con.execute(
        "SELECT * FROM read_parquet(?) WHERE niin IN "
        "('016727034','011514249','016873317','000012345') ORDER BY niin",
        [duckdb_path(out)],
    ).fetchall()
    by_niin = {r[0]: r for r in result}

    assert by_niin["016727034"] == (
        "016727034", "FEAT", "SPECIAL FEATURES",
        "NEW CAGE WILL BE SAFE FLEET\n319 ROSKE DRIVE\nELKHART,IN 46516\n"
        "PHONE;(574)266-3700",
    )
    # Documented actual behaviour for the two rows DLA's export leaves
    # ambiguous (one field short, not one field over):
    #  - "TYPE  ",""  -- the row's 5th (overflow) field is an empty quoted
    #    string, which DuckDB's CSV reader reads as NULL just like anywhere
    #    else in this file; concat_ws skips NULLs, so nothing is appended and
    #    clear_text_reply is left as the original 4th field verbatim.
    assert by_niin["011514249"] == (
        "011514249", "ZZZT", "NONDEFINITIVE SPEC/STD DATA", "TYPE  ",
    )
    #  - "","1B1TVR AMK2"  -- here it is the ORIGINAL 4th field that is empty
    #    (also read as NULL), so concat_ws's only non-NULL input is the
    #    overflow field, and that -- not the original 4th field -- is what
    #    ends up in clear_text_reply.
    assert by_niin["016873317"] == (
        "016873317", "AGAV", "END ITEM IDENTIFICATION", "1B1TVR AMK2",
    )
    # a normal row far past the sniffer sample is unaffected by the merge
    assert by_niin["000012345"] == (
        "000012345", "AAAA", "NORMAL", "JUST FINE 12345",
    )


def test_csv_to_parquet_without_merge_overflow_still_raises_on_ragged_rows(tmp_path, lake):
    """Without merge_overflow, behaviour must be unchanged: a ragged row past
    the sniffer sample still fails loudly rather than silently dropping or
    misplacing data."""
    csv = _write_csv(
        tmp_path, "V_CHARACTERISTICS.CSV", _characteristics_ragged_body(25005)
    )
    out = table_parquet_url(lake, "ds", "v_characteristics")
    con = duckdb.connect()
    with pytest.raises(duckdb.Error):
        csv_to_parquet(con, csv.as_uri(), out, merge_overflow=False)


def test_csv_relation_merge_overflow_matches_csv_to_parquet(tmp_path, lake):
    csv = _write_csv(
        tmp_path, "V_CHARACTERISTICS.CSV", _characteristics_ragged_body(25005)
    )
    con = duckdb.connect()
    rows = csv_relation(con, csv.as_uri(), merge_overflow=True).fetchall()
    assert len(rows) == 25005 + 3

    by_niin = {
        r[0]: r for r in rows
        if r[0] in ("016727034", "011514249", "016873317")
    }
    assert by_niin["016727034"][3] == (
        "NEW CAGE WILL BE SAFE FLEET\n319 ROSKE DRIVE\nELKHART,IN 46516\n"
        "PHONE;(574)266-3700"
    )
    assert by_niin["011514249"][3] == "TYPE  "
    assert by_niin["016873317"][3] == "1B1TVR AMK2"
