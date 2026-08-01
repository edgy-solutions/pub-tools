"""Table assets write through the DuckDB IO manager.

The conversion asset no longer writes Parquet itself -- it returns a lazy
DuckDB relation and the IO manager executes it. That moves three things
that used to be local concerns into a contract worth pinning:

  * the relation must still be executable when handle_output runs, which
    is later than the asset body;
  * the lake path is now owned by the IO manager, so the freshness check
    has to agree with it or the table rebuilds forever;
  * skipping is expressed by returning MaterializeResult, since Dagster
    only calls handle_output for a real output value.
"""
import pathlib

import pytest
from dagster import AssetKey, Definitions, materialize

from pub_tools.components.publog_pipeline.component import PublogPipelineComponent
from pub_tools.lake import raw_csv_url, table_parquet_url

CSV_BODY = (
    "CAGE Code,NIIN No.,Ref-No\r\n"
    "01234,000001234,ABC-1\r\n"
    "05678,000005678,DEF-2\r\n"
)

URL = "https://example.test/CAGE.zip"


@pytest.fixture
def lake(tmp_path):
    return (tmp_path / "lake").as_uri()


def _component(lake_root, monkeypatch, **overrides):
    """A component whose only source is a single fake CSV member."""
    import pub_tools.components.publog_pipeline.component as mod

    monkeypatch.setitem(mod.PUBLOG_SOURCE_MANIFEST, "CAGE.zip", ["P_CAGE.CSV"])
    return PublogPipelineComponent(
        monthly_urls=[URL],
        quarterly_urls=[],
        dest_config={"destination": {"bucket_url": lake_root}},
        **overrides,
    )


def _table_asset(component, ctx=None):
    defs = component.build_defs(ctx)
    key = AssetKey(["publog", "p_cage"])
    asset = next(a for a in defs.assets if key in a.keys)
    return defs, asset


def _stage_csv(lake_root, as_of):
    """Put a CSV where the staging asset would have."""
    url = raw_csv_url(lake_root, as_of, "cage", "P_CAGE.CSV")
    path = pathlib.Path(url.replace("file:///", "").replace("file://", ""))
    if not path.is_absolute():
        path = pathlib.Path("/" + str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(CSV_BODY, newline="")
    return path


def _as_of(component):
    from datetime import datetime

    return datetime.now().strftime("%Y-%m-01")


def test_table_lands_where_the_io_manager_owns(lake, monkeypatch, tmp_path):
    component = _component(lake, monkeypatch)
    as_of = _as_of(component)
    _stage_csv(lake, as_of)

    defs, asset = _table_asset(component, None)
    result = materialize([asset], resources=defs.resources)
    assert result.success

    expected = table_parquet_url(lake, "p_cage")
    out = pathlib.Path(expected.replace("file:///", ""))
    if not out.is_absolute():
        out = pathlib.Path("/" + str(out))
    assert out.is_dir(), f"expected a directory of parts at {out}"
    assert list(out.glob("*.parquet"))


def test_leading_zeros_survive_the_relation_path(lake, monkeypatch):
    """PUB LOG identifiers carry meaning in their leading zeros. The
    all_varchar read that protects them has to survive being expressed as a
    relation rather than a COPY statement."""
    import duckdb

    component = _component(lake, monkeypatch)
    _stage_csv(lake, _as_of(component))
    defs, asset = _table_asset(component, None)
    assert materialize([asset], resources=defs.resources).success

    out = table_parquet_url(lake, "p_cage").replace("file:///", "")
    rows = duckdb.connect().execute(
        f"SELECT cage_code, niin_no_ FROM read_parquet('{out}/**/*.parquet') "
        f"ORDER BY cage_code"
    ).fetchall()
    assert rows == [("01234", "000001234"), ("05678", "000005678")]


def test_headers_are_cleaned(lake, monkeypatch):
    import duckdb

    component = _component(lake, monkeypatch)
    _stage_csv(lake, _as_of(component))
    defs, asset = _table_asset(component, None)
    materialize([asset], resources=defs.resources)

    out = table_parquet_url(lake, "p_cage").replace("file:///", "")
    names = [
        d[0]
        for d in duckdb.connect()
        .execute(f"SELECT * FROM read_parquet('{out}/**/*.parquet') LIMIT 0")
        .description
    ]
    assert names == ["cage_code", "niin_no_", "ref_no"]


def test_row_count_metadata_comes_from_the_io_manager(lake, monkeypatch):
    """publog published dagster/row_count before the IO manager owned the
    write; it has to survive the move."""
    component = _component(lake, monkeypatch)
    _stage_csv(lake, _as_of(component))
    defs, asset = _table_asset(component, None)
    result = materialize([asset], resources=defs.resources)

    md = (
        result.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    assert md["dagster/row_count"].value == 2
    assert md["table"].text == "p_cage"


def test_rerun_skips_without_rewriting(lake, monkeypatch):
    """The freshness check stats a DIRECTORY of parts now. If it looked for
    the old single file it would find nothing and rebuild every run --
    silently, since a rebuild still succeeds."""
    component = _component(lake, monkeypatch)
    _stage_csv(lake, _as_of(component))
    defs, asset = _table_asset(component, None)

    assert materialize([asset], resources=defs.resources).success
    second = materialize([asset], resources=defs.resources)
    assert second.success

    md = (
        second.get_asset_materialization_events()[0]
        .step_materialization_data.materialization.metadata
    )
    assert "skipped_reason" in md, f"expected a skip, got {dict(md)}"


def test_missing_staged_csv_names_the_asset_to_run(lake, monkeypatch):
    component = _component(lake, monkeypatch)
    defs, asset = _table_asset(component, None)
    result = materialize([asset], resources=defs.resources, raise_on_error=False)
    assert not result.success
    detail = str(
        result.filter_events(lambda e: e.is_step_failure)[0].event_specific_data.error
    )
    assert "publog/source/cage" in detail


def test_io_manager_is_registered_for_the_broker(lake, monkeypatch):
    """The domain broker advertises assets by looking for
    physical_coordinates on the IO managers in Definitions(resources=), so
    the component has to register one rather than expect it from outside."""
    component = _component(lake, monkeypatch)
    defs, _ = _table_asset(component, None)
    managers = [
        r for r in defs.resources.values() if hasattr(r, "physical_coordinates")
    ]
    assert managers, "no IO manager exposing the mesh-publishing protocol"


def test_s3_config_advertises_a_readable_ticket(monkeypatch):
    component = _component(
        "s3://publog-lake",
        monkeypatch,
    )
    component.dest_config["credentials"] = {
        "aws_access_key_id": "key",
        "aws_secret_access_key": "secret",
        "endpoint_url": "http://minio:9000",
    }
    defs, _ = _table_asset(component, None)
    manager = next(
        r for r in defs.resources.values() if hasattr(r, "physical_coordinates")
    )
    ticket = manager.physical_coordinates(["publog", "p_cage"])
    assert ticket["source_type"] == "s3_parquet"
    # Trailing slash: a consumer's scan_parquet HEADs a slash-less S3 path
    # and 404s.
    assert ticket["physical_uri"] == "s3://publog-lake/publog/p_cage.parquet/"
    assert ticket["credentials"]["aws_endpoint_url"] == "http://minio:9000"
