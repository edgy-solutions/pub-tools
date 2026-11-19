# Lake library (CO-PT-LAKE)

`pub_tools/lake.py` is the library the [pipeline component](publog-pipeline.md) calls to stage a source archive in the object store and to read a staged CSV as a DuckDB query. It owns the staged layout (`_raw/<slug>/<date>/`), the freshness checks that make both stages incremental, the repair of a stray legacy-encoded byte, and the header and ragged-row handling that turns a CSV into a table. It does not write tables: the dag-tools DuckDB IO manager does, and this library computes where that writer puts them.

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **Staging streams each CSV from the archive straight to the object store.** The archive is downloaded to a temporary directory. Each declared member is then copied out to the lake without being expanded on local disk, so the pod's disk holds only the compressed archive. Members not in the manifest are not staged. A source that is a bare CSV is copied as is. `built`
2. **The staged layout puts the slug before the date: `_raw/<slug>/<date>/<member>`.** This lets the catalog's path-spec ingestion treat one slug as one dataset with dated partitions. The code's comment says this was checked against the catalog's own path-spec parser. The [pipeline component's](publog-pipeline.md) docstring still gives the old order. `built`
3. **A marker beside the staged files records what was staged.** The marker holds the source's `Last-Modified` and is written after the CSVs, so an interrupted staging leaves no marker and is redone. A corrupt marker reads as absent. `built`
4. **Freshness comes from object metadata, never from Dagster's event log.** The code records why: inside a run, Dagster 1.13.7 returned a materialization event that had been wiped, so a check built on it skipped work that was never done. Object modification times cannot drift that way, and deleting a file from the store makes the next run rebuild it. The 1.13.7 behaviour is the code comment's report; no run record is in the repo. `built` (the check); the Dagster behaviour is reported, not `measured` here
5. **The table location comes from the IO manager, not from this library.** `table_parquet_url` asks the dag-tools DuckDB IO manager's `asset_uri` for the path. A second copy of the layout here could drift from the writer, and a freshness check against the wrong path either rebuilds every run or never rebuilds. `built`
6. **A stray legacy-encoded byte is repaired while staging.** Staging reads each member as UTF-8. A line that is not valid UTF-8 is re-read as the Windows Western European code page, and the change is logged with an excerpt. Valid UTF-8 passes through byte for byte. A test covers a byte split across two read chunks. `built`
7. **Every column is read as text.** The CSV reader is called with all columns as text, so codes with leading zeros keep them. Short rows are padded with empty strings. Headers are lower-cased, with spaces and punctuation replaced by underscores, and colliding names are made unique. `built`
8. **One table may carry ragged rows, and only that one is allowed to.** For `v_characteristics`, fields past the declared header are folded back into the last column, up to 16 extra fields. A ragged row in any other table, or one past that bound, still fails the step. The code reports 12 known ragged rows with at most 3 extra fields. `built`
9. **The conversion is a lazy query that the IO manager runs.** `csv_relation` returns a DuckDB relation over the staged CSV. The IO manager executes it when it writes the output, so the rows stream from CSV to Parquet without passing through Python. An older function, `csv_to_parquet`, writes Parquet itself. Only tests call it, and the module docstring still names it as the conversion step. `built`
10. **The DuckDB connection fails fast if its storage extension is missing.** The connection reads the object-store extension from the directory the image bakes it into ([image, finding 2](pub-tools-image.md)) and does not fetch it. If it is missing, the connection fails at once, rather than reaching for the network at run time. It applies `DUCKDB_MEMORY_LIMIT` when that is set. `built`

## Settings and defaults

| Setting | Default | What it does |
|---|---|---|
| `DUCKDB_EXTENSION_DIRECTORY` | DuckDB's own default (a home-relative directory) | where the storage extension is loaded from; the image sets it |
| `DUCKDB_MEMORY_LIMIT` | unset: DuckDB's own default | per-connection memory cap |
| Object-store credentials | from the destination settings the component passes | used for staging, freshness checks and the connection |

## Gaps

- **Dated history is kept only for staged CSVs, not for tables** ([pipeline, finding 8](publog-pipeline.md)). `built`
- **Nothing removes staged copies under an older layout.** A prefix staged before the slug-before-date order remains unless someone deletes it. `not built`
- **The module docstring describes `csv_to_parquet` as the conversion step** (finding 9). `built` (the code); the docstring is stale.

## Not established from code

- How many staged copies under the old layout remain in a given deployment's store.

## Sources

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Staging streams declared members; bare CSV copied | `pub_tools/lake.py:259-344`; `pub_tools/assets.py:214-224` | `pub_tools_tests/test_lake.py:200`, `:239` | `built` |
| Slug before date; checked against the path-spec parser | `pub_tools/lake.py:99-131` | `pub_tools_tests/test_lake.py:157` | `built` |
| Marker written last; corrupt marker reads as absent | `pub_tools/lake.py:242-258`; `pub_tools/components/publog_pipeline/component.py:124-222` | `pub_tools_tests/test_lake.py:259`, `:266` | `built` |
| Freshness from object metadata; the reason | `pub_tools/lake.py:158-241` | `pub_tools_tests/test_lake.py:276` | `built` |
| Table location from the IO manager's `asset_uri` | `pub_tools/lake.py:132-156` | `pub_tools_tests/test_lake.py:173` | `built` |
| Legacy-encoding repair; valid UTF-8 untouched | `pub_tools/lake.py:345-453` | `pub_tools_tests/test_lake.py:381`, `:408`, `:427` | `built` |
| All columns as text; padding; header cleaning | `pub_tools/lake.py:31`, `:462-522` | `pub_tools_tests/test_lake.py:193`, `:283`, `:310`, `:322`, `:335` | `built` |
| Ragged rows folded for one table only | `pub_tools/lake.py:485-491`, `:523-549` | `pub_tools_tests/test_lake.py:471`, `:522`, `:535` | `built` |
| Lazy relation run by the IO manager; `csv_to_parquet` called only by tests | `pub_tools/lake.py:609-628`, `:629-668`, `:1-12`; `pub_tools/components/publog_pipeline/component.py:297` | `pub_tools_tests/test_lake.py:535` | `built` |
| Connection fails fast without the baked extension; memory limit | `pub_tools/lake.py:50-98` | `pub_tools_tests/test_lake.py:58`, `:72`, `:125`, `:138` | `built` |
