"""Install DuckDB extensions into the image at build time.

PUB LOG ingest reads and writes `s3://` through DuckDB's httpfs extension.
DuckDB would otherwise fetch it from duckdb.org the first time an asset runs,
which fails (or hangs) in a cluster that restricts egress -- and it would fail
per-pod, at materialization time, rather than at build time.

Run during the container build, once per architecture:

    python scripts/bake_duckdb_extensions.py

Extensions land in $DUCKDB_EXTENSION_DIRECTORY rather than DuckDB's default
`~/.duckdb`, because $HOME differs between the build (root) and the runtime
(whatever uid the cluster assigns), so a home-relative install would not be
found again. `pub_tools.lake.duckdb_connection` reads the same variable.

The install is verified in a fresh connection with auto-install disabled, so a
build only succeeds if the extension genuinely loads offline.
"""
import os
import sys

EXTENSIONS = ["httpfs"]
DEFAULT_DIRECTORY = "/opt/duckdb/extensions"


def main() -> int:
    import duckdb

    directory = os.environ.get("DUCKDB_EXTENSION_DIRECTORY") or DEFAULT_DIRECTORY
    os.makedirs(directory, exist_ok=True)
    print(f"duckdb {duckdb.__version__} -> {directory}", flush=True)

    con = duckdb.connect()
    con.execute("SET extension_directory=?", [directory])
    for name in EXTENSIONS:
        print(f"installing {name}...", flush=True)
        con.execute(f"INSTALL {name}")
    con.close()

    # Verify offline: a fresh connection, auto-install and auto-load disabled,
    # so the only way LOAD can succeed is from the baked directory. Without
    # this the build would happily produce an image that still needs the
    # network on first use.
    verify = duckdb.connect()
    verify.execute("SET extension_directory=?", [directory])
    verify.execute("SET autoinstall_known_extensions=false")
    verify.execute("SET autoload_known_extensions=false")
    for name in EXTENSIONS:
        try:
            verify.execute(f"LOAD {name}")
        except Exception as e:
            print(
                f"FAILED: {name} did not load from {directory} with "
                f"auto-install disabled: {e}",
                file=sys.stderr,
            )
            return 1
        print(f"verified {name} loads offline", flush=True)
    verify.close()

    for root, _dirs, files in os.walk(directory):
        for f in files:
            print("  " + os.path.relpath(os.path.join(root, f), directory), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
