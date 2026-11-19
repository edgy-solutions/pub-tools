# ADR-0024 The asset key is platform instance, bucket, key prefix, then table

## Status

`built`. Taken in `74445b8`; the convention is built at `pub_tools/components/publog_pipeline/component.py:62-117`.

## Governs

CO-PT-PIPELINE, CO-PT-CATALOG, CO-PT-IMAGE, FL-PT-PUBLISH.

## Decision

A table's asset key is `<platform_instance>/<bucket>/<key_prefix>/<table>`. The same segments appear in the catalog dataset name and in the object path, so the three views of one table agree. For a local lake the key is the plain `<key_prefix>/<table>`.

The staging asset key follows the same convention under the raw prefix.

## Why

The `74445b8` message says the key matches the convention the other assets in the mesh already use. It says the catalog sensor and the IO manager then derive the same name from the same key. The message says the new shape was checked against the catalog's own path-spec parser, in four shapes. The record of that check is not in the repo, so that part is `built`.

## Consequences

`74445b8` is marked BREAKING. Existing history in the catalog was orphaned under the old key, and the commit says so.

`pub_tools/components/publog_pipeline/component.py:249-253` builds an error message that names `[path[0], 'source', slug]` as the staging key. That is wrong for s3 keys. A test asserts the local-lake form only (`test_publog_io_manager.py:162`).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The key prefix carries instance and bucket for s3 lakes | `pub_tools/components/publog_pipeline/component.py:62-79` | `test_publog_io_manager.py:211` test_s3_keys_carry_instance_and_bucket | `built` |
| A local lake uses the plain key | `pub_tools/components/publog_pipeline/component.py:62-79` | `test_publog_io_manager.py:235` test_local_lake_keeps_the_plain_key | `built` |
| The same key convention is used by the other mesh assets | commit `74445b8` message | no test | `built` |
| The key shape was checked against the catalog's path-spec parser | commit `74445b8` message; no run record in the repo | no test | `built` |
| The missing-CSV error text names the wrong staging key for s3 | `pub_tools/components/publog_pipeline/component.py:249-253` | `test_publog_io_manager.py:162` test_missing_staged_csv (local lake only) | `built` |
