# pub-tools architecture records

These records **describe pub-tools as it is**. They do not propose. Where a record names something that is not built, it gives that thing a status of `accepted` or `proposed` and says so. Nothing unbuilt is called built.

pub-tools turns the public PUB LOG release into Parquet tables in an object-store lake. It publishes them to the mesh as Dagster assets that the dag-tools domain broker advertises and the catalog sensor registers. These records cover what this repo provides. The mesh-wide records (the gateway, the broker, the catalog, the alignment with outside references) are dag-tools' records, in dag-tools `docs/arch/`.

**The record shape is provisional.** These records follow the shape the dag-tools architecture lane uses: a title line with a record code, findings first with one status each, then settings, gaps, what code does not establish, and a Sources table. When the OpenDDIL lane publishes the kit's record shapes, these records will be converted to them.

## Status vocabulary

The vocabulary is the same as dag-tools `docs/arch/README.md`. Every claim carries exactly one status.

| Status | Means | Evidence it needs |
|---|---|---|
| `measured` | A recorded run against a running system showed it. | The record of the run. It must be a result that can be re-read: a CI run id, a committed result file or a release record. Having a test that could measure it is not enough. |
| `built` | Code at the pinned commit does it. | A file and line. A test that pins it, when one exists; when none does, the row says so. |
| `accepted` | A decision record or ruling accepts it, and no code does it yet. | The decision record or ruling. |
| `proposed` | It is written down as intent, but not accepted or built. | Where it is written. |

A run reported only in a commit message, a docstring or a code comment is not `measured`; the claim is `built`, with a note citing where the run is reported. This is the architect's ruling of 2026-11-19. The status vocabulary is the architect's; a lane proposes changes to it, and does not make them.

A gap with nothing built is written `not built`. A gap that exposes something now, in code at the pin, is called a **present exposure**. A fact that code, charts and tests do not establish is written **not established**, and is listed at the end of its record.

## Pins

All evidence is read at these commits. A line number is valid only at its pin.

| Source | Pin | Why this pin |
|---|---|---|
| pub-tools | `90fba4acf08ac7c6758ff349636648aac19f71a8` | `master` when these records were written |
| dag-tools | `a99779fa7075d45a21879c8a057e4cb36a58caf8` | the commit pub-tools' `uv.lock` resolves; it is what pub-tools runs, not dag-tools `master` |
| invincible-agent | `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` | `master`; its chart deploys pub-tools |

The deployed image is not built from the pub-tools pin. See [docs/cluster-build.md](../cluster-build.md): the running build is the `master` build of `1aa2cbe`. Every commit after it up to this pin changed documentation only, except `e5612fa`, whose two conversion changes the running build does not have.

## Records

| Record | Kind | State |
|---|---|---|
| [components/publog-pipeline.md](components/publog-pipeline.md) | component: the PUB LOG pipeline component | draft |
| [components/lake-library.md](components/lake-library.md) | component: staging and conversion library | draft |
| [components/catalog-wiring.md](components/catalog-wiring.md) | component: the catalog sensor declaration | draft |
| [components/pub-tools-image.md](components/pub-tools-image.md) | component: the image, run as a code location and as a broker | draft |
| [flows/publish-data-product.md](flows/publish-data-product.md) | flow: one table from the source to its catalog entry and mesh route | draft |
