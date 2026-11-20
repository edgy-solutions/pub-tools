# pub-tools architecture records

These records **describe pub-tools as it is**. They do not propose. Where a record names something that is not built, it gives that thing a status of `accepted` or `proposed` and says so. Nothing unbuilt is called built.

pub-tools turns the public PUB LOG release into Parquet tables in an object-store lake. It publishes them to the mesh as Dagster assets that the dag-tools domain broker advertises and the catalog sensor registers. These records cover what this repo provides. The mesh-wide records (the gateway, the broker, the catalog, the alignment with outside references) are dag-tools' records, in dag-tools `docs/arch/`.

**The record shape is provisional.** These records follow the shape the dag-tools architecture lane uses: a title line with a record code, findings first with one status each, then settings, gaps, what code does not establish, and a Sources table. When the OpenDDIL lane publishes the kit's record shapes, these records will be converted to them, keeping their ids.

**Structured fields.** Each component record carries a Connections table, one row per connection: the other component, the direction (calls, is called by, reads, writes), the protocol, the sign-in, what passes, the evidence and the status. The flow record's steps are a table: the actor, the action, the target, what passes, the decision or check, the deciding decisions, the evidence and the status. Each decision record names the component and flow ids it governs under `## Governs`. Views and diagrams are drawn from these tables, not from the prose. When the OpenDDIL lane sends the exact field names, the tables convert to them.

**Records are cited by id.** An id that starts `CO-PT-` or `FL-PT-` is a record here. Any other `CO-` or `FL-` id (CO-BROKER, CO-GATEWAY, CO-DAGSTER, CO-MINIO, CO-DATAHUB, CO-DATAHUB-LINEAGE, FL-NTK) is a dag-tools arch lane record, under dag-tools `docs/arch/`, not yet merged there. Alignment rows are cited with their record, as `AL-DMRA S7` or `AL-DCRA L2`. All of these are on the dag-tools lane's id list of 2026-11-19 (dag-tools `823bf3480fbbfc1f4e41a0766e228507aacf4236`); an id there is stable, and is renamed only by packet. A decision is cited by its own repository and number, as `invincible-agent ADR-0044`; a dag-tools roadmap row by its number.

**Outside systems** have no record. Each has an element id with the `XE-` prefix, defined here once and cited wherever a connection's other end or a step's actor or target is that system:

| Id | Outside system | Reached by |
|---|---|---|
| XE-PUBLOG | the public PUB LOG release: the CSV archives its publisher posts in its reading room for anyone to download | CO-PT-PIPELINE, over HTTPS, anonymously |

The id names the source, not pub-tools, so another member that reads the same source can cite the same id.

**Planes** come from the mesh's plane list, held in the dag-tools records README (dag-tools PR #111, `16da96502281c5496b1553e3ed560c2a10e77ed0`, not yet merged): `infrastructure`, `operational-data`, `data-management`, `analytical-data`. pub-tools defines none. Each component record names its one plane under "Place in the mesh", as `proposed`.

## Status vocabulary

The vocabulary is the same as dag-tools `docs/arch/README.md`. Every claim carries exactly one status.

| Status | Means | Evidence it needs |
|---|---|---|
| `measured` | A recorded run against a running system showed it. | The record of the run. It must be a result that can be re-read: a CI run id, a committed result file or a release record. Having a test that could measure it is not enough. |
| `built` | Code at the pinned commit does it. | A file and line. A test that pins it, when one exists; when none does, the row says so. |
| `accepted` | A decision record or ruling accepts it, and no code does it yet. | The decision record or ruling. |
| `proposed` | It is written down as intent, but not accepted or built. | Where it is written. |

A run reported only in a commit message, a docstring or a code comment is not `measured`; the claim is `built`, with a note citing where the run is reported. This is the architect's ruling of 2026-11-19. A later ruling the same day adds that `measured` is relative to the claim: an in-process CI run may measure a claim about the code's behaviour, citing the run and saying "in CI, in process", but not a claim about a deployed service or a real backend, which stays `built`. pub-tools' CI runs no tests, so no claim here is measured that way. The status vocabulary is the architect's; a lane proposes changes to it, and does not make them.

A gap carries exactly one status: that of the condition it states, at the pin. That is `built` when code at the pin shows the condition, cited by path and lines; a condition that is an absence cites where the thing would be. What would close a gap, if not built, is said in plain words and is not a second status. A gap that exposes something now, in code at the pin, is called a **present exposure**. A fact that code, charts and tests do not establish is written **not established**, and is listed at the end of its record.

## Pins

All evidence is read at these commits. A line number is valid only at its pin.

| Source | Pin | Why this pin |
|---|---|---|
| pub-tools | `90fba4acf08ac7c6758ff349636648aac19f71a8` | `master` when these records were written |
| dag-tools | `a99779fa7075d45a21879c8a057e4cb36a58caf8` | the commit pub-tools' `uv.lock` resolves; it is what pub-tools runs, not dag-tools `master` |
| invincible-agent | `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb` | `master`; its chart deploys pub-tools |

The deployed image is not built from the pub-tools pin. See [docs/cluster-build.md](../cluster-build.md): the running build is the `master` build of `1aa2cbe`. Every commit after it up to this pin changed documentation only, except `e5612fa`, whose two conversion changes the running build does not have.

## Records

| Id | Record | Kind | State |
|---|---|---|---|
| CO-PT-PIPELINE | [components/publog-pipeline.md](components/publog-pipeline.md) | component: the PUB LOG pipeline component | draft |
| CO-PT-LAKE | [components/lake-library.md](components/lake-library.md) | component: staging and conversion library | draft |
| CO-PT-CATALOG | [components/catalog-wiring.md](components/catalog-wiring.md) | component: the catalog sensor declaration | draft |
| CO-PT-IMAGE | [components/pub-tools-image.md](components/pub-tools-image.md) | component: the image, run as a code location and as a broker | draft |
| FL-PT-PUBLISH | [flows/publish-data-product.md](flows/publish-data-product.md) | flow: one table from the source to its catalog entry and mesh route | draft |
| ADR-0001..0045 | [decisions/README.md](decisions/README.md) | decisions: the census, one record per decision | draft |
