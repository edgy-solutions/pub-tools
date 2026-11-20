# pub-tools image (CO-PT-IMAGE)

pub-tools ships as one container image, built by the repo's workflow. The deployment chart runs that image twice. One pod is a Dagster code location that runs [CO-PT-PIPELINE](publog-pipeline.md) and [CO-PT-CATALOG](catalog-wiring.md). The other is a dag-tools domain broker (dag-tools CO-BROKER) that advertises the pipeline's tables to the mesh and answers requests to read them. This record covers what the image contains, how the chart runs it, and what the broker built into it does at the pinned dag-tools.

**This record describes; it proposes nothing.** Statuses are those of [the README](../README.md), and line numbers are at the pins listed there. The record shape is provisional.

## Findings

1. **The image installs exactly what the lock file resolves.** The workflow exports `uv.lock` to a requirements file and installs it into a Python 3.12 image. dag-tools therefore comes in at the lock's commit, not at dag-tools `master`. The `uv` binary is copied from its `latest` tag, which is not pinned. `built`
2. **The DuckDB storage extension is baked into the image.** The build installs it into a fixed directory, for each target architecture, and checks that it loads with automatic installation off. A build that cannot load it fails, so the image never needs the network to read or write the store ([CO-PT-LAKE](lake-library.md), finding 10). `built`
3. **The chart runs the image as a code location and as a broker.** The code location runs Dagster's gRPC server on `pub_tools.definitions`. The broker runs the dag-tools broker application with the same module named as its definitions. The image's own default command, a Dagster web server, is not what the chart runs. `built`
4. **The broker advertises the tables by reading the IO manager the pipeline registers.** At start-up it loads the definitions, reads `physical_coordinates` off the DuckDB IO manager for each asset, and registers with the mesh gateway (dag-tools CO-GATEWAY). `built`
5. **At the pinned dag-tools, a read request is answered with the producer's own store credentials.** The IO manager's coordinates include the key pair pub-tools writes with. The broker stores them as the asset's routing ticket and returns that ticket unchanged to whoever resolves the asset. A reader therefore holds pub-tools' write credentials. Every dag-tools release from v0.3.0 replaces this with coordinates plus a credential minted per read (invincible-agent ADR-0044, carried out in dag-tools commit `c198373`, 20 commits after the pin); pub-tools has not moved. `built` (at the pin); the replacement is `built` in dag-tools but not in what pub-tools runs
6. **At the pinned dag-tools, registration and resolution carry no credential.** The broker registers with the gateway without authenticating, and its resolve endpoint does not check the caller. The pin is a dag-tools commit of 2026-08-14; dag-tools added a service-token guard on both endpoints in v0.11.0. The gateway is deployed separately, at 0.14.0, and has that guard on registration, but the chart leaves its posture unset, which means observe: an unauthenticated registration is logged and served. Enforcing it would refuse pub-tools' own registrations, because the broker at the pin sends no service token. `built`
7. **The chart gives the code location and the broker the store's key pair as plain values.** They are not read from a secret. The code location also gets the endpoint, region, lake root, catalog address and DuckDB memory cap. The broker gets the key pair, endpoint and lake root. `built`
8. **The chart caps the code location at one run at a time and 3 Gi of memory.** `built`
9. **Images are published only from `master` and from version tags.** A pull request builds the image for both architectures and does not push it. The workflow runs no tests, so a green pull request means the image builds. `built`

## Connections

One row per connection, read from this component's side. Direction is one of calls, is called by, reads or writes. An id that starts `CO-PT-` is a record here; any other `CO-` id is a dag-tools arch lane record (dag-tools `docs/arch/components/`). Exposures are written out under Gaps; this table only marks them.

| # | Other component | Direction | Protocol | Sign-in | What passes | Evidence | Status |
|---|---|---|---|---|---|---|---|
| 1 | CO-DAGSTER | is called by | Dagster's code-location gRPC API, on every interface | none: plain gRPC, with no TLS option in the command the chart runs | the definitions of [CO-PT-PIPELINE](publog-pipeline.md) and [CO-PT-CATALOG](catalog-wiring.md); run requests | invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:93-104` | `built` |
| 2 | [CO-PT-PIPELINE](publog-pipeline.md) | reads | an in-process import of `pub_tools.definitions:defs`, in the broker pod | none | the asset keys and the IO manager's `physical_coordinates`, holding the store key pair | invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:234`, `:239-240`; dag-tools `dag_tools/domain_broker/main.py:41-80`, `:75` | `built` |
| 3 | CO-GATEWAY | calls | HTTP POST to `/api/v1/internal/register`, at start and every 120 seconds | none at the pin; the gateway at 0.14.0 observes and serves (present exposure, Gaps) | the broker's own address and its list of names | dag-tools `dag_tools/domain_broker/main.py:327-343`, `:346-375`; invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:241-244`; dag-tools v0.14.0 `dag_tools/central_gateway/internal_door.py:21` | `built` |
| 4 | CO-GATEWAY | is called by | HTTP POST to `/api/v1/internal/resolve` | none: no caller check at the pin (present exposure, Gaps) | in, one name; out, the stored ticket, holding the store key pair (present exposure, Gaps) | dag-tools `dag_tools/domain_broker/main.py:451-468` | `built` |

## Settings and defaults

| Setting | Where | Value at the pin |
|---|---|---|
| Base image | workflow | Python 3.12, slim |
| Architectures | workflow | amd64 and arm64 |
| `DUCKDB_EXTENSION_DIRECTORY` | image | a fixed directory under `/opt` |
| Default command | image | Dagster web server on `pub_tools.definitions`; the chart overrides it |
| Code location command | chart | Dagster gRPC server on `pub_tools.definitions` |
| Broker command | chart | the dag-tools broker application, definitions `pub_tools.definitions:defs` |
| Memory limit, code location | chart | 3 Gi |
| Concurrent runs, code location | chart | 1 |

## Gaps

- **Present exposure: readers receive write credentials** (finding 5). Any caller that resolves a pub-tools table gets the key pair pub-tools writes with. Moving the dag-tools pin to any release from v0.3.0 closes this in code; PR #8 makes that move, to v0.17.0, and is the remedy. PR #8 stays a draft until both of these are true (the architect's ruling of 2026-11-19, narrowing that of 2026-11-16): Lane 1 has shown on the shared test cluster that role assumption works for a regular store user, which has not been shown (Not established); and the four chart pieces below (items 2 to 5) are in a chart release, which they are not: the chart at the pin sets none of them. dag-tools tracks the exposure as its roadmap row 3 (dag-tools issue #72); the broker's side is dag-tools CO-BROKER, finding 3. `built`: dag-tools `dag_tools/io_managers/duckdb.py:465-476`; dag-tools `dag_tools/domain_broker/main.py:451-468`; `pub_tools_tests/test_publog_io_manager.py:185`.
- **Present exposure: registration and resolution are unauthenticated** (finding 6). Any caller that can reach the broker can resolve a table, and so receive the key pair above. Any caller that can reach the deployed gateway can register a route for any name, replacing the one there, because its guard only observes. The broker at the pin has no authentication on resolve; on registration the gateway has it, `built` and not enforced.
  **Enforcing both depends on:**
  1. pub-tools PR #8, which moves the dag-tools pin to v0.17.0, so the broker presents a service token when it registers and guards its resolve endpoint. `proposed`, and held as above: every release after the pin also brings the per-read credentials of finding 5, which need role assumption.
  2. The chart giving pub-tools' broker the three `MESH_SERVICE_*` settings, the secret from a secret. `proposed`: a hold condition for PR #8 in the architect's ruling of 2026-11-19.
  3. An identity-provider client for pub-tools' broker whose entitlement claim names it as a service. Whether one exists is not shown by code; it is listed under Not established.
  4. The chart listing that service in the gateway's `GATEWAY_REGISTER_CALLERS`, and the gateway in the broker's `BROKER_RESOLVE_CALLERS`. An empty list refuses every caller under enforce. `proposed`: a hold condition for PR #8 in the architect's ruling of 2026-11-19.
  5. `INTERNAL_DOOR_POSTURE` set to enforce. `proposed`: the chart's changelog plans it after its gauges read clean.
- **The key pair is a plain chart value, not a secret** (finding 7). `built`
- **The `uv` tool is taken from an unpinned tag** (finding 1). `built`
- **CI runs no tests** (finding 9). `built`: `.github/workflows/build-container.yml:1-142`, the only workflow, has no test step.

## Not established from code

- Which commit the running image was built from. [docs/cluster-build.md](../../cluster-build.md) records the build the cluster ran when it was written; code cannot show what runs now.
- Whether the store allows role assumption for the consumer identity the next chart release creates, which the per-read credential path needs in every dag-tools release after the pin.
- Whether the broker's gateway registration succeeds in a given deployment.
- Whether pub-tools' broker has an identity-provider client whose entitlement claim names it as a service. The identity provider holds it, not code.

## Place in the mesh

The plane is taken from the mesh's plane list, which is held in the dag-tools records README under "Planes" (dag-tools PR #111, `16da96502281c5496b1553e3ed560c2a10e77ed0`, not yet merged). This member defines no plane of its own. Each component sits in exactly one plane. The placement is `proposed` until the list is merged and the placement is accepted.

| Aspect | What this record shows | Alignment row | Status |
|---|---|---|---|
| Plane | `operational-data`: the producer's own deployable unit. It serves [CO-PT-PIPELINE](publog-pipeline.md)'s definitions to CO-DAGSTER and runs this member's CO-BROKER (Connections 1 to 4) | AL-DCRA L2 | `proposed` |

## Sources

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`. dag-tools paths are at `a99779fa7075d45a21879c8a057e4cb36a58caf8`. invincible-agent paths are at `6b3e8efdd4a4d2a172fb394ca5338c23413e57bb`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Lock exported and installed; Python 3.12; `uv` from `latest` | `.github/workflows/build-container.yml:27-29`, `:49`, `:56`, `:66-71` | no test | `built` |
| Extension baked and checked | `.github/workflows/build-container.yml:73-87`; `scripts/bake_duckdb_extensions.py` | `pub_tools_tests/test_lake.py:58`, `:72` | `built` |
| Run as code location and as broker; default command overridden | invincible-agent `helm/invincible-agent/templates/user-deployments.yaml:93-104`, `:227-250`; `.github/workflows/build-container.yml:97` | no test | `built` |
| Broker reads `physical_coordinates`; registers with the gateway | dag-tools `dag_tools/domain_broker/main.py:75`, `:327-343` | `pub_tools_tests/test_publog_io_manager.py:173` | `built` |
| Ticket carries the producer's key pair, returned unchanged | dag-tools `dag_tools/io_managers/duckdb.py:428`, `:465-476`; dag-tools `dag_tools/domain_broker/main.py:451-468` | `pub_tools_tests/test_publog_io_manager.py:185` | `built` |
| Releases from v0.3.0 replace it | dag-tools `c198373`, carrying out invincible-agent ADR-0044; first tag v0.3.0 | at v0.15.0, `test_publog_io_manager.py:185` fails: the ticket has no `credentials` | `built` in dag-tools |
| No credential on register or resolve; the gateway overwrites a name's route on any register | dag-tools `dag_tools/domain_broker/main.py:327-343`, `:451-468`; dag-tools `dag_tools/central_gateway/main.py:96-110`; dag-tools `4096a11` (the guard, first in v0.11.0); dag-tools v0.14.0 `dag_tools/central_gateway/internal_door.py:21`, `main.py:166`; invincible-agent `helm/invincible-agent/values.yaml:1032-1037`, `helm/invincible-agent/CHANGELOG.md:1586` | no test | `built` |
| What enforcing depends on: the broker's token on register, its guarded resolve, the settings it reads, empty caller lists refusing | dag-tools v0.17.0 `dag_tools/domain_broker/main.py:977`, `:1170-1171`; dag-tools v0.17.0 `dag_tools/central_gateway/internal_door.py:79-80`; dag-tools v0.14.0 `dag_tools/central_gateway/internal_door.py:68-70`, `:143-179`; invincible-agent `helm/invincible-agent/CHANGELOG.md:574-576`; the chart sets no `MESH_SERVICE_*` and no caller list (searched under `helm/`) | PR #8; the chart pieces it also needs are Gaps items 2 to 5 | `proposed` |
| Key pair as plain chart values; other settings | invincible-agent `helm/invincible-agent/values.yaml:1197-1265` | invincible-agent `tests/test_pub_tools_chart_env_is_read_by_its_code.py` (`5ae27397`) | `built` |
| One run at a time; 3 Gi | invincible-agent `helm/invincible-agent/values.yaml:424`, `:1197-1265` | no test | `built` |
| Push only from `master` and tags; no tests in CI | `.github/workflows/build-container.yml:3-11`, `:124-131`, `:137`, `:140` | no test | `built` |
