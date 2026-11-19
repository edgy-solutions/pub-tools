# ADR-0043 Credentials are never written to a file

## Status

`accepted`. Written in `AGENTS.md:17-20`; two commits breached it.

## Governs

CO-PT-PIPELINE.

## Decision

Credentials come from the environment and are not written to any file, even for a mock run. The deployed component file uses a well-known development default pair as a fallback for local runs.

## Why

The rule gives the safety reason. The fallback pair is an example default, not a live credential.

## Consequences

Commits `28ff07b` and `f02dc26` breached the rule, as `--stat` shows. The pair also appears in the example compose file. The locations are `pub_tools/components/publog_instance/component.yaml:24-25` and `example/docker-compose.yml:15-16`. Whether any file carries a real credential was not checked beyond those two.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The rule is written | `AGENTS.md:17-20` | no test | `accepted` |
| A development default pair is in two files | `pub_tools/components/publog_instance/component.yaml:22-26`; `example/docker-compose.yml:15-16` | no test | `built` |
| Two commits breached the rule | commits `28ff07b`, `f02dc26` (`--stat`) | no test | `built` |
