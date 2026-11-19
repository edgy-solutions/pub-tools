# ADR-0029 One image runs both the webserver and the code server, with two startups

## Status

`built`. Taken in `472d75c`; the deployment side is in invincible-agent at `6b3e8efd`.

## Decision

The deployment chart runs one pub-tools image in two roles. The webserver is the image's default command. The code server is started by the chart with a different command. Both roles load the same definitions.

## Why

The `472d75c` message gives the single image as the reason. A sidecar split (`98765a9`) was tried and reverted (`47e39b8`). The invincible-agent chart comment at `helm/invincible-agent/values.yaml:1174-1181` describes the two startups. This record names the sidecar commits only. Their detailed reasons were not checked.

## Consequences

Both roles must stay in step with one image tag. The pin lives in the deployment chart, at `helm/invincible-agent/values.yaml:53-56`.

The default command in this repo's image is the webserver (`.github/workflows/build-container.yml:97`).

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The image's default command is the webserver | `.github/workflows/build-container.yml:97` | no test | `built` |
| The chart documents the two startups | invincible-agent `6b3e8efd`, `helm/invincible-agent/values.yaml:1174-1181` | no test | `accepted` |
| A sidecar was tried and reverted | commits `98765a9`, `47e39b8` | no test | `built` |
