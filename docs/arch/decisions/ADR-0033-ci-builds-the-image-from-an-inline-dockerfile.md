# ADR-0033 CI builds the image from an inline Dockerfile

## Status

`built`. Taken in the first CI commit and extended in `7144e56`.

## Decision

The workflow writes a Dockerfile inline and builds with it, so the image does not depend on a Dockerfile in the repo. The build exports the locked requirements with `uv export` and installs them with `uv pip`. It runs QEMU and builds for two architectures. It pushes only when the event is not a pull request.

## Why

The `7144e56` message adds the second architecture. The workflow comments say it is needed because deployments run on arm64. No reason for the inline Dockerfile is recorded. The build is named the golden Dockerfile; the repo has no other.

## Consequences

The Dockerfile text is part of the workflow. Changing the image means editing the workflow. No test runs the build.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| The Dockerfile is inline | `.github/workflows/build-container.yml:27-99` | no test | `built` |
| The build uses uv export and uv pip | `.github/workflows/build-container.yml:29,71` | no test | `built` |
| QEMU and two platforms are configured | `.github/workflows/build-container.yml:100-105,133-142` | no test | `built` |
| The push is off on pull requests | `.github/workflows/build-container.yml:133-142` | no test | `built` |
