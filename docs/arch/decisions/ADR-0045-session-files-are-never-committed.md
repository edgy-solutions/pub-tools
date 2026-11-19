# ADR-0045 Session files are never committed

## Status

`built`. Taken in `d2a3e37` (PR #1); the rule is at `AGENTS.md:29-32`.

## Decision

Lane packets and handoffs live in `sessions/`, which `.gitignore` excludes at line 17. Nobody commits, pushes or force-adds one. A sender places a packet in the recipient's `sessions/` folder.

Scope: the owner of this rule is the invincible-agent `AGENTS.md` (ruled 2026-11-12, commit `fd7df47d`). This repo applies it; it is not a separate decision.

## Why

The invincible-agent record gives the rule and applies it to every repo. It lists pub-tools among them (`AGENTS.md:129-138`, at `6b3e8efd`). The ignore line enforces the main part.

## Consequences

`.gitignore` stops an ordinary add. It does not stop `git add -f`, which the rule forbids by agreement.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| sessions/ is ignored | `.gitignore:17` | no test | `built` |
| The rule is written in this repo | `AGENTS.md:29-32` | no test | `accepted` |
| The owning rule lists pub-tools | invincible-agent `6b3e8efd`, `AGENTS.md:129-138` | no test | `accepted` |
