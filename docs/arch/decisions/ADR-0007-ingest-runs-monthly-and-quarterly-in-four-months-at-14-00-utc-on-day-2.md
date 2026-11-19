# ADR-0007 Ingest runs monthly, and quarterly in four months, at 14:00 UTC on day 2

## Status

`built`. Taken in `8fed6c8`; the schedules are built at `pub_tools/components/publog_pipeline/component.py:531-565`.

## Governs

CO-PT-PIPELINE, FL-PT-PUBLISH.

## Decision

The monthly schedule fires at 14:00 UTC on day 2 of each month. The quarterly schedule fires at the same time in January, April, July and October. Cron strings and the time zone can each be overridden by an environment variable: `PUBLOG_MONTHLY_CRON`, `PUBLOG_QUARTERLY_CRON` and `PUBLOG_SCHEDULE_TZ`.

## Why

The docstring says the source publishes on the first business day, and day 2 gives a buffer. The commit message says the schedules are wired with an overridable cron and time zone.

## Consequences

Nothing checks that the source has published by day 2 in every month. That is not established. If it has not, the Last-Modified check finds nothing new and the run does little.

## Evidence

pub-tools paths are at `90fba4acf08ac7c6758ff349636648aac19f71a8`.

| Claim | Source | Test | Status |
|---|---|---|---|
| Default crons and UTC time zone | `pub_tools/components/publog_pipeline/component.py:402-409` | no test | `built` |
| The deployed component reads the three settings from the environment | `pub_tools/components/publog_instance/component.yaml:8-10` | no test | `built` |
| One schedule per job, quarterly only when quarterly URLs exist | `pub_tools/components/publog_pipeline/component.py:531-565` | no test | `built` |
