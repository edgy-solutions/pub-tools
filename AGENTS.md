# Guide for AI Agents Operating on `pub-tools`

Welcome, AI Agent! When interacting with this project, you must adhere strictly to this operational guide to maintain safety, predictability, and efficiency.

## Pre-Flight Checklist
Before modifying *any* code or executing external network commands, you **MUST**:
1. Read `README.md` to understand the current scaffolding and execution paths.
2. Read `llms.txt` to align your semantic context with the core business logic (DLA PUB LOG ingestion).
3. Read `.cursorrules` to ensure your proposed edits comply with the strict tech stack (`uv`, Dagster `@asset`/components, `dlt`, Parquet destination).

## Operational Safety Boundaries

### 1. Destructive Actions
- **DO NOT** delete, drop, or overwrite S3 Data Lake paths, local database files (`sqlite`, `duckdb`), or existing raw data archives without explicit, verified permission from the user in a `notify_user` response.
- **DO NOT** overwrite core `dlt` resources or Dagster definitions unless the user specifically requests a total refactor. 

### 2. Environment Variables & Secrets
- **AWS Credentials**: This pipeline relies on `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for writing to S3.
- **NEVER** hardcode or write these credentials to *any* file, even if doing a mock execution. Assume they exist in the execution environment.
- If testing locally, default to `/tmp` paths for destinations to avoid accidentally touching production S3 buckets if keys are present.

### 3. Maintaining the Documentation Trifecta
As an agent operating on this project, part of your job is self-maintenance of the AI guardrails.
If you install a new core technology (e.g., adding `duckdb` or `dbt`), you MUST concurrently update:
1.  `llms.txt` (if the domain/intent changes).
2.  `.cursorrules` (if the enforced stack rules change).
3.  `README.md` (to ensure human developers can run it).
