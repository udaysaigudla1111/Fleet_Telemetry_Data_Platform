# Fleet Telemetry Data Platform

## Goal
Portfolio project to learn data engineering on AWS by building it from scratch.
Owner is a full stack dev (2.5 yrs: React, TypeScript, Python, Node) learning
SQL, AWS CDK and data engineering. Explain what you build and why, in short
notes, so I learn as we go. Don't dump everything at once; one phase at a time.

## Stack
- IaC: AWS CDK (Python). Region: ap-southeast-2
- Storage: S3 zones (raw / curated / scripts), Parquet, partitioned by date
- Processing: Glue (PySpark), Athena, dbt (dbt-athena)
- Streaming (later): Kinesis Data Streams -> Lambda -> S3
- Orchestration (later): Airflow in local Docker
- API: FastAPI on Lambda (Mangum) via API Gateway, reading Athena
- Frontend: React + TypeScript dashboard
- CI/CD: GitHub Actions

## Dataset
Kaggle "APS Failure at Scania Trucks": ~60k train / 16k test rows, 170
anonymized sensors, label `class` (neg/pos), missing values as the string "na",
~1-2% positive. No timestamps or vehicle IDs, so a script adds a synthetic
`vehicle_id` (~2000 trucks) and `event_date` (last 12 months). Mark these as
synthetic in code and README.

## Phases
1. CDK foundation: S3 buckets, IAM roles, budget alert
2. Batch: raw CSV -> Glue PySpark (na->null, cast numeric, is_failure flag)
   -> curated Parquet -> Glue catalog -> Athena queries
3. dbt: dim_vehicle, dim_date, fact_sensor_readings, fact_failures,
   mart_fleet_health + tests
4. Streaming simulator -> Kinesis -> Lambda -> S3
5. Airflow orchestration (local Docker)
6. FastAPI on Lambda over Athena
7. React dashboard (fleet health, risk ranking, sensor drill-down)
8. CI/CD, monitoring, README with architecture diagram

## Rules
- Keep AWS cost near zero: pay-per-use services only; no always-on
  resources (Redshift, MWAA, NAT gateway, RDS).
- Run `cdk diff` and show me the summary before any `cdk deploy`.
- Never hardcode credentials; never commit secrets. Use .gitignore.
- Never edit files in the raw zone.
- Commit after each working step with a clear message.
- Each phase ends with a short "what I learned / how to verify" note
  in docs/.

## Current status
Phase 1 DEPLOYED (2026-11-02): stack FleetTelemetryFoundation in account
703230062750, ap-southeast-2 (Free Plan, region-locked). CDKToolkit bootstrapped.
Auth: `aws login --profile fleet`, then `aws configure export-credentials
--profile fleet --format powershell | Invoke-Expression` (CDK can't read
login_session profiles). Owner runs commands themselves on Windows/PowerShell;
give step-by-step instructions.

Phase 2 IN PROGRESS (as of 2026-11-17):
- DONE: Kaggle data in data/kaggle/archive/; scripts/prepare_dataset.py adds
  synthetic reading_id/vehicle_id/event_date -> data/prepared/; uploaded to
  s3://fleet-telemetry-raw-703230062750-ap-southeast-2/scania_aps/split={train,test}/
- DONE (code only, NOT deployed): glue/jobs/curate_scania_aps.py (PySpark ETL);
  infra/stacks/batch_stack.py (FleetTelemetryBatch: script upload, Glue DB
  fleet_telemetry, Glue job fleet-telemetry-curate-scania-aps, Athena workgroup
  fleet-telemetry); wired in app.py; 11 pytest tests pass; synth OK.
- 2026-11-19: `cdk diff` REVIEWED + APPROVED (Foundation: 5 export Outputs +
  a BucketDeployment ownership tag on the scripts bucket, no replacements;
  Batch: all new, IAM only for the upload Lambda; nothing touches raw).
- 2026-11-20: `cdk deploy --all` DONE. Verified: Batch CREATE_COMPLETE, Glue job
  + DB + workgroup exist, script at s3://<scripts>/glue/jobs/. Glue works on
  the Free Plan. (diff/synth have no --all; deploy does.)
- 2026-11-20: first Glue run SUCCEEDED (159 s). Curated: 365 Parquet files
  (59.6 MiB), table fleet_telemetry.scania_aps_curated (175 cols + partition
  event_date). Owner ran the 4 Athena queries. docs/phase-2-batch.md written.
Phase 2 DONE.
- NEXT: owner makes the first git commit (nothing committed yet), then Phase 3 (dbt).