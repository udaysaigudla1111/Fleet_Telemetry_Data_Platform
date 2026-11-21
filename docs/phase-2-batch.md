# Phase 2: Batch ETL (raw CSV -> Glue PySpark -> Parquet -> Athena)

## What got built

| Piece | Where | Why |
|---|---|---|
| `scripts/prepare_dataset.py` | runs on my laptop, once | The Kaggle data has no IDs or dates. Adds **synthetic** `reading_id`, `vehicle_id` (`TRK-0001`..`TRK-2000`) and `event_date` (2025-11-01..2026-10-31). Seeded, so every run gives the same output. |
| Raw data | `s3://<raw>/scania_aps/split={train,test}/*.csv` | Uploaded with `aws s3 cp`. Never edited after upload. |
| `glue/jobs/curate_scania_aps.py` | Glue (PySpark) | Reads raw, runs quality checks, cleans types, writes curated Parquet and the catalog table. |
| `FleetTelemetryBatch` stack | `infra/stacks/batch_stack.py` | Uploads the script to S3, creates the Glue database `fleet_telemetry`, the Glue job and the Athena workgroup. Creates **no buckets**, so it's safe to destroy. |
| Glue job `fleet-telemetry-curate-scania-aps` | Glue 5.0, 2 x G.1X, 15 min timeout, 0 retries | Small and bounded: a stuck job is killed instead of billing for hours. |
| Table `fleet_telemetry.scania_aps_curated` | Glue Data Catalog | Created by the job's `saveAsTable`, not by CDK. Points at the curated bucket. |
| Athena workgroup `fleet-telemetry` | Athena | Results go to `s3://<scripts>/athena-results/` (deleted after 7 days). Each query is cancelled if it would scan more than 1 GB. |

### What the job does

```
raw CSV (all strings)
  -> quality gate: fail if input is empty, class not neg/pos, or a non-"na" value isn't numeric
  -> "na" -> null, sensors -> double, event_date -> date, add is_failure (class = 'pos')
  -> curated Parquet, one file per day:  scania_aps/event_date=YYYY-MM-DD/part-*.parquet
  -> register/refresh the table in the Glue Data Catalog
```

`mode("overwrite")` rebuilds curated completely on every run, so running it twice gives
the same result (it's idempotent). That's fine because curated can always be rebuilt
from raw.

## First run (2026-11-20)

| Check | Result |
|---|---|
| Job run | `SUCCEEDED` in 159 s (~$0.04) |
| Curated bucket | 365 objects (one per day), 59.6 MiB |
| Catalog table | 175 columns + partition column `event_date` (date) |
| Rows | train 60,000 (1,000 failures), test 16,000 (375 failures) = 76,000 |

The Parquet output (59.6 MiB) is slightly **bigger** than the CSV input (56.3 MiB). That's
expected at this size: each of the 365 files has its own header/footer for 175
columns, and sensors are stored as 8-byte doubles. Parquet's benefit here is speed
and reading only the columns a query needs, not total size. At GB/TB scale, with
bigger files, it compresses far better than CSV.

## What I learned

- **Two transform steps, two places.** `prepare_dataset.py` fakes the parts that would
  come from the real source system (IDs, dates), and runs once on my laptop. The Glue job
  is the real, repeatable pipeline step in AWS.
- **Database vs table vs data.** CDK created the *database* (an empty folder of
  definitions). The Glue job's `saveAsTable` created the *table*, which is only
  metadata: columns, types and the S3 location. The *data* is Parquet files in S3.
  `--enable-glue-datacatalog` is what makes Spark use the Glue catalog.
- **Read everything as strings, then cast on purpose.** `cast("double")` silently
  turns bad values into null, so the job counts them first and fails loudly.
- **Partitioning.** `partitionBy("event_date")` turns the column into folder names.
  The value isn't stored in the files. Athena shows it as `date (Partitioned)`.
  `WHERE event_date = DATE '2026-10-06'` reads 1 folder of 365 (*partition pruning*).
  Good partition columns are ones you filter by often and that have a moderate number
  of values. `vehicle_id` (2000 values, ~38 rows each) would cause the small files problem.
- **SQL quotes.** `'...'` is a value and `"..."` is a name. Write the value in
  the column's type: `'train'` (string), `DATE '2026-10-06'` (date), `2026` (number).
  `event_date = '2026-10-06'` fails because it compares date with varchar.
- **Athena workgroup = settings, not storage.** Data stays in curated, and results are
  CSVs in the scripts bucket. The workgroup decides where results go and how much
  a query may scan.
- **CDK details.** Cross-stack references become CloudFormation exports and imports.
  `BucketDeployment` is a Lambda that syncs a local folder to S3 on deploy.
  Only `cdk deploy` has `--all`. `synth` and `diff` cover every stack by default.
- **Who starts the job.** Now: me, with the CLI. In Phase 5: an Airflow DAG
  (`Glue job -> dbt run -> dbt test`) running in local Docker.

## How to verify

```powershell
cd infra
.\.venv\Scripts\Activate.ps1
python -m pytest -q                       # 11 tests, no AWS needed

aws login --profile fleet --region ap-southeast-2
aws configure export-credentials --profile fleet --format powershell | Invoke-Expression
$env:BUDGET_EMAIL = "you@example.com"
npx aws-cdk diff                          # review before any deploy
npx aws-cdk deploy --all

# Run the job and watch it
aws glue start-job-run --job-name fleet-telemetry-curate-scania-aps --profile fleet --region ap-southeast-2
aws glue get-job-runs --job-name fleet-telemetry-curate-scania-aps --max-items 1 `
  --query "JobRuns[0].[JobRunState,ExecutionTime,ErrorMessage]" --output text --profile fleet --region ap-southeast-2

# Output exists
aws s3 ls s3://fleet-telemetry-curated-<acct>-ap-southeast-2/scania_aps/ --recursive --summarize --profile fleet
aws glue get-table --database-name fleet_telemetry --name scania_aps_curated --profile fleet --region ap-southeast-2
```

In the Athena console (workgroup **`fleet-telemetry`**, database `fleet_telemetry`):

```sql
-- 1. Row counts: expect train 60000/1000, test 16000/375
SELECT split, COUNT(*) AS rows, SUM(CAST(is_failure AS INT)) AS failures
FROM scania_aps_curated GROUP BY split;

-- 2. "na" became null
SELECT reading_id, vehicle_id, event_date, class, is_failure, aa_000, ab_000
FROM scania_aps_curated LIMIT 10;

-- 3. Partition pruning: compare "Data scanned" with query 1
SELECT COUNT(*) FROM scania_aps_curated WHERE event_date = DATE '2026-10-06';

-- 4. Trucks with the most failures
SELECT vehicle_id, COUNT(*) AS failures FROM scania_aps_curated
WHERE is_failure GROUP BY vehicle_id ORDER BY failures DESC LIMIT 10;
```

## Cost

| Item | Cost |
|---|---|
| Glue job | ~$0.44 per DPU-hour x 2 DPU x ~2.5 min = about **$0.04 per run** |
| Athena | $5 per TB scanned. Whole table is ~60 MB, so about **$0.0003** per full scan |
| Catalog, workgroup, script upload Lambda | effectively $0 |
| S3 (raw + curated, ~115 MB) | under $0.01/month |
