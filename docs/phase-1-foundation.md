# Phase 1: CDK foundation

## What got built

| Resource | Why |
|---|---|
| `fleet-telemetry-raw-<acct>-<region>` (S3) | Source CSVs exactly as received. **Versioned** + **RETAIN** so a bad overwrite or `cdk destroy` can't lose source data. Old versions expire after 30 days. |
| `fleet-telemetry-curated-<acct>-<region>` (S3) | Cleaned Parquet, partitioned by `event_date`. Rebuildable from raw, so `DESTROY` + auto-empty. |
| `fleet-telemetry-scripts-<acct>-<region>` (S3) | Glue job code + Athena query results (`athena-results/` auto-expires after 7 days). |
| `fleet-telemetry-glue-job-role` (IAM) | Role Glue assumes in Phase 2: **read** raw, **read** scripts, **read/write** curated. |
| `fleet-telemetry-monthly` (Budget) | $5/month; emails at 50% and 80% actual and at 100% forecast. |

All buckets: public access blocked, SSE-S3 encryption, HTTPS-only bucket policy,
ACLs disabled (`BUCKET_OWNER_ENFORCED`), incomplete multipart uploads cleaned
after 7 days.

## What I learned

- **Data lake zones.** Raw is the source of truth and is never edited. Curated is
  a *derived* artifact, so it's safe to delete and rebuild. That difference drives
  the removal policy and versioning choices.
- **CDK = code that generates CloudFormation.** `cdk synth` writes the template to
  `cdk.out/`. `cdk diff` compares it with what's deployed. `cdk deploy` applies the
  change via CloudFormation.
- **`grant_*` methods** write least-privilege IAM policies for you.
  `bucket.grant_read(role)` gives only `s3:GetObject*` and `s3:List*` on that
  bucket. The test `test_glue_role_cannot_write_raw` enforces the "never edit raw"
  rule in code.
- **`auto_delete_objects`** adds a small Lambda custom resource that empties the
  bucket before CloudFormation deletes it. It only runs during deploy and destroy,
  and costs effectively $0.
- **Budgets alert, they don't block.** A budget will email you but won't stop
  spending. The FORECASTED alert gives a warning before the bill arrives.

## How to verify

```bash
cd infra
source .venv/bin/activate
pip install -r requirements-dev.txt

# 1. Unit tests (no AWS needed)
python -m pytest -q

# 2. Synthesize the CloudFormation template (no AWS needed)
npx aws-cdk synth -c budget_email=you@example.com

# 3. Against a real account (needs AWS CLI credentials)
npx aws-cdk bootstrap aws://<ACCOUNT_ID>/ap-southeast-2 -c budget_email=you@example.com   # once per account/region
npx aws-cdk diff   -c budget_email=you@example.com      # review first
npx aws-cdk deploy -c budget_email=you@example.com

# 4. Check it
aws s3 ls | grep fleet-telemetry                      # 3 buckets
aws iam get-role --role-name fleet-telemetry-glue-job-role
aws budgets describe-budgets --account-id <ACCOUNT_ID>
```

After the first deploy, AWS Budgets sends a confirmation email to the subscriber
address. Notifications start after you accept it.

## Setup notes from the first real deploy (Windows + AWS Free Plan)

Deployed 2026-11-02 to account `703230062750`, `ap-southeast-2`. What tripped me up:

| Problem | Cause | Fix |
|---|---|---|
| Region `ap-south-1` rejected at login | The Free Plan account (an "Open Horizons" project) is locked to one region, `ap-southeast-2` | Switched the whole project to `ap-southeast-2` |
| MFA / IAM Identity Center unavailable | Not included in the Free Plan; I sign in through an `AccountFullAccessRole`, not as root | Use `aws login` (CLI v2.32+) instead |
| CDK: `NoCredentials` after `aws login` | CDK can't read the new `login_session` profile format yet | Export temporary keys into the shell (below) |
| CDK: `ExpiredToken` | Exported keys are a short-lived snapshot | Re-run the export line |
| `ModuleNotFoundError: aws_cdk` | venv not activated | `.\.venv\Scripts\Activate.ps1` |
| `ENOTEMPTY ... jsii-kernel` on synth | Windows locks jsii's temp files (antivirus) | Delete `%TEMP%\jsii-kernel-*`, re-run |
| `python3` opens Microsoft Store | Windows alias | `cdk.json` uses `python app.py` |

Daily PowerShell setup:

```powershell
cd infra
.\.venv\Scripts\Activate.ps1
aws login --profile fleet --region ap-southeast-2      # when the session has expired
aws configure export-credentials --profile fleet --format powershell | Invoke-Expression
$env:BUDGET_EMAIL = "you@example.com"                   # then plain `npx aws-cdk diff` works
```

## Cost

S3 storage is about $0.025/GB-month in ap-southeast-2 (Sydney). The dataset is about 50 MB, so
this is effectively $0. IAM is free. The first 2 budgets are free. CDK bootstrap
creates a staging bucket and an ECR repo, which cost nothing while empty or near
empty.
