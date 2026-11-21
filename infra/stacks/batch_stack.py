"""Phase 2 batch processing: Glue ETL job, Glue Data Catalog database, Athena workgroup.

Uses the buckets and the Glue role from FoundationStack; creates no storage of its
own. That keeps this stack safe to change, redeploy or destroy at any time.

Cost: all pay-per-use. The Glue job bills only while it runs (about $0.44 per
DPU-hour, 2 DPUs, a few minutes per run). The catalog database and the
workgroup are free; Athena bills $5/TB scanned, capped per query below.
"""

from pathlib import Path

from aws_cdk import (
    CfnOutput,
    Stack,
    aws_athena as athena,
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
)
from constructs import Construct

PROJECT = "fleet-telemetry"
DATABASE_NAME = "fleet_telemetry"  # Glue/Athena names: lowercase + underscores only
CURATED_TABLE = "scania_aps_curated"
SCRIPTS_PREFIX = "glue/jobs/"

# infra/stacks/batch_stack.py -> parents[2] is the project root
GLUE_JOBS_DIR = Path(__file__).resolve().parents[2] / "glue" / "jobs"

# Athena cancels any single query that would scan more than this.
MAX_BYTES_SCANNED_PER_QUERY = 1_000_000_000  # 1 GB = at most ~$0.005 per query


class BatchStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        raw_bucket: s3.IBucket,
        curated_bucket: s3.IBucket,
        scripts_bucket: s3.IBucket,
        glue_role: iam.IRole,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------
        # 1. Upload glue/jobs/*.py to s3://<scripts>/glue/jobs/ on every deploy.
        #    Glue can only run scripts that are in S3.
        # ------------------------------------------------------------------
        scripts_upload = s3deploy.BucketDeployment(
            self,
            "GlueScripts",
            sources=[s3deploy.Source.asset(str(GLUE_JOBS_DIR), exclude=["__pycache__", "*.pyc"])],
            destination_bucket=scripts_bucket,
            destination_key_prefix=SCRIPTS_PREFIX,
            # prune only touches files under glue/jobs/ (e.g. a deleted script);
            # athena-results/ and anything else in the bucket are left alone.
            prune=True,
        )

        # ------------------------------------------------------------------
        # 2. Glue Data Catalog database: a named group of table definitions.
        #    Holds no data. The Glue job creates the table inside it.
        # ------------------------------------------------------------------
        database = glue.CfnDatabase(
            self,
            "Database",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=DATABASE_NAME,
                description="Fleet telemetry data lake (Scania APS; vehicle_id/event_date are synthetic)",
            ),
        )

        # ------------------------------------------------------------------
        # 3. Glue job: runs glue/jobs/curate_scania_aps.py as the Phase 1 role.
        # ------------------------------------------------------------------
        curate_job = glue.CfnJob(
            self,
            "CurateScaniaApsJob",
            name=f"{PROJECT}-curate-scania-aps",
            description="raw Scania APS CSV -> curated Parquet partitioned by event_date + catalog table",
            role=glue_role.role_arn,  # the job runs AS this role (read raw, write curated)
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",  # Spark job (vs "pythonshell" for plain Python)
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/{SCRIPTS_PREFIX}curate_scania_aps.py",
            ),
            glue_version="5.0",  # Spark 3.5, Python 3.11
            worker_type="G.1X",  # 1 DPU per worker: 4 vCPU, 16 GB RAM
            number_of_workers=2,  # the minimum; plenty for ~56 MB of CSV
            timeout=15,  # minutes. A stuck job is killed instead of billing for hours
            max_retries=0,  # a failed run should be looked at, not silently retried
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=1),
            default_arguments={
                # Read by getResolvedOptions() in the script
                "--raw_path": f"s3://{raw_bucket.bucket_name}/scania_aps/",
                "--curated_path": f"s3://{curated_bucket.bucket_name}/scania_aps/",
                "--database": DATABASE_NAME,
                "--table": CURATED_TABLE,
                # Glue features
                "--enable-glue-datacatalog": "true",  # Spark's saveAsTable -> Glue Data Catalog
                "--enable-continuous-cloudwatch-log": "true",  # live logs in CloudWatch while it runs
                "--job-language": "python",
            },
        )
        # Make sure the script is in S3 before the job that points at it exists.
        curate_job.node.add_dependency(scripts_upload)
        curate_job.add_resource_dependency(database)

        # ------------------------------------------------------------------
        # 4. Athena workgroup: settings for every query run in it.
        # ------------------------------------------------------------------
        workgroup = athena.CfnWorkGroup(
            self,
            "WorkGroup",
            name=PROJECT,
            description="Fleet telemetry queries: results in the scripts bucket, scan capped per query",
            state="ENABLED",
            recursive_delete_option=True,  # let `cdk destroy` remove it even with saved queries
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    # Phase 1 lifecycle rule deletes athena-results/ after 7 days
                    output_location=f"s3://{scripts_bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
                enforce_work_group_configuration=True,  # clients can't override the above
                bytes_scanned_cutoff_per_query=MAX_BYTES_SCANNED_PER_QUERY,
                publish_cloud_watch_metrics_enabled=True,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3"
                ),
            ),
        )

        # Printed after `cdk deploy`, so you don't have to look names up.
        CfnOutput(self, "GlueJobName", value=curate_job.ref)
        CfnOutput(self, "DatabaseName", value=DATABASE_NAME)
        CfnOutput(self, "CuratedTable", value=f"{DATABASE_NAME}.{CURATED_TABLE}")
        CfnOutput(self, "AthenaWorkGroup", value=workgroup.ref)
