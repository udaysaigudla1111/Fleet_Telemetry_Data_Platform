"""Phase 1 foundation: S3 data-lake zones, IAM roles, and a cost budget alert.

Everything here is pay-per-use: S3 bills per GB stored / request, IAM and
Budgets are free (the first two budgets per account are free).
"""

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_budgets as budgets,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct

PROJECT = "fleet-telemetry"


class FoundationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        budget_email: str,
        monthly_budget_usd: int = 5,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------
        # S3 zones
        # raw      -> source files exactly as received (immutable, versioned)
        # curated  -> cleaned, typed Parquet partitioned by event_date
        # scripts  -> Glue job code, Athena query results, other artifacts
        # ------------------------------------------------------------------
        self.raw_bucket = self._bucket(
            "Raw",
            zone="raw",
            versioned=True,  # an accidental overwrite/delete stays recoverable
            removal_policy=RemovalPolicy.RETAIN,  # source data survives `cdk destroy`
            extra_rules=[
                # Old versions only exist after a mistake; don't pay for them forever.
                s3.LifecycleRule(noncurrent_version_expiration=Duration.days(30)),
            ],
        )
        self.curated_bucket = self._bucket(
            "Curated",
            zone="curated",
            # Curated data can always be rebuilt from raw, so it's safe to delete.
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.scripts_bucket = self._bucket(
            "Scripts",
            zone="scripts",
            removal_policy=RemovalPolicy.DESTROY,
            extra_rules=[
                # Athena writes a result file for every query; clean them up.
                s3.LifecycleRule(prefix="athena-results/", expiration=Duration.days(7)),
            ],
        )

        # ------------------------------------------------------------------
        # IAM: Glue job role (used in Phase 2)
        # Least privilege: READ raw, READ scripts, READ/WRITE curated.
        # Glue can never modify the raw zone.
        # ------------------------------------------------------------------
        self.glue_job_role = iam.Role(
            self,
            "GlueJobRole",
            role_name=f"{PROJECT}-glue-job-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Glue ETL jobs: read raw + scripts, write curated",
            managed_policies=[
                # Glue catalog, CloudWatch logs, etc. (AWS-maintained policy)
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        self.raw_bucket.grant_read(self.glue_job_role)
        self.scripts_bucket.grant_read(self.glue_job_role)
        self.curated_bucket.grant_read_write(self.glue_job_role)

        # ------------------------------------------------------------------
        # Budget alert: emails at 50% / 80% of actual spend and when the
        # month is FORECAST to go over 100%.
        # ------------------------------------------------------------------
        def notification(kind: str, threshold: float):
            return budgets.CfnBudget.NotificationWithSubscribersProperty(
                notification=budgets.CfnBudget.NotificationProperty(
                    notification_type=kind,
                    comparison_operator="GREATER_THAN",
                    threshold=threshold,
                    threshold_type="PERCENTAGE",
                ),
                subscribers=[
                    budgets.CfnBudget.SubscriberProperty(
                        subscription_type="EMAIL", address=budget_email
                    )
                ],
            )

        budgets.CfnBudget(
            self,
            "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name=f"{PROJECT}-monthly",
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(
                    amount=monthly_budget_usd, unit="USD"
                ),
            ),
            notifications_with_subscribers=[
                notification("ACTUAL", 50),
                notification("ACTUAL", 80),
                notification("FORECASTED", 100),
            ],
        )

    def _bucket(
        self,
        construct_id: str,
        *,
        zone: str,
        removal_policy: RemovalPolicy,
        versioned: bool = False,
        extra_rules: list[s3.LifecycleRule] | None = None,
    ) -> s3.Bucket:
        """Secure-by-default bucket shared by every zone."""
        return s3.Bucket(
            self,
            f"{construct_id}Bucket",
            # Bucket names are global; account + region make them unique.
            bucket_name=f"{PROJECT}-{zone}-{self.account}-{self.region}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            object_ownership=s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
            versioned=versioned,
            removal_policy=removal_policy,
            # Empties the bucket on `cdk destroy` (only for rebuildable zones).
            auto_delete_objects=removal_policy == RemovalPolicy.DESTROY,
            lifecycle_rules=[
                s3.LifecycleRule(abort_incomplete_multipart_upload_after=Duration.days(7)),
                *(extra_rules or []),
            ],
        )
