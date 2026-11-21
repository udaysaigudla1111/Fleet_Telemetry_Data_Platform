"""Unit tests for the Phase 2 batch stack (Glue job, catalog database, Athena workgroup).

Synthesizes Foundation + Batch together in memory, like app.py does. No AWS needed.
"""

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from stacks.batch_stack import BatchStack
from stacks.foundation_stack import FoundationStack

ENV = cdk.Environment(account="123456789012", region="ap-southeast-2")


@pytest.fixture(scope="module")
def template() -> Template:
    app = cdk.App()
    foundation = FoundationStack(app, "Foundation", budget_email="test@example.com", env=ENV)
    batch = BatchStack(
        app,
        "Batch",
        raw_bucket=foundation.raw_bucket,
        curated_bucket=foundation.curated_bucket,
        scripts_bucket=foundation.scripts_bucket,
        glue_role=foundation.glue_job_role,
        env=ENV,
    )
    return Template.from_stack(batch)


def test_batch_stack_creates_no_buckets(template):
    # Storage belongs to Foundation; Batch must stay safe to destroy.
    template.resource_count_is("AWS::S3::Bucket", 0)


def test_catalog_database(template):
    template.has_resource_properties(
        "AWS::Glue::Database", {"DatabaseInput": {"Name": "fleet_telemetry"}}
    )


def test_glue_job_is_small_and_bounded(template):
    template.has_resource_properties(
        "AWS::Glue::Job",
        {
            "Name": "fleet-telemetry-curate-scania-aps",
            "GlueVersion": "5.0",
            "WorkerType": "G.1X",
            "NumberOfWorkers": 2,
            "Timeout": 15,
            "MaxRetries": 0,
            "Command": Match.object_like({"Name": "glueetl"}),
        },
    )


def test_glue_job_passes_script_arguments(template):
    job = next(iter(template.find_resources("AWS::Glue::Job").values()))
    args = job["Properties"]["DefaultArguments"]
    for name in ("--raw_path", "--curated_path", "--database", "--table"):
        assert name in args, f"script reads {name} via getResolvedOptions"
    assert args["--database"] == "fleet_telemetry"
    assert args["--table"] == "scania_aps_curated"
    assert args["--enable-glue-datacatalog"] == "true"


def test_glue_job_uses_foundation_role(template):
    # The role comes from the Foundation stack -> imported, not created here.
    template.resource_count_is("AWS::IAM::Role", 1)  # only the BucketDeployment helper's role
    job = next(iter(template.find_resources("AWS::Glue::Job").values()))
    assert "Fn::ImportValue" in job["Properties"]["Role"]


def test_athena_workgroup_caps_cost(template):
    template.has_resource_properties(
        "AWS::Athena::WorkGroup",
        {
            "Name": "fleet-telemetry",
            "WorkGroupConfiguration": Match.object_like(
                {
                    "EnforceWorkGroupConfiguration": True,
                    "BytesScannedCutoffPerQuery": 1_000_000_000,
                }
            ),
        },
    )
