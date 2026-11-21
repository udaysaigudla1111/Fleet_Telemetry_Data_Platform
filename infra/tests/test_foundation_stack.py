"""Unit tests: synthesize the stack in memory and assert on the CloudFormation.

No AWS account or credentials needed; runs in CI later (Phase 8).
"""

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template

from stacks.foundation_stack import FoundationStack


@pytest.fixture(scope="module")
def template() -> Template:
    app = cdk.App()
    stack = FoundationStack(
        app,
        "Test",
        budget_email="test@example.com",
        env=cdk.Environment(account="123456789012", region="ap-southeast-2"),
    )
    return Template.from_stack(stack)


def test_three_zone_buckets(template):
    for zone in ("raw", "curated", "scripts"):
        template.has_resource_properties(
            "AWS::S3::Bucket",
            {"BucketName": f"fleet-telemetry-{zone}-123456789012-ap-southeast-2"},
        )


def test_buckets_block_public_access_and_encrypt(template):
    buckets = template.find_resources("AWS::S3::Bucket")
    assert len(buckets) == 3
    for bucket in buckets.values():
        props = bucket["Properties"]
        assert props["PublicAccessBlockConfiguration"]["BlockPublicPolicy"] is True
        assert "BucketEncryption" in props


def test_raw_is_versioned_and_retained(template):
    template.has_resource(
        "AWS::S3::Bucket",
        {
            "Properties": Match.object_like(
                {
                    "BucketName": Match.string_like_regexp("raw"),
                    "VersioningConfiguration": {"Status": "Enabled"},
                }
            ),
            "DeletionPolicy": "Retain",
        },
    )


def test_glue_role_cannot_write_raw(template):
    """Rule: never edit the raw zone. Glue gets read-only actions on raw."""
    raw_id = next(
        k
        for k, v in template.find_resources("AWS::S3::Bucket").items()
        if "raw" in v["Properties"]["BucketName"]
    )
    policies = template.find_resources("AWS::IAM::Policy")
    write_actions = {"s3:PutObject", "s3:DeleteObject*", "s3:Abort*", "s3:PutObjectLegalHold"}
    for policy in policies.values():
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
            if raw_id in str(stmt.get("Resource")):
                actions = stmt["Action"] if isinstance(stmt["Action"], list) else [stmt["Action"]]
                assert not write_actions & set(actions), f"raw write granted: {actions}"


def test_budget_has_alerts(template):
    template.has_resource_properties(
        "AWS::Budgets::Budget",
        {
            "Budget": Match.object_like(
                {"BudgetLimit": {"Amount": 5, "Unit": "USD"}, "TimeUnit": "MONTHLY"}
            ),
            "NotificationsWithSubscribers": Match.array_with(
                [Match.object_like({"Notification": Match.object_like({"NotificationType": "FORECASTED"})})]
            ),
        },
    )
