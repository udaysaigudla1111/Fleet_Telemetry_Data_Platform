#!/usr/bin/env python3
"""CDK entry point. `cdk synth` / `cdk diff` / `cdk deploy` all start here."""

import os

import aws_cdk as cdk

from stacks.batch_stack import BatchStack
from stacks.foundation_stack import FoundationStack

app = cdk.App()

# Email isn't a secret, but it's personal: keep it out of git.
# Pass it with `cdk deploy -c budget_email=you@example.com` or BUDGET_EMAIL env var.
budget_email = app.node.try_get_context("budget_email") or os.environ.get("BUDGET_EMAIL")
if not budget_email:
    raise SystemExit(
        "Set the budget alert email: `-c budget_email=you@example.com` or BUDGET_EMAIL=..."
    )

# Account comes from your AWS CLI credentials; region is fixed by the project
# (the Free Plan account only allows ap-southeast-2).
env = cdk.Environment(account=os.environ.get("CDK_DEFAULT_ACCOUNT"), region="ap-southeast-2")

# Phase 1: storage, IAM, budget. Rarely changes.
foundation = FoundationStack(
    app,
    "FleetTelemetryFoundation",
    budget_email=budget_email,
    env=env,
)

# Phase 2: Glue job, catalog database, Athena workgroup. Uses Foundation's
# buckets and role; CDK wires the cross-stack references and deploy order.
BatchStack(
    app,
    "FleetTelemetryBatch",
    raw_bucket=foundation.raw_bucket,
    curated_bucket=foundation.curated_bucket,
    scripts_bucket=foundation.scripts_bucket,
    glue_role=foundation.glue_job_role,
    env=env,
)

cdk.Tags.of(app).add("Project", "fleet-telemetry")

app.synth()
