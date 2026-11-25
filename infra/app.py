#!/usr/bin/env python3
"""AWS CDK Entrypoint wiring Streaming Data Lake, Redshift Serverless & QuickSight BI."""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import aws_cdk as cdk  # noqa: E402

from infra.kinesis_datalake_stack import KinesisDataLakeStack  # noqa: E402
from infra.quicksight_bi_stack import QuickSightBiStack  # noqa: E402
from infra.redshift_warehouse_stack import RedshiftWarehouseStack  # noqa: E402

app = cdk.App()

env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT", "123456789012"),
    region=os.getenv("CDK_DEFAULT_REGION", os.getenv("AWS_REGION", "us-east-1")),
)

datalake_stack = KinesisDataLakeStack(
    app,
    "EvmKinesisDataLakeStack",
    env=env,
    description="Kinesis On-Demand Stream, Firehose S3 Bronze/Silver Open Lake, VPC S3 Endpoint & KMS",
)

redshift_stack = RedshiftWarehouseStack(
    app,
    "EvmRedshiftWarehouseStack",
    vpc=datalake_stack.vpc,
    kms_key=datalake_stack.kms_key,
    datalake_bucket=datalake_stack.datalake_bucket,
    event_stream=datalake_stack.event_stream,
    env=env,
    description="Redshift Serverless for Zero-ETL Kinesis Streaming MVs, S3 Spectrum & Gold Star Schema",
)

quicksight_stack = QuickSightBiStack(
    app,
    "EvmQuickSightBiStack",
    redshift_workgroup_name="evm-enterprise-wg",
    env=env,
    description="Governed QuickSight-as-Code DataSource and SPICE Semantic Dataset",
)
quicksight_stack.add_dependency(redshift_stack)

cdk.Tags.of(app).add("Project", "aws-redshift-semantic-bi-lakehouse")
cdk.Tags.of(app).add("Architecture", "Kinesis-S3-Redshift-Semantic-BI")
cdk.Tags.of(app).add("ManagedBy", "AWS-CDK")

app.synth()
