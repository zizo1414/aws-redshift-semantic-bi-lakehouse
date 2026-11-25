"""Tests for AWS CDK Stacks (Kinesis, S3 Lake, Redshift, QuickSight) & Databricks UniForm."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

from databricks.pipelines.silver_delta_uniform_stream import build_uniform_ddl

ROOT_DIR = Path(__file__).resolve().parent.parent
HAS_CDK = importlib.util.find_spec("aws_cdk") is not None


class TestDatabricksAndSqlSpecs(unittest.TestCase):
    def test_databricks_delta_uniform_ddl_enables_iceberg_compat(self) -> None:
        ddl = build_uniform_ddl("evm_enterprise_prod", "enterprise-evm-lakehouse-prod")
        self.assertIn("delta.universalFormat.enabledFormats' = 'iceberg'", ddl)
        self.assertIn("CLUSTER BY (region_code, project_id)", ddl)

    def test_redshift_sql_ddls_define_streaming_mv_spectrum_and_sortkeys(self) -> None:
        mv_sql = (ROOT_DIR / "redshift" / "sql" / "02_kinesis_streaming_mv.sql").read_text()
        star_sql = (ROOT_DIR / "redshift" / "sql" / "03_gold_star_schema_ddl.sql").read_text()
        spec_sql = (ROOT_DIR / "redshift" / "sql" / "01_spectrum_external_lake.sql").read_text()

        self.assertIn("FROM KINESIS", mv_sql)
        self.assertIn("AUTO REFRESH YES", mv_sql)
        self.assertIn("DISTKEY (project_sk)", star_sql)
        self.assertIn("COMPOUND SORTKEY (snapshot_date, region_code, project_sk)", star_sql)
        self.assertIn("CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_lake", spec_sql)


@unittest.skipUnless(HAS_CDK, "aws-cdk-lib installed in .venv / GitHub Actions CI")
class TestAwsCdkInfrastructureStacks(unittest.TestCase):
    def test_cdk_stacks_synthesize_secure_kinesis_redshift_and_quicksight(self) -> None:
        import aws_cdk as cdk
        from aws_cdk.assertions import Template

        from infra.kinesis_datalake_stack import KinesisDataLakeStack
        from infra.quicksight_bi_stack import QuickSightBiStack
        from infra.redshift_warehouse_stack import RedshiftWarehouseStack

        app = cdk.App()
        env = cdk.Environment(account="123456789012", region="us-east-1")

        lake_stack = KinesisDataLakeStack(app, "TestLakeStack", env=env)
        rs_stack = RedshiftWarehouseStack(
            app,
            "TestRedshiftStack",
            vpc=lake_stack.vpc,
            kms_key=lake_stack.kms_key,
            datalake_bucket=lake_stack.datalake_bucket,
            event_stream=lake_stack.event_stream,
            env=env,
        )
        qs_stack = QuickSightBiStack(
            app,
            "TestQuickSightStack",
            redshift_workgroup_name="evm-enterprise-wg",
            env=env,
        )

        lake_tmpl = Template.from_stack(lake_stack)
        lake_tmpl.resource_count_is("AWS::Kinesis::Stream", 1)
        lake_tmpl.resource_count_is("AWS::KinesisFirehose::DeliveryStream", 1)
        lake_tmpl.has_resource_properties(
            "AWS::EC2::VPCEndpoint",
            {"VpcEndpointType": "Gateway"},
        )

        rs_tmpl = Template.from_stack(rs_stack)
        rs_tmpl.has_resource_properties(
            "AWS::RedshiftServerless::Workgroup",
            {"PubliclyAccessible": False, "EnhancedVpcRouting": True},
        )

        qs_tmpl = Template.from_stack(qs_stack)
        qs_tmpl.has_resource_properties(
            "AWS::QuickSight::DataSet",
            {"ImportMode": "SPICE"},
        )


if __name__ == "__main__":
    unittest.main()
