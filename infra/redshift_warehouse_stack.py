"""Stack 2: Amazon Redshift Serverless for Streaming Ingestion, Spectrum & Star Schema."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesis as kinesis
from aws_cdk import aws_kms as kms
from aws_cdk import aws_redshiftserverless as redshiftserverless
from aws_cdk import aws_s3 as s3
from constructs import Construct


class RedshiftWarehouseStack(cdk.Stack):
    """Provisions Redshift Serverless Namespace + Workgroup with Kinesis & Spectrum IAM."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        vpc: ec2.IVpc,
        kms_key: kms.IKey,
        datalake_bucket: s3.IBucket,
        event_stream: kinesis.IStream,
        **kwargs: object,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Least-Privilege Redshift IAM Role for Zero-ETL Kinesis Streaming & S3 Spectrum
        self.redshift_role = iam.Role(
            self,
            "RedshiftStreamingAndSpectrumRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("redshift.amazonaws.com"),
                iam.ServicePrincipal("redshift-serverless.amazonaws.com"),
            ),
        )
        datalake_bucket.grant_read(self.redshift_role)
        event_stream.grant_read(self.redshift_role)
        kms_key.grant_decrypt(self.redshift_role)
        self.redshift_role.add_to_policy(
            iam.PolicyStatement(
                sid="GlueCatalogSpectrumReadAccess",
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartitions",
                ],
                resources=["*"],
            )
        )

        # 2. Dedicated Security Group for Private Redshift Serverless Workgroup
        self.redshift_sg = ec2.SecurityGroup(
            self,
            "RedshiftServerlessSg",
            vpc=vpc,
            description="Private security group for Redshift Serverless & QuickSight VPC connector",
            allow_all_outbound=True,
        )

        # 3. Redshift Serverless Namespace (Storage, KMS encryption & default IAM role)
        self.namespace = redshiftserverless.CfnNamespace(
            self,
            "EvmRedshiftNamespace",
            namespace_name="evm-enterprise-ns",
            db_name="evm_dw",
            kms_key_id=kms_key.key_arn,
            iam_roles=[self.redshift_role.role_arn],
            default_iam_role_arn=self.redshift_role.role_arn,
            log_exports=["userlog", "connectionlog", "useractivitylog"],
        )

        # 4. Redshift Serverless Workgroup (Compute RPUs in isolated VPC subnets)
        isolated_subnets = vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
        ).subnet_ids

        self.workgroup = redshiftserverless.CfnWorkgroup(
            self,
            "EvmRedshiftWorkgroup",
            workgroup_name="evm-enterprise-wg",
            namespace_name=self.namespace.namespace_name,
            base_capacity=32,
            publicly_accessible=False,
            enhanced_vpc_routing=True,
            subnet_ids=isolated_subnets,
            security_group_ids=[self.redshift_sg.security_group_id],
        )
        self.workgroup.add_dependency(self.namespace)

        cdk.CfnOutput(self, "RedshiftNamespaceName", value=self.namespace.namespace_name)
        cdk.CfnOutput(self, "RedshiftWorkgroupName", value=self.workgroup.workgroup_name)
        cdk.CfnOutput(self, "RedshiftIamRoleArn", value=self.redshift_role.role_arn)
