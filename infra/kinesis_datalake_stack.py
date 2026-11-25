"""Stack 1: Amazon Kinesis Data Streams, Firehose S3 Parquet/Iceberg Lake & Glue Catalog."""

from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_glue as glue,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_kinesisfirehose as firehose,
    aws_kms as kms,
    aws_s3 as s3,
)
from constructs import Construct


class KinesisDataLakeStack(cdk.Stack):
    """Provisions VPC + Free S3 Gateway Endpoint, KMS CMK, Kinesis Stream, Firehose & S3 Lake."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: object) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Customer-Managed KMS Key for S3 Lake, Kinesis & Redshift encryption
        self.kms_key = kms.Key(
            self,
            "LakehouseCmk",
            alias="alias/evm-semantic-lakehouse-cmk",
            enable_key_rotation=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # 2. Isolated Enterprise VPC with S3 Gateway Endpoint (zero NAT Gateway S3 fees!)
        self.vpc = ec2.Vpc(
            self,
            "LakehouseVpc",
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="IsolatedDataPlane",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                )
            ],
        )
        self.vpc.add_gateway_endpoint(
            "S3GatewayEndpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )

        # 3. Open Data Lake Bucket (queried by Databricks Delta UniForm & Redshift Spectrum)
        self.datalake_bucket = s3.Bucket(
            self,
            "OpenDataLakeBucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="BronzeIntelligentTiering",
                    prefix="bronze/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=cdk.Duration.days(30),
                        )
                    ],
                )
            ],
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True,
        )

        # 4. Amazon Kinesis Data Stream (On-Demand Mode for spiky global worklog/burn bursts)
        self.event_stream = kinesis.Stream(
            self,
            "EvmOperationalEventsStream",
            stream_name="evm-operational-events-stream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            encryption=kinesis.StreamEncryption.KMS,
            encryption_key=self.kms_key,
            retention_period=cdk.Duration.days(7),
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        # 5. AWS Glue Data Catalog Database for Redshift Spectrum & Iceberg UniForm tables
        self.glue_db = glue.CfnDatabase(
            self,
            "EvmLakehouseCatalogDb",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="evm_lakehouse_catalog",
                description="Shared Glue Catalog for S3 Iceberg / Delta UniForm & Redshift Spectrum",
            ),
        )

        # 6. Amazon Data Firehose archiving Kinesis events continuously to S3 Bronze
        firehose_role = iam.Role(
            self,
            "FirehoseDeliveryRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )
        self.event_stream.grant_read(firehose_role)
        self.datalake_bucket.grant_read_write(firehose_role)
        self.kms_key.grant_encrypt_decrypt(firehose_role)

        self.delivery_stream = firehose.CfnDeliveryStream(
            self,
            "KinesisToS3BronzeFirehose",
            delivery_stream_name="evm-kinesis-to-s3-bronze",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                kinesis_stream_arn=self.event_stream.stream_arn,
                role_arn=firehose_role.role_arn,
            ),
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=self.datalake_bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                prefix="bronze/kinesis_events/event_date=!{timestamp:yyyy-MM-dd}/",
                error_output_prefix="bronze/kinesis_dlq/!{firehose:error-output-type}/",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60,
                    size_in_m_bs=64,
                ),
                compression_format="GZIP",
            ),
        )
        self.delivery_stream.node.add_dependency(firehose_role)

        cdk.CfnOutput(self, "DataLakeBucketName", value=self.datalake_bucket.bucket_name)
        cdk.CfnOutput(self, "KinesisStreamName", value=self.event_stream.stream_name)
        cdk.CfnOutput(self, "GlueCatalogDatabase", value="evm_lakehouse_catalog")
