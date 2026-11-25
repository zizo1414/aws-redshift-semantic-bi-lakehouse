"""Databricks PySpark Structured Streaming & Delta UniForm Curation (Zero-dbt).

Ingests raw Kinesis Firehose events from Amazon S3 via Auto Loader (`cloudFiles`)
with `Trigger.AvailableNow` and writes Liquid-Clustered Delta tables with
**Delta UniForm (Iceberg metadata)** enabled so Amazon Redshift Spectrum can
query the exact same S3 Parquet files with zero storage duplication.
"""

from __future__ import annotations

import argparse


def build_uniform_ddl(catalog: str, bucket: str) -> str:
    """Return Unity Catalog DDL enabling Liquid Clustering + Delta UniForm (Iceberg)."""
    return f"""
CREATE TABLE IF NOT EXISTS {catalog}.silver.kinesis_events_curated (
    event_id          STRING,
    event_timestamp   TIMESTAMP,
    project_id        STRING,
    region_code       STRING,
    event_type        STRING,
    billable_hours    DECIMAL(10, 2),
    poc_hours         DECIMAL(10, 2),
    labor_cost_usd    DECIMAL(14, 2),
    cloud_cost_usd    DECIMAL(14, 2),
    ev_delta_usd      DECIMAL(14, 2),
    _rescued_data     STRING
)
USING DELTA
CLUSTER BY (region_code, project_id)
LOCATION 's3://{bucket}/silver/kinesis_events_curated/'
TBLPROPERTIES (
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
""".strip()


def run_spark_uniform_stream(catalog: str, bucket: str) -> None:  # pragma: no cover
    """Execute incremental Auto Loader stream on Databricks Serverless Compute."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.appName("EVMDeltaUniFormSilver").getOrCreate()
    spark.sql(build_uniform_ddl(catalog, bucket))

    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"s3://{bucket}/checkpoints/evm_schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"s3://{bucket}/bronze/kinesis_events/")
    )

    deduped = raw_stream.withWatermark("event_timestamp", "2 hours").dropDuplicates(
        ["event_id", "event_timestamp"]
    )

    (
        deduped.withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .writeStream.format("delta")
        .option("checkpointLocation", f"s3://{bucket}/checkpoints/evm_silver")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.silver.kinesis_events_curated")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="EVM Delta UniForm Silver Pipeline")
    parser.add_argument("--catalog", default="evm_enterprise_prod")
    parser.add_argument("--bucket", default="enterprise-evm-lakehouse-prod")
    args = parser.parse_args()
    run_spark_uniform_stream(args.catalog, args.bucket)


if __name__ == "__main__":
    main()
