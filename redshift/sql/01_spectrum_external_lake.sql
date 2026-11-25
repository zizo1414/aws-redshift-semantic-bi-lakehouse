-- =============================================================================
-- 01_spectrum_external_lake.sql
-- Zero-Copy Cold Lakehouse Layer: Amazon Redshift Spectrum over S3 Iceberg /
-- Databricks Delta UniForm tables registered in AWS Glue Data Catalog.
-- =============================================================================

CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_lake
FROM DATA CATALOG
DATABASE 'evm_lakehouse_catalog'
IAM_ROLE default
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- Multi-year SOW project baselines & SCD Type 2 budget revisions stored on S3
-- Curated by Databricks Structured Streaming with Delta UniForm (Iceberg metadata)
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_lake.ext_project_baselines_iceberg (
    project_sk            BIGINT,
    project_id            VARCHAR(32),
    project_name          VARCHAR(128),
    region_code           VARCHAR(16),
    practice_area         VARCHAR(64),
    contract_type         VARCHAR(32),
    project_manager       VARCHAR(64),
    bac_usd               DECIMAL(18, 2),
    start_date            DATE,
    target_end_date       DATE,
    baseline_version      INT,
    effective_from        TIMESTAMP,
    effective_to          TIMESTAMP,
    is_current            BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://enterprise-evm-lakehouse-prod/silver/project_baselines/';

-- Archived raw Kinesis operational events landed on S3 by Amazon Data Firehose
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_lake.ext_kinesis_events_archive (
    event_id              VARCHAR(64),
    event_timestamp       TIMESTAMP,
    project_id            VARCHAR(32),
    event_type            VARCHAR(32),
    hours_logged          DECIMAL(10, 2),
    labor_cost_usd        DECIMAL(14, 2),
    cloud_cost_usd        DECIMAL(14, 2),
    ev_delta_usd          DECIMAL(14, 2)
)
PARTITIONED BY (region_code VARCHAR(16), event_date DATE)
STORED AS PARQUET
LOCATION 's3://enterprise-evm-lakehouse-prod/bronze/kinesis_events/';
