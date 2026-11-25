-- =============================================================================
-- 03_gold_star_schema_ddl.sql
-- Physically Tuned Amazon Redshift Gold Star Schema:
--   • Small dimensions (<1M rows) -> DISTSTYLE ALL (zero broadcast network shuffle)
--   • Large daily facts           -> DISTSTYLE KEY on project_sk (co-located joins!)
--   • COMPOUND SORTKEY            -> Maximizes 1 MB Zone Map block skipping on date + region
--   • AZ64 & ZSTD Encodings       -> Optimal columnar compression for numeric/string columns
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.dim_region (
    region_code             VARCHAR(16)    NOT NULL ENCODE ZSTD,
    region_name             VARCHAR(64)    NOT NULL ENCODE ZSTD,
    maturity_stage          VARCHAR(32)    NOT NULL ENCODE ZSTD,
    vp_owner                VARCHAR(64)    NOT NULL ENCODE ZSTD,
    target_utilization_pct  DECIMAL(5,2)   NOT NULL ENCODE AZ64,
    PRIMARY KEY (region_code)
)
DISTSTYLE ALL
SORTKEY (region_code);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    snapshot_date           DATE           NOT NULL ENCODE AZ64,
    fiscal_year             INT            NOT NULL ENCODE AZ64,
    fiscal_quarter          VARCHAR(8)     NOT NULL ENCODE ZSTD,
    month_name              VARCHAR(16)    NOT NULL ENCODE ZSTD,
    is_month_end            BOOLEAN        NOT NULL ENCODE RAW,
    PRIMARY KEY (snapshot_date)
)
DISTSTYLE ALL
SORTKEY (snapshot_date);

CREATE TABLE IF NOT EXISTS gold.dim_project (
    project_sk              BIGINT         NOT NULL ENCODE AZ64,
    project_id              VARCHAR(32)    NOT NULL ENCODE ZSTD,
    project_name            VARCHAR(128)   NOT NULL ENCODE ZSTD,
    region_code             VARCHAR(16)    NOT NULL ENCODE ZSTD,
    practice_area           VARCHAR(64)    NOT NULL ENCODE ZSTD,
    contract_type           VARCHAR(32)    NOT NULL ENCODE ZSTD,
    project_manager         VARCHAR(64)    NOT NULL ENCODE ZSTD,
    bac_usd                 DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    start_date              DATE           NOT NULL ENCODE AZ64,
    target_end_date         DATE           NOT NULL ENCODE AZ64,
    is_current              BOOLEAN        NOT NULL ENCODE RAW,
    PRIMARY KEY (project_sk)
)
DISTSTYLE ALL
SORTKEY (project_sk, region_code);

-- Fact 1: Cumulative Daily EVM Snapshot Fact (Co-located on project_sk)
CREATE TABLE IF NOT EXISTS gold.fact_evm_daily (
    snapshot_date           DATE           NOT NULL ENCODE RAW,
    project_sk              BIGINT         NOT NULL ENCODE AZ64,
    region_code             VARCHAR(16)    NOT NULL ENCODE ZSTD,
    bac_usd                 DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    cum_pv_usd              DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    cum_ev_usd              DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    cum_ac_labor_usd        DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    cum_ac_cloud_usd        DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    cum_ac_total_usd        DECIMAL(18,2)  NOT NULL ENCODE AZ64,
    pct_complete            DECIMAL(6,4)   NOT NULL ENCODE AZ64,
    PRIMARY KEY (snapshot_date, project_sk),
    FOREIGN KEY (project_sk) REFERENCES gold.dim_project(project_sk),
    FOREIGN KEY (region_code) REFERENCES gold.dim_region(region_code)
)
DISTSTYLE KEY
DISTKEY (project_sk)
COMPOUND SORTKEY (snapshot_date, region_code, project_sk);

-- Fact 2: Regional Engineering Billable & PoC Utilization Fact
CREATE TABLE IF NOT EXISTS gold.fact_utilization_daily (
    work_date               DATE           NOT NULL ENCODE RAW,
    project_sk              BIGINT         NOT NULL ENCODE AZ64,
    region_code             VARCHAR(16)    NOT NULL ENCODE ZSTD,
    role_title              VARCHAR(32)    NOT NULL ENCODE ZSTD,
    billable_hours          DECIMAL(10,2)  NOT NULL ENCODE AZ64,
    poc_investment_hours    DECIMAL(10,2)  NOT NULL ENCODE AZ64,
    bench_ramp_hours        DECIMAL(10,2)  NOT NULL ENCODE AZ64,
    capacity_hours          DECIMAL(10,2)  NOT NULL ENCODE AZ64,
    PRIMARY KEY (work_date, project_sk, role_title)
)
DISTSTYLE KEY
DISTKEY (project_sk)
COMPOUND SORTKEY (work_date, region_code);
