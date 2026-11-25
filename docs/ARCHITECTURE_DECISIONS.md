# Architecture Decision Records (ADRs)

## ADR-001: Zero-ETL Hot Path via Amazon Redshift Streaming Ingestion over Kinesis
* **Context:** Executive PMOs require sub-10-second visibility into runaway PoC/sandbox cloud spend and intraday delivery burn without maintaining brittle Lambda/Glue micro-batch glue code.
* **Decision:** Use **Amazon Redshift Streaming Ingestion** (`CREATE EXTERNAL SCHEMA ... FROM KINESIS` + `CREATE MATERIALIZED VIEW ... AUTO REFRESH YES`) parsing JSON events via Redshift's native `SUPER` data type, while **Amazon Data Firehose** archives the same stream to S3 Bronze for long-term replayability.
* **Consequence:** Eliminates custom consumer infrastructure on the hot path while keeping 7-day stream replay + infinite S3 durability.

## ADR-002: Zero-`dbt` Lakehouse Interoperability via Databricks Delta UniForm & Redshift Spectrum
* **Context:** Data Science / Spark teams curate multi-year SOW baselines and SCD Type 2 change orders in **Databricks**, while Finance & Executive BI standardize on **Amazon Redshift**. Copying terabytes between S3 and Redshift managed SSD wastes money and introduces sync lag.
* **Decision:** Enable **Delta UniForm (`delta.universalFormat.enabledFormats = 'iceberg'`)** on Databricks Silver tables in S3 and query cold historical partitions directly in place via **Amazon Redshift Spectrum**, merging active hot snapshots via native Redshift PL/pgSQL (`sp_merge_daily_evm_snapshot`).
* **Consequence:** Zero `dbt` tool sprawl, zero storage duplication on S3, and unified governance across Unity Catalog and AWS Glue.

## ADR-003: Redshift Physical Star Schema (`DISTKEY` + `COMPOUND SORTKEY` + Zone Maps)
* **Context:** High-concurrency BI tools (QuickSight & Power BI DirectQuery) suffer when Redshift shuffles fact joins across compute slices or scans multi-year history for a single executive snapshot.
* **Decision:**
  * Small dimensions (`dim_project`, `dim_region`, `dim_date`) use `DISTSTYLE ALL`.
  * Large facts (`fact_evm_daily`, `fact_utilization_daily`) use `DISTSTYLE KEY` with `DISTKEY(project_sk)` and `COMPOUND SORTKEY(snapshot_date, region_code, project_sk)` with `AZ64` numeric compression.
* **Consequence:** Fact-to-fact and fact-to-dimension joins execute locally with zero network redistribution, and Redshift's **1 MB Zone Maps** skip 90%+ of storage blocks on date + regional RLS predicates.

## ADR-004: Single Governed Semantic Compiler Preventing the Naive `AVG(CPI)` BI Trap
* **Context:** Earned Value Management KPIs (`CPI`, `SPI`, `EAC`, `TCPI`) are **non-additive ratios**. Allowing self-service BI authors to aggregate project-level `CPI` with `AVG(cpi)` allows tiny $180K PoCs (`CPI = 1.45`) to mask $4.8M core banking migrations failing at `CPI = 0.82`.
* **Decision:** Author all base measures, weighted ratio expressions (`SUM(EV) / NULLIF(SUM(AC), 0)`), and Row-Level Security personas once in `semantic/evm_semantic_model.toml`, compiling deterministically to Redshift SQL Views, Databricks Unity Catalog Metric Views, QuickSight Calculated Fields, and Power BI DAX.
* **Consequence:** 100% mathematical parity across every BI surface with CI drift enforcement (`python src/semantic_compiler.py --check`).

## ADR-005: Dual Enterprise BI Serving — QuickSight Hybrid SPICE/DirectQuery & Power BI Composite Mode
* **Context:** AWS-native teams prefer serverless **Amazon QuickSight**, while enterprise corporate finance teams rely on **Microsoft Power BI**.
* **Decision:** Provision **QuickSight-as-Code** via AWS CDK (SPICE for daily Star Schema speed + DirectQuery for `<10s` Kinesis burn alerts) AND compile a ready-to-import **Power BI Tabular Model (`EVM_Semantic_Model.bim`)** using Composite Mode (`DirectQuery` facts + `Dual` dimensions).
