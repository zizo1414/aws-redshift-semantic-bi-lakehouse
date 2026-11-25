# Enterprise Streaming Lakehouse, Governed Semantic Layer & Executive EVM BI Platform

![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20S3%20%7C%20Redshift%20%7C%20QuickSight-FF9900?logo=amazonaws)
![Databricks](https://img.shields.io/badge/Databricks-Delta%20UniForm%20%7C%20Unity%20Catalog-FF3621?logo=databricks)
![Power BI](https://img.shields.io/badge/Power%20BI-Tabular%20.bim%20%7C%20DAX%20%7C%20DirectQuery-F2C811?logo=powerbi&logoColor=black)
![IaC](https://img.shields.io/badge/IaC-AWS%20CDK%20v2%20%2B%20DABs-2496ED)
![Zero dbt](https://img.shields.io/badge/Transformations-100%25%20Native%20AWS%20%26%20Databricks%20(Zero%20dbt)-success)

Production-grade reference architecture combining **Amazon Kinesis Data Streams**, an **Amazon S3 Open Data Lake (Apache Iceberg / Databricks Delta UniForm)**, **Amazon Redshift Serverless** (*Zero-ETL Streaming Ingestion + Redshift Spectrum + Physically Tuned Star Schema*), and a **Single Governed Semantic & Row-Level Security (RLS) Compiler** serving **Amazon QuickSight** and **Microsoft Power BI**.

Built around a high-stakes executive domain: **Global Enterprise Earned Value Management (EVM), Regional Workforce Utilization & Real-Time Cloud Burn Intelligence** across `AMER`, `EMEA`, `MENA`, and `APAC`.

---

## End-to-End Architecture

```mermaid
flowchart LR
    subgraph Producers["Global Operational Streams (AMER / EMEA / MENA / APAC)"]
        E1["Timesheet & Utilization Logs"]
        E2["Milestone % Complete Events"]
        E3["Sandbox & PoC Cloud Burn"]
    end

    Producers --> KDS["Amazon Kinesis Data Streams (On-Demand)"]

    KDS -->|"Zero-ETL Hot Path (<10s)"| RSMV["Redshift Streaming Ingestion (Auto-Refresh MV)"]
    KDS -->|"Firehose Archival"| S3["Amazon S3 Open Data Lake (Parquet / Iceberg)"]

    S3 <-->|"Delta UniForm + Liquid Clustering"| DBX["Databricks Serverless (PySpark Auto Loader)"]
    S3 -->|"Zero-Copy Cold Scan"| SPEC["Redshift Spectrum External Tables"]

    RSMV --> RS["Amazon Redshift Gold Star Schema (DISTKEY / COMPOUND SORTKEY)"]
    SPEC --> RS

    subgraph Semantic["Governed Semantic & RLS Compiler (Zero dbt)"]
        TOML["semantic/evm_semantic_model.toml"]
    end

    RS --> TOML
    DBX --> TOML

    TOML -->|"SQL + Native RLS"| V1["Redshift & Unity Catalog Metric Views"]
    TOML -->|"CDK Dashboard-as-Code"| QS["Amazon QuickSight (SPICE + DirectQuery)"]
    TOML -->|"Tabular .bim + DAX"| PBI["Power BI Composite DirectQuery Model"]
    TOML -->|"$0 Offline Mode"| ST["Streamlit Executive BI Cockpit"]
```

---

## Why This Architecture Matters (Key Engineering Highlights)

### 1. Eliminating the Naive `AVG(CPI)` BI Trap via a Governed Semantic Layer
Earned Value Management metrics (**CPI**, **SPI**, **EAC**, **TCPI**) are **non-additive ratios**. When BI analysts drag a column and pick `Average of CPI`, a small $180K PoC (`CPI = 1.45`) masks a $4.8M core banking migration bleeding budget at `CPI = 0.82` (inflating apparent portfolio health by **+400+ bps**).

`semantic/evm_semantic_model.toml` defines every metric and Row-Level Security persona **once** and compiles deterministically via `src/semantic_compiler.py` into **4 synchronized targets** (enforced in CI with `--check`):

| Target Layer | Compiled Artifact | How Governance & Performance Work |
| :--- | :--- | :--- |
| **Amazon Redshift** | `redshift/sql/05_governed_semantic_views.sql` | Weighted `SUM(cum_ev_usd) / NULLIF(SUM(cum_ac_total_usd), 0)` + `CREATE RLS POLICY` |
| **Databricks Unity Catalog** | `databricks/sql/unity_catalog_metric_views.sql` | Native `CREATE VIEW ... WITH METRICS LANGUAGE YAML` + `SET ROW FILTER` |
| **Amazon QuickSight** | `infra/quicksight_bi_stack.py` + `bi/quicksight/*.json` | Hybrid **SPICE** (Star Schema) + **DirectQuery** (`<10s` Kinesis anomalies) + RLS |
| **Microsoft Power BI** | `bi/powerbi/EVM_Semantic_Model.bim` + `evm_measures.dax` | Composite Mode (`DirectQuery` facts + `Dual` dimensions) + `DIVIDE()` DAX & RLS Roles |

### 2. Physical Amazon Redshift Star Schema Tuning (`DISTKEY` + `SORTKEY` + Zone Maps)
* **Zero-Shuffle Joins:** Small dimensions (`dim_project`, `dim_region`, `dim_date`) use `DISTSTYLE ALL`, while `fact_evm_daily` and `fact_utilization_daily` share `DISTKEY(project_sk)` so joins execute slice-locally with zero network redistribution.
* **1 MB Zone-Map Pruning:** `COMPOUND SORTKEY(snapshot_date, region_code, project_sk)` + `AZ64` encoding skips **85%–94%** of storage blocks on executive date + regional RLS queries.
* **Zero-ETL Streaming MV:** `redshift/sql/02_kinesis_streaming_mv.sql` ingests live Kinesis streams directly in SQL using Redshift's `SUPER` type and `AUTO REFRESH YES` — zero Lambda/Glue middleman.

### 3. Zero-`dbt` Lakehouse Interoperability (Delta UniForm ↔ Redshift Spectrum)
Databricks curates S3 Silver tables with `delta.universalFormat.enabledFormats = 'iceberg'`, allowing Redshift Spectrum (`01_spectrum_external_lake.sql`) to read the exact same S3 Parquet files with **zero storage duplication** and **zero third-party transformation tools**.

---

## Quickstart: Run 100% Locally in 60 Seconds (`$0` Cloud Bill)

```bash
# 1. Generate multi-region S3 Parquet baselines + Kinesis stream & run full warehouse simulator
make run-local

# 2. Launch the interactive 4-Sheet Executive BI Cockpit (RLS Switcher + Live Kinesis Injector)
make demo

# 3. Verify zero semantic drift & run unit / integration tests
python3 src/semantic_compiler.py --check
python3 -m unittest discover -s tests -v
```

### Cloud Deployment & Teardown (AWS CDK)

```bash
# Install full Python + AWS CDK CLI toolchain
make setup

# Synthesize & deploy Kinesis + S3 Open Lake + Redshift Serverless + QuickSight-as-Code
make synth
make deploy-aws

# Tear down all cloud resources cleanly when finished
make destroy-aws
make clean
```

---

## Repository Structure

```text
├── app/
│   └── executive_bi_cockpit.py           # Interactive Streamlit replica of QuickSight & Power BI
├── bi/
│   ├── powerbi/
│   │   ├── EVM_Semantic_Model.bim        # Import-ready Power BI Tabular Model + RLS Roles
│   │   ├── evm_measures.dax              # Compiled canonical DAX EVM & S-Curve measures
│   │   └── redshift_composite_directquery.pq
│   └── quicksight/
│       ├── dashboard_4_sheet_definition.json
│       └── dataset_spice_directquery_rls.json
├── databricks/
│   ├── databricks.yml                    # Databricks Asset Bundle (Serverless Job)
│   ├── pipelines/silver_delta_uniform_stream.py
│   └── sql/unity_catalog_metric_views.sql
├── docs/
│   ├── ARCHITECTURE_DECISIONS.md
│   ├── FINOPS_COST_ESTIMATE.md
│   └── WELL_ARCHITECTED_REVIEW.md
├── infra/                                # AWS CDK v2 Python Stacks
│   ├── app.py
│   ├── kinesis_datalake_stack.py
│   ├── redshift_warehouse_stack.py
│   └── quicksight_bi_stack.py
├── redshift/sql/                         # Native Redshift Spectrum, Streaming MV, DDL & PL/pgSQL
│   ├── 01_spectrum_external_lake.sql
│   ├── 02_kinesis_streaming_mv.sql
│   ├── 03_gold_star_schema_ddl.sql
│   ├── 04_sp_merge_evm_and_vacuum.sql
│   └── 05_governed_semantic_views.sql
├── scripts/
│   ├── export_env.py
│   └── generate_data.py
├── semantic/
│   └── evm_semantic_model.toml           # Single Source of Truth for Metrics & RLS
├── src/
│   ├── kinesis_simulator.py
│   ├── local_runner.py
│   ├── monte_carlo_evm.py
│   ├── redshift_simulator.py
│   └── semantic_compiler.py
└── tests/                                # EVM Math, Compiler Drift, Kinesis/Redshift & CDK Tests
```
