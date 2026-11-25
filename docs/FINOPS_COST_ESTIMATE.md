# Cloud FinOps & Monthly Cost Estimate (`us-east-1`)

Estimated monthly run-rate for an enterprise PMO monitoring **250 global programs**, **~5M monthly Kinesis operational events**, **500 GB S3 Open Data Lake**, and **150 executive BI readers**:

| Layer | AWS / Databricks Service | Sizing & FinOps Guardrail | Est. Monthly Cost |
| :--- | :--- | :--- | ---: |
| **Streaming** | Amazon Kinesis Data Streams (On-Demand) | ~5 GB/mo ingested, zero idle shard over-provisioning | **$29.00** |
| **Lake Archival** | Amazon Data Firehose -> S3 Bronze | 64 MB / 60s buffering + GZIP/Parquet compression | **$14.50** |
| **Open Data Lake** | Amazon S3 + Intelligent-Tiering + VPC Gateway Endpoint | 500 GB Parquet + **Free S3 VPC Gateway Endpoint ($0 NAT fees!)** | **$11.50** |
| **Lakehouse ETL** | Databricks Serverless Jobs (`Trigger.AvailableNow`) | Incremental 15-min micro-batches instead of 24/7 cluster | **$85.00** |
| **Data Warehouse** | Amazon Redshift Serverless (32 Base RPUs, Auto-Pause) | Auto-pause during off-hours (~110 active RPU-hours/wk) | **$528.00** |
| **BI (AWS Native)** | Amazon QuickSight Enterprise (SPICE + Capacity Pricing) | 3 Authors ($72) + 150 Readers on session capacity | **$322.00** |
| **Security & KMS** | AWS KMS CMK + Glue Catalog | 1 CMK + <1M Glue metadata requests | **$3.00** |
| **Total Production Run-Rate** | | | **~$993.00 / mo** |
| **Local Demo Simulator (`DEMO_MODE=true`)** | Local Python + Parquet + SQLite + Streamlit | Zero cloud credentials required | **$0.00 / mo** |

### Top 4 FinOps Design Wins Built Into This Repo
1. **Free S3 VPC Gateway Endpoint** in `infra/kinesis_datalake_stack.py` eliminates `$0.045/GB` NAT Gateway charges on Redshift Spectrum and Databricks S3 scans.
2. **QuickSight SPICE Offloading** serves 95% of executive dashboard hits from in-memory SPICE, letting Redshift Serverless scale down between scheduled refreshes.
3. **`Trigger.AvailableNow` on Databricks** processes S3 Bronze-to-Silver Delta UniForm increments on batch compute pricing rather than an always-on cluster.
4. **Redshift `COMPOUND SORTKEY` + `AZ64` Encoding** prunes >85% of 1 MB blocks on date/region queries, slashing RPU compute seconds per DirectQuery hit.
