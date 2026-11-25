# AWS Well-Architected Framework Review (6 Pillars)

| Pillar | Architectural Controls Implemented |
| :--- | :--- |
| **1. Operational Excellence** | 100% Infrastructure-as-Code (AWS CDK + Databricks Asset Bundles), QuickSight-as-Code, automated CI drift validation (`semantic_compiler.py --check`), and one-command local teardown (`make destroy-aws` / `make clean`). |
| **2. Security** | Customer-Managed KMS encryption across S3, Kinesis, and Redshift Serverless; `BlockPublicAccess.BLOCK_ALL` + TLS enforcement; private isolated VPC subnets (`publicly_accessible=False`); multi-layer Row-Level Security across Redshift RLS, Unity Catalog Row Filters, QuickSight RLS, and Power BI Roles. |
| **3. Reliability** | Kinesis 7-day replay retention + Firehose S3 Bronze immutable archive + Dead-Letter error prefix; idempotent `event_id` stream deduplication and staging `DELETE + INSERT` warehouse merges. |
| **4. Performance Efficiency** | Sub-10s Redshift Streaming Ingestion Materialized Views (`SUPER` JSON parsing); `DISTSTYLE ALL` dimensions + `DISTKEY(project_sk)` facts + `COMPOUND SORTKEY` 1 MB Zone-Map pruning; Power BI Composite Mode & QuickSight SPICE. |
| **5. Cost Optimization** | Zero NAT Gateways (S3 Gateway Endpoint); Kinesis On-Demand; Databricks `Trigger.AvailableNow`; Delta UniForm eliminating duplicate Iceberg/Delta S3 copies. |
| **6. Sustainability** | Columnar Parquet/ZSTD/AZ64 compression, Zone-Map block skipping, and serverless scale-to-zero compute minimize wasted CPU cycles and storage footprint. |
