#!/usr/bin/env python3
"""End-to-End Offline Runner: Kinesis -> S3 Parquet Lake -> Redshift Star Schema & Semantic BI."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.generate_data import generate_portfolio_datasets  # noqa: E402
from src.kinesis_simulator import KinesisStreamSimulator  # noqa: E402
from src.monte_carlo_evm import run_portfolio_monte_carlo  # noqa: E402
from src.redshift_simulator import RedshiftSemanticWarehouse  # noqa: E402
from src.semantic_compiler import compile_all  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent


def run_pipeline() -> dict[str, object]:
    """Execute complete streaming-to-semantic-BI simulation and print executive readout."""
    lake_dir = ROOT_DIR / "data" / "lakehouse"
    landing_jsonl = ROOT_DIR / "data" / "landing" / "kinesis_events.jsonl"
    if not landing_jsonl.exists():
        generate_portfolio_datasets()

    compiled_files = compile_all(check_only=False)

    # 1. Kinesis Stream Ingestion + Firehose S3 Bronze Parquet Archival
    kinesis = KinesisStreamSimulator(stream_name="evm-operational-events-stream", num_shards=4)
    stream_count = kinesis.ingest_jsonl_file(landing_jsonl)
    bronze_parquet = kinesis.flush_firehose_to_s3_parquet(lake_dir / "bronze" / "kinesis_events")

    # 2. Mount S3 Spectrum Parquet Lake + Refresh Redshift Streaming MV + Merge Star Schema
    warehouse = RedshiftSemanticWarehouse()
    warehouse.mount_spectrum_s3_lake(lake_dir)
    mv_inserted = warehouse.refresh_kinesis_streaming_mv(kinesis.records)
    fact_rows = warehouse.execute_sp_merge_evm_snapshots()

    # 3. Query Global CFO vs. Regional MENA Persona + Zone-Map Diagnostics + Monte Carlo
    cfo_kpis = warehouse.query_executive_kpis(rls_persona="GLOBAL_CFO")
    mena_kpis = warehouse.query_executive_kpis(rls_persona="VP_MENA")
    zonemap = warehouse.simulate_zonemap_pruning(
        target_date=str(cfo_kpis["snapshot_date"]), rls_persona="VP_MENA"
    )
    quadrant_df = warehouse.query_project_quadrant_matrix(rls_persona="GLOBAL_CFO")
    mc_df = run_portfolio_monte_carlo(quadrant_df)
    anomalies = warehouse.query_intraday_burn_anomalies(rls_persona="GLOBAL_CFO")

    print("=" * 82)
    print(" AWS KINESIS + S3 DATA LAKE + REDSHIFT SEMANTIC BI LAKEHOUSE (ZERO-DBT RUNNER)")
    print("=" * 82)
    print(f" • Semantic Artifacts Compiled : {len(compiled_files)} (Redshift, UC, QuickSight, Power BI)")
    print(f" • Kinesis Stream Events       : {stream_count} routed across 4 shards -> {bronze_parquet.name}")
    print(f" • Redshift Streaming MV Rows  : {mv_inserted} (<10s auto-refresh simulation)")
    print(f" • Gold Star Schema Fact Rows  : {fact_rows} (DISTKEY=project_sk, COMPOUND SORTKEY)")
    print(
        f" • Redshift 1MB Zone-Map Skip  : {zonemap['pruned_blocks']}/{zonemap['total_1mb_blocks']} "
        f"blocks skipped ({zonemap['block_skip_efficiency_pct']}% I/O saved for VP_MENA)"
    )
    print("-" * 82)
    print(
        f" [GLOBAL_CFO] BAC=${cfo_kpis['bac_usd']:,.0f} | EV=${cfo_kpis['ev_usd']:,.0f} | "
        f"AC=${cfo_kpis['ac_total_usd']:,.0f} | Weighted CPI={cfo_kpis['cpi']:.3f} "
        f"(vs Naive AVG CPI={cfo_kpis['naive_unweighted_avg_cpi']:.3f}!) | EAC=${cfo_kpis['eac_usd']:,.0f}"
    )
    print(
        f" [VP_MENA]    Scoped Projects={mena_kpis['active_projects']} | Weighted CPI={mena_kpis['cpi']:.3f} | "
        f"Billable={mena_kpis['billable_utilization_pct']:.1f}% | "
        f"Global PoC Investment={mena_kpis['poc_investment_pct']:.1f}%"
    )
    print(f" [WATCHDOG]   Active Intraday Cloud Burn Anomalies Detected: {len(anomalies)}")
    print(f" [MONTE CARLO] Highest Overrun Risk Program: {mc_df.iloc[0]['project_id']} "
          f"(P90 EAC=${mc_df.iloc[0]['p90_conservative_eac_usd']:,.0f})")
    print("=" * 82)

    return {
        "cfo_kpis": cfo_kpis,
        "mena_kpis": mena_kpis,
        "zonemap": zonemap,
        "anomalies_count": len(anomalies),
    }


if __name__ == "__main__":
    run_pipeline()
