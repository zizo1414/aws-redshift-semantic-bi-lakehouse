#!/usr/bin/env python3
"""Generate synthetic multi-region Enterprise EVM baselines & Kinesis operational streams.

Models 24 enterprise Cloud, Data Lakehouse & GenAI programs across AMER, EMEA, MENA,
and APAC over 16 weekly snapshots — explicitly capturing mature delivery pods vs.
high-leverage global PoC accelerator investments in emerging regional pods (MENA).
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT_DIR = Path(__file__).resolve().parent.parent
LAKE_DIR = ROOT_DIR / "data" / "lakehouse"
LANDING_DIR = ROOT_DIR / "data" / "landing"

REGIONS: list[dict[str, Any]] = [
    {
        "region_code": "AMER",
        "region_name": "Americas",
        "maturity_stage": "Mature Scale",
        "vp_owner": "Sarah Jenkins",
        "target_utilization_pct": 80.0,
    },
    {
        "region_code": "EMEA",
        "region_name": "Europe",
        "maturity_stage": "Mature Scale",
        "vp_owner": "Marcus Weber",
        "target_utilization_pct": 78.0,
    },
    {
        "region_code": "MENA",
        "region_name": "Middle East & North Africa",
        "maturity_stage": "Strategic Growth Ramp",
        "vp_owner": "Tariq Al-Mansoor",
        "target_utilization_pct": 60.0,
    },
    {
        "region_code": "APAC",
        "region_name": "Asia-Pacific",
        "maturity_stage": "Expansion",
        "vp_owner": "Kenji Takahashi",
        "target_utilization_pct": 75.0,
    },
]

PRACTICES = [
    "Cloud EDW Migration",
    "Lakehouse & Streaming",
    "Enterprise GenAI & BI",
    "FinOps & Governance",
]


def generate_portfolio_datasets(seed: int = 42) -> dict[str, int]:
    """Create S3 Silver Parquet baselines, utilization facts, and Kinesis JSONL stream."""
    rng = random.Random(seed)
    LAKE_DIR.mkdir(parents=True, exist_ok=True)
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    base_start = date(2026, 6, 1)
    num_weeks = 16
    snapshot_dates = [base_start + timedelta(days=7 * w) for w in range(num_weeks)]

    # 1. Dimension: Regions
    df_regions = pd.DataFrame(REGIONS)
    regions_dir = LAKE_DIR / "silver" / "dim_region"
    regions_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df_regions), regions_dir / "part-000.parquet")

    # 2. Dimension: Dates
    date_rows = [
        {
            "snapshot_date": d.isoformat(),
            "fiscal_year": d.year,
            "fiscal_quarter": f"Q{((d.month - 1) // 3) + 1}",
            "month_name": d.strftime("%B"),
            "is_month_end": (d + timedelta(days=7)).month != d.month,
        }
        for d in snapshot_dates
    ]
    dates_dir = LAKE_DIR / "silver" / "dim_date"
    dates_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(date_rows)), dates_dir / "part-000.parquet")

    # 3. Silver S3 Spectrum Baselines (24 global programs with realistic risk profiles)
    project_specs = [
        # AMER (7 programs — mix of large troubled migration & efficient builds)
        ("PRJ-AMER-101", "Global Core Banking Redshift Migration", "AMER", 4_800_000.0, 0.82, 0.86, "Fixed-Price"),
        ("PRJ-AMER-102", "Retail Real-Time Kinesis Inventory Hub", "AMER", 2_200_000.0, 1.06, 1.04, "T&M"),
        ("PRJ-AMER-103", "Healthcare Claims Lakehouse Unification", "AMER", 3_500_000.0, 0.95, 0.93, "Fixed-Price"),
        ("PRJ-AMER-104", "FinTech Fraud Streaming Modernization", "AMER", 1_900_000.0, 1.08, 1.02, "T&M"),
        ("PRJ-AMER-105", "Media Telemetry Iceberg Platform", "AMER", 2_600_000.0, 0.89, 0.96, "Fixed-Price"),
        ("PRJ-AMER-106", "Insurance Actuarial Semantic BI", "AMER", 1_400_000.0, 1.03, 1.01, "T&M"),
        ("PRJ-AMER-107", "QuickSight Enterprise PoC Fast-Track", "AMER", 180_000.0, 1.45, 1.20, "Fixed-Price"),
        # EMEA (7 programs)
        ("PRJ-EMEA-201", "EU Sovereign Banking Unity Catalog Mesh", "EMEA", 3_900_000.0, 0.97, 0.95, "Fixed-Price"),
        ("PRJ-EMEA-202", "Auto IoT Connected Fleet Kinesis Lake", "EMEA", 4_200_000.0, 0.79, 0.84, "Fixed-Price"),
        ("PRJ-EMEA-203", "UK Energy Grid Forecasting Warehouse", "EMEA", 2_100_000.0, 1.04, 1.03, "T&M"),
        ("PRJ-EMEA-204", "Nordics Logistics Semantic Star Schema", "EMEA", 1_650_000.0, 1.01, 0.98, "T&M"),
        ("PRJ-EMEA-205", "Pharma R&D Document RAG + Redshift", "EMEA", 2_800_000.0, 0.93, 0.91, "Fixed-Price"),
        ("PRJ-EMEA-206", "Luxury Retail Power BI DirectQuery Hub", "EMEA", 1_300_000.0, 1.09, 1.05, "T&M"),
        ("PRJ-EMEA-207", "DACH FinOps QuickSight Accelerator", "EMEA", 220_000.0, 1.38, 1.15, "Fixed-Price"),
        # MENA (5 programs — high-efficiency flagship + global PoC accelerators)
        ("PRJ-MENA-301", "GCC Sovereign Banking Lakehouse & BI", "MENA", 3_200_000.0, 1.05, 1.02, "Fixed-Price"),
        ("PRJ-MENA-302", "Smart City Mobility Kinesis Telemetry", "MENA", 2_750_000.0, 0.94, 0.89, "Fixed-Price"),
        ("PRJ-MENA-303", "Regional Telecom Revenue Assurance DW", "MENA", 1_850_000.0, 1.02, 0.97, "T&M"),
        ("PRJ-MENA-304", "Global Enterprise EVM BI Accelerator PoC", "MENA", 350_000.0, 1.28, 1.18, "Internal-Global-PoC"),
        ("PRJ-MENA-305", "Cross-Region Cloud FinOps Semantic PoC", "MENA", 290_000.0, 1.22, 1.12, "Internal-Global-PoC"),
        # APAC (5 programs)
        ("PRJ-APAC-401", "SuperApp Payments Streaming Warehouse", "APAC", 3_600_000.0, 0.98, 1.01, "T&M"),
        ("PRJ-APAC-402", "Semiconductor Fab Yield Lakehouse", "APAC", 2_950_000.0, 0.86, 0.90, "Fixed-Price"),
        ("PRJ-APAC-403", "ANZ Retail Supply Chain QuickSight BI", "APAC", 1_500_000.0, 1.07, 1.04, "T&M"),
        ("PRJ-APAC-404", "Tokyo Wealth Management Semantic Layer", "APAC", 2_250_000.0, 0.96, 0.94, "Fixed-Price"),
        ("PRJ-APAC-405", "SEA Cross-Border Cargo Tracking Stream", "APAC", 1_450_000.0, 1.03, 0.99, "T&M"),
    ]

    projects: list[dict[str, Any]] = []
    kinesis_events: list[dict[str, Any]] = []
    utilization_rows: list[dict[str, Any]] = []
    event_seq = 10000

    for idx, (pid, pname, reg, bac, target_cpi, target_spi, ctype) in enumerate(project_specs, start=1):
        projects.append(
            {
                "project_sk": idx,
                "project_id": pid,
                "project_name": pname,
                "region_code": reg,
                "practice_area": PRACTICES[(idx - 1) % len(PRACTICES)],
                "contract_type": ctype,
                "project_manager": f"PM-{reg}-{idx:02d}",
                "bac_usd": bac,
                "start_date": base_start.isoformat(),
                "target_end_date": (base_start + timedelta(days=140)).isoformat(),
                "baseline_version": 1,
                "is_current": True,
            }
        )

        # S-curve logistic progression across 16 weeks
        prev_pv_frac = 0.0
        for w_idx, snap_date in enumerate(snapshot_dates, start=1):
            progress_t = w_idx / 20.0  # ~80% through schedule at week 16
            # Smooth S-curve shape
            pv_frac = min(0.95, round((progress_t**1.25) * 1.05, 4))
            delta_pv = max(0.005, pv_frac - prev_pv_frac)
            prev_pv_frac = pv_frac

            weekly_noise_spi = rng.uniform(0.96, 1.04)
            weekly_noise_cpi = rng.uniform(0.96, 1.04)
            ev_delta = round(bac * delta_pv * target_spi * weekly_noise_spi, 2)
            total_ac_delta = round(ev_delta / (target_cpi * weekly_noise_cpi), 2)
            cloud_share = 0.24 if target_cpi < 0.88 else 0.14
            cloud_delta = round(total_ac_delta * cloud_share, 2)
            labor_delta = round(total_ac_delta - cloud_delta, 2)

            is_poc = ctype == "Internal-Global-PoC"
            billable_hrs = 0.0 if is_poc else round(labor_delta / 145.0, 2)
            poc_hrs = round(labor_delta / 130.0, 2) if is_poc else round(billable_hrs * 0.06, 2)

            event_seq += 1
            ts_str = datetime.combine(snap_date, datetime.min.time()).replace(hour=14).isoformat()
            kinesis_events.append(
                {
                    "event_id": f"EVT-{event_seq}",
                    "event_timestamp": ts_str,
                    "project_id": pid,
                    "region_code": reg,
                    "event_type": "WEEKLY_EXECUTION_PULSE",
                    "billable_hours": billable_hrs,
                    "poc_hours": poc_hrs,
                    "labor_cost_usd": labor_delta,
                    "cloud_cost_usd": cloud_delta,
                    "ev_delta_usd": ev_delta,
                }
            )

            # Regional utilization capacity model (captures realistic MENA new-pod ramp + PoC focus)
            if reg == "MENA":
                bench_hrs = round((billable_hrs + poc_hrs) * 0.42, 2)
                poc_boost = round(poc_hrs + (billable_hrs * 0.22), 2)
                cap_hrs = round(billable_hrs + poc_boost + bench_hrs, 2)
                utilization_rows.append(
                    {
                        "work_date": snap_date.isoformat(),
                        "project_sk": idx,
                        "region_code": reg,
                        "role_title": "Solutions Architect",
                        "billable_hours": billable_hrs,
                        "poc_investment_hours": poc_boost,
                        "bench_ramp_hours": bench_hrs,
                        "capacity_hours": cap_hrs,
                    }
                )
            else:
                bench_hrs = round((billable_hrs + poc_hrs) * 0.14, 2)
                cap_hrs = round(billable_hrs + poc_hrs + bench_hrs, 2)
                utilization_rows.append(
                    {
                        "work_date": snap_date.isoformat(),
                        "project_sk": idx,
                        "region_code": reg,
                        "role_title": "Solutions Architect",
                        "billable_hours": billable_hrs,
                        "poc_investment_hours": poc_hrs,
                        "bench_ramp_hours": bench_hrs,
                        "capacity_hours": cap_hrs,
                    }
                )

    # Inject 2 explicit intraday runaway cloud burn anomalies on latest date for Watchdog Alerting
    latest_ts = datetime.combine(snapshot_dates[-1], datetime.min.time()).replace(hour=18).isoformat()
    for anomaly_pid, anomaly_reg, cloud_spike in [
        ("PRJ-AMER-101", "AMER", 3850.00),
        ("PRJ-EMEA-202", "EMEA", 2940.00),
    ]:
        event_seq += 1
        kinesis_events.append(
            {
                "event_id": f"EVT-{event_seq}",
                "event_timestamp": latest_ts,
                "project_id": anomaly_pid,
                "region_code": anomaly_reg,
                "event_type": "CLOUD_SANDBOX_IDLE_SPIKE",
                "billable_hours": 0.0,
                "poc_hours": 0.0,
                "labor_cost_usd": 0.0,
                "cloud_cost_usd": cloud_spike,
                "ev_delta_usd": 0.0,
            }
        )

    baselines_dir = LAKE_DIR / "silver" / "project_baselines"
    baselines_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(projects)), baselines_dir / "part-000.parquet")

    util_dir = LAKE_DIR / "silver" / "utilization_daily"
    util_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(pd.DataFrame(utilization_rows)), util_dir / "part-000.parquet")

    jsonl_path = LANDING_DIR / "kinesis_events.jsonl"
    with jsonl_path.open("w", encoding="utf-8") as fh:
        for evt in kinesis_events:
            fh.write(json.dumps(evt) + "\n")

    return {
        "projects": len(projects),
        "snapshots": len(snapshot_dates),
        "kinesis_events": len(kinesis_events),
        "utilization_rows": len(utilization_rows),
    }


if __name__ == "__main__":
    stats = generate_portfolio_datasets()
    print(f"[OK] Generated S3 Parquet Lake & Kinesis stream payloads: {stats}")
