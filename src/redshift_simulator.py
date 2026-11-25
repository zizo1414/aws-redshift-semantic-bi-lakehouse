"""Local Amazon Redshift Serverless Engine: Streaming MV, Spectrum, Star Schema & RLS.

Simulates:
  1. Zero-ETL Redshift Streaming Ingestion (`mv_live_project_burn` over Kinesis)
  2. Redshift Spectrum external queries directly over S3 Parquet baselines
  3. Gold Star Schema (`DISTKEY` / `COMPOUND SORTKEY`) & 1 MB Zone-Map block skipping
  4. Governed Semantic Layer weighted EVM rollups + Row-Level Security (RLS) enforcement
"""

from __future__ import annotations

import json
import sqlite3
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from src.kinesis_simulator import KinesisRecord

ROOT_DIR = Path(__file__).resolve().parent.parent
SPEC_PATH = ROOT_DIR / "semantic" / "evm_semantic_model.toml"


class RedshiftSemanticWarehouse:
    """In-memory / SQLite-backed simulator of Amazon Redshift Star Schema + Semantic RLS."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        with SPEC_PATH.open("rb") as fh:
            self.semantic_spec: dict[str, Any] = tomllib.load(fh)
        self.rls_map: dict[str, list[str]] = {
            p["role"]: list(p["allowed_regions"]) for p in self.semantic_spec["rls_personas"]
        }
        self._init_star_schema()

    def _init_star_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dim_region (
                region_code TEXT PRIMARY KEY,
                region_name TEXT NOT NULL,
                maturity_stage TEXT NOT NULL,
                vp_owner TEXT NOT NULL,
                target_utilization_pct REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_date (
                snapshot_date TEXT PRIMARY KEY,
                fiscal_year INTEGER NOT NULL,
                fiscal_quarter TEXT NOT NULL,
                month_name TEXT NOT NULL,
                is_month_end INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS dim_project (
                project_sk INTEGER PRIMARY KEY,
                project_id TEXT UNIQUE NOT NULL,
                project_name TEXT NOT NULL,
                region_code TEXT NOT NULL,
                practice_area TEXT NOT NULL,
                contract_type TEXT NOT NULL,
                project_manager TEXT NOT NULL,
                bac_usd REAL NOT NULL,
                start_date TEXT NOT NULL,
                target_end_date TEXT NOT NULL,
                is_current INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mv_live_project_burn (
                event_id TEXT PRIMARY KEY,
                shard_id TEXT NOT NULL,
                sequence_number TEXT NOT NULL,
                event_timestamp TEXT NOT NULL,
                project_id TEXT NOT NULL,
                region_code TEXT NOT NULL,
                event_type TEXT NOT NULL,
                billable_hours REAL NOT NULL,
                poc_hours REAL NOT NULL,
                labor_cost_usd REAL NOT NULL,
                cloud_cost_usd REAL NOT NULL,
                ev_delta_usd REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS fact_evm_daily (
                snapshot_date TEXT NOT NULL,
                project_sk INTEGER NOT NULL,
                region_code TEXT NOT NULL,
                bac_usd REAL NOT NULL,
                cum_pv_usd REAL NOT NULL,
                cum_ev_usd REAL NOT NULL,
                cum_ac_labor_usd REAL NOT NULL,
                cum_ac_cloud_usd REAL NOT NULL,
                cum_ac_total_usd REAL NOT NULL,
                pct_complete REAL NOT NULL,
                PRIMARY KEY (snapshot_date, project_sk)
            );

            CREATE TABLE IF NOT EXISTS fact_utilization_daily (
                work_date TEXT NOT NULL,
                project_sk INTEGER NOT NULL,
                region_code TEXT NOT NULL,
                role_title TEXT NOT NULL,
                billable_hours REAL NOT NULL,
                poc_investment_hours REAL NOT NULL,
                bench_ramp_hours REAL NOT NULL,
                capacity_hours REAL NOT NULL,
                PRIMARY KEY (work_date, project_sk, role_title)
            );
            """
        )

    def mount_spectrum_s3_lake(self, lake_dir: Path) -> None:
        """Load S3 Silver Parquet tables via simulated Redshift Spectrum external scan."""
        df_reg = pq.read_table(lake_dir / "silver" / "dim_region" / "part-000.parquet").to_pandas()
        df_date = pq.read_table(lake_dir / "silver" / "dim_date" / "part-000.parquet").to_pandas()
        df_proj = pq.read_table(
            lake_dir / "silver" / "project_baselines" / "part-000.parquet"
        ).to_pandas()
        df_util = pq.read_table(
            lake_dir / "silver" / "utilization_daily" / "part-000.parquet"
        ).to_pandas()

        df_reg.to_sql("dim_region", self.conn, if_exists="replace", index=False)
        df_date.to_sql("dim_date", self.conn, if_exists="replace", index=False)
        df_proj.drop(columns=["baseline_version"], errors="ignore").to_sql(
            "dim_project", self.conn, if_exists="replace", index=False
        )
        df_util.to_sql("fact_utilization_daily", self.conn, if_exists="replace", index=False)

    def refresh_kinesis_streaming_mv(self, records: list[KinesisRecord]) -> int:
        """Incrementally refresh `mv_live_project_burn` from Kinesis stream records (<10s path)."""
        inserted = 0
        with self.conn:
            for rec in records:
                payload = json.loads(rec.kinesis_data)
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO mv_live_project_burn (
                        event_id, shard_id, sequence_number, event_timestamp,
                        project_id, region_code, event_type, billable_hours,
                        poc_hours, labor_cost_usd, cloud_cost_usd, ev_delta_usd
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["event_id"],
                        rec.shard_id,
                        rec.sequence_number,
                        payload["event_timestamp"],
                        payload["project_id"],
                        payload["region_code"],
                        payload["event_type"],
                        float(payload.get("billable_hours", 0.0)),
                        float(payload.get("poc_hours", 0.0)),
                        float(payload.get("labor_cost_usd", 0.0)),
                        float(payload.get("cloud_cost_usd", 0.0)),
                        float(payload.get("ev_delta_usd", 0.0)),
                    ),
                )
                inserted += cur.rowcount
        return inserted

    def execute_sp_merge_evm_snapshots(self) -> int:
        """Execute idempotent Gold Star Schema merge across all snapshot dates (zero-dbt)."""
        dates = [
            r[0]
            for r in self.conn.execute(
                "SELECT snapshot_date FROM dim_date ORDER BY snapshot_date"
            ).fetchall()
        ]
        projects = pd.read_sql_query("SELECT * FROM dim_project WHERE is_current = 1", self.conn)
        events = pd.read_sql_query("SELECT * FROM mv_live_project_burn", self.conn)
        events["event_date"] = events["event_timestamp"].str.slice(0, 10)

        rows: list[dict[str, Any]] = []
        for idx, snap_str in enumerate(dates, start=1):
            progress_t = idx / 20.0
            pv_frac = min(0.95, round((progress_t**1.25) * 1.05, 4))
            evts_upto = events[events["event_date"] <= snap_str]
            grouped = (
                evts_upto.groupby("project_id", as_index=False)[
                    ["labor_cost_usd", "cloud_cost_usd", "ev_delta_usd"]
                ].sum()
                if not evts_upto.empty
                else pd.DataFrame(
                    columns=["project_id", "labor_cost_usd", "cloud_cost_usd", "ev_delta_usd"]
                )
            )
            merged = projects.merge(grouped, on="project_id", how="left").fillna(0.0)

            for _, proj in merged.iterrows():
                bac = float(proj["bac_usd"])
                cum_pv = round(bac * pv_frac, 2)
                cum_ev = round(float(proj["ev_delta_usd"]), 2)
                cum_labor = round(float(proj["labor_cost_usd"]), 2)
                cum_cloud = round(float(proj["cloud_cost_usd"]), 2)
                cum_ac = round(cum_labor + cum_cloud, 2)
                rows.append(
                    {
                        "snapshot_date": snap_str,
                        "project_sk": int(proj["project_sk"]),
                        "region_code": str(proj["region_code"]),
                        "bac_usd": bac,
                        "cum_pv_usd": cum_pv,
                        "cum_ev_usd": cum_ev,
                        "cum_ac_labor_usd": cum_labor,
                        "cum_ac_cloud_usd": cum_cloud,
                        "cum_ac_total_usd": cum_ac,
                        "pct_complete": round(cum_ev / bac, 4) if bac > 0 else 0.0,
                    }
                )

        df_fact = pd.DataFrame(rows).sort_values(
            ["snapshot_date", "region_code", "project_sk"]
        )
        with self.conn:
            self.conn.execute("DELETE FROM fact_evm_daily")
            df_fact.to_sql("fact_evm_daily", self.conn, if_exists="append", index=False)
        return len(df_fact)

    def simulate_zonemap_pruning(
        self, target_date: str, rls_persona: str = "GLOBAL_CFO", rows_per_block: int = 24
    ) -> dict[str, Any]:
        """Simulate Redshift 1 MB Zone-Map block skipping enabled by COMPOUND SORTKEY."""
        df_sorted = pd.read_sql_query(
            "SELECT snapshot_date, region_code FROM fact_evm_daily "
            "ORDER BY snapshot_date, region_code, project_sk",
            self.conn,
        )
        allowed_regions = set(self.rls_map[rls_persona])
        total_blocks = 0
        scanned_blocks = 0
        for start in range(0, len(df_sorted), rows_per_block):
            chunk = df_sorted.iloc[start : start + rows_per_block]
            total_blocks += 1
            min_d, max_d = chunk["snapshot_date"].min(), chunk["snapshot_date"].max()
            chunk_regions = set(chunk["region_code"].unique())
            if min_d <= target_date <= max_d and (chunk_regions & allowed_regions):
                scanned_blocks += 1

        pruned = total_blocks - scanned_blocks
        return {
            "total_1mb_blocks": total_blocks,
            "scanned_blocks": scanned_blocks,
            "pruned_blocks": pruned,
            "block_skip_efficiency_pct": round(100.0 * pruned / max(1, total_blocks), 1),
        }

    def _validate_persona(self, persona: str) -> str:
        if persona not in self.rls_map:
            raise PermissionError(f"Unknown RLS persona: {persona}")
        regions = self.rls_map[persona]
        return ", ".join(f"'{r}'" for r in regions)

    def query_executive_kpis(
        self, rls_persona: str = "GLOBAL_CFO", snapshot_date: str | None = None
    ) -> dict[str, float]:
        """Query Governed Semantic KPIs strictly honoring Row-Level Security & weighted ratios."""
        regions_in = self._validate_persona(rls_persona)
        if snapshot_date is None:
            snapshot_date = self.conn.execute(
                "SELECT MAX(snapshot_date) FROM fact_evm_daily"
            ).fetchone()[0]

        row = self.conn.execute(
            f"""
            SELECT
                COUNT(DISTINCT project_sk) AS active_projects,
                SUM(bac_usd) AS bac_usd,
                SUM(cum_pv_usd) AS pv_usd,
                SUM(cum_ev_usd) AS ev_usd,
                SUM(cum_ac_labor_usd) AS ac_labor_usd,
                SUM(cum_ac_cloud_usd) AS ac_cloud_usd,
                SUM(cum_ac_total_usd) AS ac_total_usd,
                AVG(cum_ev_usd / NULLIF(cum_ac_total_usd, 0)) AS naive_unweighted_avg_cpi
            FROM fact_evm_daily
            WHERE snapshot_date = ?
              AND region_code IN ({regions_in})
            """,
            (snapshot_date,),
        ).fetchone()

        bac = float(row[1] or 0.0)
        pv = float(row[2] or 0.0)
        ev = float(row[3] or 0.0)
        ac_labor = float(row[4] or 0.0)
        ac_cloud = float(row[5] or 0.0)
        ac = float(row[6] or 0.0)
        naive_cpi = float(row[7] or 0.0)

        cpi = round(ev / ac, 4) if ac > 0 else 0.0
        spi = round(ev / pv, 4) if pv > 0 else 0.0
        eac = round(bac / cpi, 2) if cpi > 0 else bac
        eac_comp = round(ac + ((bac - ev) / (cpi * spi)), 2) if (cpi * spi) > 0 else bac
        vac = round(bac - eac, 2)
        tcpi = round((bac - ev) / (bac - ac), 4) if (bac - ac) > 0 else 9.99

        util_row = self.conn.execute(
            f"""
            SELECT
                SUM(billable_hours),
                SUM(poc_investment_hours),
                SUM(capacity_hours)
            FROM fact_utilization_daily
            WHERE region_code IN ({regions_in})
            """
        ).fetchone()
        bill_h = float(util_row[0] or 0.0)
        poc_h = float(util_row[1] or 0.0)
        cap_h = float(util_row[2] or 1.0)

        return {
            "snapshot_date": snapshot_date,
            "active_projects": int(row[0] or 0),
            "bac_usd": round(bac, 2),
            "pv_usd": round(pv, 2),
            "ev_usd": round(ev, 2),
            "ac_labor_usd": round(ac_labor, 2),
            "ac_cloud_usd": round(ac_cloud, 2),
            "ac_total_usd": round(ac, 2),
            "cv_usd": round(ev - ac, 2),
            "sv_usd": round(ev - pv, 2),
            "cpi": cpi,
            "naive_unweighted_avg_cpi": round(naive_cpi, 4),
            "spi": spi,
            "eac_usd": eac,
            "eac_composite_usd": eac_comp,
            "vac_usd": vac,
            "tcpi": tcpi,
            "billable_utilization_pct": round(100.0 * bill_h / cap_h, 2),
            "poc_investment_pct": round(100.0 * poc_h / cap_h, 2),
        }

    def query_scurve_timeseries(self, rls_persona: str = "GLOBAL_CFO") -> pd.DataFrame:
        regions_in = self._validate_persona(rls_persona)
        return pd.read_sql_query(
            f"""
            SELECT
                snapshot_date,
                ROUND(SUM(bac_usd), 2) AS bac_usd,
                ROUND(SUM(cum_pv_usd), 2) AS pv_usd,
                ROUND(SUM(cum_ev_usd), 2) AS ev_usd,
                ROUND(SUM(cum_ac_total_usd), 2) AS ac_total_usd
            FROM fact_evm_daily
            WHERE region_code IN ({regions_in})
            GROUP BY snapshot_date
            ORDER BY snapshot_date
            """,
            self.conn,
        )

    def query_project_quadrant_matrix(self, rls_persona: str = "GLOBAL_CFO") -> pd.DataFrame:
        regions_in = self._validate_persona(rls_persona)
        latest = self.conn.execute("SELECT MAX(snapshot_date) FROM fact_evm_daily").fetchone()[0]
        df = pd.read_sql_query(
            f"""
            SELECT
                p.project_id,
                p.project_name,
                f.region_code,
                p.practice_area,
                p.contract_type,
                f.bac_usd,
                f.cum_pv_usd,
                f.cum_ev_usd,
                f.cum_ac_total_usd,
                ROUND(f.cum_ev_usd / NULLIF(f.cum_ac_total_usd, 0), 3) AS cpi,
                ROUND(f.cum_ev_usd / NULLIF(f.cum_pv_usd, 0), 3) AS spi,
                ROUND((f.bac_usd - f.cum_ev_usd) / NULLIF(f.bac_usd - f.cum_ac_total_usd, 0), 3) AS tcpi
            FROM fact_evm_daily f
            INNER JOIN dim_project p ON f.project_sk = p.project_sk
            WHERE f.snapshot_date = ?
              AND f.region_code IN ({regions_in})
            ORDER BY cpi ASC
            """,
            self.conn,
            params=(latest,),
        )
        df["eac_usd"] = (df["bac_usd"] / df["cpi"]).round(2)
        df["vac_usd"] = (df["bac_usd"] - df["eac_usd"]).round(2)

        def _classify(r: pd.Series) -> str:
            if r["cpi"] >= 1.0 and r["spi"] >= 1.0:
                return "Q1: Optimal (Under Budget & Ahead)"
            if r["cpi"] >= 0.92 and r["spi"] >= 0.92:
                return "Q2: Healthy Watchlist"
            if r["cpi"] < 0.92 and r["spi"] >= 0.92:
                return "Q3: Cost Overrun Risk"
            if r["cpi"] >= 0.92 and r["spi"] < 0.92:
                return "Q4: Schedule Slippage"
            return "CRITICAL: Dual Cost & Schedule Breach"

        df["quadrant"] = df.apply(_classify, axis=1)
        return df

    def query_regional_utilization(self, rls_persona: str = "GLOBAL_CFO") -> pd.DataFrame:
        regions_in = self._validate_persona(rls_persona)
        return pd.read_sql_query(
            f"""
            SELECT
                u.region_code,
                r.region_name,
                r.maturity_stage,
                r.target_utilization_pct,
                ROUND(100.0 * SUM(u.billable_hours) / NULLIF(SUM(u.capacity_hours), 0), 1) AS billable_pct,
                ROUND(100.0 * SUM(u.poc_investment_hours) / NULLIF(SUM(u.capacity_hours), 0), 1) AS poc_investment_pct,
                ROUND(100.0 * SUM(u.bench_ramp_hours) / NULLIF(SUM(u.capacity_hours), 0), 1) AS bench_ramp_pct
            FROM fact_utilization_daily u
            INNER JOIN dim_region r ON u.region_code = r.region_code
            WHERE u.region_code IN ({regions_in})
            GROUP BY u.region_code, r.region_name, r.maturity_stage, r.target_utilization_pct
            ORDER BY billable_pct DESC
            """,
            self.conn,
        )

    def query_intraday_burn_anomalies(self, rls_persona: str = "GLOBAL_CFO") -> pd.DataFrame:
        regions_in = self._validate_persona(rls_persona)
        return pd.read_sql_query(
            f"""
            SELECT
                event_id,
                event_timestamp,
                project_id,
                region_code,
                event_type,
                labor_cost_usd,
                cloud_cost_usd,
                ev_delta_usd,
                CASE
                    WHEN cloud_cost_usd > 1500 AND ev_delta_usd = 0 THEN 'CRITICAL_IDLE_CLOUD_BURN'
                    WHEN (labor_cost_usd + cloud_cost_usd) > 35000 THEN 'HIGH_VELOCITY_BURN'
                    ELSE 'NORMAL'
                END AS burn_alert_status
            FROM mv_live_project_burn
            WHERE region_code IN ({regions_in})
              AND (
                  (cloud_cost_usd > 1500 AND ev_delta_usd = 0)
                  OR event_type = 'CLOUD_SANDBOX_IDLE_SPIKE'
              )
            ORDER BY event_timestamp DESC
            """,
            self.conn,
        )

    def inject_live_spike_and_remerge(
        self, project_id: str, region_code: str, cloud_spike_usd: float
    ) -> KinesisRecord:
        """Helper used by Streamlit Cockpit to inject a live Kinesis burn anomaly."""
        latest_date = self.conn.execute("SELECT MAX(snapshot_date) FROM dim_date").fetchone()[0]
        ts = f"{latest_date}T21:15:00"
        rec = KinesisRecord(
            shard_id="shardId-000000000001",
            partition_key=project_id,
            sequence_number=datetime.now().strftime("%Y%m%d%H%M%S%f"),
            kinesis_data=json.dumps(
                {
                    "event_id": f"LIVE-{datetime.now().strftime('%H%M%S%f')}",
                    "event_timestamp": ts,
                    "project_id": project_id,
                    "region_code": region_code,
                    "event_type": "CLOUD_SANDBOX_IDLE_SPIKE",
                    "billable_hours": 0.0,
                    "poc_hours": 0.0,
                    "labor_cost_usd": 0.0,
                    "cloud_cost_usd": cloud_spike_usd,
                    "ev_delta_usd": 0.0,
                }
            ),
        )
        self.refresh_kinesis_streaming_mv([rec])
        self.execute_sp_merge_evm_snapshots()
        return rec

    def close(self) -> None:
        self.conn.close()
