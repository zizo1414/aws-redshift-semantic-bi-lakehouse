"""Tests for Kinesis shard routing, idempotency, Redshift Zone Maps & live burn spikes."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts.generate_data import generate_portfolio_datasets
from src.kinesis_simulator import KinesisStreamSimulator
from src.redshift_simulator import RedshiftSemanticWarehouse

ROOT_DIR = Path(__file__).resolve().parent.parent


class TestKinesisAndRedshiftSimulator(unittest.TestCase):
    def test_kinesis_deterministic_sharding_and_idempotent_dedup(self) -> None:
        sim = KinesisStreamSimulator(num_shards=4)
        evt = {
            "event_id": "EVT-DUP-001",
            "event_timestamp": "2026-09-01T12:00:00",
            "project_id": "PRJ-MENA-301",
            "region_code": "MENA",
            "event_type": "WEEKLY_EXECUTION_PULSE",
            "labor_cost_usd": 1200.0,
            "cloud_cost_usd": 300.0,
            "ev_delta_usd": 1600.0,
        }
        r1 = sim.put_record(evt)
        r2 = sim.put_record(evt)  # Duplicate retry on same partition key
        self.assertEqual(r1.shard_id, r2.shard_id)

        with tempfile.TemporaryDirectory() as tmp:
            out_pq = sim.flush_firehose_to_s3_parquet(Path(tmp))
            self.assertTrue(out_pq.exists())

        wh = RedshiftSemanticWarehouse()
        inserted = wh.refresh_kinesis_streaming_mv([r1, r2])
        self.assertEqual(inserted, 1)
        wh.close()

    def test_redshift_zonemap_pruning_and_live_spike_watchdog(self) -> None:
        generate_portfolio_datasets(seed=42)
        lake_dir = ROOT_DIR / "data" / "lakehouse"
        landing_jsonl = ROOT_DIR / "data" / "landing" / "kinesis_events.jsonl"

        sim = KinesisStreamSimulator()
        sim.ingest_jsonl_file(landing_jsonl)
        wh = RedshiftSemanticWarehouse()
        wh.mount_spectrum_s3_lake(lake_dir)
        wh.refresh_kinesis_streaming_mv(sim.records)
        wh.execute_sp_merge_evm_snapshots()

        latest = str(wh.query_executive_kpis("GLOBAL_CFO")["snapshot_date"])
        zm = wh.simulate_zonemap_pruning(latest, rls_persona="VP_MENA", rows_per_block=24)
        self.assertGreaterEqual(zm["block_skip_efficiency_pct"], 85.0)

        before_anomalies = len(wh.query_intraday_burn_anomalies("VP_MENA"))
        wh.inject_live_spike_and_remerge("PRJ-MENA-301", "MENA", 9200.0)
        after_anomalies = len(wh.query_intraday_burn_anomalies("VP_MENA"))
        self.assertEqual(after_anomalies, before_anomalies + 1)
        wh.close()


if __name__ == "__main__":
    unittest.main()
