"""Unit tests verifying Governed EVM Ratio Math, Monte Carlo Risk & Row-Level Security."""

from __future__ import annotations

import unittest
from pathlib import Path

from scripts.generate_data import generate_portfolio_datasets
from src.kinesis_simulator import KinesisStreamSimulator
from src.monte_carlo_evm import run_portfolio_monte_carlo
from src.redshift_simulator import RedshiftSemanticWarehouse

ROOT_DIR = Path(__file__).resolve().parent.parent


class TestEvmSemanticMathAndRls(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        generate_portfolio_datasets(seed=42)
        cls.lake_dir = ROOT_DIR / "data" / "lakehouse"
        cls.landing_jsonl = ROOT_DIR / "data" / "landing" / "kinesis_events.jsonl"

        kinesis = KinesisStreamSimulator()
        kinesis.ingest_jsonl_file(cls.landing_jsonl)
        cls.wh = RedshiftSemanticWarehouse()
        cls.wh.mount_spectrum_s3_lake(cls.lake_dir)
        cls.wh.refresh_kinesis_streaming_mv(kinesis.records)
        cls.wh.execute_sp_merge_evm_snapshots()

    def test_weighted_cpi_diverges_from_naive_avg_cpi_trap(self) -> None:
        """Prove why a Semantic Layer is required: weighted SUM(EV)/SUM(AC) != AVG(CPI)."""
        cfo = self.wh.query_executive_kpis(rls_persona="GLOBAL_CFO")
        self.assertEqual(cfo["active_projects"], 24)
        expected_weighted_cpi = round(cfo["ev_usd"] / cfo["ac_total_usd"], 4)
        self.assertAlmostEqual(cfo["cpi"], expected_weighted_cpi, places=4)
        # Small high-CPI PoCs inflate naive unweighted AVG(CPI) significantly above true portfolio CPI!
        self.assertGreater(cfo["naive_unweighted_avg_cpi"] - cfo["cpi"], 0.04)

    def test_eac_vac_and_tcpi_identities(self) -> None:
        cfo = self.wh.query_executive_kpis(rls_persona="GLOBAL_CFO")
        self.assertAlmostEqual(cfo["cv_usd"], round(cfo["ev_usd"] - cfo["ac_total_usd"], 2), places=1)
        self.assertAlmostEqual(cfo["sv_usd"], round(cfo["ev_usd"] - cfo["pv_usd"], 2), places=1)
        self.assertAlmostEqual(cfo["eac_usd"], round(cfo["bac_usd"] / cfo["cpi"], 2), places=0)
        self.assertAlmostEqual(cfo["vac_usd"], round(cfo["bac_usd"] - cfo["eac_usd"], 2), places=1)
        self.assertGreater(cfo["tcpi"], 0.5)

    def test_rls_strictly_isolates_regional_personas(self) -> None:
        cfo = self.wh.query_executive_kpis(rls_persona="GLOBAL_CFO")
        mena = self.wh.query_executive_kpis(rls_persona="VP_MENA")
        emea = self.wh.query_executive_kpis(rls_persona="VP_EMEA")

        self.assertEqual(mena["active_projects"], 5)
        self.assertEqual(emea["active_projects"], 7)
        self.assertLess(mena["bac_usd"], cfo["bac_usd"])
        # MENA pod demonstrates higher Global PoC Investment % than global average
        self.assertGreater(mena["poc_investment_pct"], cfo["poc_investment_pct"])

        quad_mena = self.wh.query_project_quadrant_matrix(rls_persona="VP_MENA")
        self.assertEqual(set(quad_mena["region_code"].unique()), {"MENA"})

        with self.assertRaises(PermissionError):
            self.wh.query_executive_kpis(rls_persona="UNAUTHORIZED_ROLE")

    def test_monte_carlo_percentile_ordering(self) -> None:
        quad = self.wh.query_project_quadrant_matrix(rls_persona="GLOBAL_CFO")
        mc = run_portfolio_monte_carlo(quad, trials=500, seed=7)
        self.assertEqual(len(mc), 24)
        for _, row in mc.iterrows():
            self.assertLessEqual(row["p10_optimistic_eac_usd"], row["p50_median_eac_usd"])
            self.assertLessEqual(row["p50_median_eac_usd"], row["p90_conservative_eac_usd"])
            self.assertGreaterEqual(row["prob_on_budget_pct"], 0.0)
            self.assertLessEqual(row["prob_on_budget_pct"], 100.0)


if __name__ == "__main__":
    unittest.main()
