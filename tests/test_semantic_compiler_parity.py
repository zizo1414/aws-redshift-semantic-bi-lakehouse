"""Tests verifying Zero-Drift Semantic Layer Compilation & Zero-dbt Enforcement."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from src.semantic_compiler import compile_all, load_semantic_spec

ROOT_DIR = Path(__file__).resolve().parent.parent


class TestSemanticCompilerParity(unittest.TestCase):
    def test_compile_all_and_zero_drift_check(self) -> None:
        outputs = compile_all(check_only=False)
        self.assertEqual(len(outputs), 7)
        # Second pass in --check mode must pass with zero drift
        checked = compile_all(check_only=True)
        self.assertEqual(len(checked), 7)

    def test_powerbi_bim_and_quicksight_specs_match_canonical_metrics(self) -> None:
        compile_all(check_only=False)
        spec = load_semantic_spec()
        expected_labels = {
            m["label"] for m in spec["base_measures"] + spec["derived_metrics"]
        }

        bim = json.loads(
            (ROOT_DIR / "bi" / "powerbi" / "EVM_Semantic_Model.bim").read_text(encoding="utf-8")
        )
        fact_table = next(t for t in bim["model"]["tables"] if t["name"] == "fact_evm_daily")
        bim_measure_names = {m["name"] for m in fact_table["measures"]}
        self.assertEqual(expected_labels, bim_measure_names)

        # Verify all 5 RLS roles compiled into Power BI Tabular Model
        bim_roles = {r["name"] for r in bim["model"]["roles"]}
        self.assertEqual(
            bim_roles, {"GLOBAL_CFO", "VP_MENA", "VP_EMEA", "VP_AMER", "VP_APAC"}
        )

        # Verify QuickSight calculated fields include every derived EVM metric
        qs = json.loads(
            (ROOT_DIR / "bi" / "quicksight" / "dataset_spice_directquery_rls.json").read_text(
                encoding="utf-8"
            )
        )
        qs_calc_keys = {c["metricKey"] for c in qs["datasets"][0]["calculatedFields"]}
        expected_derived_keys = {m["name"] for m in spec["derived_metrics"]}
        self.assertEqual(qs_calc_keys, expected_derived_keys)

    def test_zero_dbt_dependencies_in_project(self) -> None:
        req = (ROOT_DIR / "requirements.txt").read_text(encoding="utf-8").lower()
        req_dev = (ROOT_DIR / "requirements-dev.txt").read_text(encoding="utf-8").lower()
        self.assertNotIn("dbt", req)
        self.assertNotIn("dbt", req_dev)
        self.assertFalse((ROOT_DIR / "dbt_project.yml").exists())


if __name__ == "__main__":
    unittest.main()
