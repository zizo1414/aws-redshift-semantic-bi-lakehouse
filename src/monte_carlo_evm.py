"""Monte Carlo Probabilistic EVM Cost & Schedule Risk Forecasting Engine."""

from __future__ import annotations

import numpy as np
import pandas as pd


def run_portfolio_monte_carlo(
    df_projects: pd.DataFrame, trials: int = 2_000, seed: int = 42
) -> pd.DataFrame:
    """Simulate P10, P50, and P90 Estimate at Completion (EAC) per project."""
    rng = np.random.default_rng(seed)
    results = []

    for _, row in df_projects.iterrows():
        bac = float(row["bac_usd"])
        ev = float(row["cum_ev_usd"])
        ac = float(row["cum_ac_total_usd"])
        cpi = max(0.45, float(row["cpi"]))
        remaining_work = max(0.0, bac - ev)

        # Sample future CPI around observed CPI with 12% relative volatility
        sampled_cpi = np.clip(rng.normal(loc=cpi, scale=0.12 * cpi, size=trials), 0.35, 1.80)
        simulated_eac = ac + (remaining_work / sampled_cpi)

        prob_within_bac = float(np.mean(simulated_eac <= bac) * 100.0)
        results.append(
            {
                "project_id": row["project_id"],
                "project_name": row["project_name"],
                "region_code": row["region_code"],
                "bac_usd": round(bac, 2),
                "deterministic_eac_usd": round(float(row["eac_usd"]), 2),
                "p10_optimistic_eac_usd": round(float(np.percentile(simulated_eac, 10)), 2),
                "p50_median_eac_usd": round(float(np.percentile(simulated_eac, 50)), 2),
                "p90_conservative_eac_usd": round(float(np.percentile(simulated_eac, 90)), 2),
                "prob_on_budget_pct": round(prob_within_bac, 1),
            }
        )

    return pd.DataFrame(results).sort_values("prob_on_budget_pct", ascending=True)
