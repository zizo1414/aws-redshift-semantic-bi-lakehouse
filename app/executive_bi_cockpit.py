"""Streamlit Executive BI Cockpit — Local Interactive Replica of QuickSight & Power BI.

Demonstrates:
  1. Live Row-Level Security (RLS) Persona Switching (GLOBAL_CFO, VP_MENA, VP_EMEA, VP_AMER, VP_APAC)
  2. Live Kinesis Cloud/Labor Spike Injection into Redshift Auto-Refresh Streaming MV
  3. EVM S-Curve (Cum PV vs EV vs AC + EAC Forecast), CPI x SPI Quadrant Matrix & Monte Carlo P90
  4. Semantic Layer Rosetta Stone (Redshift SQL vs Unity Catalog Metric View vs QuickSight vs Power BI DAX)
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt  # noqa: E402
import streamlit as st  # noqa: E402

from scripts.generate_data import generate_portfolio_datasets  # noqa: E402
from src.kinesis_simulator import KinesisStreamSimulator  # noqa: E402
from src.monte_carlo_evm import run_portfolio_monte_carlo  # noqa: E402
from src.redshift_simulator import RedshiftSemanticWarehouse  # noqa: E402
from src.semantic_compiler import compile_all  # noqa: E402

ROOT_DIR = Path(__file__).resolve().parent.parent

st.set_page_config(
    page_title="Enterprise EVM & Semantic BI Lakehouse",
    layout="wide",
)


@st.cache_resource
def bootstrap_warehouse() -> RedshiftSemanticWarehouse:
    lake_dir = ROOT_DIR / "data" / "lakehouse"
    landing_jsonl = ROOT_DIR / "data" / "landing" / "kinesis_events.jsonl"
    if not landing_jsonl.exists():
        generate_portfolio_datasets()
    compile_all(check_only=False)

    kinesis = KinesisStreamSimulator()
    kinesis.ingest_jsonl_file(landing_jsonl)
    kinesis.flush_firehose_to_s3_parquet(lake_dir / "bronze" / "kinesis_events")

    wh = RedshiftSemanticWarehouse()
    wh.mount_spectrum_s3_lake(lake_dir)
    wh.refresh_kinesis_streaming_mv(kinesis.records)
    wh.execute_sp_merge_evm_snapshots()
    return wh


wh = bootstrap_warehouse()

st.sidebar.title("Governed Semantic Controls")
persona = st.sidebar.selectbox(
    "Row-Level Security (RLS) Persona",
    options=["GLOBAL_CFO", "VP_MENA", "VP_EMEA", "VP_AMER", "VP_APAC"],
    index=0,
)

st.sidebar.markdown("---")
st.sidebar.subheader("Simulate Kinesis Event Spike")
proj_df_sidebar = wh.query_project_quadrant_matrix(rls_persona=persona)
spike_proj = st.sidebar.selectbox(
    "Target Project",
    options=proj_df_sidebar["project_id"].tolist(),
)
spike_amt = st.sidebar.slider("Idle Cloud Sandbox Burn ($)", 2_000, 25_000, 6_500, step=500)
if st.sidebar.button("Inject Live Kinesis Burn Event"):
    target_reg = proj_df_sidebar.loc[
        proj_df_sidebar["project_id"] == spike_proj, "region_code"
    ].iloc[0]
    wh.inject_live_spike_and_remerge(spike_proj, str(target_reg), float(spike_amt))
    st.sidebar.success(f"Streamed ${spike_amt:,.0f} burn event to {spike_proj}!")

kpis = wh.query_executive_kpis(rls_persona=persona)
zonemap = wh.simulate_zonemap_pruning(str(kpis["snapshot_date"]), rls_persona=persona)

st.title("Enterprise Streaming Lakehouse, Semantic Layer & Executive EVM BI")
st.caption(
    f"Active RLS Persona: **{persona}** | Snapshot: **{kpis['snapshot_date']}** | "
    f"Scoped Programs: **{kpis['active_projects']}** | "
    f"Redshift 1MB Zone-Map Pruning: **{zonemap['block_skip_efficiency_pct']}% blocks skipped**"
)

# Executive KPI Ribbon
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Budget at Completion (BAC)", f"${kpis['bac_usd']:,.0f}")
c2.metric("Earned Value (EV)", f"${kpis['ev_usd']:,.0f}", f"SV: ${kpis['sv_usd']:,.0f}")
c3.metric("Actual Cost (AC)", f"${kpis['ac_total_usd']:,.0f}", f"CV: ${kpis['cv_usd']:,.0f}")
c4.metric(
    "Weighted CPI",
    f"{kpis['cpi']:.2f}",
    f"Naive AVG trap: {kpis['naive_unweighted_avg_cpi']:.2f}",
)
c5.metric("Weighted SPI", f"{kpis['spi']:.2f}", f"TCPI: {kpis['tcpi']:.2f}")
c6.metric("Estimate at Completion (EAC)", f"${kpis['eac_usd']:,.0f}", f"VAC: ${kpis['vac_usd']:,.0f}")

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "1. EVM S-Curve & Forecast",
        "2. CPI x SPI Risk Quadrant & Monte Carlo",
        "3. Global Capacity & PoC Leverage",
        "4. Kinesis Burn Watchdog & Semantic Rosetta",
    ]
)

with tab1:
    scurve_df = wh.query_scurve_timeseries(rls_persona=persona)
    melted = scurve_df.melt(
        id_vars=["snapshot_date"],
        value_vars=["pv_usd", "ev_usd", "ac_total_usd"],
        var_name="Metric",
        value_name="USD",
    )
    chart_scurve = (
        alt.Chart(melted)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("snapshot_date:T", title="Weekly Snapshot Date"),
            y=alt.Y("USD:Q", title="Cumulative Portfolio Value ($ USD)"),
            color=alt.Color("Metric:N", title="EVM Curve"),
            tooltip=["snapshot_date:T", "Metric:N", alt.Tooltip("USD:Q", format="$,.0f")],
        )
        .properties(height=380)
    )
    st.altair_chart(chart_scurve, use_container_width=True)

with tab2:
    quad_df = wh.query_project_quadrant_matrix(rls_persona=persona)
    scatter = (
        alt.Chart(quad_df)
        .mark_circle(opacity=0.85)
        .encode(
            x=alt.X("spi:Q", title="Schedule Performance Index (SPI)", scale=alt.Scale(zero=False)),
            y=alt.Y("cpi:Q", title="Cost Performance Index (CPI)", scale=alt.Scale(zero=False)),
            size=alt.Size("bac_usd:Q", title="BAC ($)"),
            color=alt.Color("quadrant:N", title="Health Quadrant"),
            tooltip=["project_id", "project_name", "region_code", "bac_usd", "cpi", "spi", "eac_usd", "tcpi"],
        )
        .properties(height=360)
    )
    st.altair_chart(scatter, use_container_width=True)
    st.subheader("Monte Carlo Probabilistic EAC Risk Table (2,000 Trials)")
    st.dataframe(run_portfolio_monte_carlo(quad_df), use_container_width=True)

with tab3:
    util_df = wh.query_regional_utilization(rls_persona=persona)
    util_melted = util_df.melt(
        id_vars=["region_code", "maturity_stage"],
        value_vars=["billable_pct", "poc_investment_pct", "bench_ramp_pct"],
        var_name="Allocation",
        value_name="Percent",
    )
    bar_util = (
        alt.Chart(util_melted)
        .mark_bar()
        .encode(
            x=alt.X("region_code:N", title="Global Delivery Region"),
            y=alt.Y("Percent:Q", title="Capacity Share (%)"),
            color=alt.Color("Allocation:N"),
            tooltip=["region_code", "maturity_stage", "Allocation", "Percent"],
        )
        .properties(height=340)
    )
    st.altair_chart(bar_util, use_container_width=True)
    st.dataframe(util_df, use_container_width=True)

with tab4:
    st.subheader("Intraday Kinesis Streaming MV Anomalies (<10s Latency Path)")
    anomalies_df = wh.query_intraday_burn_anomalies(rls_persona=persona)
    st.dataframe(anomalies_df, use_container_width=True)

    st.subheader("Semantic Layer Rosetta Stone (Compiled from `evm_semantic_model.toml`)")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Power BI DAX (`bi/powerbi/evm_measures.dax`)**")
        st.code((ROOT_DIR / "bi" / "powerbi" / "evm_measures.dax").read_text()[:1100], language="dax")
    with col_b:
        st.markdown("**Databricks Unity Catalog Metric View (`unity_catalog_metric_views.sql`)**")
        st.code(
            (ROOT_DIR / "databricks" / "sql" / "unity_catalog_metric_views.sql").read_text()[:1100],
            language="sql",
        )
