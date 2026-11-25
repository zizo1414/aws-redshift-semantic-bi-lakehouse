-- =============================================================================
-- 04_sp_merge_evm_and_vacuum.sql
-- Pure Native Redshift Transformation & Table Maintenance (Zero-dbt Pattern):
--   • Idempotent Staging DELETE + INSERT merge inside an explicit transaction
--   • Blends S3 Spectrum project baselines with Kinesis Streaming MV deltas
--   • Runs ANALYZE & checks SVV_TABLE_INFO unsorted_pct health
-- =============================================================================

CREATE OR REPLACE PROCEDURE gold.sp_merge_daily_evm_snapshot(p_snapshot_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Build temp staging table distributed identically to target fact (zero shuffle!)
    DROP TABLE IF EXISTS #stg_evm_daily;
    CREATE TEMP TABLE #stg_evm_daily
    DISTKEY (project_sk)
    SORTKEY (snapshot_date, region_code, project_sk)
    AS
    WITH stream_deltas AS (
        SELECT
            project_id,
            SUM(labor_cost_usd)                       AS delta_labor_usd,
            SUM(cloud_cost_usd)                       AS delta_cloud_usd,
            SUM(ev_delta_usd)                         AS delta_ev_usd
        FROM gold.mv_live_project_burn
        WHERE DATE(event_timestamp) <= p_snapshot_date
        GROUP BY project_id
    )
    SELECT
        p_snapshot_date                               AS snapshot_date,
        b.project_sk                                  AS project_sk,
        b.region_code                                 AS region_code,
        b.bac_usd                                     AS bac_usd,
        ROUND(
            b.bac_usd * LEAST(
                1.0,
                GREATEST(0.0, DATEDIFF(day, b.start_date, p_snapshot_date)::DECIMAL
                / NULLIF(DATEDIFF(day, b.start_date, b.target_end_date), 0))
            ),
            2
        )                                             AS cum_pv_usd,
        COALESCE(s.delta_ev_usd, 0)                   AS cum_ev_usd,
        COALESCE(s.delta_labor_usd, 0)                AS cum_ac_labor_usd,
        COALESCE(s.delta_cloud_usd, 0)                AS cum_ac_cloud_usd,
        COALESCE(s.delta_labor_usd, 0) + COALESCE(s.delta_cloud_usd, 0) AS cum_ac_total_usd,
        ROUND(COALESCE(s.delta_ev_usd, 0) / NULLIF(b.bac_usd, 0), 4)    AS pct_complete
    FROM spectrum_lake.ext_project_baselines_iceberg b
    LEFT JOIN stream_deltas s
        ON b.project_id = s.project_id
    WHERE b.is_current = TRUE;

    -- 2. Atomic Idempotent Replace for p_snapshot_date
    DELETE FROM gold.fact_evm_daily
    WHERE snapshot_date = p_snapshot_date;

    INSERT INTO gold.fact_evm_daily
    SELECT * FROM #stg_evm_daily;

    DROP TABLE IF EXISTS #stg_evm_daily;
END;
$$;

-- Table Health & Zone-Map Maintenance Diagnostics Query
-- Run periodically via Amazon Redshift Scheduled Queries:
-- VACUUM SORT ONLY gold.fact_evm_daily TO 99 PERCENT;
-- ANALYZE gold.fact_evm_daily PREDICATE COLUMNS;
SELECT
    "schema",
    "table",
    diststyle,
    sortkey1,
    size AS size_mb,
    tbl_rows,
    unsorted AS unsorted_pct,
    stats_off AS stats_staleness_pct
FROM svv_table_info
WHERE "schema" = 'gold'
ORDER BY size_mb DESC;
