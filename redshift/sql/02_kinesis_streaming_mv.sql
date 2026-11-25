-- =============================================================================
-- 02_kinesis_streaming_mv.sql
-- Zero-ETL Hot Streaming Path: Amazon Redshift Streaming Ingestion directly
-- over Amazon Kinesis Data Streams (<10s latency, zero Lambda/Glue middleman).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

CREATE EXTERNAL SCHEMA IF NOT EXISTS kinesis_evm
FROM KINESIS
IAM_ROLE default;

-- Auto-refreshing Incremental Materialized View parsing JSON Kinesis records
-- into strongly-typed Redshift columns via the native SUPER data type.
CREATE MATERIALIZED VIEW gold.mv_live_project_burn
AUTO REFRESH YES
DISTSTYLE KEY
DISTKEY (project_id)
SORTKEY (ingested_at, region_code)
AS
SELECT
    approximate_arrival_timestamp                         AS ingested_at,
    partition_key                                         AS shard_partition_key,
    sequence_number                                       AS kinesis_seq_no,
    JSON_PARSE(kinesis_data)                              AS raw_event,
    raw_event.event_id::VARCHAR(64)                       AS event_id,
    raw_event.event_timestamp::TIMESTAMP                  AS event_timestamp,
    raw_event.project_id::VARCHAR(32)                     AS project_id,
    raw_event.region_code::VARCHAR(16)                    AS region_code,
    raw_event.event_type::VARCHAR(32)                     AS event_type,
    COALESCE(raw_event.billable_hours::DECIMAL(10,2), 0)  AS billable_hours,
    COALESCE(raw_event.poc_hours::DECIMAL(10,2), 0)       AS poc_hours,
    COALESCE(raw_event.labor_cost_usd::DECIMAL(14,2), 0)  AS labor_cost_usd,
    COALESCE(raw_event.cloud_cost_usd::DECIMAL(14,2), 0)  AS cloud_cost_usd,
    COALESCE(raw_event.ev_delta_usd::DECIMAL(14,2), 0)    AS ev_delta_usd
FROM kinesis_evm."evm-operational-events-stream"
WHERE CAN_JSON_PARSE(kinesis_data);

-- Intraday Burn Watchdog View for DirectQuery QuickSight & Power BI Alert Tiles
CREATE OR REPLACE VIEW gold.v_intraday_burn_anomalies AS
SELECT
    m.project_id,
    m.region_code,
    DATE_TRUNC('hour', m.event_timestamp)                 AS burn_hour,
    SUM(m.labor_cost_usd + m.cloud_cost_usd)              AS hourly_actual_burn_usd,
    SUM(m.cloud_cost_usd)                                 AS hourly_cloud_burn_usd,
    SUM(m.ev_delta_usd)                                   AS hourly_ev_unlocked_usd,
    CASE
        WHEN SUM(m.cloud_cost_usd) > 1500
             AND SUM(m.ev_delta_usd) = 0 THEN 'CRITICAL_IDLE_CLOUD_BURN'
        WHEN SUM(m.labor_cost_usd + m.cloud_cost_usd) > 5000 THEN 'HIGH_VELOCITY_BURN'
        ELSE 'NORMAL'
    END                                                   AS burn_alert_status
FROM gold.mv_live_project_burn m
GROUP BY 1, 2, 3;
