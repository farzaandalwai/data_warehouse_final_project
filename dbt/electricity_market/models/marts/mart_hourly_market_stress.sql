-- =============================================================================
-- Model   : mart_hourly_market_stress
-- Layer   : Mart
-- Source  : int_hourly_market_features
-- Purpose : Analytics-ready table that classifies each hour as a price spike
--           event based on the 95th percentile LMP threshold per trading hub.
--
-- Key outputs:
--   lmp_p95_threshold  — 95th percentile LMP by trading hub (rolling baseline)
--   price_spike_flag   — 1 if lmp_avg exceeds the hub's p95 threshold, else 0
--   stress_severity    — Categorical label: NORMAL / ELEVATED / SPIKE / EXTREME
-- =============================================================================

with features as (

    select * from {{ ref('int_hourly_market_features') }}

),

-- Compute the 95th percentile LMP threshold per trading hub across the full
-- historical dataset. This acts as the static spike detection threshold.
-- TODO: Consider replacing with a rolling 30-day or 90-day percentile window
--       for a time-aware threshold that adapts to seasonal patterns.
hub_thresholds as (

    select
        trading_hub,
        percentile_cont(0.95) within group (order by lmp_avg) as lmp_p95_threshold,
        percentile_cont(0.99) within group (order by lmp_avg) as lmp_p99_threshold,
        avg(lmp_avg)                                          as lmp_historical_mean,
        stddev(lmp_avg)                                       as lmp_historical_stddev
    from features
    group by trading_hub

),

-- Join thresholds back to the hourly feature table and apply spike flags
classified as (

    select
        f.hour_utc,
        f.trading_hub,

        -- LMP metrics
        f.lmp_avg,
        f.lmp_min,
        f.lmp_max,
        f.lmp_stddev,

        -- Component breakdown
        f.energy_component_avg,
        f.congestion_component_avg,
        f.loss_component_avg,

        -- Thresholds
        t.lmp_p95_threshold,
        t.lmp_p99_threshold,
        t.lmp_historical_mean,
        t.lmp_historical_stddev,

        -- Price spike flag: 1 = spike detected
        case when f.lmp_avg >= t.lmp_p95_threshold then 1 else 0 end as price_spike_flag,

        -- Stress severity label
        case
            when f.lmp_avg >= t.lmp_p99_threshold               then 'EXTREME'
            when f.lmp_avg >= t.lmp_p95_threshold               then 'SPIKE'
            when f.lmp_avg >= t.lmp_historical_mean
                              + t.lmp_historical_stddev          then 'ELEVATED'
            else                                                      'NORMAL'
        end as stress_severity,

        -- Grid operations context
        f.demand_mwh,
        f.demand_forecast_mwh,
        f.net_generation_mwh,
        f.interchange_mwh,
        f.demand_supply_gap_mwh,
        f.demand_forecast_error_mwh,

        -- Lag features (available for downstream ML scoring)
        f.lmp_lag_1h,
        f.lmp_lag_24h,
        f.lmp_lag_168h,

        -- Calendar features
        f.hour_of_day,
        f.day_of_week,
        f.is_weekend,
        f.month,
        f.year

    from features f
    left join hub_thresholds t
        on f.trading_hub = t.trading_hub

)

select * from classified
