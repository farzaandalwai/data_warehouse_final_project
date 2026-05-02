-- =============================================================================
-- Model   : mart_price_spike_dashboard
-- Layer   : Mart
-- Source  : mart_hourly_market_stress
-- Purpose : Dashboard-ready view of price spike events and market stress
--           conditions. Selects a curated, BI-tool-friendly column set and
--           adds pre-formatted labels and rounded metrics.
--
-- Consumers: Tableau, Power BI, or any BI tool connected via DASHBOARD_READONLY_ROLE
-- =============================================================================

with stress as (

    select * from {{ ref('mart_hourly_market_stress') }}

),

dashboard_ready as (

    select
        -- Time dimensions
        hour_utc,
        year,
        month,
        hour_of_day,
        day_of_week,
        is_weekend,

        -- Trading hub
        trading_hub,

        -- Price metrics (rounded for display)
        round(lmp_avg, 2)              as lmp_avg_usd_mwh,
        round(lmp_min, 2)              as lmp_min_usd_mwh,
        round(lmp_max, 2)              as lmp_max_usd_mwh,
        round(lmp_stddev, 2)           as lmp_stddev_usd_mwh,
        round(lmp_p95_threshold, 2)    as spike_threshold_p95,
        round(lmp_p99_threshold, 2)    as spike_threshold_p99,

        -- LMP component breakdown
        round(energy_component_avg, 2)      as energy_component_avg,
        round(congestion_component_avg, 2)  as congestion_component_avg,
        round(loss_component_avg, 2)        as loss_component_avg,

        -- Spike classification
        price_spike_flag,
        stress_severity,

        -- Grid operations context
        round(demand_mwh, 0)                as demand_mwh,
        round(demand_forecast_mwh, 0)       as demand_forecast_mwh,
        round(net_generation_mwh, 0)        as net_generation_mwh,
        round(interchange_mwh, 0)           as interchange_mwh,
        round(demand_supply_gap_mwh, 0)     as demand_supply_gap_mwh,
        round(demand_forecast_error_mwh, 0) as demand_forecast_error_mwh,

        -- Lag features for sparklines / trend charts
        round(lmp_lag_1h, 2)               as lmp_lag_1h,
        round(lmp_lag_24h, 2)              as lmp_lag_24h,
        round(lmp_lag_168h, 2)             as lmp_lag_168h,

        -- Derived: percentage deviation from p95 threshold
        case
            when lmp_p95_threshold > 0
            then round((lmp_avg - lmp_p95_threshold) / lmp_p95_threshold * 100, 1)
        end                                 as pct_above_spike_threshold

    from stress

    -- Only surface complete hours (avoid partial aggregations)
    where lmp_avg is not null

)

select * from dashboard_ready
order by hour_utc, trading_hub
