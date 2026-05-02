-- =============================================================================
-- Model   : int_hourly_market_features
-- Layer   : Intermediate
-- Sources : stg_caiso_lmp, stg_eia_hourly_ops
-- Purpose : Join hourly CAISO LMP data with EIA grid operations data and
--           engineer time-series lag features for the forecasting models.
--
-- Lag features computed (real-time market LMP):
--   lmp_lag_1h    — LMP from 1 hour prior   (same-day short-term signal)
--   lmp_lag_24h   — LMP from 24 hours prior  (same-time-yesterday signal)
--   lmp_lag_168h  — LMP from 168 hours prior (same-time-last-week signal)
--
-- Calendar features:
--   hour_of_day   — 0–23
--   day_of_week   — 0=Monday … 6=Sunday
--   is_weekend    — boolean flag
--   month         — 1–12
-- =============================================================================

with caiso as (

    select * from {{ ref('stg_caiso_lmp') }}
    where market = 'RTM'   -- Focus on real-time market for feature engineering

),

eia as (

    select * from {{ ref('stg_eia_hourly_ops') }}
    where balancing_authority = 'CISO'

),

-- Join LMP with grid operations on the hour
joined as (

    select
        caiso.hour_utc,
        caiso.trading_hub,

        -- LMP metrics
        caiso.lmp_avg,
        caiso.lmp_min,
        caiso.lmp_max,
        caiso.lmp_stddev,
        caiso.energy_component_avg,
        caiso.congestion_component_avg,
        caiso.loss_component_avg,

        -- EIA grid operations
        eia.demand_mwh,
        eia.demand_forecast_mwh,
        eia.net_generation_mwh,
        eia.interchange_mwh,
        eia.demand_supply_gap_mwh,
        eia.demand_forecast_error_mwh

    from caiso
    left join eia
        on caiso.hour_utc = eia.hour_utc

),

-- Compute lag features using LAG window function partitioned by trading hub
with_lag_features as (

    select
        *,

        -- Short-term lag: 1 hour prior
        lag(lmp_avg, 1) over (
            partition by trading_hub
            order by hour_utc
        ) as lmp_lag_1h,

        -- Same-time-yesterday: 24 hours prior
        lag(lmp_avg, 24) over (
            partition by trading_hub
            order by hour_utc
        ) as lmp_lag_24h,

        -- Same-time-last-week: 168 hours prior
        lag(lmp_avg, 168) over (
            partition by trading_hub
            order by hour_utc
        ) as lmp_lag_168h,

        -- Calendar features
        hour(hour_utc)                          as hour_of_day,
        dayofweek(hour_utc)                     as day_of_week,   -- 0=Mon in Snowflake
        case when dayofweek(hour_utc) >= 5
             then true else false end           as is_weekend,
        month(hour_utc)                         as month,
        year(hour_utc)                          as year

    from joined

)

select * from with_lag_features
