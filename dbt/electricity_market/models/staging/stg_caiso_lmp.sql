-- =============================================================================
-- Model   : stg_caiso_lmp
-- Layer   : Staging
-- Source  : raw.caiso_lmp_5min
-- Purpose : Aggregate 5-minute CAISO LMP data to hourly granularity by
--           trading hub and market type.
--           Computes mean, min, max, and standard deviation of LMP within
--           each hour to capture intra-hour volatility.
--           Materialised as a view.
-- =============================================================================

with source as (

    select * from {{ source('raw', 'caiso_lmp_5min') }}

),

-- Truncate interval_start to the hour to create the grouping key
hourly_buckets as (

    select
        date_trunc('hour', interval_start)  as hour_utc,
        upper(trim(trading_hub))            as trading_hub,
        upper(trim(market))                 as market,

        -- LMP aggregates within the hour
        avg(lmp)                            as lmp_avg,
        min(lmp)                            as lmp_min,
        max(lmp)                            as lmp_max,
        stddev(lmp)                         as lmp_stddev,

        -- Component averages (energy, congestion, loss)
        avg(energy_component)               as energy_component_avg,
        avg(congestion_component)           as congestion_component_avg,
        avg(loss_component)                 as loss_component_avg,

        -- Count of 5-min intervals aggregated (expect 12 per complete hour)
        count(*)                            as interval_count,

        -- Metadata
        source_system,
        max(loaded_at)                      as loaded_at

    from source

    where interval_start is not null
      and trading_hub is not null

    group by 1, 2, 3, source_system

)

select * from hourly_buckets
