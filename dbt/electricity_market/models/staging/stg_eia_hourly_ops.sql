-- =============================================================================
-- Model   : stg_eia_hourly_ops
-- Layer   : Staging
-- Source  : raw.eia_hourly_ops
-- Purpose : Clean and type-cast EIA hourly operations data.
--           Apply basic null handling and column standardisation.
--           Materialised as a view — no additional storage cost.
-- =============================================================================

with source as (

    select * from {{ source('raw', 'eia_hourly_ops') }}

),

cleaned as (

    select
        -- Primary keys / dimensions
        period                                          as hour_utc,
        upper(trim(balancing_authority))                as balancing_authority,

        -- Demand metrics
        demand_mwh,
        demand_forecast_mwh,

        -- Supply / interchange metrics
        net_generation_mwh,
        interchange_mwh,

        -- Derived: demand-supply imbalance (positive = demand exceeds generation)
        demand_mwh - net_generation_mwh                as demand_supply_gap_mwh,

        -- Derived: forecast error (positive = actual exceeded forecast)
        demand_mwh - demand_forecast_mwh               as demand_forecast_error_mwh,

        -- Metadata
        source_system,
        loaded_at

    from source

    -- Exclude rows where the primary key components are null
    where period is not null
      and balancing_authority is not null

)

select * from cleaned
