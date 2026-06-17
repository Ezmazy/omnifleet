-- =============================================================
-- fct_trip_operations : ONE ROW PER TRIP (the gold fact)
-- =============================================================
-- Grain: one trip (trip_id is the degenerate dimension / business key).
-- It pulls together three things per trip:
--   1. the trip row itself (vehicle, driver, route, times)  -> stg_trips
--   2. the cargo rollup (weight, value, fees)               -> stg_trip_cargo_agg
--   3. the telemetry rollup (tick counts + fuel consumed)   -> stg_trip_telemetry_agg
-- and then DERIVES the money columns using the tunable rates below.
-- =============================================================
{{ config(
    materialized='incremental',
    unique_key='trip_sk',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

-- -------------------------------------------------------------
-- TUNABLE BUSINESS RATES  (change these freely, no real source data)
-- diesel ~18 EGP/L is a 2025-ish Egypt pump price midpoint.
-- base fees + surcharge are made-up but sensible defaults.
-- -------------------------------------------------------------
{% set diesel_price_per_l   = 18.0 %}   -- EGP per liter of diesel
{% set surcharge_rate_per_kg = 2.5 %}   -- EGP per kg of cargo (weight surcharge)

-- per cargo-type base handling fee (EGP per trip carrying that type)
-- cold / sensitive cargo costs more to handle.
{% set base_fee_case = "
    case
        when cargo_type = 'Deep frozen'        then 1200.0
        when cargo_type = 'Frozen'             then  800.0
        when cargo_type = 'Pharmaceutical'     then 1000.0
        when cargo_type = 'Chilled'            then  500.0
        when cargo_type = 'Controlled ambient' then  300.0
        else 250.0
    end
" %}

with trips as (
    select * from {{ source('staging', 'stg_trips') }}
    {% if is_incremental() and var('target_date', none) is not none %}
    -- daily run: only the trips that STARTED on the target date
    where actual_start_time::date = '{{ var('target_date') }}'::date
    {% endif %}
),

cargo_agg as (
    select * from {{ source('staging', 'stg_trip_cargo_agg') }}
),

telem_agg as (
    select * from {{ source('staging', 'stg_trip_telemetry_agg') }}
),

-- join the three per-trip inputs together
joined as (
    select
        t.trip_id,
        t.vehicle_id,
        t.driver_id,
        t.route_id,
        t.scheduled_start_time,
        t.scheduled_end_time,
        t.actual_start_time,
        t.actual_end_time,

        -- cargo rollup (may be null if a trip had no cargo rows)
        c.cargo_type,
        coalesce(c.total_cargo_weight_kg, 0) as total_cargo_weight_kg,
        coalesce(c.total_cargo_value_egp, 0) as total_cargo_value_egp,

        -- telemetry rollup (may be null if no sensor data for that trip)
        coalesce(tel.quick_pings_count,   0) as quick_pings_count,
        coalesce(tel.slow_pings_count,    0) as slow_pings_count,
        coalesce(tel.engine_fault_ticks,  0) as engine_fault_ticks,
        coalesce(tel.speeding_ticks,      0) as speeding_ticks,
        coalesce(tel.drift_ticks,         0) as drift_ticks,
        coalesce(tel.battery_fault_ticks, 0) as battery_fault_ticks,
        coalesce(tel.door_open_ticks,     0) as door_open_ticks,
        coalesce(tel.cargo_breach_ticks,  0) as cargo_breach_ticks,
        coalesce(tel.total_fuel_consumed_l, 0) as total_fuel_consumed_l,
        coalesce(tel.total_distance_km,     0) as total_distance_km
    from trips t
    left join cargo_agg c on c.trip_id = t.trip_id
    left join telem_agg tel on tel.trip_id = t.trip_id
)

select
    -- surrogate + degenerate keys
    {{ make_sk(['trip_id']) }} as trip_sk,
    trip_id as trip_id_bk,

    -- dimension foreign keys (surrogates, hashed the same way as the dims)
    {{ make_sk(['vehicle_id']) }} as vehicle_key_sk,
    {{ make_sk(['driver_id']) }}  as driver_key_sk,
    {{ make_sk(['route_id']) }}   as route_key_sk,
    -- date key maps to actual_start_time (YYYYMMDD int) -> dim_date.date_sk
    cast(to_char(actual_start_time, 'YYYYMMDD') as int)    as start_date_key_sk,

    -- keep the degenerate cargo_type on the fact for easy cold-chain grouping
    cargo_type,

    -- ---- operational / telemetry measures ----
    total_distance_km,
    quick_pings_count,
    slow_pings_count,
    engine_fault_ticks,
    speeding_ticks,
    drift_ticks,
    battery_fault_ticks,
    door_open_ticks,
    cargo_breach_ticks,

    -- delay measures (handy for the route domain questions)
    extract(epoch from (actual_end_time   - scheduled_end_time))   / 60.0 as delivery_delay_min,
    extract(epoch from (actual_start_time - scheduled_start_time)) / 60.0 as loading_lag_min,

    -- ---- fuel + money measures (DERIVED with the tunable rates) ----
    total_fuel_consumed_l,
    round( (total_fuel_consumed_l * {{ diesel_price_per_l }})::numeric, 2) as fuel_cost_egp,

    total_cargo_weight_kg,
    total_cargo_value_egp,

    -- base handling fee depends on cargo type
    round( ({{ base_fee_case }})::numeric, 2)                          as base_cargo_type_fees,
    -- weight surcharge = weight * rate
    round( (total_cargo_weight_kg * {{ surcharge_rate_per_kg }})::numeric, 2) as weight_surcharge_fees,

    -- total trip cost = fuel + base fee + weight surcharge
    round( (
        (total_fuel_consumed_l * {{ diesel_price_per_l }})
        + ({{ base_fee_case }})
        + (total_cargo_weight_kg * {{ surcharge_rate_per_kg }})
    )::numeric, 2) as total_trip_cost_egp

from joined
