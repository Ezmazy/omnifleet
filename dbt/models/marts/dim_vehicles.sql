-- dim_vehicles : one row per vehicle, surrogate key added.
-- source: staging.stg_vehicles (cleaned vehicles.csv)
{{ config(materialized='table') }}

select
    -- surrogate key = stable hash of the business key
    {{ make_sk(['vehicle_id']) }} as vehicle_sk,
    vehicle_id,
    model,
    payload_capacity,
    tank_capacity_l,
    fuel_type,
    -- current_mileage kept on the dim so Q2.2 (mileage bracket vs wear) works
    current_mileage
from {{ source('staging', 'stg_vehicles') }}
