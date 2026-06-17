-- dim_routes : one row per route, with origin + destination depot details
-- already flattened in (staging joined depots in for us).
{{ config(materialized='table') }}

select
    {{ make_sk(['route_id']) }} as route_sk,
    route_id,
    origin_depot_id,
    origin_address,
    origin_governorate,
    origin_lat,
    origin_lon,
    destination_depot_id,
    destination_address,
    destination_governorate,
    destination_lat,
    destination_lon
from {{ source('staging', 'stg_routes') }}
