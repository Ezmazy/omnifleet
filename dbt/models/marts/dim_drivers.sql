-- dim_drivers : one row per driver.
{{ config(materialized='table') }}

select
    {{ make_sk(['driver_id']) }} as driver_sk,
    driver_id,
    first_name,
    last_name,
    -- full name is handy for the dashboard "top dangerous drivers" chart
    first_name || ' ' || last_name as full_name,
    license_class,
    phone_number
from {{ source('staging', 'stg_drivers') }}
