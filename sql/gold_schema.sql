-- =============================================================
-- OmniFleet V003 - Gold Schema (prebuilt tables)
-- =============================================================
-- This file is run ONCE by Airflow (create_gold_schema task) or by hand.
-- It builds the things dbt does NOT build:
--   1. staging schema  (spark writes the silver tables here for dbt)
--   2. dim_date        (a normal calendar, dbt just references it)
--   3. live_* tables   (the streaming job writes alerts here for Grafana)
--
-- dbt builds the star itself (dim_vehicles, dim_drivers, dim_routes,
-- fct_trip_operations) into the public schema.
-- =============================================================

-- 1. schema that spark staging_load.py writes into
CREATE SCHEMA IF NOT EXISTS staging;


-- =============================================================
-- 2. dim_date  (prebuilt calendar)
-- the trips data spans 2022-01-01 .. 2025-05-08, so we cover
-- 2022-01-01 .. 2025-12-31 to be safe.
-- date_sk is an integer like 20240115 (YYYYMMDD) so it is easy to join.
-- =============================================================
DROP TABLE IF EXISTS dim_date CASCADE;
CREATE TABLE dim_date (
    date_sk      INT PRIMARY KEY,      -- 20240115
    full_date    DATE NOT NULL,
    year         INT,
    quarter      INT,
    month        INT,
    month_name   VARCHAR(12),
    day_of_week  VARCHAR(12),
    is_weekend   BOOLEAN
);

-- fill the calendar with a generate_series (one row per day)
INSERT INTO dim_date (date_sk, full_date, year, quarter, month, month_name, day_of_week, is_weekend)
SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INT)              AS date_sk,
    d::date                                          AS full_date,
    EXTRACT(YEAR    FROM d)::int                     AS year,
    EXTRACT(QUARTER FROM d)::int                     AS quarter,
    EXTRACT(MONTH   FROM d)::int                     AS month,
    TRIM(TO_CHAR(d, 'Month'))                        AS month_name,
    TRIM(TO_CHAR(d, 'Day'))                          AS day_of_week,
    -- in Egypt the weekend is Friday + Saturday
    (EXTRACT(DOW FROM d) IN (5, 6))                  AS is_weekend
FROM generate_series('2022-01-01'::date, '2025-12-31'::date, '1 day') AS d;


-- =============================================================
-- 3. LIVE ALERT TABLES  (written by stream_enrich.py, read by Grafana)
-- These are NOT part of the dbt star. They power the real-time
-- "Active Incident Map". One row per detected incident.
-- =============================================================

-- 3a. one row per vehicle = its CURRENT status on the map
--     stream_enrich UPSERTs this so the map always shows latest state.
DROP TABLE IF EXISTS live_vehicle_status CASCADE;
CREATE TABLE live_vehicle_status (
    vehicle_id        INT PRIMARY KEY,
    trip_id           INT,
    last_event_time   TIMESTAMP,
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    -- the highest-priority colour for this vehicle right now
    -- RED > ORANGE > YELLOW > BLUE > WHITE
    incident_color    VARCHAR(10),
    incident_label    VARCHAR(60),
    updated_at        TIMESTAMP DEFAULT NOW()
);

-- 3b. append-only feed of every critical/important incident (right sidebar)
DROP TABLE IF EXISTS live_incident_feed CASCADE;
CREATE TABLE live_incident_feed (
    incident_id       BIGSERIAL PRIMARY KEY,
    vehicle_id        INT,
    trip_id           INT,
    driver_id         INT,
    event_time        TIMESTAMP NOT NULL,
    incident_color    VARCHAR(10),      -- RED / ORANGE / YELLOW / BLUE
    incident_type     VARCHAR(40),      -- FUEL_FRAUD / DOOR_OPEN / CARGO_BREACH / SPEEDING / DRIFT / BATTERY_LOW / ENGINE_WEAR
    detail            VARCHAR(200),     -- "Fuel siphoned 16.2L" etc
    lat               DOUBLE PRECISION,
    lon               DOUBLE PRECISION,
    detected_at       TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_feed_time ON live_incident_feed (event_time DESC);

-- 3c. stateful fuel memory for the slow-sensor stream
--     keeps the previous fuel reading per vehicle so we can compute the drop.
DROP TABLE IF EXISTS live_fuel_state CASCADE;
CREATE TABLE live_fuel_state (
    vehicle_id        INT PRIMARY KEY,
    last_fuel_l       DOUBLE PRECISION,
    last_event_time   TIMESTAMP
);

-- done
