-- =============================================================
-- OmniFleet V003 - Dashboard Queries
-- Copy-paste these into Superset SQL Lab (BI charts) or
-- Grafana panel query editors (live charts).
-- Each block is labeled with the chart it powers.
-- =============================================================


-- #############################################################
-- GRAFANA  (live "Active Incident Map" - reads the live_* tables)
-- #############################################################

-- [G1] GEOMAP markers. Returns a numeric "severity" so the map can
--      colour each dot by threshold (4=red,3=orange,2=yellow,1=blue,0=white).
SELECT
    vehicle_id,
    lat,
    lon,
    CASE incident_color
        WHEN 'RED'    THEN 4
        WHEN 'ORANGE' THEN 3
        WHEN 'YELLOW' THEN 2
        WHEN 'BLUE'   THEN 1
        ELSE 0
    END AS severity,
    incident_label,
    last_event_time
FROM live_vehicle_status;

-- [G2] LIVE INCIDENT FEED table (right sidebar, newest first)
SELECT event_time, vehicle_id, incident_color, incident_type, detail
FROM live_incident_feed
ORDER BY event_time DESC
LIMIT 50;

-- [G3] FLEET SUMMARY stat tiles (bottom ticker) - returns 4 numbers
SELECT
    COUNT(*)                                                   AS total_active,
    COUNT(*) FILTER (WHERE incident_color = 'WHITE')           AS nominal,
    COUNT(*) FILTER (WHERE incident_color IN ('YELLOW','BLUE'))AS warnings,
    COUNT(*) FILTER (WHERE incident_color IN ('RED','ORANGE')) AS critical
FROM live_vehicle_status;


-- #############################################################
-- SUPERSET  (BI dashboard - reads the gold star: fct + dims)
-- For each: paste into SQL Lab, Run, then Save > Save dataset.
-- #############################################################

-- ===== DRIVER DOMAIN =====

-- [Q1.1] Top dangerous drivers  -> BAR CHART
--   chart: dimension = full_name, metric = SUM(aggressive_ticks), sort desc, row limit 5
SELECT d.full_name,
       SUM(f.speeding_ticks + f.drift_ticks) AS aggressive_ticks
FROM fct_trip_operations f
JOIN dim_drivers d ON d.driver_sk = f.driver_key_sk
GROUP BY d.full_name;

-- [Q1.2] Speeding vs engine stress  -> SCATTER PLOT
--   chart: X = speeding_ticks, Y = engine_fault_ticks (one dot per trip)
SELECT trip_id_bk, speeding_ticks, engine_fault_ticks
FROM fct_trip_operations
WHERE speeding_ticks > 0 OR engine_fault_ticks > 0;


-- ===== VEHICLE DOMAIN =====

-- [Q2.1] Battery risk pool  -> PIE / DONUT CHART
--   chart: dimension = risk_band, metric = SUM(vehicles)
SELECT risk_band, COUNT(*) AS vehicles FROM (
    SELECT f.vehicle_key_sk,
           CASE WHEN SUM(f.battery_fault_ticks) > 50 THEN 'At risk'
                ELSE 'Healthy' END AS risk_band
    FROM fct_trip_operations f
    GROUP BY f.vehicle_key_sk
) t
GROUP BY risk_band;

-- [Q2.2] Mileage bracket vs engine wear by model  -> STACKED BAR
--   chart: X = mileage_bracket, breakdown = model, metric = SUM(avg_engine_fault)
SELECT v.model,
       CASE WHEN v.current_mileage < 100000 THEN '0-100k'
            WHEN v.current_mileage < 250000 THEN '100k-250k'
            ELSE '250k+' END AS mileage_bracket,
       AVG(f.engine_fault_ticks) AS avg_engine_fault
FROM fct_trip_operations f
JOIN dim_vehicles v ON v.vehicle_sk = f.vehicle_key_sk
GROUP BY v.model, mileage_bracket;


-- ===== COLD CHAIN DOMAIN =====

-- [Q3.1] Breaches by cargo type  -> TREEMAP (or PIE)
--   chart: dimension = cargo_type, metric = SUM(breach_ticks)
SELECT cargo_type, SUM(cargo_breach_ticks) AS breach_ticks
FROM fct_trip_operations
WHERE cargo_type IS NOT NULL
GROUP BY cargo_type;

-- [Q3.2] Breach risk vs distance  -> LINE CHART (add trend line)
--   chart: X = distance_bin_km, metric = AVG(avg_breach_ticks)
SELECT FLOOR(total_distance_km / 50.0) * 50 AS distance_bin_km,
       AVG(cargo_breach_ticks) AS avg_breach_ticks
FROM fct_trip_operations
WHERE total_distance_km > 0
GROUP BY distance_bin_km
ORDER BY distance_bin_km;


-- ===== ROUTE DOMAIN =====

-- [Q4.1] Densest corridors + delay  -> TABLE or BAR
--   chart: dimensions = origin_governorate, destination_governorate;
--          metrics = SUM(trip_volume), AVG(avg_delay_min)
SELECT r.origin_governorate,
       r.destination_governorate,
       COUNT(*)              AS trip_volume,
       AVG(f.delivery_delay_min) AS avg_delay_min
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate, r.destination_governorate;

-- [Q4.2] Loading-lag bottlenecks by governorate  -> BAR CHART
--   chart: dimension = origin_governorate, metric = AVG(avg_loading_lag_min), sort desc
SELECT r.origin_governorate,
       AVG(f.loading_lag_min) AS avg_loading_lag_min
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate;


-- ===== FUEL & FINANCE DOMAIN =====

-- [Q5.1a] Total fuel exposure  -> BIG NUMBER
--   chart: metric = SUM(fuel_cost_egp)
SELECT SUM(fuel_cost_egp) AS total_fuel_cost_egp
FROM fct_trip_operations;

-- [Q5.1b] Fuel cost by governorate  -> BAR CHART
--   chart: dimension = origin_governorate, metric = SUM(fuel_cost_egp), sort desc
SELECT r.origin_governorate,
       SUM(f.fuel_cost_egp) AS fuel_cost_egp
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate;

-- [Q5.2] Net profit by route  -> BAR CHART (or Waterfall)
--   chart: dimension = corridor, metric = SUM(net_profit_egp), sort desc
SELECT r.origin_governorate || ' -> ' || r.destination_governorate AS corridor,
       SUM(f.base_cargo_type_fees + f.weight_surcharge_fees)        AS revenue_egp,
       SUM(f.fuel_cost_egp)                                         AS fuel_cost_egp,
       SUM(f.base_cargo_type_fees + f.weight_surcharge_fees - f.fuel_cost_egp) AS net_profit_egp
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY corridor;
