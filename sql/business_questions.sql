-- =============================================================
-- OmniFleet V003 - Business Question Queries (BI dashboard)
-- Run against the gold star in postgres "public" schema.
-- These map 1:1 to the dashboard blueprint.
-- =============================================================

-- ===== 1. DRIVER DOMAIN =====

-- Q1.1 Top 5 most dangerous drivers (aggressive ticks = speeding + drift)
SELECT d.driver_id, d.full_name,
       SUM(f.speeding_ticks) AS speeding,
       SUM(f.drift_ticks)    AS drift,
       SUM(f.speeding_ticks + f.drift_ticks) AS aggressive_ticks
FROM fct_trip_operations f
JOIN dim_drivers d ON d.driver_sk = f.driver_key_sk
GROUP BY d.driver_id, d.full_name
ORDER BY aggressive_ticks DESC
LIMIT 5;

-- Q1.2 Throttle/speeding vs engine stress (scatter: one dot per trip)
SELECT trip_id_bk, speeding_ticks, engine_fault_ticks
FROM fct_trip_operations
WHERE speeding_ticks > 0 OR engine_fault_ticks > 0;


-- ===== 2. VEHICLE DOMAIN =====

-- Q2.1 Vehicles with critical battery degradation (fault ticks > 50)
SELECT COUNT(DISTINCT f.vehicle_key_sk) AS vehicles_at_risk
FROM fct_trip_operations f
WHERE f.battery_fault_ticks > 50;

-- (companion: list them)
SELECT v.vehicle_id, v.model, SUM(f.battery_fault_ticks) AS battery_fault_ticks
FROM fct_trip_operations f
JOIN dim_vehicles v ON v.vehicle_sk = f.vehicle_key_sk
GROUP BY v.vehicle_id, v.model
HAVING SUM(f.battery_fault_ticks) > 50
ORDER BY battery_fault_ticks DESC;

-- Q2.2 Mileage bracket vs avg engine wear, by model
SELECT v.model,
       CASE
         WHEN v.current_mileage < 100000 THEN '0-100k'
         WHEN v.current_mileage < 250000 THEN '100k-250k'
         ELSE '250k+'
       END AS mileage_bracket,
       ROUND(AVG(f.engine_fault_ticks)::numeric, 1) AS avg_engine_fault_ticks
FROM fct_trip_operations f
JOIN dim_vehicles v ON v.vehicle_sk = f.vehicle_key_sk
GROUP BY v.model, mileage_bracket
ORDER BY v.model, mileage_bracket;


-- ===== 3. COLD CHAIN DOMAIN =====

-- Q3.1 Cold-chain breaches by cargo type
SELECT cargo_type, SUM(cargo_breach_ticks) AS breach_ticks
FROM fct_trip_operations
WHERE cargo_type IS NOT NULL
GROUP BY cargo_type
ORDER BY breach_ticks DESC;

-- Q3.2 Breach risk vs haul distance (50 km bins)
SELECT FLOOR(total_distance_km / 50.0) * 50 AS distance_bin_km,
       ROUND(AVG(cargo_breach_ticks)::numeric, 2) AS avg_breach_ticks,
       COUNT(*) AS trips
FROM fct_trip_operations
WHERE total_distance_km > 0
GROUP BY distance_bin_km
ORDER BY distance_bin_km;


-- ===== 4. ROUTE DOMAIN =====

-- Q4.1 Highest-density corridors + their avg delivery delay
SELECT r.origin_governorate, r.destination_governorate,
       COUNT(*) AS trip_volume,
       ROUND(AVG(f.delivery_delay_min)::numeric, 1) AS avg_delay_min
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate, r.destination_governorate
ORDER BY trip_volume DESC
LIMIT 15;

-- Q4.2 Depot loading-lag bottlenecks by origin governorate
SELECT r.origin_governorate,
       ROUND(AVG(f.loading_lag_min)::numeric, 1) AS avg_loading_lag_min,
       COUNT(*) AS trips
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate
ORDER BY avg_loading_lag_min DESC;


-- ===== 5. FUEL & FINANCE DOMAIN =====

-- Q5.1 Fuel-fraud leakage in EGP, grouped by origin governorate
-- (trips whose fuel consumption is abnormally high flag potential theft;
--  here we report fuel cost as the financial exposure)
SELECT r.origin_governorate,
       ROUND(SUM(f.fuel_cost_egp)::numeric, 0) AS fuel_cost_egp,
       ROUND(SUM(f.total_fuel_consumed_l)::numeric, 0) AS liters
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate
ORDER BY fuel_cost_egp DESC;

-- Q5.2 Net profit by route = (base fees + weight surcharge) - fuel cost
SELECT r.origin_governorate, r.destination_governorate,
       ROUND(SUM(f.base_cargo_type_fees + f.weight_surcharge_fees)::numeric, 0) AS revenue_egp,
       ROUND(SUM(f.fuel_cost_egp)::numeric, 0) AS fuel_cost_egp,
       ROUND(SUM(f.base_cargo_type_fees + f.weight_surcharge_fees - f.fuel_cost_egp)::numeric, 0) AS net_profit_egp
FROM fct_trip_operations f
JOIN dim_routes r ON r.route_sk = f.route_key_sk
GROUP BY r.origin_governorate, r.destination_governorate
ORDER BY net_profit_egp DESC
LIMIT 15;


-- ===== STREAMING / LIVE (Grafana Active Incident Map) =====

-- current incident state per vehicle (map dots)
SELECT vehicle_id, lat, lon, incident_color, incident_label, last_event_time
FROM live_vehicle_status;

-- live incident feed (right sidebar, newest first)
SELECT event_time, vehicle_id, incident_color, incident_type, detail
FROM live_incident_feed
ORDER BY event_time DESC
LIMIT 50;

-- fleet summary ticker
SELECT
  COUNT(*) AS total_active,
  COUNT(*) FILTER (WHERE incident_color = 'WHITE') AS nominal,
  COUNT(*) FILTER (WHERE incident_color IN ('YELLOW','BLUE')) AS warnings,
  COUNT(*) FILTER (WHERE incident_color IN ('RED','ORANGE')) AS critical
FROM live_vehicle_status;
