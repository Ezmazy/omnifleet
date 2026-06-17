import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# Enforce stable random seed across all vector math operations
random.seed(42)
np.random.seed(42)

print("Initializing High-Velocity Telemetry Simulation Engine...")

# Create isolated target output directories to keep your workspace organized
os.makedirs("quick_sensors", exist_ok=True)
os.makedirs("slow_sensors", exist_ok=True)

# ==========================================
# 1. OPTIMIZED CACHING & DATA LOOKUPS
# ==========================================
print("Loading core dimensional structures into memory caches...")
try:
    df_vehicles = pd.read_csv("vehicles.csv")
    df_depots = pd.read_csv("depots.csv")
    df_routes = pd.read_csv("routes.csv")
    df_trips = pd.read_csv("trips.csv")
    df_trip_cargo = pd.read_csv("trip_cargo.csv")
    df_cargo = pd.read_csv("cargo.csv")
except FileNotFoundError as e:
    print(f"Error: {e}. Ensure all previous generation scripts have run successfully!")
    exit()

# Map vehicles properties for high-speed indexing
vehicle_lookup = df_vehicles.set_index('vehicle_id').to_dict('index')

# Map depots coordinates
depot_geo = df_depots.set_index('depot_id')[['lat', 'lon']].to_dict('index')

# Map routes to extract origin/destination details instantly
route_lookup = df_routes.set_index('route_id')[['origin_depot_id', 'destination_depot_id']].to_dict('index')

# Pre-aggregate cargo profiles per trip to optimize processing times
print("Compiling thermal safe-zones per trip...")
trip_cargo_joined = df_trip_cargo.merge(df_cargo, on='cargo_id', how='left')
trip_thermal_bounds = trip_cargo_joined.groupby('trip_id').agg({
    'min_temp': 'min',
    'max_temp': 'max'
}).to_dict('index')

# Group trips by vehicle to stream calculation loops one vehicle file at a time
trips_by_vehicle = {v_id: x for v_id, x in df_trips.groupby('vehicle_id')}

print("Data caches ready. Commencing parallelized pipeline simulation...")

# ==========================================
# 2. SECTOR SIMULATION STREAMING LOOP
# ==========================================
# Looping safely from vehicle 0 up to 499
for v_id in range(500):
    if v_id not in trips_by_vehicle:
        continue
        
    v_trips = trips_by_vehicle[v_id].sort_values(by="actual_start_time")
    v_config = vehicle_lookup.get(v_id, {"tank_capacity_l": 80, "current_mileage": 100})
    
    # Initialize baseline running parameters for the vehicle
    cumulative_odometer = float(v_config["current_mileage"])
    current_fuel = float(v_config["tank_capacity_l"])
    
    quick_records = []
    slow_records = []
    
    # Trigger deterministic vehicle-level anomalies (e.g., bad alternator/battery cells)
    has_faulty_battery = (v_id % 17 == 0) # 17 is a prime number to naturally space out the anomalies
    has_engine_wear = (v_id % 13 == 0)    # Sustained high RPM profile
    
    for _, trip_row in v_trips.iterrows():
        t_id = int(trip_row['trip_id'])
        r_id = int(trip_row['route_id'])
        
        # Pull trip spatial coordinates
        r_info = route_lookup[r_id]
        orig_id = r_info['origin_depot_id']
        dest_id = r_info['destination_depot_id']
        
        lat_start, lon_start = depot_geo[orig_id]['lat'], depot_geo[orig_id]['lon']
        lat_end, lon_end = depot_geo[dest_id]['lat'], depot_geo[dest_id]['lon']
        
        # Pull trip temporal bounds
        t_start = datetime.strptime(trip_row['actual_start_time'], '%Y-%m-%d %H:%M:%S')
        t_end = datetime.strptime(trip_row['actual_end_time'], '%Y-%m-%d %H:%M:%S')
        
        duration_sec = int((t_end - t_start).total_seconds())
        if duration_sec <= 0:
            duration_sec = 3600 # Fallback default to 1 hour
            
        # Extract package thermal thresholds (default to standard chilled bounds if empty)
        t_bounds = trip_thermal_bounds.get(t_id, {'min_temp': 0.0, 'max_temp': 8.0})
        min_allowed_temp = t_bounds['min_temp']
        max_allowed_temp = t_bounds['max_temp']
        
        # Trigger deterministic trip-level anomalies
        has_aggressive_driver = (t_id % 11 == 0) # Hard cornering, speeding, and erratic braking
        has_fuel_thief = (t_id % 45 == 0)        # Sudden siphoning/leakage drops
        has_reefer_failure = (t_id % 29 == 0)    # Cooling breakdown breach
        has_security_breach = (t_id % 53 == 0)   # Door unlocked on open highway
        
        # -----------------------------------------------------------------
        # COMPILING QUICK SENSORS GENERATION (3-Second Ticks)
# -----------------------------------------------------------------
        # To avoid system lockups on ultra-long hauls, cap individual trip ticks to 4 hours
        quick_ticks = min(duration_sec, 14400) 
        
        for sec_offset in range(0, quick_ticks, 3):
            tick_time = t_start + timedelta(seconds=sec_offset)
            progress_ratio = sec_offset / duration_sec
            
            # Linear GPS navigation trace moving smoothly along Egyptian corridors
            current_lat = lat_start + (lat_end - lat_start) * progress_ratio
            current_lon = lon_start + (lon_end - lon_start) * progress_ratio
            
            # Odometer calculation incremental addition
            distance_added = (0.058 * random.uniform(0.9, 1.1)) # Scaled approximation
            cumulative_odometer += distance_added
            
            # 1. RPM Simulation & Anomaly Logic
            if has_engine_wear:
                rpm = random.randint(4200, 5600) # Constant high RPM strain -> Maintenance Warning
            else:
                rpm = random.randint(1800, 2900) if random.random() > 0.1 else random.randint(900, 1200)
                
            # 2. Throttle & Acceleration Simulation (Aggressive Driver Checks)
            if has_aggressive_driver and random.random() < 0.15:
                throttle = random.randint(85, 100)
                accel = round(random.uniform(0.75, 1.25), 2) # High G-force spikes -> Bad Driver Behavior
            else:
                throttle = random.randint(15, 55)
                accel = round(random.uniform(0.02, 0.28), 2)
                
            # 3. Battery Voltage Simulation
            if has_faulty_battery and random.random() < 0.20:
                battery = round(random.uniform(10.2, 11.4), 2) # Critical low voltage drop
            else:
                battery = round(random.uniform(13.6, 14.2), 2)
                
            quick_records.append([
                v_id, v_id, tick_time.strftime('%Y-%m-%d %H:%M:%S'),
                round(current_lat, 6), round(current_lon, 6),
                round(cumulative_odometer, 2), rpm, throttle, accel, battery
            ])
            
        # -----------------------------------------------------------------
        # COMPILING SLOW SENSORS GENERATION (1-Minute Ticks)
# -----------------------------------------------------------------
        slow_ticks = range(0, duration_sec, 60)
        fuel_loss_per_min = (v_config["tank_capacity_l"] / 450.0) # Burn index calculation
        
        for min_offset in slow_ticks:
            tick_time = t_start + timedelta(seconds=min_offset)
            progress_ratio = min_offset / duration_sec
            
            # 1. Fuel Burn & Theft Simulation
            if has_fuel_thief and 0.4 < progress_ratio < 0.5:
                current_fuel -= random.uniform(12.0, 18.0) # Sharp, localized volume drop -> Fuel Fraud
                has_fuel_thief = False # Prevent multiple drops in the same trip
            else:
                current_fuel -= fuel_loss_per_min * random.uniform(0.8, 1.2)
                
            # Guardrail to handle empty tanks smoothly
            if current_fuel < 2.0: 
                current_fuel = v_config["tank_capacity_l"] # Auto-refuel simulation rule
                
            # 2. Door Status Security Logic
            if min_offset == 0 or min_offset + 60 >= duration_sec:
                door_open = 1 # Open during terminal loading/unloading operations
            elif has_security_breach and 0.7 < progress_ratio < 0.75:
                door_open = 1 # Cargo doors swinging open on the active highway -> Security Alert
            else:
                door_open = 0
                
            # 3. Refrigeration Cold-Chain Safety Logic
            if has_reefer_failure and progress_ratio > 0.5:
                # Temperature spikes significantly past safe operating limits
                cargo_temp = max_allowed_temp + random.uniform(5.0, 15.0) 
            else:
                # Lock temperature safely within cargo contractual safety thresholds
                cargo_temp = random.uniform(min_allowed_temp, max_allowed_temp)
                
            slow_records.append([
                v_id, v_id, tick_time.strftime('%Y-%m-%d %H:%M:%S'),
                round(current_fuel, 2), door_open, round(cargo_temp, 2)
            ])
            
    # ==========================================
    # 3. CONVERT AND SAVE VEHICLE FILES TO DISK
    # ==========================================
    # Write the high-velocity Quick Sensor stream file for this vehicle
    df_q_out = pd.DataFrame(quick_records, columns=[
        "vehicle_id", "sensor_id", "timestamp", "lat", "lon", 
        "odometer_km", "rpm", "throttle_pct", "accel_ay_abs", "battery_v"
    ])
    df_q_out.to_csv(f"quick_sensors/{v_id}.csv", index=False)
    
    # Write the analytical Slow Sensor stream file for this vehicle
    df_s_out = pd.DataFrame(slow_records, columns=[
        "vehicle_id", "sensor_id", "timestamp", "fuel_amount_l", "is_door_open", "cargo_temp_c"
    ])
    df_s_out.to_csv(f"slow_sensors/{v_id}.csv", index=False)
    
    # Progress indicator log to track file output updates
    if v_id % 50 == 0:
        print(f" -> Telemetry complete for batches up to file vehicle_{v_id}.csv")

print("\nSuccess! High-velocity pipelines generated successfully inside 'quick_sensors/' and 'slow_sensors/'.")