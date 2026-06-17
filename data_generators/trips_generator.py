import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Lock random seed for 100% reproducible data generation
random.seed(42)
np.random.seed(42)

print("Loading dimensional datasets...")
try:
    df_depots = pd.read_csv("depots.csv")
    df_routes = pd.read_csv("routes.csv")
except FileNotFoundError:
    print("Error: Please run your previous Depots/Routes generation script first!")
    exit()

print(f"Loaded {len(df_depots)} depots and {len(df_routes)} routes.")

# ==========================================
# 1. HAVERSINE DISTANCE MATRIX GENERATOR
# ==========================================
def calculate_haversine_km(lat1, lon1, lat2, lon2):
    """Calculates great-circle distance between two GPS coordinates."""
    R = 6371.0 # Earth radius in kilometers
    p = np.pi / 180
    a = 0.5 - np.cos((lat2 - lat1) * p)/2 + np.cos(lat1 * p) * np.cos(lat2 * p) * (1 - np.cos((lon2 - lon1) * p)) / 2
    return 2 * R * np.arcsin(np.sqrt(a))

# Map depot lat/lon for fast lookup
depot_geo = df_depots.set_index('depot_id')[['lat', 'lon']].to_dict('index')

print("Mapping distances and travel times for all 4,830 routes...")
route_metrics = {}
for _, row in df_routes.iterrows():
    r_id = int(row['route_id'])
    orig = int(row['origin_depot_id'])
    dest = int(row['destination_depot_id'])
    
    # Extract coordinates
    lat1, lon1 = depot_geo[orig]['lat'], depot_geo[orig]['lon']
    lat2, lon2 = depot_geo[dest]['lat'], depot_geo[dest]['lon']
    
    distance_km = calculate_haversine_km(lat1, lon1, lat2, lon2)
    # Assume 70 km/h average commercial velocity + 1 hour buffer overhead
    travel_hours = max(1.5, (distance_km / 70.0) + 1.0) 
    
    route_metrics[r_id] = {
        'distance_km': round(distance_km, 2),
        'duration_hours': round(travel_hours, 2),
        'origin_depot': orig,
        'dest_depot': dest
    }

# ==========================================
# 2. TRANSACTION LOGISTICS ENGINE (200,000 Rows)
# ==========================================
print("Initializing resource schedules for 2024...")
TOTAL_TRIPS = 200000

# Available resource bounds specified by configuration
NUM_VEHICLES = 500
NUM_DRIVERS = 600
ROUTE_IDS = df_routes['route_id'].tolist()

# Tracking schedules: Map asset IDs to their next available datetime slot
# Initialize all assets on Jan 1st, 2024 at 00:00:00
base_time = datetime(2022, 1, 1, 0, 0, 0)
vehicle_availability = {v_id: base_time for v_id in range(NUM_VEHICLES)}
driver_availability = {d_id: base_time for d_id in range(NUM_DRIVERS)}

trips_data = []

print("Generating 200,000 sequential non-overlapping trips...")
for t_id in range(TOTAL_TRIPS):
    # To keep processing incredibly fast and avoid infinite check-loops, 
    # we pick a random subset of resources and find the ones available earliest.
    candidate_vehicles = random.sample(range(NUM_VEHICLES), 5)
    vehicle_id = min(candidate_vehicles, key=lambda v: vehicle_availability[v])
    
    candidate_drivers = random.sample(range(NUM_DRIVERS), 5)
    driver_id = min(candidate_drivers, key=lambda d: driver_availability[d])
    
    # Route selection
    route_id = random.choice(ROUTE_IDS)
    metrics = route_metrics[route_id]
    
    # Scheduled Start Time matches the asset's current earliest free timestamp
    # Add a small random idle gap (1 to 4 hours) so the asset isn't moving instantly
    idle_gap = random.randint(1, 4)
    sched_start = max(vehicle_availability[vehicle_id], driver_availability[driver_id]) + timedelta(hours=idle_gap)
    
    # Calculate scheduled end based on route travel duration rules
    sched_end = sched_start + timedelta(hours=metrics['duration_hours'])
    
    # Apply raw delivery variance to create realistic messy audit trails
    # actual_start can lag scheduled_start by 1 to 3 days (e.g., loading delays)
    start_delay_days = random.randint(0, 3)
    start_delay_hours = random.randint(0, 23)
    actual_start = sched_start + timedelta(days=start_delay_days, hours=start_delay_hours)
    
    # actual_end varies around scheduled end (-1 day early up to +3 days late due to traffic/breakdowns)
    end_variance_days = random.randint(-1, 3)
    end_variance_hours = random.randint(0, 23)
    actual_end = sched_end + timedelta(days=end_variance_days, hours=end_variance_hours)
    
    # Hard guardrail: actual_end cannot happen before actual_start
    if actual_end <= actual_start:
        actual_end = actual_start + timedelta(hours=max(1.5, metrics['duration_hours'] * random.uniform(0.9, 1.3)))
        
    # Commit the block
    trips_data.append({
        "trip_id": t_id,
        "vehicle_id": vehicle_id,
        "driver_id": driver_id,
        "route_id": route_id,
        "scheduled_start_time": sched_start.strftime('%Y-%m-%d %H:%M:%S'),
        "scheduled_end_time": sched_end.strftime('%Y-%m-%d %H:%M:%S'),
        "actual_start_time": actual_start.strftime('%Y-%m-%d %H:%M:%S'),
        "actual_end_time": actual_end.strftime('%Y-%m-%d %H:%M:%S')
    })
    
    # Update resource schedules to ensure they cannot be double-booked
    # They are busy until they physically finish the delivery (actual_end_time)
    vehicle_availability[vehicle_id] = actual_end
    driver_availability[driver_id] = actual_end

    if t_id % 50000 == 0 and t_id > 0:
        print(f"-> Processed {t_id} rows successfully...")

df_trips = pd.DataFrame(trips_data)

# Sort chronologically by scheduled start time so it reads like a real database append log
df_trips = df_trips.sort_values(by="scheduled_start_time").reset_index(drop=True)
# Re-assign sequential trip IDs after the sort so they remain index clean
df_trips['trip_id'] = range(TOTAL_TRIPS)

# ==========================================
# 3. EXPORT ARTIFACT
# ==========================================
print("\nWriting out final transaction matrix to CSV...")
df_trips.to_csv("trips.csv", index=False)

print("\n=== Validation Audit ===")
print(f"Trips Dataset Shape: {df_trips.shape}")
print(f"Min Date Checked:     {df_trips['scheduled_start_time'].min()}")
print(f"Max Date Checked:     {df_trips['scheduled_start_time'].max()}")

# Double booking data sanity check assertion
concurrent_errors = df_trips.duplicated(subset=['vehicle_id', 'scheduled_start_time']).sum()
print(f"Double-booked vehicle collision conflicts: {concurrent_errors} (Expected: 0)")