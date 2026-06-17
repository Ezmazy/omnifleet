import pandas as pd
import numpy as np
import random

# Lock seed for perfect, immutable reproducibility
random.seed(42)
np.random.seed(42)

print("Loading prerequisite datasets...")
try:
    df_trips = pd.read_csv("trips.csv")
    df_cargo = pd.read_csv("cargo.csv")
except FileNotFoundError:
    print("Error: Make sure 'trips.csv' and 'cargo.csv' exist in your workspace directory!")
    exit()

print(f"Loaded {len(df_trips)} Trips and {len(df_cargo)} Cargo packages.")

# ==========================================
# 1. OPTIMIZED INDEX MAPPING
# ==========================================
print("Grouping cargo items by thermal/cargo type for type-safe matching...")
cargo_types_pool = df_cargo['cargo_type'].unique()
cargo_by_type = {c_type: df_cargo[df_cargo['cargo_type'] == c_type]['cargo_id'].tolist() for c_type in cargo_types_pool}

# ==========================================
# 2. BRIDGE MATRIX GENERATION WITH DISTRIBUTION WEIGHTS
# ==========================================
print("Generating bridge records with precise load_status probabilities...")

# Define target operational splits requested
status_options = ['Delivered', 'In Transit', 'Loaded']
status_weights = [0.90, 0.06, 0.04]

bridge_data = []

# Iterating through all 200,000 trips
for _, row in df_trips.iterrows():
    trip_id = int(row['trip_id'])
    
    # Pick a uniform matching type configuration
    selected_type = random.choice(cargo_types_pool)
    eligible_cargo_ids = cargo_by_type[selected_type]
    
    # Determine item density per run
    num_packages = random.randint(1, 5)
    assigned_cargos = random.choices(eligible_cargo_ids, k=num_packages)
    assigned_cargos = list(set(assigned_cargos)) # De-duplicate package IDs within same run
    
    # Non-uniformly assign execution statuses using target probability weights array
    assigned_statuses = random.choices(status_options, weights=status_weights, k=len(assigned_cargos))
    
    # Append records to bridge table matrix
    for cargo_id, status in zip(assigned_cargos, assigned_statuses):
        bridge_data.append({
            "trip_id": trip_id,
            "cargo_id": cargo_id,
            "load_status": status
        })

df_trip_cargo = pd.DataFrame(bridge_data)

# ==========================================
# 3. EXPORT REVISED BRIDGE ARTIFACT
# ==========================================
print("\nWriting out finalized type-safe bridge table to CSV...")
df_trip_cargo.to_csv("trip_cargo.csv", index=False)

print("\n=== Validation & Data Integrity Audit ===")
print(f"Trip_Cargo Table Rows Generated: {len(df_trip_cargo):,}")

# Output audit metrics to verify distribution requirements
print("\nGenerated load_status Percentage Distribution:")
distribution = df_trip_cargo['load_status'].value_counts(normalize=True) * 100
for status, pct in distribution.items():
    print(f" -> {status}: {pct:.2f}%")

# Double check type-safety boundary guardrails
check_df = df_trip_cargo.merge(df_cargo[['cargo_id', 'cargo_type']], on='cargo_id', how='left')
types_per_trip = check_df.groupby('trip_id')['cargo_type'].nunique()
mixed_trips = (types_per_trip > 1).sum()
print(f"\nTrips breaking temperature type separation constraints: {mixed_trips} (Expected: 0)")