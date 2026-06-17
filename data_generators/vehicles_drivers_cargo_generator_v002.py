import pandas as pd
import random

# Set seed for perfect, stable reproducibility
random.seed(42)

print("Starting natural data generation for independent master tables...")

# ==========================================
# 1. VEHICLES TABLE GENERATION (100 Rows)
# ==========================================
print("Generating Vehicles data (with market-weighted distribution)...")

# Define realistic fleet weight splits (e.g., Sprinter & Transit dominate)
vehicle_models_config = {
    "Ford Transit": {"payload": 1500, "tank": 80, "prob_weight": 0.35},
    "Mercedes-Benz Sprinter": {"payload": 1700, "tank": 75, "prob_weight": 0.40},
    "Renault Master": {"payload": 1400, "tank": 85, "prob_weight": 0.15},
    "Toyota Hiace": {"payload": 1200, "tank": 70, "prob_weight": 0.10}
}

models_list = list(vehicle_models_config.keys())
model_weights = [cfg["prob_weight"] for cfg in vehicle_models_config.values()]

# Sample the vehicle models realistically
sampled_models = random.choices(models_list, weights=model_weights, k=500)

vehicles_data = []
for v_id, model in enumerate(sampled_models):
    config = vehicle_models_config[model]
    vehicles_data.append({
        "vehicle_id": v_id,
        "model": model,
        "payload_capacity": config["payload"],
        "tank_capacity_l": config["tank"],
        "fuel_type": "Diesel",
        "current_mileage": random.randint(50, 450)
    })

df_vehicles = pd.DataFrame(vehicles_data)

# ==========================================
# 2. DRIVER TABLE GENERATION (150 Rows)
# ==========================================
print("Generating Drivers data...")

first_names_pool = [
    "Ahmed", "Mohamed", "Youssef", "Omar", "Mahmoud", "Ali", "Ibrahim", "Khaled", 
    "Mostafa", "Tarek", "Karim", "Hany", "Amr", "Sameh", "Wael", "Sherif", 
    "Ziad", "Eslam", "Amjad", "Hazem", "Ramy", "Yasser", "Ayman", "Hassan", "Walid"
]
last_names_pool = [
    "Abd-Elnour", "Mansour", "Hassan", "El-Sayed", "Abdel-Rahman", "Moussa", "Khalil", 
    "Ghanem", "Fawzy", "Osman", "Salama", "Zaki", "Badawy", "Radi", "Nasser", 
    "Samy", "Kamel", "Farahat", "Ragab", "Gomaa", "Soliman", "Said", "Bakr", "Aziz"
]

drivers_data = []
for d_id in range(600):
    f_name = random.choice(first_names_pool)
    l_name = random.choice(last_names_pool)
    license_cls = random.choice(["Category_B", "Category_C1"])
    
    prefix = random.choice(["012", "011", "010", "015"])
    suffix = "".join([str(random.randint(0, 9)) for _ in range(8)])
    phone_num = f"{prefix}{suffix}"
    
    drivers_data.append({
        "driver_id": d_id,
        "first_name": f_name,
        "last_name": l_name,
        "license_class": license_cls,
        "phone_number": phone_num
    })

df_drivers = pd.DataFrame(drivers_data)

# ==========================================
# 3. CARGO TABLE GENERATION (50,000 Rows)
# ==========================================
print("Generating Cargo data (with logistics-weighted volume)...")

# Define realistic market splits: Ambient & Chilled make up 70% of freight volume
cargo_configs = {
    "Controlled ambient": {"max_t": 15, "min_t": 8, "w_range": (0.2, 5.0), "p_range": (100, 5000), "prob_weight": 0.40},
    "Chilled": {"max_t": 8, "min_t": 0, "w_range": (1.0, 14.0), "p_range": (50, 5000), "prob_weight": 0.30},
    "Frozen": {"max_t": -18, "min_t": -100, "w_range": (1.0, 15.0), "p_range": (100, 10000), "prob_weight": 0.15},
    "Pharmaceutical": {"max_t": 8, "min_t": 2, "w_range": (0.4, 3.0), "p_range": (700, 70000), "prob_weight": 0.10},
    "Deep frozen": {"max_t": -40, "min_t": -1000, "w_range": (0.07, 3.0), "p_range": (1000, 60000), "prob_weight": 0.05}
}

cargo_types_list = list(cargo_configs.keys())
cargo_weights_list = [cfg["prob_weight"] for cfg in cargo_configs.values()]

# Sample cargo categories organically using the weights
sampled_cargo_types = random.choices(cargo_types_list, weights=cargo_weights_list, k=50000)

cargo_data = []
for c_id, c_type in enumerate(sampled_cargo_types):
    config = cargo_configs[c_type]
    
    min_w, max_w = config["w_range"]
    weight = round(random.uniform(min_w, max_w), 2)
    
    min_p, max_p = config["p_range"]
    weight_ratio = (weight - min_w) / (max_w - min_w) if (max_w - min_w) > 0 else 0.5
    
    variance = random.uniform(0.8, 1.2)
    calculated_price = min_p + (max_p - min_p) * weight_ratio * variance
    final_price = max(min_p, min(max_p, round(calculated_price, 2)))
    
    cargo_data.append({
        "cargo_id": c_id,
        "cargo_type": c_type,
        "max_temp": config["max_t"],
        "min_temp": config["min_t"],
        "weight_kg": weight,
        "price_egp": final_price
    })

df_cargo = pd.DataFrame(cargo_data)

# ==========================================
# 4. EXPORT ARTIFACTS
# ==========================================
print("\nSaving files to disk...")
df_vehicles.to_csv("vehicles.csv", index=False)
df_drivers.to_csv("drivers.csv", index=False)
df_cargo.to_csv("cargo.csv", index=False)

print("\n=== Verified Natural Distributions ===")
print("\n[Vehicles Model Count]:")
print(df_vehicles['model'].value_counts())
print("\n[Cargo Type Count]:")
print(df_cargo['cargo_type'].value_counts())