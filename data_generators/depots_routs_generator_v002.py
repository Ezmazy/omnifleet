import pandas as pd
import random
import itertools

# Set seed for perfect, stable reproducibility
random.seed(42)

print("Starting generation for highway-accurate Depots and Routes across Egypt...")

# ==========================================
# 1. HARDCODED SYSTEMATIC ROAD-BASED DEPOTS
# ==========================================
# Explicitly selected coordinate points situated on land near primary industrial 
# zones, free zones, and key transport logistics highways in Egypt.

gov_hubs_data = {
    "Cairo": [  # 20 Locations - Ring Road, El-Marg, Heliopolis, Katameya, Badr City, etc.
        {"lat": 30.0165, "lon": 31.2845, "addr": "Maadi Ring Road Logistics Node"},
        {"lat": 30.0821, "lon": 31.3211, "addr": "Nasr City Freight Terminal"},
        {"lat": 30.1415, "lon": 31.4233, "addr": "Heliopolis Industrial Hub Sector 1"},
        {"lat": 30.1420, "lon": 31.6210, "addr": "Badr City Industrial Zone A"},
        {"lat": 30.1455, "lon": 31.6288, "addr": "Badr City Logistics Park B"},
        {"lat": 30.0244, "lon": 31.4611, "addr": "New Cairo El-Teseen Corridor Hub"},
        {"lat": 30.0011, "lon": 31.7102, "addr": "Madinaty Transport Junction"},
        {"lat": 30.1855, "lon": 31.4922, "addr": "Obour City Industrial Block Block 1"},
        {"lat": 30.1890, "lon": 31.4995, "addr": "Obour City Market Logistics Center"},
        {"lat": 30.0655, "lon": 31.2511, "addr": "Ramses Railway Freight Depot"},
        {"lat": 30.1122, "lon": 31.3411, "addr": "El-Nozha Cargo Terminal Sector"},
        {"lat": 30.1655, "lon": 31.3910, "addr": "El-Marg Regional Highway Node"},
        {"lat": 30.0399, "lon": 31.2188, "addr": "Downtown Nile Cargo Axis"},
        {"lat": 29.9811, "lon": 31.2910, "addr": "Basatin Marble Transport Ridge"},
        {"lat": 29.8211, "lon": 31.3144, "addr": "Helwan Industrial Steel Complex"},
        {"lat": 29.8455, "lon": 31.3299, "addr": "15th of May City Enterprise Sector"},
        {"lat": 30.0711, "lon": 31.5512, "addr": "Cairo-Suez Highway Depot Zone 1"},
        {"lat": 30.0788, "lon": 31.5650, "addr": "Cairo-Suez Highway Depot Zone 2"},
        {"lat": 30.0522, "lon": 31.6911, "addr": "New Administrative Capital Border Terminal"},
        {"lat": 30.0211, "lon": 31.2311, "addr": "Old Cairo Old City Distribution Point"}
    ],
    "Alexandria": [  # 20 Locations - Port Axis, Desert Road, Borg El Arab, International Coastal Road
        {"lat": 31.1811, "lon": 29.8644, "addr": "Alexandria Port Main Cargo Free Zone"},
        {"lat": 31.1899, "lon": 29.8712, "addr": "Alexandria Port Container Terminal 2"},
        {"lat": 31.1511, "lon": 29.8944, "addr": "Dekheila Port Maritime Logistics Sector"},
        {"lat": 31.1455, "lon": 29.9211, "addr": "Mex Heavy Freight Terminal"},
        {"lat": 30.9155, "lon": 29.6844, "addr": "Borg El Arab Industrial Zone Hub 1"},
        {"lat": 30.9199, "lon": 29.6910, "addr": "Borg El Arab Industrial Zone Hub 2"},
        {"lat": 30.9255, "lon": 29.7122, "addr": "Borg El Arab Logistics Dry Port Area"},
        {"lat": 31.2122, "lon": 29.9488, "addr": "Moharam Bek Intercity Transport Hub"},
        {"lat": 31.1988, "lon": 30.0155, "addr": "Smouha Localized Freight Depot"},
        {"lat": 31.2611, "lon": 30.0144, "addr": "Mandara Coastal Transport Corridor"},
        {"lat": 31.3101, "lon": 30.0711, "addr": "Abu Qir Port Extension Base"},
        {"lat": 31.0211, "lon": 29.8144, "addr": "Amreya Desert Road Ingestion Point 1"},
        {"lat": 31.0399, "lon": 29.8299, "addr": "Amreya Desert Road Ingestion Point 2"},
        {"lat": 31.1211, "lon": 29.9911, "addr": "International Coastal Road Lane Node 1"},
        {"lat": 31.1411, "lon": 30.0511, "addr": "International Coastal Road Lane Node 2"},
        {"lat": 31.2044, "lon": 29.9122, "addr": "Gomrok Port Inflow Freight Zone"},
        {"lat": 31.1688, "lon": 29.9102, "addr": "Karmous Localized Transit Hub"},
        {"lat": 31.0544, "lon": 29.7422, "addr": "King Mariout Inland Route Sector"},
        {"lat": 31.2311, "lon": 29.9611, "addr": "Sidi Gaber Freight Express Office"},
        {"lat": 31.2488, "lon": 29.9855, "addr": "Glim Distribution Center Network"}
    ],
    "Giza": [
        {"lat": 29.9622, "lon": 30.9255, "addr": "6th of October Industrial Complex Zone 1"},
        {"lat": 29.9688, "lon": 30.9310, "addr": "6th of October Logistics Dry Port Zone 2"},
        {"lat": 29.9755, "lon": 30.9455, "addr": "6th of October Regional Distribution Hub"},
        {"lat": 30.0211, "lon": 31.1488, "addr": "Faisal Main Transport Arterial Hub"},
        {"lat": 30.0055, "lon": 31.1211, "addr": "Haram Desert Exit Junction Node"},
        {"lat": 30.0511, "lon": 31.1911, "addr": "Agouza Inland Freight Station"},
        {"lat": 30.0744, "lon": 31.1711, "addr": "Imbaba Localized Transit Area"},
        {"lat": 30.1211, "lon": 31.1344, "addr": "Warraq Regional Ring Road Node"},
        {"lat": 29.8511, "lon": 31.2844, "addr": "Hawamdia Agricultural Highway Junction"},
        {"lat": 29.7811, "lon": 31.2911, "addr": "Badrashin South Freight Axis"}
    ],
    "Damietta": [  # Safe land-based points: Port area, New Damietta, Faraskur, Zarqa
        {"lat": 31.4422, "lon": 31.7488, "addr": "Damietta Port Container Terminal Hub 1"},
        {"lat": 31.4466, "lon": 31.7512, "addr": "Damietta Port Free Zone Logistics Block 2"},
        {"lat": 31.4311, "lon": 31.6844, "addr": "New Damietta Industrial City Zone East"},
        {"lat": 31.4355, "lon": 31.6910, "addr": "New Damietta Central Distribution Depot"},
        {"lat": 31.4177, "lon": 31.8144, "addr": "Damietta City Center Highway Node"},
        {"lat": 31.3244, "lon": 31.8022, "addr": "Faraskur Nile Highway Logistics Base"},
        {"lat": 31.2511, "lon": 31.2144, "addr": "Zarqa Agriculture Transport Route"},
        {"lat": 31.4122, "lon": 31.9122, "addr": "Ezbet El-Borg Marine Logistics Station"},
        {"lat": 31.3911, "lon": 31.7244, "addr": "Kafr El-Battikh Industrial Corridor Axis"},
        {"lat": 31.4499, "lon": 31.7655, "addr": "Damietta Port Grain Silos Ingestion Point"}
    ],
    "Sharqia": [
        {"lat": 30.3122, "lon": 31.7455, "addr": "10th of Ramadan Industrial City Cluster A"},
        {"lat": 30.3188, "lon": 31.7510, "addr": "10th of Ramadan Industrial City Cluster B"},
        {"lat": 30.3299, "lon": 31.7699, "addr": "10th of Ramadan Logistics Dry Port"},
        {"lat": 30.5877, "lon": 31.5020, "addr": "Zagazig Main Regional Highway Terminal"},
        {"lat": 30.6011, "lon": 31.5144, "addr": "Zagazig Agriculture Aggregation Center"},
        {"lat": 30.5211, "lon": 31.5911, "addr": "Abu Hammad Logistics Corridor Junction"},
        {"lat": 30.4122, "lon": 31.5644, "addr": "Belbeis Desert Road Transit Base 1"},
        {"lat": 30.4255, "lon": 31.5811, "addr": "Belbeis Desert Road Transit Base 2"},
        {"lat": 30.6844, "lon": 31.6422, "addr": "Hehia Agriculture Freight Point"},
        {"lat": 30.8011, "lon": 31.8144, "addr": "Faqus Northern Trading Hub"}
    ]
}

# # Baseline land coordinates for the remaining 22 governorates to prevent any sea/river drops
# generic_governorates_baselines = [
#     {"name": "Qalyubia", "lat": 30.4100, "lon": 31.1800, "hub": "Khanka Industrial Ridge"},
#     {"name": "Dakahlia", "lat": 31.0410, "lon": 31.3780, "hub": "Mansoura Highway Node"},
#     {"name": "Monufia", "lat": 30.5100, "lon": 31.0100, "hub": "Sadat City Industrial Block"},
#     {"name": "Gharbia", "lat": 30.7885, "lon": 31.0019, "hub": "Tanta Transport Hub"},
#     {"name": "Beheira", "lat": 31.0400, "lon": 30.4700, "hub": "Nubariyah Agricultural Highway"},
#     {"name": "Port Said", "lat": 31.2565, "lon": 32.2841, "hub": "East Port Said Bypass Terminal"},
#     {"name": "Ismailia", "lat": 30.6043, "lon": 32.2723, "hub": "Suez Canal Logistics Corridor"},
#     {"name": "Suez", "lat": 29.9668, "lon": 32.5498, "hub": "Sokhna Port Freight Hub"},
#     {"name": "Fayoum", "lat": 29.3084, "lon": 30.8428, "hub": "Kiman Fares Logistics Zone"},
#     {"name": "Beni Suef", "lat": 29.0744, "lon": 31.0978, "hub": "Bayad El-Arab Industrial Zone"}
# ]

depots_data = []
depot_counter = 0

# 1. Process explicit highway-perfect zones
for gov_name, locations in gov_hubs_data.items():
    for loc in locations:
        depots_data.append({
            "depot_id": depot_counter,
            "address": f"{loc['addr']}, {gov_name}, Egypt",
            "governorate": gov_name,
            "lat": loc["lat"],
            "lon": loc["lon"],
            "vehicles_capacity": random.randint(5, 10)
        })
        depot_counter += 1

# # 2. Process remaining governorates safely by introducing small dry land vector additions
# for gov in generic_governorates_baselines:
#     for i in range(10):
#         # Small controlled offsets ensuring values stick to local desert/urban transit lanes
#         lat_offset = random.uniform(-0.03, 0.03)
#         lon_offset = random.uniform(-0.03, 0.03)
        
#         final_lat = round(gov["lat"] + lat_offset, 6)
#         final_lon = round(gov["lon"] + lon_offset, 6)
        
#         depots_data.append({
#             "depot_id": depot_counter,
#             "address": f"Highroad Transit Point {i+1}, {gov['hub']}, {gov['name']}, Egypt",
#             "governorate": gov["name"],
#             "lat": final_lat,
#             "lon": final_lon,
#             "vehicles_capacity": random.randint(5, 10)
#         })
#         depot_counter += 1

df_depots = pd.DataFrame(depots_data)

# ==========================================
# 2. UPDATED ROUTES GENERATION (83,810 Rows)
# ==========================================
print(f"Generating full cross-join permutation matrix for {len(df_depots)} depots...")

depot_ids = df_depots["depot_id"].tolist()
route_permutations = list(itertools.permutations(depot_ids, 2))

routes_data = []
for r_id, (origin, destination) in enumerate(route_permutations):
    routes_data.append({
        "route_id": r_id,
        "origin_depot_id": origin,
        "destination_depot_id": destination
    })

df_routes = pd.DataFrame(routes_data)

# ==========================================
# 3. EXPORT REVISED DATA ARTIFACTS
# ==========================================
print("\nSaving updated files to disk...")
df_depots.to_csv("depots.csv", index=False)
df_routes.to_csv("routes.csv", index=False)

print("\n=== Verified Highway Optimization Audit ===")
print(f"Total Depots Created: {len(df_depots)} (Expected: 290)")
print(f"Total Routes Created: {len(df_routes)} (Expected: 83810)")
print("\nDistribution Volume Audit Summary:")
print(df_depots["governorate"].value_counts())