# =============================================================
# OmniFleet V003 - Silver Build (static)
# =============================================================
# Reads the 7 raw bronze tables, cleans + casts them, validates the
# foreign keys, and writes tidy SILVER tables back to MinIO.
#
# Silver tables produced:
#   silver_vehicles, silver_drivers, silver_cargo,
#   silver_routes (depots already joined in -> flat),
#   silver_trips, silver_trip_cargo
#
# Run by Airflow task "silver_build", or by hand with spark-submit.
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

my_jars = "/usr/local/spark/jars/extra/hadoop-aws-3.3.4.jar," \
          "/usr/local/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar"

spark = SparkSession.builder \
    .appName("SilverBuild") \
    .config("spark.jars", my_jars) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "omnifleet") \
    .config("spark.hadoop.fs.s3a.secret.key", "omnifleet123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BRONZE = "s3a://omnifleet-bronze/static"
SILVER = "s3a://omnifleet-silver"


# small helper to save a silver table and print the row count
def save_silver(df, name):
    out = f"{SILVER}/{name}"
    df.write.mode("overwrite").parquet(out)
    print(f"  {name}: {df.count()} rows")


# -------------------------------------------------------------
# 1. silver_vehicles
# -------------------------------------------------------------
print("Building silver_vehicles ...")
v = spark.read.parquet(f"{BRONZE}/bronze_vehicles")
silver_vehicles = v.select(
    F.col("vehicle_id").cast("int").alias("vehicle_id"),
    F.trim("model").alias("model"),
    F.col("payload_capacity").cast("double").alias("payload_capacity"),
    F.col("tank_capacity_l").cast("double").alias("tank_capacity_l"),
    F.trim("fuel_type").alias("fuel_type"),
    F.col("current_mileage").cast("double").alias("current_mileage"),
).dropDuplicates(["vehicle_id"])
save_silver(silver_vehicles, "silver_vehicles")


# -------------------------------------------------------------
# 2. silver_drivers
# -------------------------------------------------------------
print("Building silver_drivers ...")
d = spark.read.parquet(f"{BRONZE}/bronze_drivers")
silver_drivers = d.select(
    F.col("driver_id").cast("int").alias("driver_id"),
    F.trim("first_name").alias("first_name"),
    F.trim("last_name").alias("last_name"),
    F.trim("license_class").alias("license_class"),
    F.trim("phone_number").alias("phone_number"),
).dropDuplicates(["driver_id"])
save_silver(silver_drivers, "silver_drivers")


# -------------------------------------------------------------
# 3. silver_cargo
# -------------------------------------------------------------
print("Building silver_cargo ...")
c = spark.read.parquet(f"{BRONZE}/bronze_cargo")
silver_cargo = c.select(
    F.col("cargo_id").cast("int").alias("cargo_id"),
    F.trim("cargo_type").alias("cargo_type"),
    F.col("max_temp").cast("double").alias("max_temp"),
    F.col("min_temp").cast("double").alias("min_temp"),
    F.col("weight_kg").cast("double").alias("weight_kg"),
    F.col("price_egp").cast("double").alias("price_egp"),
).dropDuplicates(["cargo_id"])
save_silver(silver_cargo, "silver_cargo")


# -------------------------------------------------------------
# 4. silver_routes  (join depots in for origin + destination)
# -------------------------------------------------------------
print("Building silver_routes ...")
r = spark.read.parquet(f"{BRONZE}/bronze_routes")
dep = spark.read.parquet(f"{BRONZE}/bronze_depots").select(
    F.col("depot_id").cast("int").alias("depot_id"),
    F.trim("address").alias("address"),
    F.trim("governorate").alias("governorate"),
    F.col("lat").cast("double").alias("lat"),
    F.col("lon").cast("double").alias("lon"),
)

routes_typed = r.select(
    F.col("route_id").cast("int").alias("route_id"),
    F.col("origin_depot_id").cast("int").alias("origin_depot_id"),
    F.col("destination_depot_id").cast("int").alias("destination_depot_id"),
)

# join origin depot details
o = dep.select(
    F.col("depot_id").alias("origin_depot_id"),
    F.col("address").alias("origin_address"),
    F.col("governorate").alias("origin_governorate"),
    F.col("lat").alias("origin_lat"),
    F.col("lon").alias("origin_lon"),
)
# join destination depot details
ds = dep.select(
    F.col("depot_id").alias("destination_depot_id"),
    F.col("address").alias("destination_address"),
    F.col("governorate").alias("destination_governorate"),
    F.col("lat").alias("destination_lat"),
    F.col("lon").alias("destination_lon"),
)

silver_routes = routes_typed \
    .join(o, "origin_depot_id", "left") \
    .join(ds, "destination_depot_id", "left") \
    .select(
        "route_id",
        "origin_depot_id", "origin_address", "origin_governorate", "origin_lat", "origin_lon",
        "destination_depot_id", "destination_address", "destination_governorate",
        "destination_lat", "destination_lon",
    ).dropDuplicates(["route_id"])
save_silver(silver_routes, "silver_routes")


# -------------------------------------------------------------
# 5. silver_trips  (cast times, validate FKs against vehicles/drivers/routes)
# -------------------------------------------------------------
print("Building silver_trips ...")
t = spark.read.parquet(f"{BRONZE}/bronze_trips")
trips_typed = t.select(
    F.col("trip_id").cast("int").alias("trip_id"),
    F.col("vehicle_id").cast("int").alias("vehicle_id"),
    F.col("driver_id").cast("int").alias("driver_id"),
    F.col("route_id").cast("int").alias("route_id"),
    F.to_timestamp("scheduled_start_time").alias("scheduled_start_time"),
    F.to_timestamp("scheduled_end_time").alias("scheduled_end_time"),
    F.to_timestamp("actual_start_time").alias("actual_start_time"),
    F.to_timestamp("actual_end_time").alias("actual_end_time"),
).dropDuplicates(["trip_id"])

# FK validation: keep only trips whose vehicle/driver/route exist
good_v = silver_vehicles.select("vehicle_id")
good_d = silver_drivers.select("driver_id")
good_r = silver_routes.select("route_id")

before = trips_typed.count()
silver_trips = trips_typed \
    .join(good_v, "vehicle_id", "left_semi") \
    .join(good_d, "driver_id", "left_semi") \
    .join(good_r, "route_id", "left_semi")
after = silver_trips.count()
if before != after:
    print(f"  WARNING: dropped {before - after} trips with bad FKs")
save_silver(silver_trips, "silver_trips")


# -------------------------------------------------------------
# 6. silver_trip_cargo  (cast, validate FKs against trips + cargo)
# -------------------------------------------------------------
print("Building silver_trip_cargo ...")
tc = spark.read.parquet(f"{BRONZE}/bronze_trip_cargo")
tc_typed = tc.select(
    F.col("trip_id").cast("int").alias("trip_id"),
    F.col("cargo_id").cast("int").alias("cargo_id"),
    F.trim("load_status").alias("load_status"),
)

good_t = silver_trips.select("trip_id")
good_c = silver_cargo.select("cargo_id")

before = tc_typed.count()
silver_trip_cargo = tc_typed \
    .join(good_t, "trip_id", "left_semi") \
    .join(good_c, "cargo_id", "left_semi")
after = silver_trip_cargo.count()
if before != after:
    print(f"  WARNING: dropped {before - after} trip_cargo rows with bad FKs")
save_silver(silver_trip_cargo, "silver_trip_cargo")


print("Silver build finished!")
spark.stop()
