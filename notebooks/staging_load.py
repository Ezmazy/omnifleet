# =============================================================
# OmniFleet V003 - Staging Load (Silver -> Postgres)
# =============================================================
# dbt-postgres cannot read parquet from MinIO, so Spark copies the
# silver tables (plus two per-trip rollups) into postgres staging.*
# where dbt reads them.
#
# Tables written:
#   staging.stg_vehicles, stg_drivers, stg_routes, stg_trips
#   staging.stg_trip_cargo_agg      (cargo rolled up per trip)
#   staging.stg_trip_telemetry_agg  (sensor ticks rolled up per trip)
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

my_jars = "/usr/local/spark/jars/extra/hadoop-aws-3.3.4.jar," \
          "/usr/local/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar," \
          "/usr/local/spark/jars/extra/postgresql-42.7.3.jar"

spark = SparkSession.builder \
    .appName("StagingLoad") \
    .config("spark.jars", my_jars) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "omnifleet") \
    .config("spark.hadoop.fs.s3a.secret.key", "omnifleet123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

SILVER = "s3a://omnifleet-silver"

# postgres connection
PG_URL = "jdbc:postgresql://postgres:5432/omnifleet"
PG_PROPS = {
    "user": "omnifleet",
    "password": "omnifleet123",
    "driver": "org.postgresql.Driver",
}


def write_pg(df, table):
    df.write.jdbc(url=PG_URL, table=table, mode="overwrite", properties=PG_PROPS)
    print(f"  {table}: {df.count()} rows")


print("Copying silver tables to postgres staging ...")

# 1. straight copies (silver -> staging)
write_pg(spark.read.parquet(f"{SILVER}/silver_vehicles"), "staging.stg_vehicles")
write_pg(spark.read.parquet(f"{SILVER}/silver_drivers"),  "staging.stg_drivers")
write_pg(spark.read.parquet(f"{SILVER}/silver_routes"),   "staging.stg_routes")
write_pg(spark.read.parquet(f"{SILVER}/silver_trips"),    "staging.stg_trips")

# 2. cargo rollup per trip: total weight + total value, plus the
#    (homogeneous) cargo_type for the trip.
print("Computing per-trip cargo rollup ...")
tc = spark.read.parquet(f"{SILVER}/silver_trip_cargo")
cargo = spark.read.parquet(f"{SILVER}/silver_cargo")

trip_cargo_agg = tc.join(cargo, "cargo_id", "left").groupBy("trip_id").agg(
    # cargo is homogeneous per trip, so first() cargo_type is fine
    F.first("cargo_type").alias("cargo_type"),
    F.round(F.sum("weight_kg"), 2).alias("total_cargo_weight_kg"),
    F.round(F.sum("price_egp"), 2).alias("total_cargo_value_egp"),
)
write_pg(trip_cargo_agg, "staging.stg_trip_cargo_agg")

# 3. telemetry rollup per trip is written DIRECTLY to Postgres by
#    telemetry_trip_agg.py (the low-memory pandas job), so nothing to do here.
print("Telemetry rollup already in staging (written by telemetry_trip_agg). Skipping.")

print("Staging load finished! dbt can run now.")
spark.stop()
