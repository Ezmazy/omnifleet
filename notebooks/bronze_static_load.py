# =============================================================
# OmniFleet V003 - Bronze Static Load
# =============================================================
# Reads the 7 static CSV files and saves them AS-IS into MinIO (bronze).
# Bronze rule: NO cleaning, NO type casting. Just land the raw data.
# Everything is read as string so nothing breaks on load.
#
# Run by Airflow task "bronze_static_load", or by hand:
#   docker exec omnifleet-spark-v003 spark-submit \
#     --jars <minio jars> /home/jovyan/work/bronze_static_load.py
# =============================================================

from pyspark.sql import SparkSession

# 1. jars we need to talk to MinIO (s3a)
my_jars = "/usr/local/spark/jars/extra/hadoop-aws-3.3.4.jar," \
          "/usr/local/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar"

# 2. spark session with MinIO settings
spark = SparkSession.builder \
    .appName("BronzeStaticLoad") \
    .config("spark.jars", my_jars) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "omnifleet") \
    .config("spark.hadoop.fs.s3a.secret.key", "omnifleet123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. the 7 files -> their bronze table names
#    all live in /home/jovyan/work/data/
DATA_DIR = "/home/jovyan/work/data"
BRONZE = "s3a://omnifleet-bronze/static"

files_to_load = {
    "vehicles.csv":   "bronze_vehicles",
    "drivers.csv":    "bronze_drivers",
    "cargo.csv":      "bronze_cargo",
    "depots.csv":     "bronze_depots",
    "routes.csv":     "bronze_routes",
    "trips.csv":      "bronze_trips",
    "trip_cargo.csv": "bronze_trip_cargo",
}

# 4. load each file and write it to bronze as parquet
for file_name, table_name in files_to_load.items():
    path = f"{DATA_DIR}/{file_name}"
    print(f"Reading: {path}")

    # read everything as string (raw bronze, no casting)
    df = spark.read.csv(path, header=True, inferSchema=False)

    out_path = f"{BRONZE}/{table_name}"
    df.write.mode("overwrite").parquet(out_path)
    print(f"Saved {df.count()} rows to {out_path}")

print("Bronze static load finished!")
spark.stop()
