# =============================================================
# OmniFleet V003 - Bronze Streaming (always-on)
# =============================================================
# Reads the 2 Kafka topics and lands the RAW JSON in MinIO bronze.
# No parsing, no logic - this is the replay/audit layer.
#
# Run detached:
#   docker exec -d omnifleet-spark-v003 bash -c "spark-submit \
#     --jars <minio + kafka jars> /home/jovyan/work/bronze_streaming.py \
#     > /home/jovyan/work/bronze_streaming.log 2>&1"
# =============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

my_jars = "/usr/local/spark/jars/extra/hadoop-aws-3.3.4.jar," \
          "/usr/local/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar," \
          "/usr/local/spark/jars/extra/spark-sql-kafka-0-10_2.12-3.5.3.jar," \
          "/usr/local/spark/jars/extra/kafka-clients-3.4.1.jar," \
          "/usr/local/spark/jars/extra/spark-token-provider-kafka-0-10_2.12-3.5.3.jar," \
          "/usr/local/spark/jars/extra/commons-pool2-2.11.1.jar"

spark = SparkSession.builder \
    .appName("BronzeStreaming") \
    .config("spark.jars", my_jars) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "omnifleet") \
    .config("spark.hadoop.fs.s3a.secret.key", "omnifleet123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# read a topic and land its raw value in bronze
def land_topic(topic, folder):
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    bronze = stream.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_json"),
        F.col("topic").alias("topic"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingestion_ts"),
        F.to_date(F.col("timestamp")).alias("ingest_date"),
    )

    return bronze.writeStream \
        .format("parquet") \
        .option("path", f"s3a://omnifleet-bronze/stream/{folder}") \
        .option("checkpointLocation", f"s3a://omnifleet-bronze/_checkpoints/{folder}") \
        .partitionBy("ingest_date") \
        .trigger(processingTime="30 seconds") \
        .outputMode("append") \
        .start()


q1 = land_topic("vehicle.quick.sensors", "bronze_quick_sensors")
print("Landing vehicle.quick.sensors -> bronze")
q2 = land_topic("vehicle.slow.sensors", "bronze_slow_sensors")
print("Landing vehicle.slow.sensors -> bronze")

print("Bronze streaming running... (Ctrl+C to stop)")
spark.streams.awaitAnyTermination()
