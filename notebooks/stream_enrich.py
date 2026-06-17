# =============================================================
# OmniFleet V003 - Stream Enrichment -> Live Alerts  (RELIABLE version)
# =============================================================
# Reads the 2 Kafka topics, derives ACCURATE incident flags, and writes
# live tables in postgres that power the Grafana "Active Incident Map".
#
# What makes this version reliable:
#   1. ACTIVE-TRIP LOOKUP: at startup we build, per vehicle, the list of
#      its trip time-windows + that trip's REAL cargo thermal bounds,
#      driver_id and trip_id (from trips.csv / trip_cargo.csv / cargo.csv).
#      So every sensor row is matched to the exact trip it belongs to.
#   2. EXACT cargo breach: cargo_temp is compared to that trip's real
#      min_temp / max_temp (not a guessed wide band).
#   3. MID-ROUTE door: a door-open only alerts if it happens away from
#      the loading/unloading window (so we don't alarm on normal loading).
#   4. trip_id + driver_id are attached to every incident.
#   5. ROLLING COLOUR: a vehicle's map colour = the highest-severity
#      incident in the last 2 minutes; it returns to WHITE when clear.
# =============================================================

import bisect
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# -------- thresholds (match the data generator) --------
THROTTLE_SPEEDING = 85
DRIFT_G = 0.75
BATTERY_LOW = 11.4
FUEL_DROP_FRAUD = 10.0
DOOR_EDGE_SECONDS = 180     # ignore door-open within 3 min of trip start/end (loading)

NUM_CARS = 100              # we only stream vehicles 0..99 (matches producer)
DATA_DIR = "/home/jovyan/work/data"

my_jars = "/usr/local/spark/jars/extra/hadoop-aws-3.3.4.jar," \
          "/usr/local/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar," \
          "/usr/local/spark/jars/extra/spark-sql-kafka-0-10_2.12-3.5.3.jar," \
          "/usr/local/spark/jars/extra/kafka-clients-3.4.1.jar," \
          "/usr/local/spark/jars/extra/spark-token-provider-kafka-0-10_2.12-3.5.3.jar," \
          "/usr/local/spark/jars/extra/commons-pool2-2.11.1.jar," \
          "/usr/local/spark/jars/extra/postgresql-42.7.3.jar"

spark = SparkSession.builder \
    .appName("StreamEnrich") \
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

PG = dict(host="postgres", dbname="omnifleet", user="omnifleet", password="omnifleet123")


def pg_conn():
    return psycopg2.connect(**PG)


# =============================================================
# 1. BUILD THE ACTIVE-TRIP INDEX (once, at startup)
# =============================================================
def build_trip_index():
    print("Building active-trip index from CSVs ...")
    trips = pd.read_csv(f"{DATA_DIR}/trips.csv")
    tc = pd.read_csv(f"{DATA_DIR}/trip_cargo.csv")
    cargo = pd.read_csv(f"{DATA_DIR}/cargo.csv")

    trips = trips[trips["vehicle_id"] < NUM_CARS]

    tcj = tc.merge(cargo, on="cargo_id", how="left")
    bounds = tcj.groupby("trip_id").agg(
        min_temp=("min_temp", "min"),
        max_temp=("max_temp", "max"),
        cargo_type=("cargo_type", "first"),
    ).reset_index()
    trips = trips.merge(bounds, on="trip_id", how="left")

    index = {}
    for r in trips.itertuples():
        try:
            s = datetime.strptime(str(r.actual_start_time)[:19], "%Y-%m-%d %H:%M:%S").timestamp()
            e = datetime.strptime(str(r.actual_end_time)[:19], "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:
            continue
        v = int(r.vehicle_id)
        index.setdefault(v, []).append(
            (s, e, int(r.trip_id), int(r.driver_id), r.cargo_type, r.min_temp, r.max_temp))

    starts = {}
    for v in index:
        index[v].sort(key=lambda x: x[0])
        starts[v] = [w[0] for w in index[v]]
    print(f"  indexed {sum(len(x) for x in index.values())} trips for {len(index)} vehicles")
    return index, starts


TRIP_INDEX, TRIP_STARTS = build_trip_index()


def active_trip(vehicle_id, t_epoch):
    lst = TRIP_INDEX.get(vehicle_id)
    if not lst:
        return None
    starts = TRIP_STARTS[vehicle_id]
    i = bisect.bisect_right(starts, t_epoch) - 1
    if i < 0:
        return None
    s, e, trip_id, driver_id, ctype, mn, mx = lst[i]
    if s <= t_epoch <= e:
        return (trip_id, driver_id, ctype, mn, mx, s, e)
    return None


def to_epoch(ts_str):
    try:
        return datetime.strptime(str(ts_str)[:19], "%Y-%m-%d %H:%M:%S").timestamp()
    except Exception:
        return None


def refresh_colours(cur):
    cur.execute("""
        UPDATE live_vehicle_status s
        SET incident_color = c.color, incident_label = c.itype, last_event_time = c.et
        FROM (
            SELECT DISTINCT ON (vehicle_id) vehicle_id,
                   incident_color AS color, incident_type AS itype, event_time AS et,
                   CASE incident_color WHEN 'RED' THEN 4 WHEN 'ORANGE' THEN 3
                        WHEN 'YELLOW' THEN 2 WHEN 'BLUE' THEN 1 ELSE 0 END AS sev
            FROM live_incident_feed
            WHERE detected_at > now() - interval '2 minutes'
            ORDER BY vehicle_id, sev DESC, event_time DESC
        ) c
        WHERE s.vehicle_id = c.vehicle_id;
    """)
    cur.execute("""
        UPDATE live_vehicle_status
        SET incident_color = 'WHITE', incident_label = 'OK'
        WHERE vehicle_id NOT IN (
            SELECT vehicle_id FROM live_incident_feed
            WHERE detected_at > now() - interval '2 minutes'
        );
    """)


# =============================================================
# 2. QUICK STREAM (3-sec) - position + speeding / drift / battery
# =============================================================
quick_schema = StructType([
    StructField("vehicle_id", StringType()), StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()), StructField("lat", StringType()),
    StructField("lon", StringType()), StructField("odometer_km", StringType()),
    StructField("rpm", StringType()), StructField("throttle_pct", StringType()),
    StructField("accel_ay_abs", StringType()), StructField("battery_v", StringType()),
])

quick = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "vehicle.quick.sensors") \
    .option("startingOffsets", "latest").load() \
    .select(F.from_json(F.col("value").cast("string"), quick_schema).alias("j")).select("j.*")


def process_quick(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    positions = {}   # vehicle_id -> latest (vid, lat, lon, ts) this batch
    incidents = []
    for r in rows:
        try:
            vid = int(r["vehicle_id"]); t = to_epoch(r["timestamp"])
            lat = float(r["lat"]); lon = float(r["lon"])
            throttle = float(r["throttle_pct"]); accel = float(r["accel_ay_abs"]); batt = float(r["battery_v"])
        except (TypeError, ValueError):
            continue
        # keep only the LATEST position per vehicle in this batch (a vehicle
        # pings several times per batch; upserting the same vehicle_id twice in
        # one statement is what Postgres rejects). Dict keyed by vehicle_id wins.
        positions[vid] = (vid, lat, lon, r["timestamp"])

        at = active_trip(vid, t) if t else None
        trip_id = at[0] if at else None
        driver_id = at[1] if at else None

        if batt < BATTERY_LOW:
            incidents.append((vid, trip_id, driver_id, r["timestamp"], "BLUE", "BATTERY_LOW",
                              f"Battery {batt}V", lat, lon))
        if throttle > THROTTLE_SPEEDING:
            incidents.append((vid, trip_id, driver_id, r["timestamp"], "YELLOW", "SPEEDING",
                              f"Throttle {throttle}%", lat, lon))
        elif accel > DRIFT_G:
            incidents.append((vid, trip_id, driver_id, r["timestamp"], "YELLOW", "DRIFT",
                              f"Lateral G {accel}", lat, lon))

    conn = pg_conn(); cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO live_vehicle_status (vehicle_id, lat, lon, last_event_time, incident_color, incident_label)
        VALUES %s
        ON CONFLICT (vehicle_id) DO UPDATE SET
            lat = EXCLUDED.lat, lon = EXCLUDED.lon, last_event_time = EXCLUDED.last_event_time
    """, [(v, lat, lon, ts, "WHITE", "OK") for (v, lat, lon, ts) in positions.values()])

    if incidents:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO live_incident_feed
            (vehicle_id, trip_id, driver_id, event_time, incident_color, incident_type, detail, lat, lon)
            VALUES %s
        """, incidents)

    refresh_colours(cur)
    conn.commit(); cur.close(); conn.close()
    print(f"quick batch {batch_id}: {len(positions)} positions, {len(incidents)} incidents")


# =============================================================
# 3. SLOW STREAM (1-min) - fuel fraud + EXACT cargo breach + mid-route door
# =============================================================
slow_schema = StructType([
    StructField("vehicle_id", StringType()), StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()), StructField("fuel_amount_l", StringType()),
    StructField("is_door_open", StringType()), StructField("cargo_temp_c", StringType()),
])

slow = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "vehicle.slow.sensors") \
    .option("startingOffsets", "latest").load() \
    .select(F.from_json(F.col("value").cast("string"), slow_schema).alias("j")).select("j.*")


def process_slow(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return
    conn = pg_conn(); cur = conn.cursor()
    incidents = []
    for r in rows:
        try:
            vid = int(r["vehicle_id"]); t = to_epoch(r["timestamp"])
            fuel = float(r["fuel_amount_l"]); door = int(r["is_door_open"]); temp = float(r["cargo_temp_c"])
        except (TypeError, ValueError):
            continue

        at = active_trip(vid, t) if t else None
        trip_id = at[0] if at else None
        driver_id = at[1] if at else None

        cur.execute("SELECT last_fuel_l FROM live_fuel_state WHERE vehicle_id=%s", (vid,))
        prev = cur.fetchone()
        fuel_drop = (prev[0] - fuel) if (prev and prev[0] is not None and prev[0] > fuel) else 0.0
        cur.execute("""
            INSERT INTO live_fuel_state (vehicle_id, last_fuel_l, last_event_time) VALUES (%s,%s,%s)
            ON CONFLICT (vehicle_id) DO UPDATE SET last_fuel_l=EXCLUDED.last_fuel_l,
                last_event_time=EXCLUDED.last_event_time
        """, (vid, fuel, r["timestamp"]))

        if fuel_drop >= FUEL_DROP_FRAUD:
            incidents.append((vid, trip_id, driver_id, r["timestamp"], "RED", "FUEL_FRAUD",
                              f"Fuel siphoned {round(fuel_drop,1)}L"))
        elif door == 1 and at is not None and (t > at[5] + DOOR_EDGE_SECONDS) and (t < at[6] - DOOR_EDGE_SECONDS):
            incidents.append((vid, trip_id, driver_id, r["timestamp"], "RED", "DOOR_OPEN",
                              "Cargo door open mid-route"))
        elif at is not None and (temp > at[4] or temp < at[3]):
            incidents.append((vid, trip_id, driver_id, r["timestamp"], "ORANGE", "CARGO_BREACH",
                              f"Temp {temp}C (limit {at[3]}..{at[4]})"))

    if incidents:
        psycopg2.extras.execute_values(cur, """
            INSERT INTO live_incident_feed
            (vehicle_id, trip_id, driver_id, event_time, incident_color, incident_type, detail)
            VALUES %s
        """, incidents)

    refresh_colours(cur)
    conn.commit(); cur.close(); conn.close()
    print(f"slow batch {batch_id}: {len(rows)} rows, {len(incidents)} incidents")


# =============================================================
# 4. start both streams
# =============================================================
q = quick.writeStream.foreachBatch(process_quick) \
    .option("checkpointLocation", "s3a://omnifleet-bronze/_checkpoints/enrich_quick") \
    .trigger(processingTime="10 seconds").start()

s = slow.writeStream.foreachBatch(process_slow) \
    .option("checkpointLocation", "s3a://omnifleet-bronze/_checkpoints/enrich_slow") \
    .trigger(processingTime="15 seconds").start()

print("Stream enrichment running (reliable mode): exact cargo bounds + active-trip lookup")
spark.streams.awaitAnyTermination()
