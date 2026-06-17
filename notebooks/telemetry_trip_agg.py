# =============================================================
# OmniFleet V003 - Telemetry -> Per-Trip Aggregates  (LOW-MEMORY version)
# =============================================================
# WHY THIS VERSION: the 3-second quick-sensor data is huge (tens of GB for
# all vehicles). Loading it all into Spark at once runs out of memory on a
# normal laptop. This version processes the sensor files ONE VEHICLE AT A
# TIME with pandas, aggregates, discards, and moves on - so memory stays
# flat (under ~1 GB) no matter how many vehicles. Tested: ~25s and <700MB
# for ~16M sensor rows.
#
# Output (MinIO silver): silver_trip_telemetry_agg  (one row per trip)
#   trip_id, quick_pings_count, slow_pings_count, engine_fault_ticks,
#   speeding_ticks, drift_ticks, battery_fault_ticks, door_open_ticks,
#   cargo_breach_ticks, total_fuel_consumed_l, total_distance_km
# =============================================================

import glob
import os
import time

import pandas as pd
import s3fs   # lets pandas write parquet straight to MinIO

# -------- thresholds (match the data generator) --------
RPM_FAULT = 4200
THROTTLE_SPEEDING = 85
DRIFT_G = 0.75
BATTERY_LOW = 11.4

DATA_DIR = "/home/jovyan/work/data"
QUICK_DIR = f"{DATA_DIR}/quick_sensors"
SLOW_DIR = f"{DATA_DIR}/slow_sensors"

# MinIO (s3) connection for pandas/pyarrow
STORAGE = dict(
    key="omnifleet",
    secret="omnifleet123",
    client_kwargs={"endpoint_url": "http://minio:9000"},
)
SILVER = "omnifleet-silver"


def _read_silver(name, fs):
    """Read a Spark-written parquet directory from MinIO robustly.
    Spark writes a FOLDER of part-*.parquet files; we list and read them
    explicitly so pyarrow never trips on the directory form."""
    base = f"{SILVER}/{name}"
    parts = [p for p in fs.ls(base) if p.endswith(".parquet")]
    if not parts:
        raise FileNotFoundError(f"No parquet parts under {base}")
    frames = [pd.read_parquet(f"s3://{p}", filesystem=fs) for p in parts]
    return pd.concat(frames, ignore_index=True)


def read_trips_index():
    """Load trip windows (per vehicle) + per-trip cargo thermal bounds. Small."""
    print("Loading trips + cargo bounds ...")
    fs = s3fs.S3FileSystem(**STORAGE)
    trips = _read_silver("silver_trips", fs)
    trips = trips[["trip_id", "vehicle_id", "actual_start_time", "actual_end_time"]].copy()
    trips["s"] = pd.to_datetime(trips["actual_start_time"])
    trips["e"] = pd.to_datetime(trips["actual_end_time"])

    tc = _read_silver("silver_trip_cargo", fs)[["trip_id", "cargo_id"]]
    cg = _read_silver("silver_cargo", fs)[["cargo_id", "min_temp", "max_temp"]]
    bounds = tc.merge(cg, on="cargo_id", how="left").groupby("trip_id").agg(
        tmin=("min_temp", "min"), tmax=("max_temp", "max")).reset_index()

    trips_by_v = {v: g.sort_values("s") for v, g in trips.groupby("vehicle_id")}
    return trips_by_v, bounds, fs


def assign_to_trip(df, vid, trips_by_v):
    """Match each sensor row to the trip whose [start,end] window contains it."""
    g = trips_by_v.get(vid)
    if g is None:
        return None
    df = df.copy()
    df["event_time"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("event_time")
    m = pd.merge_asof(df, g[["s", "e", "trip_id"]], left_on="event_time",
                      right_on="s", direction="backward")
    return m[(m.event_time >= m.s) & (m.event_time <= m.e)]


def main():
    t0 = time.time()
    trips_by_v, bounds, fs = read_trips_index()

    # ---------- QUICK sensors, file by file ----------
    print("Aggregating quick sensors (per vehicle) ...")
    qres = []
    for f in sorted(glob.glob(f"{QUICK_DIR}/*.csv")):
        vid = int(os.path.basename(f).split(".")[0])
        df = pd.read_csv(f)
        m = assign_to_trip(df, vid, trips_by_v)
        if m is None or len(m) == 0:
            continue
        m["ef"] = (m.rpm.astype(float) > RPM_FAULT).astype(int)
        m["sp"] = (m.throttle_pct.astype(float) > THROTTLE_SPEEDING).astype(int)
        m["dr"] = (m.accel_ay_abs.astype(float) > DRIFT_G).astype(int)
        m["bf"] = (m.battery_v.astype(float) < BATTERY_LOW).astype(int)
        qres.append(m.groupby("trip_id").agg(
            quick_pings_count=("trip_id", "size"),
            engine_fault_ticks=("ef", "sum"),
            speeding_ticks=("sp", "sum"),
            drift_ticks=("dr", "sum"),
            battery_fault_ticks=("bf", "sum"),
            total_distance_km=("odometer_km", lambda x: round(float(x.astype(float).max() - x.astype(float).min()), 2)),
        ))
        del df, m
    quick_agg = pd.concat(qres).reset_index() if qres else pd.DataFrame()
    print(f"  quick: {len(quick_agg)} trips")

    # ---------- SLOW sensors, file by file ----------
    print("Aggregating slow sensors (per vehicle) ...")
    sres = []
    for f in sorted(glob.glob(f"{SLOW_DIR}/*.csv")):
        vid = int(os.path.basename(f).split(".")[0])
        df = pd.read_csv(f)
        m = assign_to_trip(df, vid, trips_by_v)
        if m is None or len(m) == 0:
            continue
        m = m.merge(bounds, on="trip_id", how="left")
        m["breach"] = ((m.cargo_temp_c.astype(float) > m.tmax) |
                       (m.cargo_temp_c.astype(float) < m.tmin)).astype(int)
        # fuel = sum of positive drops between consecutive readings (handles refuels)
        m = m.sort_values(["trip_id", "event_time"])
        m["prev"] = m.groupby("trip_id")["fuel_amount_l"].shift(1)
        m["fdrop"] = (m["prev"].astype(float) - m.fuel_amount_l.astype(float)).clip(lower=0).fillna(0)
        sres.append(m.groupby("trip_id").agg(
            slow_pings_count=("trip_id", "size"),
            door_open_ticks=("is_door_open", lambda x: int(x.astype(int).sum())),
            cargo_breach_ticks=("breach", "sum"),
            total_fuel_consumed_l=("fdrop", lambda x: round(float(x.sum()), 2)),
        ))
        del df, m
    slow_agg = pd.concat(sres).reset_index() if sres else pd.DataFrame()
    print(f"  slow: {len(slow_agg)} trips")

    # ---------- combine + write to MinIO silver ----------
    if len(quick_agg) and len(slow_agg):
        telem = quick_agg.merge(slow_agg, on="trip_id", how="outer")
    elif len(quick_agg):
        telem = quick_agg
    else:
        telem = slow_agg
    # fill any missing tick columns with 0
    for c in ["quick_pings_count", "slow_pings_count", "engine_fault_ticks", "speeding_ticks",
              "drift_ticks", "battery_fault_ticks", "door_open_ticks", "cargo_breach_ticks",
              "total_fuel_consumed_l", "total_distance_km"]:
        if c not in telem.columns:
            telem[c] = 0
    telem = telem.fillna(0)

    # optional audit copy to MinIO (non-fatal: the real output is Postgres below)
    try:
        out = f"{SILVER}/silver_trip_telemetry_agg/part-0.parquet"
        telem.to_parquet(f"s3://{out}", filesystem=fs, index=False)
        print(f"  wrote parquet audit copy to MinIO: {len(telem)} trips")
    except Exception as ex:
        print(f"  (skipped MinIO parquet copy: {ex})")

    # ALSO write straight to Postgres staging (so the dbt step has it without
    # needing a separate Spark job to copy it). This is the table dbt reads.
    import psycopg2
    import psycopg2.extras
    cols = ["trip_id", "quick_pings_count", "slow_pings_count", "engine_fault_ticks",
            "speeding_ticks", "drift_ticks", "battery_fault_ticks", "door_open_ticks",
            "cargo_breach_ticks", "total_fuel_consumed_l", "total_distance_km"]
    telem = telem[cols].astype({c: "int" for c in cols[:-2]})
    conn = psycopg2.connect(host="postgres", dbname="omnifleet",
                            user="omnifleet", password="omnifleet123")
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cur.execute("DROP TABLE IF EXISTS staging.stg_trip_telemetry_agg;")
    cur.execute("""
        CREATE TABLE staging.stg_trip_telemetry_agg (
            trip_id INT, quick_pings_count INT, slow_pings_count INT,
            engine_fault_ticks INT, speeding_ticks INT, drift_ticks INT,
            battery_fault_ticks INT, door_open_ticks INT, cargo_breach_ticks INT,
            total_fuel_consumed_l DOUBLE PRECISION, total_distance_km DOUBLE PRECISION
        );
    """)
    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO staging.stg_trip_telemetry_agg ({','.join(cols)}) VALUES %s",
        list(telem.itertuples(index=False, name=None)),
        page_size=5000,
    )
    conn.commit()
    cur.execute("SELECT count(*) FROM staging.stg_trip_telemetry_agg;")
    n = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"  staging.stg_trip_telemetry_agg: {n} rows written to Postgres")
    print(f"Telemetry trip aggregation finished in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
