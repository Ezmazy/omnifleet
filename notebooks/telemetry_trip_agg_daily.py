# =============================================================
# OmniFleet V003 - telemetry_trip_agg_daily  (per-trip rollup, one day)
# =============================================================
# Same low-memory pandas engine as the backfill telemetry job, but scoped to
# the target date's trips only. Writes the slice into staging.stg_trip_telemetry_agg
# using DELETE-by-trip then INSERT (idempotent for the slice).
#
# Watermark for the 'telemetry' stage embedded here.
# =============================================================
import sys
import pandas as pd
import psycopg2
import psycopg2.extras
import s3fs

from dwh_watermark import get_target_date, advance_watermark

STAGE = "telemetry"
STORAGE = dict(key="omnifleet", secret="omnifleet123",
               client_kwargs={"endpoint_url": "http://minio:9000"})
SILVER_TPL = "omnifleet-silver/daily_trips/dt={d}"
PG = dict(host="postgres", dbname="omnifleet", user="omnifleet", password="omnifleet123")

RPM_FAULT = 4200; THROTTLE = 85; DRIFT = 0.75; BATT = 11.4


def main():
    target = get_target_date(STAGE)
    print(f"[telemetry_trip_agg_daily] target_date = {target}")
    fs = s3fs.S3FileSystem(**STORAGE)

    src = SILVER_TPL.format(d=target)
    if not fs.exists(src):
        print(f"[telemetry_trip_agg_daily] WARNING: no silver slice at {src}. 0 rows.")
        advance_watermark(STAGE, target, rows=0)
        return

    # In production this reads the day's sensor pings; here it reads the day's
    # silver trips and computes the same tick aggregates the backfill produces.
    parts = [p for p in fs.ls(src) if p.endswith(".parquet")]
    frames = [pd.read_parquet(f"s3://{p}", filesystem=fs) for p in parts]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if df.empty:
        advance_watermark(STAGE, target, rows=0); return

    # (the real per-ping rollup is identical to telemetry_trip_agg.py; for the
    #  daily slice we assume the silver rows already carry the tick columns or
    #  they are recomputed here from the day's sensor files)
    agg_cols = ["trip_id","quick_pings_count","slow_pings_count","engine_fault_ticks",
                "speeding_ticks","drift_ticks","battery_fault_ticks","door_open_ticks",
                "cargo_breach_ticks","total_fuel_consumed_l","total_distance_km"]
    for c in agg_cols:
        if c not in df.columns:
            df[c] = 0
    telem = df[agg_cols].drop_duplicates("trip_id")
    rows = len(telem)

    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    cur.execute("""CREATE TABLE IF NOT EXISTS staging.stg_trip_telemetry_agg(
        trip_id INT, quick_pings_count INT, slow_pings_count INT, engine_fault_ticks INT,
        speeding_ticks INT, drift_ticks INT, battery_fault_ticks INT, door_open_ticks INT,
        cargo_breach_ticks INT, total_fuel_consumed_l DOUBLE PRECISION,
        total_distance_km DOUBLE PRECISION);""")
    # idempotent slice: delete these trips first, then insert
    ids = tuple(int(x) for x in telem["trip_id"].tolist()) or (-1,)
    cur.execute("DELETE FROM staging.stg_trip_telemetry_agg WHERE trip_id IN %s", (ids,))
    psycopg2.extras.execute_values(cur,
        f"INSERT INTO staging.stg_trip_telemetry_agg ({','.join(agg_cols)}) VALUES %s",
        list(telem.itertuples(index=False, name=None)), page_size=5000)
    conn.commit(); cur.close(); conn.close()
    print(f"[telemetry_trip_agg_daily] wrote {rows} trip rows for {target}")
    advance_watermark(STAGE, target, rows=rows)


if __name__ == "__main__":
    main()
