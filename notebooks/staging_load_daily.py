# =============================================================
# OmniFleet V003 - staging_load_daily  (silver slice -> Postgres staging)
# =============================================================
# Loads the target date's silver trips into staging.stg_trips (and the cargo
# rollup), using DELETE-by-date then INSERT so re-running a date is idempotent.
# Telemetry is already in staging (written by telemetry_trip_agg_daily).
#
# Watermark for the 'staging' stage embedded here.
# =============================================================
import sys
import pandas as pd
import psycopg2
import psycopg2.extras
import s3fs

from dwh_watermark import get_target_date, advance_watermark

STAGE = "staging"
STORAGE = dict(key="omnifleet", secret="omnifleet123",
               client_kwargs={"endpoint_url": "http://minio:9000"})
SILVER_TPL = "omnifleet-silver/daily_trips/dt={d}"
PG = dict(host="postgres", dbname="omnifleet", user="omnifleet", password="omnifleet123")


def main():
    target = get_target_date(STAGE)
    print(f"[staging_load_daily] target_date = {target}")
    fs = s3fs.S3FileSystem(**STORAGE)

    src = SILVER_TPL.format(d=target)
    if not fs.exists(src):
        print(f"[staging_load_daily] WARNING: no silver slice at {src}. 0 rows.")
        advance_watermark(STAGE, target, rows=0)
        return

    parts = [p for p in fs.ls(src) if p.endswith(".parquet")]
    frames = [pd.read_parquet(f"s3://{p}", filesystem=fs) for p in parts]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    rows = len(df)

    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    # delete this date's trips then insert (idempotent)
    cur.execute("""DELETE FROM staging.stg_trips
                   WHERE actual_start_time::date = %s::date""", (target,))
    # (column list mirrors stg_trips; assumes silver carries the same columns)
    if rows:
        cols = list(df.columns)
        psycopg2.extras.execute_values(cur,
            f"INSERT INTO staging.stg_trips ({','.join(cols)}) VALUES %s",
            list(df.itertuples(index=False, name=None)), page_size=5000)
    conn.commit(); cur.close(); conn.close()
    print(f"[staging_load_daily] loaded {rows} trips for {target}")
    advance_watermark(STAGE, target, rows=rows)


if __name__ == "__main__":
    main()
