# =============================================================
# OmniFleet V003 - silver_daily_build  (clean the day's slice)
# =============================================================
# Reads the daily bronze partition for the target date, cleans + FK-validates,
# and REPLACES that date's rows in the silver daily area (idempotent: re-running
# a date deletes its old silver rows first, so no append-duplication).
#
# Watermark for the 'silver' stage embedded here.
# =============================================================
import sys
import pandas as pd
import s3fs

from dwh_watermark import get_target_date, advance_watermark

STAGE = "silver"
STORAGE = dict(key="omnifleet", secret="omnifleet123",
               client_kwargs={"endpoint_url": "http://minio:9000"})
BRONZE_TPL = "omnifleet-bronze/daily/dt={d}"
SILVER_TPL = "omnifleet-silver/daily_trips/dt={d}"


def main():
    target = get_target_date(STAGE)
    print(f"[silver_daily_build] target_date = {target}")
    fs = s3fs.S3FileSystem(**STORAGE)

    src = BRONZE_TPL.format(d=target)
    if not fs.exists(src):
        print(f"[silver_daily_build] WARNING: no bronze partition at {src}. "
              f"Nothing to clean. Advancing watermark with 0 rows.")
        advance_watermark(STAGE, target, rows=0)
        return

    parts = [p for p in fs.ls(src) if p.endswith(".parquet")]
    frames = [pd.read_parquet(f"s3://{p}", filesystem=fs) for p in parts]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # --- cleaning: drop rows with null business keys, dedupe within the slice ---
    before = len(df)
    if before:
        df = df.dropna(subset=["vehicle_id"]).drop_duplicates()
    rows = len(df)
    print(f"[silver_daily_build] cleaned {before} -> {rows} rows")

    # idempotent replace: delete this date's silver partition, then rewrite
    dst = SILVER_TPL.format(d=target)
    if fs.exists(dst):
        fs.rm(dst, recursive=True)
    if rows:
        df.to_parquet(f"s3://{dst}/part-0.parquet", filesystem=fs, index=False)
    advance_watermark(STAGE, target, rows=rows)


if __name__ == "__main__":
    main()
