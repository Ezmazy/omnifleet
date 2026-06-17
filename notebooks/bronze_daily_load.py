# =============================================================
# OmniFleet V003 - bronze_daily_load  (one day's slice from MinIO)
# =============================================================
# bronze_streaming.py continuously lands raw Kafka events into MinIO at
# s3://omnifleet-bronze/stream/dt=YYYY-MM-DD/. This job reads the ONE partition
# for the target date and writes it to the daily bronze area for downstream
# silver to pick up.
#
# Watermark for the 'bronze' stage is embedded here. Empty partition (a quiet
# day with no streamed data) logs a warning and exits 0 - not an error.
# =============================================================
import sys

import s3fs

from dwh_watermark import get_target_date, advance_watermark

STAGE = "bronze"
STORAGE = dict(key="omnifleet", secret="omnifleet123",
               client_kwargs={"endpoint_url": "http://minio:9000"})
SRC_TPL = "omnifleet-bronze/stream/dt={d}"
DST_TPL = "omnifleet-bronze/daily/dt={d}"


def main():
    target = get_target_date(STAGE)
    print(f"[bronze_daily_load] target_date = {target}")
    fs = s3fs.S3FileSystem(**STORAGE)

    src = SRC_TPL.format(d=target)
    if not fs.exists(src):
        print(f"[bronze_daily_load] WARNING: no streamed partition at {src}. "
              f"Quiet day - nothing to load. Advancing watermark with 0 rows.")
        advance_watermark(STAGE, target, rows=0)
        return

    parts = [p for p in fs.ls(src) if p.endswith(".parquet")]
    rows = 0
    dst = DST_TPL.format(d=target)
    for p in parts:
        # copy partition forward into the daily bronze zone (raw -> bronze)
        fname = p.split("/")[-1]
        fs.copy(p, f"{dst}/{fname}")
        rows += 1   # count files; row count is computed in silver
    print(f"[bronze_daily_load] copied {rows} part-files into {dst}")
    advance_watermark(STAGE, target, rows=rows)


if __name__ == "__main__":
    main()
