# =============================================================
# OmniFleet V003 - DWH watermark helpers  (NOT an Airflow task)
# =============================================================
# Imported by the daily scripts (bronze_daily_load.py, silver_daily_build.py,
# telemetry_trip_agg_daily.py, staging_load_daily.py). The watermark logic is
# EMBEDDED inside each script - this module just holds the shared functions so
# we don't copy-paste the same SQL five times.
#
# One row per pipeline STAGE in dwh.etl_checkpoints. Each stage tracks the last
# date it successfully processed, so a stage only ever advances after its own
# work succeeded. Re-running a date that a stage already did is safe (the data
# scripts replace-by-date, and the dbt fact merges on trip_sk).
#
# BACKFILL boundary: the historical load covered up to 2025-05-08, so every
# stage is seeded to that date and the daily pipeline starts at 2025-05-09.
# =============================================================
from datetime import date, datetime, timedelta

import psycopg2

PG = dict(host="postgres", dbname="omnifleet", user="omnifleet", password="omnifleet123")
BACKFILL_END = "2025-05-08"   # last date the one-time backfill processed


def _conn():
    return psycopg2.connect(**PG)


def ensure_checkpoints():
    """Create dwh.etl_checkpoints if missing and seed each stage. Idempotent."""
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS dwh;
        CREATE TABLE IF NOT EXISTS dwh.etl_checkpoints (
            stage_name           TEXT PRIMARY KEY,
            last_processed_date  DATE NOT NULL,
            last_run_at          TIMESTAMP NOT NULL DEFAULT now(),
            rows_processed       INT,
            status               TEXT
        );
    """)
    for stage in ("bronze", "silver", "telemetry", "staging", "dbt"):
        cur.execute("""
            INSERT INTO dwh.etl_checkpoints (stage_name, last_processed_date, status)
            VALUES (%s, %s::date, 'seeded')
            ON CONFLICT (stage_name) DO NOTHING;
        """, (stage, BACKFILL_END))
    conn.commit(); cur.close(); conn.close()


def get_target_date(stage: str) -> str:
    """Return the next date this stage should process = last_processed_date + 1 day."""
    ensure_checkpoints()
    conn = _conn(); cur = conn.cursor()
    cur.execute("SELECT last_processed_date FROM dwh.etl_checkpoints WHERE stage_name=%s", (stage,))
    last = cur.fetchone()[0]
    cur.close(); conn.close()
    return (last + timedelta(days=1)).isoformat()


def advance_watermark(stage: str, target_date: str, rows: int):
    """Mark this stage as having successfully processed target_date."""
    conn = _conn(); cur = conn.cursor()
    cur.execute("""
        UPDATE dwh.etl_checkpoints
           SET last_processed_date = %s::date,
               last_run_at = now(),
               rows_processed = %s,
               status = 'success'
         WHERE stage_name = %s;
    """, (target_date, rows, stage))
    conn.commit(); cur.close(); conn.close()
    print(f"[watermark] stage '{stage}' advanced to {target_date} ({rows} rows).")
