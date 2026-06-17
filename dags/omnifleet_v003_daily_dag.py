"""
OmniFleet V003 - DAILY incremental ETL (REAL)
=============================================
Runs at 01:00 every day. Processes ONE day's slice on top of the historical
backfill, with watermark idempotency and a dbt incremental MERGE so the gold
fact never gets duplicate trips.

Tasks (exactly the six pipeline stages - nothing else is a task):
    bronze_daily_load -> silver_daily_build -> telemetry_trip_agg_daily
      -> staging_load_daily -> dbt_run_daily -> dbt_test_daily

Container layout (important):
  - omnifleet-spark-v003 : has the daily python scripts at /home/jovyan/work
    and psycopg2/pandas/s3fs. Runs the first 4 stages AND the watermark helpers.
  - omnifleet-dbt-v003   : has dbt + the project at /dbt, but NO /home/jovyan/work
    mount. So dbt is invoked here directly (like the backfill DAG does), while
    the target-date lookup + watermark advance run in the spark container.

The watermark logic lives INSIDE the scripts (dwh_watermark.py helper) - it is
NOT a task. Each stage advances its checkpoint only on success, so a failure
leaves the watermark untouched and the next run retries the same date.
"""
from __future__ import annotations
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK = "omnifleet-spark-v003"
DBT   = "omnifleet-dbt-v003"
WORK  = "/home/jovyan/work"
DBT_DIR = "/dbt"


# run a python daily script inside the SPARK container (has scripts + libs)
def py(script: str) -> str:
    return (f"docker exec {SPARK} bash -c "
            f"\"pip install s3fs psycopg2-binary pyarrow --quiet 2>/dev/null; "
            f"python {WORK}/{script}\"")


# dbt_run: get target date from the watermark (spark container, which has the
# helper), run dbt incrementally in the DBT container with that date, then
# advance the 'dbt' watermark (spark container again). Chained with && so a
# dbt failure aborts before the watermark advances.
def dbt_run_cmd() -> str:
    # Multi-step, written as a single bash script with NO deeply-nested quotes.
    # Airflow runs this on the scheduler host (which can docker exec, like the
    # backfill DAG). Steps: get target date (spark) -> dbt run (dbt) -> advance
    # watermark (spark). 'set -e' aborts before the watermark if dbt fails.
    return (
        "set -e\n"
        f"TARGET=$(docker exec {SPARK} python {WORK}/get_dbt_target.py)\n"
        "echo \"dbt target_date=$TARGET\"\n"
        f"docker exec {DBT} bash -lc \"cd {DBT_DIR} && dbt run --select +fct_trip_operations --vars '{{target_date: $TARGET}}' --profiles-dir {DBT_DIR}\"\n"
        f"docker exec {SPARK} python {WORK}/advance_dbt_wm.py \"$TARGET\"\n"
    )


def dbt_test_cmd() -> str:
    return (f"docker exec {DBT} bash -c "
            f"\"cd {DBT_DIR} && dbt test --select fct_trip_operations --profiles-dir {DBT_DIR}\"")


default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="omnifleet_v003_daily",
    description="Daily incremental ETL with watermark + dbt incremental merge",
    schedule="0 1 * * *",
    start_date=datetime(2025, 5, 9),
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    tags=["omnifleet", "daily", "incremental"],
) as dag:

    bronze_daily_load = BashOperator(
        task_id="bronze_daily_load", bash_command=py("bronze_daily_load.py"))
    silver_daily_build = BashOperator(
        task_id="silver_daily_build", bash_command=py("silver_daily_build.py"))
    telemetry_trip_agg_daily = BashOperator(
        task_id="telemetry_trip_agg_daily", bash_command=py("telemetry_trip_agg_daily.py"))
    staging_load_daily = BashOperator(
        task_id="staging_load_daily", bash_command=py("staging_load_daily.py"))
    dbt_run_daily = BashOperator(
        task_id="dbt_run_daily", bash_command=dbt_run_cmd())
    dbt_test_daily = BashOperator(
        task_id="dbt_test_daily", bash_command=dbt_test_cmd())

    (bronze_daily_load >> silver_daily_build >> telemetry_trip_agg_daily
     >> staging_load_daily >> dbt_run_daily >> dbt_test_daily)
