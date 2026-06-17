# =============================================================
# OmniFleet V003 - Airflow Batch Pipeline DAG
# =============================================================
# Orchestrates the BATCH path only (the streaming jobs run always-on
# outside Airflow). Order:
#   create_gold_schema -> bronze_static_load -> silver_build
#   -> telemetry_trip_agg -> staging_load -> dbt_run -> dbt_test
#
# Spark jobs are launched with docker exec into the spark container.
# =============================================================

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# the MinIO jars every spark job needs
MINIO_JARS = ("/usr/local/spark/jars/extra/hadoop-aws-3.3.4.jar,"
              "/usr/local/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar")
# staging_load also needs the postgres jar
PG_JAR = "/usr/local/spark/jars/extra/postgresql-42.7.3.jar"

SPARK = "omnifleet-spark-v003"
WORK = "/home/jovyan/work"


def spark_submit(script, extra_jars=""):
    jars = MINIO_JARS + ("," + extra_jars if extra_jars else "")
    return (
        f"docker exec {SPARK} spark-submit "
        f"--jars {jars} "
        f"--driver-class-path {jars.replace(',', ':')} "
        f"{WORK}/{script}"
    )


default_args = {"owner": "omnifleet", "retries": 1}

with DAG(
    dag_id="omnifleet_v003_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,     # triggered manually
    catchup=False,
    tags=["omnifleet", "v003", "batch"],
) as dag:

    # 0. build staging schema + dim_date + live tables (idempotent)
    create_gold_schema = BashOperator(
        task_id="create_gold_schema",
        bash_command=(
            f"docker exec {SPARK} bash -c "
            f"\"PGPASSWORD=omnifleet123 psql -h postgres -U omnifleet -d omnifleet "
            f"-f {WORK}/gold_schema.sql\""
        ),
    )

    bronze_static_load = BashOperator(
        task_id="bronze_static_load",
        bash_command=spark_submit("bronze_static_load.py"),
    )

    silver_build = BashOperator(
        task_id="silver_build",
        bash_command=spark_submit("silver_build.py"),
    )

    telemetry_trip_agg = BashOperator(
        task_id="telemetry_trip_agg",
        # low-memory pandas job (NOT spark) - installs its deps then runs
        bash_command=(
            f"docker exec {SPARK} bash -c "
            f"\"pip install s3fs psycopg2-binary pyarrow --quiet 2>/dev/null; "
            f"python {WORK}/telemetry_trip_agg.py\""
        ),
    )

    staging_load = BashOperator(
        task_id="staging_load",
        bash_command=spark_submit("staging_load.py", extra_jars=PG_JAR),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"docker exec omnifleet-dbt-v003 bash -c "
            f"\"cd /dbt && dbt run --profiles-dir /dbt\""
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"docker exec omnifleet-dbt-v003 bash -c "
            f"\"cd /dbt && dbt test --profiles-dir /dbt\""
        ),
    )

    create_gold_schema >> bronze_static_load >> silver_build \
        >> telemetry_trip_agg >> staging_load >> dbt_run >> dbt_test
