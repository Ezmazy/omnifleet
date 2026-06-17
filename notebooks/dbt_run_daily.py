# =============================================================
# OmniFleet V003 - dbt_run_daily  (incremental merge for one day)
# =============================================================
# Runs dbt incrementally for the target date. The fct_trip_operations model
# is materialized='incremental', unique_key='trip_sk', strategy='merge' - so
# this MERGEs the day's trips into the gold fact: new trips inserted, existing
# updated, NEVER duplicated.
#
# Watermark for the 'dbt' stage is advanced on success (embedded here).
# =============================================================
import subprocess
import sys

from dwh_watermark import get_target_date, advance_watermark

DBT_PROJECT = "/home/jovyan/work/dbt"   # adjust if your dbt project path differs
STAGE = "dbt"


def main():
    target = get_target_date(STAGE)
    print(f"[dbt_run_daily] target_date = {target}")

    cmd = [
        "dbt", "run",
        "--select", "+fct_trip_operations",
        "--vars", f"{{target_date: {target}}}",
        "--project-dir", DBT_PROJECT,
        "--profiles-dir", DBT_PROJECT,
    ]
    print("[dbt_run_daily] " + " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout[-3000:])
    if r.returncode != 0:
        print(r.stderr[-3000:])
        sys.exit(r.returncode)

    # dbt_run advancing the watermark is the pipeline's "committed" point
    advance_watermark(STAGE, target, rows=-1)
    print("[dbt_run_daily] done.")


if __name__ == "__main__":
    main()
