# =============================================================
# OmniFleet V003 - dbt_test_daily  (quality gate on the changed model)
# =============================================================
# Tests only fct_trip_operations (fast; validates what the daily run just
# merged). Does NOT advance any watermark - it's a gate, not a stage.
# =============================================================
import subprocess
import sys

DBT_PROJECT = "/home/jovyan/work/dbt"


def main():
    cmd = [
        "dbt", "test",
        "--select", "fct_trip_operations",
        "--project-dir", DBT_PROJECT,
        "--profiles-dir", DBT_PROJECT,
    ]
    print("[dbt_test_daily] " + " ".join(cmd))
    r = subprocess.run(cmd, capture_output=True, text=True)
    print(r.stdout[-3000:])
    if r.returncode != 0:
        print(r.stderr[-3000:])
        sys.exit(r.returncode)
    print("[dbt_test_daily] all tests passed.")


if __name__ == "__main__":
    main()
