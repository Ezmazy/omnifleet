# OmniFleet — Technical Documentation

Full technical reference for the OmniFleet data platform. For a quick overview
see [`README.md`](README.md); this document is the deep dive.

---

## Table of contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Technology stack](#3-technology-stack)
4. [Data model](#4-data-model)
5. [Synthetic data generators](#5-synthetic-data-generators)
6. [The streaming (real-time) path](#6-the-streaming-real-time-path)
7. [The batch path and medallion layers](#7-the-batch-path-and-medallion-layers)
8. [dbt: the gold star schema](#8-dbt-the-gold-star-schema)
9. [The watermark and incremental loading](#9-the-watermark-and-incremental-loading)
10. [Airflow orchestration](#10-airflow-orchestration)
11. [Dashboards](#11-dashboards)
12. [Setup and run guide](#12-setup-and-run-guide)
13. [Services and credentials](#13-services-and-credentials)
14. [Repository layout](#14-repository-layout)
15. [Troubleshooting](#15-troubleshooting)

---

## 1. Overview

OmniFleet is a fleet-telematics data platform. Vehicles emit sensor readings;
the platform ingests them, detects incidents in real time, and builds an
analytical warehouse for business intelligence.

It solves four measurable problems for fleet and cold-chain operators: financial
leakage (fuel theft, idling), transparency gaps (unsafe driving), critical cargo
risk (refrigeration failures), and reactive maintenance (unplanned breakdowns).

The platform runs two pipelines simultaneously, decoupled by purpose:

- **Real-time** answers "is any truck in trouble right now?" in seconds.
- **Batch** answers "what happened and what did it cost?" as a daily warehouse.

All data is synthetic and reproducible (fixed seed = 42); no real fleet or
personal data is used.

---

## 2. Architecture

```
                       ┌──────────── REAL-TIME PATH (seconds) ────────────┐
 sensors ─► producer ─► Kafka ─┬─► stream_enrich (Spark) ─► live_* tables ─► Grafana
                               └─► bronze_streaming (Spark) ─► MinIO bronze/stream/ (raw archive)

                       ┌──────────── BATCH PATH (daily 01:00) ────────────┐
 MinIO bronze/stream ─► silver (clean, FK-validate) ─► telemetry rollup (pandas)
                     ─► Postgres staging ─► dbt (star schema, incremental MERGE)
                     ─► Gold: fct_trip_operations + dims ─► Superset / Power BI
```

**Design principles** (from the project proposal):

- **Real-time vs analytical decoupling** — the two paths never block each other.
- **End-to-end automation & monitoring** — Airflow orchestrates the batch; the
  stream is always-on with checkpointed recovery.
- **Scalability & cost-efficiency** — object storage for the lake, a warehouse
  only for serving; both scale independently.
- **Cloud-ready** — MinIO is S3-compatible, so `s3a://` paths move to AWS S3
  unchanged; dbt adapters swap PostgreSQL for Snowflake/BigQuery with no model
  rewrites.

---

## 3. Technology stack

| Concern | Tool | Role |
|---|---|---|
| Containerization | Docker Compose | runs all services on one network |
| Streaming ingestion | Apache Kafka (KRaft) | buffers sensor messages; 2 topics, 6 partitions each |
| Processing | Apache Spark 3.5.3 | Structured Streaming (live) + batch jobs |
| Lake storage | MinIO | S3-compatible object store for Bronze/Silver Parquet |
| Warehouse | PostgreSQL 16 | staging, gold star schema, live tables, watermark |
| Transformation | dbt (postgres adapter) | builds dims + incremental fact, runs tests |
| Orchestration | Apache Airflow 2.9 | two DAGs: backfill + daily incremental |
| Live dashboard | Grafana | geomap, incident feed, fleet status tiles |
| BI dashboard | Apache Superset / Power BI | the 10 business-question charts |
| Caching (Superset) | Redis | Superset results/cache backend |

---

## 4. Data model

### Gold star schema

One fact table at trip grain, four dimensions, joined by MD5 surrogate keys.

**`fct_trip_operations`** (one row per trip):

| Column | Meaning |
|---|---|
| `trip_sk` | surrogate key = MD5(trip_id) |
| `trip_id_bk` | business key (degenerate dimension) |
| `vehicle_key_sk`, `driver_key_sk`, `route_key_sk` | FK surrogates to dims |
| `start_date_key_sk` | YYYYMMDD int → `dim_date` |
| `cargo_type` | degenerate dimension for cold-chain grouping |
| `quick_pings_count`, `slow_pings_count` | telemetry volume |
| `engine_fault_ticks`, `speeding_ticks`, `drift_ticks`, `battery_fault_ticks`, `door_open_ticks`, `cargo_breach_ticks` | incident tick counters |
| `total_distance_km`, `total_fuel_consumed_l` | derived from sensors |
| `delivery_delay_min`, `loading_lag_min` | schedule vs actual |
| `fuel_cost_egp`, `base_cargo_type_fees`, `weight_surcharge_fees`, `total_trip_cost_egp` | derived money |
| `total_cargo_weight_kg`, `total_cargo_value_egp` | cargo rollup |

**Dimensions:**

- **`dim_vehicles`** — vehicle_sk, vehicle_id, model, payload_capacity, tank_capacity_l, fuel_type, current_mileage
- **`dim_drivers`** — driver_sk, driver_id, first_name, last_name, full_name, license_class, phone_number
- **`dim_routes`** — route_sk, route_id, origin/destination depot id + address + governorate + lat/lon (depots flattened in at Silver)
- **`dim_date`** — date_sk (YYYYMMDD), full_date, year, quarter, month, month_name, day_of_week, is_weekend (Fri+Sat)

### Derived business rates (tunable in the dbt model)

| Column | Formula |
|---|---|
| `fuel_cost_egp` | `total_fuel_consumed_l × 18.0` (EGP/L diesel) |
| `weight_surcharge_fees` | `total_cargo_weight_kg × 2.5` (EGP/kg) |
| `base_cargo_type_fees` | per type: Deep frozen 1200, Pharmaceutical 1000, Frozen 800, Chilled 500, Controlled ambient 300, else 250 |
| `total_trip_cost_egp` | fuel + base fee + weight surcharge |

### Live (streaming) tables

- **`live_vehicle_status`** — one row per vehicle: position, incident color, label (Grafana geomap)
- **`live_incident_feed`** — append-only incident log (Grafana feed)
- **`live_fuel_state`** — per-vehicle last fuel reading, for cross-batch fuel-drop detection

---

## 5. Synthetic data generators

Run once, in order, from `data_generators/` (all use seed = 42).

| Script | Produces |
|---|---|
| `vehicles_drivers_cargo_generator_v002.py` | 500 vehicles, 600 drivers, 50,000 cargo items |
| `depots_routs_generator_v002.py` | 290 depots (real Egyptian highway coords), ~83,810 routes (permutations) |
| `trips_generator.py` | 200,000 trips, 2022-01-01 → 2025-05-08, state-aware scheduling (no double-booking) |
| `trip_cargo_generator.py` | trip↔cargo bridge, homogeneous cargo per trip, 90/6/4 status split |
| `sensors.py` | per-vehicle quick (3-sec) + slow (60-sec) sensor CSVs |

### Reproducible anomalies (prime-modulo)

| Anomaly | Trigger | Effect |
|---|---|---|
| Engine wear | `vehicle_id % 13 == 0` | RPM pinned 4200–5600 |
| Bad battery | `vehicle_id % 17 == 0` | voltage 10.2–11.4 V on 20% of ticks |
| Aggressive driver | `trip_id % 11 == 0` | throttle > 85, accel > 0.75 g on 15% of ticks |
| Fuel theft | `trip_id % 45 == 0` | 12–18 L instant drop at 40–50% of trip |
| Reefer failure | `trip_id % 29 == 0` | temp spikes 5–15 °C above max after 50% |
| Door breach | `trip_id % 53 == 0` | door open mid-route at 70–75% |

Primes share no factors, so anomalies overlap realistically (e.g. vehicle 221 =
13×17 has both engine wear and a bad battery).

---

## 6. The streaming (real-time) path

### Producer → Kafka

`producer.py` replays the sensor CSVs for vehicles 0–99 into two Kafka topics:

- `vehicle.quick.sensors` — 3-second pings (GPS, rpm, throttle, accel, battery)
- `vehicle.slow.sensors` — 60-second pings (fuel, door, cargo temp)

Both topics have 6 partitions. A checkpoint file lets the producer resume where
it stopped.

### Two Spark Structured Streaming consumers

1. **`bronze_streaming.py`** — archives every raw Kafka message to MinIO under
   `omnifleet-bronze/stream/...`, partitioned by date. This is the audit archive
   *and* the source the daily batch reads from.
2. **`stream_enrich.py`** — the live alert engine.

### Stream enrichment and the in-memory join

At startup, `stream_enrich.py` builds an in-memory **active-trip index**:

```
trip_cargo ⋈ cargo  (on cargo_id)  → per-trip thermal bounds (min/max temp, cargo_type)
            ⋈ trips (on trip_id)   → per-vehicle sorted list of trip time-windows
```

For every incoming ping, `active_trip(vehicle_id, timestamp)` uses a binary
search (`bisect`) to find which trip the vehicle was on — an O(log n)
stream-static join. This attaches `trip_id`, `driver_id`, `cargo_type`, and the
cargo's real `min_temp`/`max_temp` to the ping, enabling exact cold-chain breach
detection.

### Incident colors (priority)

| Color | Priority | Trigger |
|---|---|---|
| RED | highest | fuel fraud (≥10 L drop) or door open mid-route |
| ORANGE | | cargo temperature breach |
| YELLOW | | speeding (throttle > 85) or drift (accel > 0.75 g) |
| BLUE | | low battery (< 11.4 V) |
| WHITE | lowest | nominal |

Each vehicle shows the **highest-priority** incident from the last 2 minutes;
`refresh_colours()` runs a SQL `UPDATE … FROM` that joins `live_vehicle_status`
to `live_incident_feed` to compute it. Vehicles with no recent incident reset to
WHITE.

### The de-duplication fix

A vehicle pings several times per micro-batch. Postgres rejects an
`ON CONFLICT … DO UPDATE` that touches the same key twice in one statement, so
positions are de-duplicated per vehicle in a Python dict (latest wins) before the
upsert.

---

## 7. The batch path and medallion layers

### Bronze

- `bronze_static_load.py` (backfill) — lands the dispatch CSVs (vehicles,
  drivers, cargo, depots, routes, trips, trip_cargo) into MinIO as raw Parquet.
- `bronze_daily_load.py` (daily) — copies one day's streamed partition from
  `bronze/stream/` into the daily bronze zone.

### Silver

- `silver_build.py` (backfill) / `silver_daily_build.py` (daily) — cast types,
  trim strings, de-duplicate, validate foreign keys, and flatten depots into
  routes. The daily version **replaces by date** (delete that day's partition,
  then write) for idempotency.

### Telemetry rollup (low-memory pandas)

`telemetry_trip_agg.py` / `telemetry_trip_agg_daily.py` roll the sensor data up
to one row per trip. **Why pandas, not Spark?** The quick-sensor data is tens of
GB and Spark OOM'd on a 10 GB machine. Processing one vehicle file at a time in
pandas keeps memory flat (<1 GB). Output → `staging.stg_trip_telemetry_agg`,
written with delete-by-trip then insert (idempotent).

### Staging

`staging_load.py` / `staging_load_daily.py` copy Silver into the `staging.*`
tables that dbt reads (dbt-postgres cannot read Parquet directly). The daily
version stages only the target day's trips (delete-by-date then insert).

---

## 8. dbt: the gold star schema

dbt reads `staging.*` and writes the gold star into `public`. Project files:

```
dbt/
├── dbt_project.yml         # materializations
├── profiles.yml            # postgres connection
├── macros/surrogate_key.sql# make_sk() = MD5 hash of natural key
└── models/
    ├── sources.yml         # declares staging.* + public.dim_date
    └── marts/
        ├── dim_vehicles.sql, dim_drivers.sql, dim_routes.sql
        ├── fct_trip_operations.sql
        └── marts.yml        # tests (unique, not_null, relationships)
```

### Surrogate keys

`make_sk(['vehicle_id'])` compiles to `md5(cast(vehicle_id as varchar))`. MD5 is
stable, so rebuilding a dimension keeps the same keys — the fact's foreign keys
never break.

### The incremental fact

```sql
{{ config(
    materialized='incremental',
    unique_key='trip_sk',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}
```

On a daily run the model filters to the target date and **MERGEs** by `trip_sk`:
new trips are inserted, existing ones updated, never duplicated. The backfill (no
target date / full refresh) builds everything.

### Tests (run by `dbt test`)

`unique` + `not_null` on every surrogate and business key, plus `relationships`
tests ensuring every fact FK points at a real dimension row.

---

## 9. The watermark and incremental loading

The daily pipeline is driven by a **per-stage watermark** in
`dwh.etl_checkpoints` — one row per stage (`bronze`, `silver`, `telemetry`,
`staging`, `dbt`).

```sql
CREATE TABLE dwh.etl_checkpoints (
    stage_name           TEXT PRIMARY KEY,
    last_processed_date  DATE NOT NULL,
    last_run_at          TIMESTAMP,
    rows_processed       INT,
    status               TEXT
);
```

### How it works

The logic lives in `notebooks/dwh_watermark.py` (imported by the daily scripts —
**not** an Airflow task):

- **`ensure_checkpoints()`** — creates the table and seeds all five stages to the
  backfill end date (`2025-05-08`). Idempotent.
- **`get_target_date(stage)`** — returns `last_processed_date + 1 day` = the next
  day this stage should process.
- **`advance_watermark(stage, date, rows)`** — moves the stage's marker forward,
  called **only as the stage's final step, after success**.

### Why per-stage

If bronze finishes a day but silver crashes, bronze's marker is ahead while
silver's stays behind — each stage retries exactly what it still owes. A failure
mid-pipeline never loses or doubles work.

### Idempotency

Re-running a day is safe because the data stages replace-by-date and the dbt fact
MERGEs on `trip_sk` (which is a stable MD5 of `trip_id`). The same day reprocessed
yields the identical result.

### Useful commands

```sql
-- inspect every stage
SELECT * FROM dwh.etl_checkpoints ORDER BY stage_name;
-- reprocess a day: rewind a stage, then re-run the DAG
UPDATE dwh.etl_checkpoints SET last_processed_date='2025-05-08' WHERE stage_name='bronze';
```

---

## 10. Airflow orchestration

Two DAGs:

### Backfill — `omnifleet_v003_pipeline`

- `schedule_interval=None` — triggered by hand, once.
- Loads all history (200,000 trips) in one run.
- Tasks: `create_gold_schema → bronze_static_load → silver_build →
  telemetry_trip_agg → staging_load → dbt_run → dbt_test`.

### Daily incremental — `omnifleet_v003_daily`

- `schedule="0 1 * * *"` — every night at 01:00.
- `start_date=2025-05-09` (day after backfill), `catchup=True`,
  `max_active_runs=1`.
- Tasks: `bronze_daily_load → silver_daily_build → telemetry_trip_agg_daily →
  staging_load_daily → dbt_run_daily → dbt_test_daily`.
- `dbt_run_daily` passes the target date to dbt (`--vars '{target_date: ...}'`)
  and advances the `dbt` watermark on success; `dbt_test_daily` is a quality gate
  that advances nothing.

Helper scripts `get_dbt_target.py` and `advance_dbt_wm.py` expose the watermark
to the dbt container.

---

## 11. Dashboards

### Grafana (real-time)

Reads the live tables, refreshing every few seconds:

- **Active Incident Map** — geomap of vehicles colored by highest-severity
  incident (`live_vehicle_status`).
- **Live Incident Feed** — scrolling table of recent incidents
  (`live_incident_feed`).
- **Fleet Status Tiles** — total / nominal / warnings / critical counts.

### Superset / Power BI (analytical)

The gold star answers 10 business questions across 5 domains (full SQL in
`sql/business_questions.sql` and `sql/dashboard_queries.sql`):

| Domain | Questions |
|---|---|
| Driver | top dangerous drivers · speeding↔engine-stress correlation |
| Vehicle | battery degradation · mileage vs engine wear by model |
| Cold chain | breaches by cargo type · breach vs distance |
| Route | corridor density + delay · loading lag by governorate |
| Finance | fuel fraud leakage (EGP) · net profit by route |

---

## 12. Setup and run guide

> Prerequisites: Docker Desktop + WSL2, ~10 GB RAM allocated to WSL.

1. **Spark JARs** — download the 7 connector JARs into `spark/jars/`
   (commands in `spark/jars/README.txt` / `spark_jars_README.txt`).
2. **Generate data** — run the `data_generators/` scripts in order; place output
   in `notebooks/data/` (with `quick_sensors/` and `slow_sensors/` subfolders).
3. **Start the stack** — `docker compose up -d`. Wait for health checks.
4. **Install runtime deps in Spark** (after each restart):
   `docker exec omnifleet-spark-v003 pip install kafka-python s3fs psycopg2-binary scikit-learn --quiet`
5. **One-time schema setup:**
   ```bash
   docker cp sql/gold_schema.sql omnifleet-postgres-v003:/tmp/
   docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -f /tmp/gold_schema.sql
   docker cp sql/dwh_checkpoints_setup.sql omnifleet-postgres-v003:/tmp/
   docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -f /tmp/dwh_checkpoints_setup.sql
   ```
6. **Backfill** — trigger `omnifleet_v003_pipeline` in the Airflow UI (once).
7. **Streaming** — start the three always-on jobs from a Spark terminal:
   `producer.py`, `bronze_streaming.py`, `stream_enrich.py`.
8. **Daily** — enable `omnifleet_v003_daily` in the Airflow UI.

### Verify

```bash
# gold fact loaded?
docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -c \
  "SELECT count(*) FROM public.fct_trip_operations;"          # ~200000
# watermark seeded?
docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -c \
  "SELECT * FROM dwh.etl_checkpoints ORDER BY stage_name;"
# live data flowing?
docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -c \
  "SELECT incident_color, count(*) FROM live_incident_feed GROUP BY 1;"
```

---

## 13. Services and credentials

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8089 | admin / admin |
| Grafana | http://localhost:3000 | admin / omnifleet123 |
| MinIO console | http://localhost:9001 | omnifleet / omnifleet123 |
| Kafka UI | http://localhost:8085 | — |
| Spark / JupyterLab | http://localhost:8888 | token: omnifleet |
| PostgreSQL | localhost:5432 | omnifleet / omnifleet123 |

> Local development defaults only. Change all passwords before any real
> deployment; never commit a real `.env`.

---

## 14. Repository layout

```
omnifleet/
├── dags/
│   ├── omnifleet_v003_dag.py          # backfill DAG
│   └── omnifleet_v003_daily_dag.py    # daily incremental DAG
├── notebooks/
│   ├── producer.py, bronze_streaming.py, stream_enrich.py   # streaming
│   ├── bronze_static_load.py, silver_build.py,
│   │   telemetry_trip_agg.py, staging_load.py               # backfill batch
│   ├── bronze_daily_load.py, silver_daily_build.py,
│   │   telemetry_trip_agg_daily.py, staging_load_daily.py   # daily batch
│   ├── dwh_watermark.py, get_dbt_target.py, advance_dbt_wm.py  # watermark
│   └── gold_schema.sql
├── dbt/
│   ├── dbt_project.yml, profiles.yml
│   ├── macros/surrogate_key.sql
│   └── models/{sources.yml, marts/*.sql, marts/marts.yml}
├── data_generators/        # 5 seed=42 generators
├── sql/                    # gold_schema, watermark setup, business queries
├── grafana/provisioning/   # datasource config
├── spark/jars/             # connector JARs (download via README)
├── docker-compose.yml
├── README.md
└── DOCUMENTATION.md
```

---

## 15. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `psql: command not found` in Spark container | run SQL through the postgres container instead (`docker exec omnifleet-postgres-v003 psql ...`) |
| dbt can't read Parquet | expected — Spark loads Silver into `staging.*` first; dbt reads those |
| Spark Kafka job: `ClassNotFound` / `NoSuchMethod` | a JAR version mismatch — re-download the exact versions in `spark/jars/README.txt` |
| `aws-java-sdk-bundle.jar` only a few KB | partial download — delete and re-fetch (real size ~280 MB) |
| Postgres `CardinalityViolation` on live upsert | a vehicle appears twice in one batch — fixed by per-vehicle de-dup dict in `stream_enrich.py` |
| Telemetry job OOM | use the pandas per-vehicle version, not Spark, for the rollup |
| Daily DAG sees "no partition" every night | `bronze_streaming` partition folder name must match what `bronze_daily_load` reads — align both to the same `dt=` (or `ingest_date=`) convention |
| Kafka libs missing after restart | re-run the `pip install` step (deps are not persisted in the Spark image) |

---

*OmniFleet — ITI Data Engineering Graduation Project. Synthetic data only.*
