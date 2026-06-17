# OmniFleet — Scalable End-to-End Fleet Management Data Platform

A production-style data platform for fleet telematics: it ingests live vehicle
sensor data, detects incidents in real time, and builds an analytical warehouse
for business intelligence. Built as an ITI Data Engineering graduation project.

OmniFleet runs **two pipelines at once**:

- **Real-time path** — sensor pings stream through Kafka → Spark Structured
  Streaming → PostgreSQL live tables → a live incident map in **Grafana**.
- **Batch path** — a medallion lakehouse (Bronze → Silver → Gold) on MinIO and
  PostgreSQL, transformed with **dbt** into a star schema, orchestrated by
  **Airflow**, and served to **Superset / Power BI**.

---

## Why it exists (the business problem)

Egyptian fleet and cold-chain operators lose money to four problems that go
undetected until it is too late:

| Problem | What it costs |
|---|---|
| **Financial leakage** | fuel theft, idling, false mileage |
| **Transparency gap** | no visibility into harsh braking, speeding, route drift |
| **Critical cargo risk** | refrigeration failures spoil pharma / frozen goods mid-transit |
| **Reactive maintenance** | breakdowns cause unplanned downtime at \$500–\$3,000/hour |

OmniFleet turns raw sensor data into instant alerts and weekly analytics that
make all four problems observable and measurable.

---

## Architecture at a glance

```
                         ┌──────────────── REAL-TIME PATH (seconds) ───────────────┐
  vehicle sensors ─► producer.py ─► Kafka ─► stream_enrich (Spark) ─► live_* tables ─► Grafana
                         │                 └► bronze_streaming (Spark) ─► MinIO bronze/stream/
                         │
                         └──────────────── BATCH PATH (daily 01:00) ───────────────┐
  MinIO bronze/stream ─► silver (clean) ─► telemetry rollup ─► Postgres staging
                       ─► dbt (star schema, incremental MERGE) ─► Gold fact + dims ─► Superset / Power BI
```

- **Medallion layers** — Bronze (raw) and Silver (clean) as Parquet in MinIO;
  Gold star schema in PostgreSQL.
- **Two Airflow DAGs** — a one-time **backfill** (full history) and a **daily
  incremental** load driven by a per-stage **watermark**.
- **Idempotent** — re-running any day produces the same result (replace-by-date
  in the data stages, dbt `MERGE` on `trip_sk` in the fact).

See [`DOCUMENTATION.md`](DOCUMENTATION.md) for the full deep dive.

---

## Tech stack

| Layer | Tool |
|---|---|
| Ingestion (stream) | Apache Kafka |
| Processing | Apache Spark (Structured Streaming + batch) |
| Lake storage | MinIO (S3-compatible), Parquet |
| Warehouse | PostgreSQL |
| Transformation | dbt (incremental models, tests) |
| Orchestration | Apache Airflow |
| Live dashboard | Grafana |
| BI dashboard | Apache Superset / Power BI |
| Runtime | Docker Compose (WSL2 on Windows) |

---

## Data model (Gold star schema)

One fact table, four dimensions.

- **`fct_trip_operations`** — one row per trip. Tick counters (engine fault,
  speeding, drift, battery, door-open, cargo breach), distance, fuel, delays,
  and derived money columns (fuel cost, cargo fees, weight surcharge, total).
- **`dim_vehicles`**, **`dim_drivers`**, **`dim_routes`**, **`dim_date`** —
  joined by MD5 surrogate keys.

Synthetic data: 500 vehicles, 600 drivers, 50,000 cargo items, 290 depots,
~83,810 routes, **200,000 trips** (2022-01-01 → 2025-05-08), with reproducible
anomalies injected by prime-modulo patterns (seed = 42).

---

## Quick start

> Prerequisites: Docker Desktop + WSL2, ~10 GB free RAM, and the Spark JARs
> (see [`spark/jars/README.txt`](spark/jars/README.txt)).

```bash
# 1. clone
git clone https://github.com/<your-username>/omnifleet.git
cd omnifleet

# 2. download the Spark connector JARs into spark/jars/
#    (commands in spark/jars/README.txt)

# 3. generate the synthetic data into notebooks/data/
#    (run the scripts in data_generators/ in order)

# 4. bring the stack up
docker compose up -d

# 5. one-time setup: create the gold schema + watermark
docker cp sql/gold_schema.sql omnifleet-postgres-v003:/tmp/
docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -f /tmp/gold_schema.sql
docker cp sql/dwh_checkpoints_setup.sql omnifleet-postgres-v003:/tmp/
docker exec omnifleet-postgres-v003 psql -U omnifleet -d omnifleet -f /tmp/dwh_checkpoints_setup.sql

# 6. run the backfill DAG once (Airflow UI), then enable the daily DAG
```

Full setup, including the streaming jobs, is in
[`DOCUMENTATION.md`](DOCUMENTATION.md).

### Service URLs (local)

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8089 | admin / admin |
| Grafana | http://localhost:3000 | admin / omnifleet123 |
| MinIO console | http://localhost:9001 | omnifleet / omnifleet123 |
| Kafka UI | http://localhost:8085 | — |
| Spark / JupyterLab | http://localhost:8888 | token: omnifleet |
| PostgreSQL | localhost:5432 | omnifleet / omnifleet123 |

> These are local development defaults. **Change every password before any real
> deployment** and never commit a real `.env`.

---

## Repository layout

```
omnifleet/
├── dags/                 # Airflow DAGs (backfill + daily incremental)
├── notebooks/            # Spark / pandas pipeline scripts
├── dbt/                  # dbt project (models, macros, tests)
├── data_generators/      # synthetic data generators (seed=42)
├── sql/                  # gold schema, watermark setup, business queries
├── grafana/              # Grafana provisioning (datasource)
├── spark/jars/           # Spark connector JARs (download via README)
├── docker-compose.yml    # the whole stack
├── README.md             # you are here
└── DOCUMENTATION.md       # full technical documentation
```

---

## Key engineering highlights

- **Low-memory telemetry rollup** — per-vehicle pandas processing keeps memory
  flat (<1 GB) where Spark OOM'd on tens of GB of sensor data.
- **Watermark-driven incremental loads** — five per-stage checkpoints in
  `dwh.etl_checkpoints`; each stage advances only on success, so failures retry
  safely.
- **Stream-static enrichment** — each live ping is matched to its active trip via
  an in-memory binary search (`bisect`), enabling exact cold-chain breach checks
  against that shipment's real temperature limits.
- **Idempotent by design** — replace-by-date + dbt `MERGE` on `trip_sk` mean any
  day can be reprocessed with no duplicates.

---

## Team

ITI Data Engineering — Graduation Project (Group 1):
Moayad Ehab · Mohamed Abdelnour · Abdelrahman Elezmazy · Mona Elgoba · Mohamed Osama.

## License

Released under the MIT License — see [`LICENSE`](LICENSE).
Synthetic data only; no real fleet or personal data is included.
