-- One-time (optional) setup for the daily pipeline watermark.
-- The daily scripts auto-create + seed this on first run, but you can run it
-- manually to inspect or reset.
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE TABLE IF NOT EXISTS dwh.etl_checkpoints (
    stage_name           TEXT PRIMARY KEY,
    last_processed_date  DATE NOT NULL,
    last_run_at          TIMESTAMP NOT NULL DEFAULT now(),
    rows_processed       INT,
    status               TEXT
);
-- seed all five stages to the backfill end date
INSERT INTO dwh.etl_checkpoints (stage_name, last_processed_date, status) VALUES
    ('bronze',    DATE '2025-05-08', 'seeded'),
    ('silver',    DATE '2025-05-08', 'seeded'),
    ('telemetry', DATE '2025-05-08', 'seeded'),
    ('staging',   DATE '2025-05-08', 'seeded'),
    ('dbt',       DATE '2025-05-08', 'seeded')
ON CONFLICT (stage_name) DO NOTHING;

-- inspect:        SELECT * FROM dwh.etl_checkpoints ORDER BY stage_name;
-- reset a stage:  UPDATE dwh.etl_checkpoints SET last_processed_date='2025-05-08' WHERE stage_name='bronze';
