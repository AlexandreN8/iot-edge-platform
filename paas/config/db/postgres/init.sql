CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS readings (
    id SERIAL PRIMARY KEY,
    sensor_id TEXT NOT NULL,
    type TEXT NOT NULL,
    ts DOUBLE PRECISION NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit TEXT,
    received_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS aggregates_minute (
    sensor_id TEXT NOT NULL,
    type TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL,
    sample_count INTEGER NOT NULL,
    PRIMARY KEY (sensor_id, window_start)
);
SELECT create_hypertable('aggregates_minute', 'window_start', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS aggregates_hourly (
    sensor_id TEXT NOT NULL,
    type TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    avg_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL,
    sample_count INTEGER NOT NULL,
    PRIMARY KEY (sensor_id, window_start)
);
SELECT create_hypertable('aggregates_hourly', 'window_start', if_not_exists => TRUE);