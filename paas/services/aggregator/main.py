import os
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Consumer
import psycopg2
from business_rules import (
    get_window_start,
    update_accumulator,
    finalize_window,
    get_completed_windows,
)
from logging_setup import get_logger

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"

INPUT_TOPIC = "enriched"
GROUP_ID = "aggregator-group"

MINUTE_SECONDS = 60
HOUR_SECONDS = 3600

logger = get_logger("aggregator")

_minute_accumulators = {}
_hour_accumulators = {}


def connect_postgres_with_retry(dsn, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            logger.info("Connected to Postgres", extra={"category": "infra"})
            return conn
        except psycopg2.OperationalError:
            wait = min(2 ** attempt, 30)
            logger.warning(
                "Postgres not ready, retrying",
                extra={"category": "infra", "fields": {"attempt": attempt, "max_retries": max_retries, "wait_seconds": wait}},
            )
            time.sleep(wait)
    raise RuntimeError("Unable to connect to Postgres after several attempts")


def insert_aggregate(conn, row, table_name):
    with conn.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {table_name}
                (sensor_id, type, window_start, window_end, avg_value, min_value, max_value, sample_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sensor_id, window_start) DO NOTHING""",
            (
                row["sensor_id"], row["type"],
                datetime.fromtimestamp(row["window_start"], tz=timezone.utc),
                datetime.fromtimestamp(row["window_end"], tz=timezone.utc),
                row["avg_value"], row["min_value"], row["max_value"], row["sample_count"],
            ),
        )
    conn.commit()


def flush_completed_windows(conn, accumulators, window_seconds, table_name):
    completed_keys = get_completed_windows(accumulators, time.time(), window_seconds)
    for key in completed_keys:
        acc = accumulators.pop(key)
        row = finalize_window(acc, window_seconds)
        insert_aggregate(conn, row, table_name)
        logger.info(
            "Window flushed",
            extra={"category": "business", "fields": {
                "table": table_name, "sensor_id": row["sensor_id"],
                "avg_value": round(row["avg_value"], 2), "sample_count": row["sample_count"],
            }},
        )


def run():
    pg_conn = connect_postgres_with_retry(POSTGRES_DSN)

    consumer = Consumer({
        KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])

    logger.info("Aggregator listening on topic", extra={"category": "infra", "fields": {"topic": INPUT_TOPIC}})
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is not None:
                if msg.error():
                    logger.error("Consumer error", extra={"category": "infra", "fields": {"error": str(msg.error())}})
                else:
                    payload = json.loads(msg.value())
                    minute_start = get_window_start(payload["timestamp"], MINUTE_SECONDS)
                    hour_start = get_window_start(payload["timestamp"], HOUR_SECONDS)

                    update_accumulator(_minute_accumulators, payload["sensor_id"], payload["type"], minute_start, payload["value"])
                    update_accumulator(_hour_accumulators, payload["sensor_id"], payload["type"], hour_start, payload["value"])

                    consumer.commit(msg)

            # Checked every loop iteration (message or timeout) so a sensor
            # going silent mid-window still gets flushed on time.
            flush_completed_windows(pg_conn, _minute_accumulators, MINUTE_SECONDS, "aggregates_minute")
            flush_completed_windows(pg_conn, _hour_accumulators, HOUR_SECONDS, "aggregates_hourly")
    except KeyboardInterrupt:
        logger.info("Shutdown requested", extra={"category": "infra"})
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    run()