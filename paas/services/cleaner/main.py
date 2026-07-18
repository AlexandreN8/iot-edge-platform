import os
import json
import time
from confluent_kafka import Consumer, Producer
import psycopg2
from business_rules import check_business_range, check_business_duplicate
from logging_setup import get_logger
from tracing_setup import get_tracer, inject_trace_context, extract_trace_context

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"

TOPIC = "raw"
DLQ_TOPIC = "dlq"
ENRICHED_TOPIC = "enriched"
GROUP_ID = "cleaner-group"

logger = get_logger("cleaner")
tracer = get_tracer("cleaner")
_last_seen = {}


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


def insert_reading(conn, payload):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO readings (sensor_id, type, ts, value, unit)
               VALUES (%s, %s, %s, %s, %s)""",
            (payload["sensor_id"], payload["type"], payload["timestamp"],
             payload["value"], payload["unit"]),
        )
    conn.commit()


def send_to_dlq(producer, payload, reason):
    dlq_payload = {
        "original_message": payload,  # already carries its own trace_context, nested 
        "reason": reason,
        "rejected_at": time.time(),
        "rejected_by": "cleaner",
    }
    producer.produce(DLQ_TOPIC, value=json.dumps(dlq_payload))
    producer.poll(0)


def enrich_payload(payload):
    return {
        **payload,
        "validated_at": time.time(),
        "validated_by": "cleaner",
    }


def send_to_enriched(producer, payload):
    producer.produce(ENRICHED_TOPIC, value=json.dumps(payload))
    producer.poll(0)


def run():
    pg_conn = connect_postgres_with_retry(POSTGRES_DSN)

    consumer = Consumer({
        KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    dlq_producer = Producer({KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP})
    enriched_producer = Producer({KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP})

    logger.info("Cleaner listening on topic", extra={"category": "infra", "fields": {"topic": TOPIC}})
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error", extra={"category": "infra", "fields": {"error": str(msg.error())}})
                continue

            payload = json.loads(msg.value())
            incoming_context = extract_trace_context(payload.get("trace_context", {}))

            with tracer.start_as_current_span("cleaner.process_reading", context=incoming_context) as span:
                span.set_attribute("sensor_id", payload.get("sensor_id", "unknown"))

                ok, reason = check_business_range(payload)
                if not ok:
                    span.set_attribute("outcome", "rejected_out_of_range")
                    logger.info(
                        "Reading rejected: out of range",
                        extra={"category": "business", "fields": {"sensor_id": payload["sensor_id"], "reason": reason}},
                    )
                    send_to_dlq(dlq_producer, payload, f"out_of_range: {reason}")
                    consumer.commit(msg)
                    continue

                ok, reason = check_business_duplicate(payload, _last_seen)
                if not ok:
                    span.set_attribute("outcome", "rejected_business_duplicate")
                    logger.info(
                        "Reading rejected: business duplicate",
                        extra={"category": "business", "fields": {"sensor_id": payload["sensor_id"], "reason": reason}},
                    )
                    send_to_dlq(dlq_producer, payload, f"business_duplicate: {reason}")
                    consumer.commit(msg)
                    continue

                insert_reading(pg_conn, payload)
                enriched = enrich_payload(payload)
                enriched["trace_context"] = inject_trace_context()
                send_to_enriched(enriched_producer, enriched)

                span.set_attribute("outcome", "stored_and_enriched")
                _last_seen[payload["sensor_id"]] = (payload["value"], payload["timestamp"])
                consumer.commit(msg)
                logger.info(
                    "Reading stored and enriched",
                    extra={"category": "business", "fields": {
                        "sensor_id": payload["sensor_id"], "value": payload["value"], "unit": payload["unit"],
                    }},
                )
    except KeyboardInterrupt:
        logger.info("Shutdown requested", extra={"category": "infra"})
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    run()