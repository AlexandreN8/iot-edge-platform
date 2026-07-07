import os
import json
import time
from confluent_kafka import Consumer, Producer
import psycopg2
from business_rules import check_business_range, check_business_duplicate

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")
TOPIC = "raw"
DLQ_TOPIC = "dlq"
GROUP_ID = "cleaner-group"

_last_seen = {}  


def connect_postgres_with_retry(dsn, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            print("Connected to Postgres")
            return conn
        except psycopg2.OperationalError:
            wait = min(2 ** attempt, 30)
            print(f"Postgres not ready, attempt {attempt}/{max_retries}, retrying in {wait}s")
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
        "original_message": payload,
        "reason": reason,
        "rejected_at": time.time(),
        "rejected_by": "cleaner",
    }
    producer.produce(DLQ_TOPIC, value=json.dumps(dlq_payload))
    producer.poll(0)


def run():
    pg_conn = connect_postgres_with_retry(POSTGRES_DSN)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    print(f"Cleaner listening on topic '{TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            payload = json.loads(msg.value())  # struct already validated by edge

            ok, reason = check_business_range(payload)
            if not ok:
                print(f"Rejected -> DLQ (business_out_of_range): {reason}")
                send_to_dlq(dlq_producer, payload, f"out_of_range: {reason}")
                consumer.commit(msg)
                continue

            ok, reason = check_business_duplicate(payload, _last_seen)
            if not ok:
                print(f"Rejected -> DLQ (business_duplicate): {reason}")
                send_to_dlq(dlq_producer, payload, f"business_duplicate: {reason}")
                consumer.commit(msg)
                continue

            insert_reading(pg_conn, payload)
            _last_seen[payload["sensor_id"]] = (payload["value"], payload["timestamp"])
            consumer.commit(msg)
            print(f"Stored: {payload['sensor_id']} = {payload['value']} {payload['unit']}")
    except KeyboardInterrupt:
        print("Shutdown requested...")
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    run()