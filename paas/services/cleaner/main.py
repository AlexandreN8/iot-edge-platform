import os
import json
import time
from confluent_kafka import Consumer, Producer
import psycopg2

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")
TOPIC = "raw"
DLQ_TOPIC = "dlq"
GROUP_ID = "cleaner-group"

CONTINUOUS_SENSOR_TYPES = {"co2", "temperature", "humidity", "smoke", "vibration", "power_consumption"}


BUSINESS_RANGES = {
    "co2": (0, 5000),
    "temperature": (-40, 60),
    "humidity": (0, 100),
    "smoke": (0, 1000),
}

DUPLICATE_WINDOW_SECONDS = 10
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


def check_business_range(payload):
    """ Check if the value is within the business-plausible range for its type. """
    bounds = BUSINESS_RANGES.get(payload["type"])
    if bounds is None:
        return True, None
    low, high = bounds
    if not (low <= payload["value"] <= high):
        return False, f"value {payload['value']} outside plausible range [{low}, {high}] for {payload['type']}"
    return True, None


def check_business_duplicate(payload):
    """ Same value for the same sensor within a short time window is considered a business duplicate. """
    if payload["type"] not in CONTINUOUS_SENSOR_TYPES:
        return True, None  # discrete events like occupancy or opening are not checked for duplicates

    sensor_id = payload["sensor_id"]
    last = _last_seen.get(sensor_id)
    if last is not None:
        last_value, last_ts = last
        if last_value == payload["value"] and (payload["timestamp"] - last_ts) < DUPLICATE_WINDOW_SECONDS:
            return False, f"same value {payload['value']} repeated within {DUPLICATE_WINDOW_SECONDS}s"
    return True, None


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

            ok, reason = check_business_duplicate(payload)
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