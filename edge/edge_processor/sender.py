import os
import time
import json
from confluent_kafka import Producer
from buffer import init_db, fetch_pending, mark_sent

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL = 5
BATCH_SIZE = 50
TOPIC = "raw"


def create_producer():
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def make_delivery_callback(conn, row_id):
    def callback(err, msg):
        if err is not None:
            print(f"Delivery failed for row {row_id}: {err}")
        else:
            mark_sent(conn, row_id)
            print(f"Delivered row {row_id} -> {msg.topic()} [partition {msg.partition()}]")
    return callback


def run_sender_loop(db_path="buffer.db", interval=POLL_INTERVAL):
    conn = init_db(db_path)
    producer = create_producer()

    while True:
        rows = fetch_pending(conn, limit=BATCH_SIZE)
        for row_id, payload_json in rows:
            payload = json.loads(payload_json)
            key = payload.get("sensor_id", "").encode("utf-8")
            producer.produce(
                TOPIC,
                key=key,
                value=payload_json,
                callback=make_delivery_callback(conn, row_id),
            )
        producer.poll(0)
        producer.flush(5)
        time.sleep(interval)