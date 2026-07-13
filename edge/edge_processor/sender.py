import os
import time
import json
from confluent_kafka import Producer
from buffer import init_db, fetch_pending, mark_sent, purge_expired
from heartbeat import touch_heartbeat

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POLL_INTERVAL = 5
BATCH_SIZE = 50
TOPIC = "raw"
BUFFER_TTL_SECONDS = int(os.environ.get("BUFFER_TTL_SECONDS", 6 * 3600))
HEARTBEAT_SENDER_FILE = "/tmp/heartbeat_sender"


def create_producer():  # pragma: no cover
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def make_delivery_callback(conn, row_id):
    def callback(err, msg):
        if err is not None:
            print(f"Delivery failed for row {row_id}: {err}")
        else:
            mark_sent(conn, row_id)
            print(f"Delivered row {row_id} -> {msg.topic()} [partition {msg.partition()}]")
    return callback


def run_sender_loop(db_path="buffer.db", interval=POLL_INTERVAL):  # pragma: no cover
    conn = init_db(db_path)
    producer = create_producer()

    while True:
        touch_heartbeat(HEARTBEAT_SENDER_FILE)

        purged = purge_expired(conn, BUFFER_TTL_SECONDS)
        if purged:
            print(f"Purged {purged} expired row(s) from buffer (TTL={BUFFER_TTL_SECONDS}s)")

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