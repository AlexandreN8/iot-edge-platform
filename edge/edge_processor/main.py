import paho.mqtt.client as mqtt
import os
import json
import time
import threading
from filtering import is_duplicate, is_valid_json
from schema_validation import validate
from buffer import init_db, insert
from sender import run_sender_loop
from heartbeat import touch_heartbeat
from logging_setup import get_logger
from tracing_setup import get_tracer, inject_trace_context, extract_trace_context

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", 1883))
DB_PATH = "buffer.db"
HEARTBEAT_MQTT_FILE = "/app/heartbeat_mqtt"

logger = get_logger("edge_processor")
tracer = get_tracer("edge_processor")
conn = init_db(DB_PATH)


def connect_with_retry(client, host, port, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            client.connect(host, port)
            logger.info("Connected to broker", extra={"category": "infra", "fields": {"host": host, "port": port}})
            return
        except ConnectionRefusedError:
            wait = min(2 ** attempt, 30)
            logger.warning(
                "Connection refused, retrying",
                extra={"category": "infra", "fields": {"attempt": attempt, "max_retries": max_retries, "wait_seconds": wait}},
            )
            time.sleep(wait)
    raise RuntimeError("Unable to connect to MQTT broker after several attempts")


def on_message(client, userdata, msg):
    touch_heartbeat(HEARTBEAT_MQTT_FILE)

    if not is_valid_json(msg.payload):
        logger.info("Rejected: invalid JSON", extra={"category": "infra", "fields": {"topic": msg.topic}})
        return
    if is_duplicate(msg.payload):
        logger.info("Rejected: duplicate", extra={"category": "infra", "fields": {"topic": msg.topic}})
        return
    payload = json.loads(msg.payload)
    if not validate(payload):
        logger.info("Rejected: invalid schema", extra={"category": "infra", "fields": {"payload": payload}})
        return

    incoming_context = extract_trace_context(payload.get("trace_context", {}))
    with tracer.start_as_current_span("edge_processor.receive_and_buffer", context=incoming_context) as span:
        span.set_attribute("sensor_id", payload.get("sensor_id", "unknown"))
        payload["trace_context"] = inject_trace_context()  # re-injects THIS span as the parent for the next hop
        insert(conn, json.dumps(payload))
        logger.info(
            "Reading buffered",
            extra={"category": "business", "fields": {
                "sensor_id": payload.get("sensor_id"), "type": payload.get("type"), "value": payload.get("value"),
            }},
        )


if __name__ == "__main__":
    sender_thread = threading.Thread(target=run_sender_loop, args=(DB_PATH,), daemon=True)
    sender_thread.start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    connect_with_retry(client, BROKER_HOST, BROKER_PORT)
    client.subscribe("sensors/#")
    client.loop_forever()