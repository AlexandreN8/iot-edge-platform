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

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", 1883))
DB_PATH = "buffer.db"
HEARTBEAT_MQTT_FILE = "/tmp/heartbeat_mqtt"

conn = init_db(DB_PATH)


def connect_with_retry(client, host, port, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            client.connect(host, port)
            print(f"Connected to {host}:{port}")
            return
        except ConnectionRefusedError:
            wait = min(2 ** attempt, 30)
            print(f"Connection refused, attempt {attempt}/{max_retries}, retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError("Unable to connect to MQTT broker after several attempts")


def on_message(client, userdata, msg):
    touch_heartbeat(HEARTBEAT_MQTT_FILE)

    if not is_valid_json(msg.payload):
        print(f"Rejected (invalid JSON): {msg.topic}")
        return
    if is_duplicate(msg.payload):
        print(f"Rejected (duplicate): {msg.topic}")
        return
    payload = json.loads(msg.payload)
    if not validate(payload):
        print(f"Rejected (invalid schema): {payload}")
        return
    insert(conn, json.dumps(payload))
    print(f"Buffered: {payload}")


if __name__ == "__main__":
    sender_thread = threading.Thread(target=run_sender_loop, args=(DB_PATH,), daemon=True)
    sender_thread.start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    connect_with_retry(client, BROKER_HOST, BROKER_PORT)
    client.subscribe("sensors/#")
    client.loop_forever()