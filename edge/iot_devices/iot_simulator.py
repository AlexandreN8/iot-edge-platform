import paho.mqtt.client as mqtt
import time
import random
import json
import os

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", 1883))
INTERVAL = 5

SENSORS_CONFIG = [
    {"type": "co2", "sensor_id": "co2-001", "min": 400, "max": 1500, "unit": "ppm"},
    {"type": "temperature", "sensor_id": "temp-001", "min": 0, "max": 50, "unit": "°C"},
    {"type": "humidity", "sensor_id": "hum-001", "min": 0, "max": 100, "unit": "%"},
    {"type": "occupancy", "sensor_id": "occupancy-001", "min": 0, "max": 30, "unit": "people"},
    {"type": "power_consumption", "sensor_id": "power-001", "min": 0, "max": 3000, "unit": "W"},
    {"type": "opening", "sensor_id": "opening-001", "values": [0, 1], "unit": "bool"},
    {"type": "smoke", "sensor_id": "smoke-001", "min": 0, "max": 500, "unit": "ppm"},
    {"type": "vibration", "sensor_id": "vibration-001", "min": 0, "max": 10, "unit": "mm/s"},
]


def generate_value(sensor):
    if "values" in sensor:
        return random.choice(sensor["values"])
    else:
        return round(random.uniform(sensor["min"], sensor["max"]), 2)


def build_payload(sensor, measure):
    return {
        "sensor_id": sensor["sensor_id"],
        "type": sensor["type"],
        "timestamp": time.time(),
        "value": measure,
        "unit": sensor["unit"],
    }


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


if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    connect_with_retry(client, BROKER_HOST, BROKER_PORT)
    client.loop_start()

    try:
        while True:
            for sensor in SENSORS_CONFIG:
                measure = generate_value(sensor)
                payload = build_payload(sensor, measure)
                topic = f"sensors/{sensor['sensor_id']}"
                client.publish(topic, json.dumps(payload), qos=1)
                print(f"Published on {topic}: {payload}")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("Shutdown requested...")
    finally:
        client.loop_stop()
        client.disconnect()