import paho.mqtt.client as mqtt
import time
import random
import json
import os

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", 1883))
INTERVAL = 5
FAULT_RATE = 0.15  # 15% des cycles injectent volontairement un problème

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

_last_payload_cache = {}

CONTINUOUS_SENSOR_TYPES = {"co2", "temperature", "humidity", "smoke", "vibration", "power_consumption"}

BUSINESS_PLAUSIBLE_RANGES = {
    "co2": (0, 5000),
    "temperature": (-40, 60),
    "humidity": (0, 100),
    "smoke": (0, 1000),
}


def generate_value(sensor):
    """ Generate a random value for the sensor, either from a range or from a predefined list of values. """
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

def maybe_inject_fault(sensor, payload):
    """ Inject a fault into the payload based on the FAULT_RATE. Returns a tuple of (faulty_payload, fault_label) or (None, None) if no fault is injected. """
    roll = random.random()

    if roll < FAULT_RATE * 0.2:
        # Inject a malformed JSON: return a string that is not valid JSON
        return "{not valid json, oops", "malformed_json"

    elif roll < FAULT_RATE * 0.4:
        # Inject a missing field: remove the "unit" field
        broken = dict(payload)
        broken.pop("unit", None)
        return json.dumps(broken), "missing_field"

    elif roll < FAULT_RATE * 0.6:
        # Inject a technical duplicate: same bytes as last message for this sensor
        last = _last_payload_cache.get(sensor["sensor_id"])
        if last is not None:
            return last, "duplicate_technical"
        return None, None

    elif roll < FAULT_RATE * 0.8:
        # Inject a business out-of-range value: 5x the plausible max for this sensor type
        sensor_type = sensor["type"]
        if sensor_type in BUSINESS_PLAUSIBLE_RANGES:
            implausible = dict(payload)
            _, plausible_max = BUSINESS_PLAUSIBLE_RANGES[sensor_type]
            implausible["value"] = plausible_max * 5
            return json.dumps(implausible), "business_out_of_range"
        return None, None

    elif roll < FAULT_RATE:
        # Inject a business duplicate: same value as last clean measurement for this sensor
        if sensor["type"] in CONTINUOUS_SENSOR_TYPES:
            last_clean = _last_payload_cache.get(sensor["sensor_id"])
            if last_clean is not None:
                last_data = json.loads(last_clean)
                business_dup = dict(payload)
                business_dup["value"] = last_data["value"]
                return json.dumps(business_dup), "business_duplicate"
        return None, None

    return None, None

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
                payload_json = json.dumps(payload)
                topic = f"sensors/{sensor['sensor_id']}"

                faulty_message, fault_label = maybe_inject_fault(sensor, payload)

                if faulty_message is not None:
                    client.publish(topic, faulty_message, qos=1)
                    print(f"[FAULT: {fault_label}] Published on {topic}")
                else:
                    client.publish(topic, payload_json, qos=1)
                    _last_payload_cache[sensor["sensor_id"]] = payload_json
                    print(f"Published on {topic}: {payload}")

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("Shutdown requested...")
    finally:
        client.loop_stop()
        client.disconnect()