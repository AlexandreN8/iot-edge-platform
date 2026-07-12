import paho.mqtt.client as mqtt
import time
import random
import json
import os

BROKER_HOST = os.environ.get("BROKER_HOST", "localhost")
BROKER_PORT = int(os.environ.get("BROKER_PORT", 1883))
INTERVAL = 5
FAULT_RATE = 0.05  # 5% of messages are faulty, distributed across several fault types

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
_last_clean_value = {}       # last clean value per sensor, used for random walk generation and business-duplicate fault injection
_last_opening_state = {}     # last known state of opening sensors, used for natural toggle and flapping burst generation

CONTINUOUS_SENSOR_TYPES = {"co2", "temperature", "humidity", "smoke", "vibration", "power_consumption"}
STATISTICAL_SENSOR_TYPES = {"co2", "temperature", "humidity", "smoke", "power_consumption", "vibration", "occupancy"}

BUSINESS_PLAUSIBLE_RANGES = {
    "co2": (0, 5000),
    "temperature": (-40, 60),
    "humidity": (0, 100),
    "smoke": (0, 1000),
}

RANDOM_WALK_STEP_RATIO = 0.05   # step size as a fraction of the sensor's range, for generating gradual drift in continuous sensors
OPENING_TOGGLE_PROBABILITY = 0.05  # probability, per cycle, of a natural toggle for opening sensors (independent of flapping bursts)
FLAPPING_BURST_PROBABILITY = 0.05  # probability, per cycle, of a rapid flapping burst for opening sensors
FLAPPING_BURST_TOGGLES = 6
FLAPPING_BURST_DELAY = 1.5


def generate_value(sensor):
    """
    Random walk around the sensor's last clean value, bounded to [min, max] -
    models gradual drift like a real sensor, rather than an independent
    uniform draw each cycle (which made every reading equally likely to be
    an "extreme", leaving no room for a genuine statistical anomaly signal).
    """
    if "values" in sensor:
        return generate_opening_value(sensor)

    span = sensor["max"] - sensor["min"]
    step = span * RANDOM_WALK_STEP_RATIO
    last = _last_clean_value.get(sensor["sensor_id"])

    if last is None:
        value = random.uniform(sensor["min"], sensor["max"])
    else:
        value = last + random.uniform(-step, step)
        value = max(sensor["min"], min(sensor["max"], value))

    value = round(value, 2)
    _last_clean_value[sensor["sensor_id"]] = value
    return value


def generate_opening_value(sensor):
    """ Mostly stable state, rare natural toggle - flapping bursts are injected separately as a fault. """
    last = _last_opening_state.get(sensor["sensor_id"])
    if last is None:
        last = random.choice(sensor["values"])
    elif random.random() < OPENING_TOGGLE_PROBABILITY:
        last = 1 - last
    _last_opening_state[sensor["sensor_id"]] = last
    return last


def build_payload(sensor, measure):
    return {
        "sensor_id": sensor["sensor_id"],
        "type": sensor["type"],
        "timestamp": time.time(),
        "value": measure,
        "unit": sensor["unit"],
    }


def _fault_malformed_json(sensor, payload):
    return "{not valid json, oops", "malformed_json"


def _fault_missing_field(sensor, payload):
    broken = dict(payload)
    broken.pop("unit", None)
    return json.dumps(broken), "missing_field"


def _fault_duplicate_technical(sensor, payload):
    last = _last_payload_cache.get(sensor["sensor_id"])
    if last is not None:
        return last, "duplicate_technical"
    return None, None


def _fault_business_out_of_range(sensor, payload):
    sensor_type = sensor["type"]
    if sensor_type in BUSINESS_PLAUSIBLE_RANGES:
        implausible = dict(payload)
        _, plausible_max = BUSINESS_PLAUSIBLE_RANGES[sensor_type]
        implausible["value"] = plausible_max * 5
        return json.dumps(implausible), "business_out_of_range"
    return None, None


def _fault_business_duplicate(sensor, payload):
    if sensor["type"] in CONTINUOUS_SENSOR_TYPES:
        last_clean = _last_payload_cache.get(sensor["sensor_id"])
        if last_clean is not None:
            last_data = json.loads(last_clean)
            business_dup = dict(payload)
            business_dup["value"] = last_data["value"]
            return json.dumps(business_dup), "business_duplicate"
    return None, None


def _fault_statistical_spike(sensor, payload):
    if sensor["type"] in STATISTICAL_SENSOR_TYPES:
        spike = dict(payload)
        spike["value"] = sensor["max"] if random.random() < 0.5 else sensor["min"]
        _last_clean_value[sensor["sensor_id"]] = spike["value"]
        return json.dumps(spike), "statistical_spike"
    return None, None


_FAULT_HANDLERS = [
    _fault_malformed_json,
    _fault_missing_field,
    _fault_duplicate_technical,
    _fault_business_out_of_range,
    _fault_business_duplicate,
    _fault_statistical_spike,
]


def maybe_inject_fault(sensor, payload):
    """
    Inject a fault into the payload based on FAULT_RATE. Returns a tuple of
    (faulty_payload, fault_label) or (None, None) if no fault is injected.
    Each fault category has equal weight (FAULT_RATE / len(_FAULT_HANDLERS)).
    """
    roll = random.random()
    bucket = FAULT_RATE / len(_FAULT_HANDLERS)
    bucket_index = int(roll // bucket) if roll < FAULT_RATE else -1

    if 0 <= bucket_index < len(_FAULT_HANDLERS):
        return _FAULT_HANDLERS[bucket_index](sensor, payload)
    return None, None


def maybe_inject_flapping_burst(client, sensor):
    """
    Independent, opening-only trigger: publishes several rapid toggles in a
    tight burst, then returns to normal cadence. Kept separate from
    maybe_inject_fault since it emits multiple messages instead of replacing
    one, and only makes sense for a binary sensor.
    """
    if sensor["type"] != "opening":
        return False
    if random.random() >= FLAPPING_BURST_PROBABILITY:
        return False

    print(f"[FAULT: flapping_burst] Starting rapid toggle burst on {sensor['sensor_id']}")
    state = _last_opening_state.get(sensor["sensor_id"], 0)
    topic = f"sensors/{sensor['sensor_id']}"
    for _ in range(FLAPPING_BURST_TOGGLES):
        state = 1 - state
        payload = build_payload(sensor, state)
        client.publish(topic, json.dumps(payload), qos=1)
        print(f"[FAULT: flapping_burst] Published on {topic}: {payload}")
        time.sleep(FLAPPING_BURST_DELAY)
    _last_opening_state[sensor["sensor_id"]] = state
    return True


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
                if maybe_inject_flapping_burst(client, sensor):
                    continue  # skip normal publish this cycle if a flapping burst was injected

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