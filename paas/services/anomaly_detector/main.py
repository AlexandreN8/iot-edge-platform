import os
import json
import time
from confluent_kafka import Consumer, Producer
from business_rules import check_statistical_anomaly, check_flapping_anomaly, WINDOW_SIZE

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
INPUT_TOPIC = "enriched"
ANOMALY_TOPIC = "anomalie"
GROUP_ID = "anomaly-detector-group"

_value_history = {}       # sensor_id -> [recent values] (types continuous)
_transition_history = {}  # sensor_id -> [timestamps of recent transitions] (types binary)


def update_value_history(sensor_id, value):
    values = _value_history.setdefault(sensor_id, [])
    values.append(value)
    if len(values) > WINDOW_SIZE:
        values.pop(0)


def update_transition_history(sensor_id, timestamp, value, last_values):
    last = last_values.get(sensor_id)
    if last is not None and last != value:
        transitions = _transition_history.setdefault(sensor_id, [])
        transitions.append(timestamp)
    last_values[sensor_id] = value


def send_to_anomalie(producer, payload, reason):
    anomaly_payload = {
        "original_message": payload,
        "reason": reason,
        "detected_at": time.time(),
        "detected_by": "anomaly_detector",
    }
    producer.produce(ANOMALY_TOPIC, value=json.dumps(anomaly_payload))
    producer.poll(0)


def run():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])
    anomaly_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    _last_values = {}

    print(f"Anomaly detector listening on topic '{INPUT_TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            payload = json.loads(msg.value())

            ok, reason = check_statistical_anomaly(payload, _value_history)
            if not ok:
                print(f"Anomaly detected (statistical): {reason}")
                send_to_anomalie(anomaly_producer, payload, reason)

            ok_flap, reason_flap = check_flapping_anomaly(payload, _transition_history)
            if not ok_flap:
                print(f"Anomaly detected (flapping): {reason_flap}")
                send_to_anomalie(anomaly_producer, payload, reason_flap)

            update_value_history(payload["sensor_id"], payload["value"])
            update_transition_history(payload["sensor_id"], payload["timestamp"], payload["value"], _last_values)

            consumer.commit(msg)
    except KeyboardInterrupt:
        print("Shutdown requested...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()