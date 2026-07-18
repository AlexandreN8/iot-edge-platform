import os
import json
import time
from confluent_kafka import Consumer, Producer
from business_rules import check_statistical_anomaly, check_flapping_anomaly, WINDOW_SIZE
from logging_setup import get_logger
from tracing_setup import get_tracer, extract_trace_context

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"

INPUT_TOPIC = "enriched"
ANOMALY_TOPIC = "anomalie"
GROUP_ID = "anomaly-detector-group"

logger = get_logger("anomaly_detector")
tracer = get_tracer("anomaly_detector")

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
        "original_message": payload,  # already carries its own trace_context, nested 
        "reason": reason,
        "detected_at": time.time(),
        "detected_by": "anomaly_detector",
    }
    producer.produce(ANOMALY_TOPIC, value=json.dumps(anomaly_payload))
    producer.poll(0)


def run():
    consumer = Consumer({
        KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])
    anomaly_producer = Producer({KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP})
    _last_values = {}

    logger.info("Anomaly detector listening on topic", extra={"category": "infra", "fields": {"topic": INPUT_TOPIC}})
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error", extra={"category": "infra", "fields": {"error": str(msg.error())}})
                continue

            payload = json.loads(msg.value())
            incoming_context = extract_trace_context(payload.get("trace_context", {}))

            with tracer.start_as_current_span("anomaly_detector.evaluate_reading", context=incoming_context) as span:
                span.set_attribute("sensor_id", payload.get("sensor_id", "unknown"))

                statistical_flagged = False
                flapping_flagged = False

                ok, reason = check_statistical_anomaly(payload, _value_history)
                if not ok:
                    statistical_flagged = True
                    logger.info(
                        "Anomaly detected: statistical",
                        extra={"category": "business", "fields": {"sensor_id": payload["sensor_id"], "reason": reason}},
                    )
                    send_to_anomalie(anomaly_producer, payload, reason)

                ok_flap, reason_flap = check_flapping_anomaly(payload, _transition_history)
                if not ok_flap:
                    flapping_flagged = True
                    logger.info(
                        "Anomaly detected: flapping",
                        extra={"category": "business", "fields": {"sensor_id": payload["sensor_id"], "reason": reason_flap}},
                    )
                    send_to_anomalie(anomaly_producer, payload, reason_flap)

                span.set_attribute("statistical_anomaly_detected", statistical_flagged)
                span.set_attribute("flapping_anomaly_detected", flapping_flagged)

                update_value_history(payload["sensor_id"], payload["value"])
                update_transition_history(payload["sensor_id"], payload["timestamp"], payload["value"], _last_values)

                consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutdown requested", extra={"category": "infra"})
    finally:
        consumer.close()


if __name__ == "__main__":
    run()