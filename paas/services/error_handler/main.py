import os
import json
import time
from confluent_kafka import Consumer
from prometheus_client import start_http_server, Counter, Gauge
from business_rules import classify_fault_type
from logging_setup import get_logger

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"
METRICS_PORT = int(os.environ.get("METRICS_PORT", 8000))

INPUT_TOPIC = "dlq"
GROUP_ID = "error-handler-group"

logger = get_logger("error_handler")

dlq_messages_total = Counter(
    "dlq_messages_total",
    "Total number of messages routed to the dead-letter queue",
    ["fault_type", "rejected_by"],
)
dlq_last_rejection_timestamp = Gauge(
    "dlq_last_rejection_timestamp_seconds",
    "Unix timestamp of the most recent DLQ rejection, per fault type",
    ["fault_type"],
)


def run():
    start_http_server(METRICS_PORT)
    logger.info(
        "Error handler exposing metrics",
        extra={"category": "infra", "fields": {"port": METRICS_PORT}}
    )

    consumer = Consumer({
        KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])

    logger.info(
        "Error handler listening on topic",
        extra={"category": "infra", "fields": {"topic": INPUT_TOPIC}}
    )
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(
                    "Consumer error",
                    extra={"category": "infra", "fields": {"error": str(msg.error())}}
                )
                continue

            dlq_event = json.loads(msg.value())
            reason = dlq_event.get("reason", "")
            rejected_by = dlq_event.get("rejected_by", "unknown")
            fault_type = classify_fault_type(reason)

            dlq_messages_total.labels(fault_type=fault_type, rejected_by=rejected_by).inc()
            dlq_last_rejection_timestamp.labels(fault_type=fault_type).set(time.time())

            logger.info(
                "Recorded DLQ event",
                extra={
                    "category": "business", 
                    "fields": {"fault_type": fault_type, "rejected_by": rejected_by}
                }
            )
            consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutdown requested", extra={"category": "infra"})
    finally:
        consumer.close()


if __name__ == "__main__":
    run()