import os
import json
import time
import smtplib
from confluent_kafka import Consumer
import psycopg2
from business_rules import classify_severity, should_send_email, build_email_content
from logging_setup import get_logger
from tracing_setup import get_tracer, extract_trace_context

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_BOOTSTRAP_SERVERS_KEY = "bootstrap.servers"
POSTGRES_DSN = os.environ.get("POSTGRES_DSN")

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO")

INPUT_TOPIC = "anomalie"
GROUP_ID = "alert-service-group"

logger = get_logger("alert_service")
tracer = get_tracer("alert_service")

_last_email_sent = {}
_last_global_email_sent = None


def connect_postgres_with_retry(dsn, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(dsn)
            logger.info("Connected to Postgres", extra={"category": "infra"})
            return conn
        except psycopg2.OperationalError:
            wait = min(2 ** attempt, 30)
            logger.warning(
                "Postgres not ready, retrying",
                extra={"category": "infra", "fields": {"attempt": attempt, "max_retries": max_retries, "wait_seconds": wait}},
            )
            time.sleep(wait)
    raise RuntimeError("Unable to connect to Postgres after several attempts")


def insert_alert(conn, sensor_id, sensor_type, value, reason, severity, detected_at, email_sent):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO alerts (sensor_id, type, value, reason, severity, detected_at, email_sent)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (sensor_id, sensor_type, value, reason, severity, detected_at, email_sent),
        )
    conn.commit()


def send_email(subject, plain_body, html_body):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ALERT_EMAIL_TO
    msg.attach(MIMEText(plain_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, [ALERT_EMAIL_TO], msg.as_string())


def run():
    global _last_global_email_sent
    pg_conn = connect_postgres_with_retry(POSTGRES_DSN)

    consumer = Consumer({
        KAFKA_BOOTSTRAP_SERVERS_KEY: KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([INPUT_TOPIC])

    logger.info("Alert service listening on topic", extra={"category": "infra", "fields": {"topic": INPUT_TOPIC}})
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error", extra={"category": "infra", "fields": {"error": str(msg.error())}})
                continue

            anomaly = json.loads(msg.value())
            original = anomaly["original_message"]
            sensor_id = original["sensor_id"]
            sensor_type = original["type"]
            value = original["value"]
            reason = anomaly["reason"]

            incoming_context = extract_trace_context(original.get("trace_context", {}))

            with tracer.start_as_current_span("alert_service.process_anomaly", context=incoming_context) as span:
                span.set_attribute("sensor_id", sensor_id)

                severity = classify_severity(reason)
                span.set_attribute("severity", severity)

                now = time.time()
                send_now = should_send_email(sensor_id, now, _last_email_sent, _last_global_email_sent)

                if send_now:
                    subject, plain_body, html_body = build_email_content(sensor_id, sensor_type, value, reason, severity)
                    try:
                        send_email(subject, plain_body, html_body)
                        _last_email_sent[sensor_id] = now
                        _last_global_email_sent = now
                        span.set_attribute("outcome", "email_sent")
                        logger.info(
                            "Email sent",
                            extra={"category": "business", "fields": {"sensor_id": sensor_id, "severity": severity}},
                        )
                    except smtplib.SMTPException as e:
                        span.set_attribute("outcome", "email_failed")
                        span.set_attribute("error", str(e))
                        logger.error(
                            "Email failed",
                            extra={"category": "business", "fields": {"sensor_id": sensor_id, "error": str(e)}},
                        )
                        send_now = False
                else:
                    span.set_attribute("outcome", "email_throttled")

                insert_alert(pg_conn, sensor_id, sensor_type, value, reason, severity, anomaly["detected_at"], send_now)
                consumer.commit(msg)
    except KeyboardInterrupt:
        logger.info("Shutdown requested...", extra={"category": "infra"})
    finally:
        consumer.close()
        pg_conn.close()


if __name__ == "__main__":
    run()