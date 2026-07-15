import json
import logging
from logging_setup import get_logger, JsonFormatter


def test_get_logger_returns_configured_logger():
    logger = get_logger("test-service")
    assert logger.name == "test-service"
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0].formatter, JsonFormatter)


def test_get_logger_does_not_duplicate_handlers_on_repeated_calls():
    logger1 = get_logger("test-service-dedup")
    logger2 = get_logger("test-service-dedup")
    assert logger1 is logger2
    assert len(logger1.handlers) == 1


def test_json_formatter_produces_valid_json_with_expected_fields():
    record = logging.LogRecord(
        name="aggregator", level=logging.INFO, pathname="", lineno=0,
        msg="Aggregator listening on topic", args=(), exc_info=None,
    )
    formatted = JsonFormatter().format(record)
    data = json.loads(formatted)

    assert data["level"] == "INFO"
    assert data["service"] == "aggregator"
    assert data["category"] == "infra"
    assert "timestamp" in data


def test_json_formatter_reads_category_from_extra():
    record = logging.LogRecord(
        name="aggregator", level=logging.INFO, pathname="", lineno=0,
        msg="Window flushed", args=(), exc_info=None,
    )
    record.category = "business"
    data = json.loads(JsonFormatter().format(record))
    assert data["category"] == "business"


def test_json_formatter_merges_extra_fields():
    record = logging.LogRecord(
        name="aggregator", level=logging.INFO, pathname="", lineno=0,
        msg="Window flushed", args=(), exc_info=None,
    )
    record.fields = {"sensor_id": "temp-001", "avg_value": 20.5, "sample_count": 12}
    data = json.loads(JsonFormatter().format(record))
    assert data["sensor_id"] == "temp-001"
    assert data["avg_value"] == 20.5
    assert data["sample_count"] == 12


def test_json_formatter_includes_exception_when_present():
    try:
        raise ValueError("boom")
    except ValueError:
        import sys
        record = logging.LogRecord(
            name="aggregator", level=logging.ERROR, pathname="", lineno=0,
            msg="Unexpected failure", args=(), exc_info=sys.exc_info(),
        )
    data = json.loads(JsonFormatter().format(record))
    assert "exception" in data
    assert "ValueError" in data["exception"]