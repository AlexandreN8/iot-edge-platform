import logging
import json
import time


class JsonFormatter(logging.Formatter):
    """
    Renders each log record as a single JSON line, structured for Fluent
    Bit/Loki ingestion downstream. `category` and any extra fields are
    read from the record's `extra=` dict, not hardcoded - each call site
    decides its own category and additional structured fields.
    """

    def format(self, record):
        payload = {
            "timestamp": time.time(),
            "level": record.levelname,
            "service": record.name,
            "category": getattr(record, "category", "infra"),
            "message": record.getMessage(),
        }

        extra_fields = getattr(record, "fields", None)
        if extra_fields:
            payload.update(extra_fields)

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload)


def get_logger(service_name):
    """
    Returns a configured logger for the given service 
    """
    logger = logging.getLogger(service_name)
    if logger.handlers:
        return logger  # avoid duplicate handlers if get_logger is called more than once

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger