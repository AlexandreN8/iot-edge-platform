import logging
import json
import time


class JsonFormatter(logging.Formatter):
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
    logger = logging.getLogger(service_name)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger