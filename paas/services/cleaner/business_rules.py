BUSINESS_RANGES = {
    "co2": (0, 5000),
    "temperature": (-40, 60),
    "humidity": (0, 100),
    "smoke": (0, 1000),
}

CONTINUOUS_SENSOR_TYPES = {"co2", "temperature", "humidity", "smoke", "vibration", "power_consumption"}

DUPLICATE_WINDOW_SECONDS = 10


def check_business_range(payload):
    """ Check if the value is within the business-plausible range for its type. """
    bounds = BUSINESS_RANGES.get(payload["type"])
    if bounds is None:
        return True, None
    low, high = bounds
    if not (low <= payload["value"] <= high):
        return False, f"value {payload['value']} outside plausible range [{low}, {high}] for {payload['type']}"
    return True, None


def check_business_duplicate(payload, last_seen):
    """ Same value for the same sensor within a short time window is considered a business duplicate. """
    if payload["type"] not in CONTINUOUS_SENSOR_TYPES:
        return True, None
    last = last_seen.get(payload["sensor_id"])
    if last is not None:
        last_value, last_ts = last
        if last_value == payload["value"] and (payload["timestamp"] - last_ts) < DUPLICATE_WINDOW_SECONDS:
            return False, f"same value {payload['value']} repeated within {DUPLICATE_WINDOW_SECONDS}s"
    return True, None