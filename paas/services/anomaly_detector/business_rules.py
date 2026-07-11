STATISTICAL_SENSOR_TYPES = {"co2", "temperature", "humidity", "smoke", "power_consumption", "vibration", "occupancy"}
FLAPPING_SENSOR_TYPES = {"opening"}

WINDOW_SIZE = 20          # number of recent values to consider for rolling average and stddev
STDDEV_THRESHOLD = 3       # number of standard deviations from the mean to flag as an anomaly
FLAPPING_WINDOW_SECONDS = 60
FLAPPING_MAX_TRANSITIONS = 5   # maximum transitions allowed within the window before flagging as an anomaly


def check_statistical_anomaly(payload, history):
    """
    Flags a continuous value that deviates too far from its sensor's recent
    rolling average - distinct from Cleaner's fixed plausible-range check:
    this catches a value that's still physically plausible but abnormal
    relative to that sensor's own recent behavior.
    """
    if payload["type"] not in STATISTICAL_SENSOR_TYPES:
        return True, None

    values = history.get(payload["sensor_id"], [])
    if len(values) < WINDOW_SIZE:
        return True, None  # not enough history to judge, we let it pass

    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    stddev = variance ** 0.5

    if stddev == 0:
        return True, None  # constant values, no deviation to calculate

    deviation = abs(payload["value"] - mean) / stddev
    if deviation > STDDEV_THRESHOLD:
        return False, f"value {payload['value']} deviates {deviation:.1f} stddev from recent mean {mean:.1f}"
    return True, None


def check_flapping_anomaly(payload, transition_history):
    """
    Flags a binary sensor toggling too often within a short window -
    a different anomaly shape entirely: rate of change, not value deviation.
    """
    if payload["type"] not in FLAPPING_SENSOR_TYPES:
        return True, None

    transitions = transition_history.get(payload["sensor_id"], [])
    recent = [t for t in transitions if payload["timestamp"] - t < FLAPPING_WINDOW_SECONDS]
    if len(recent) > FLAPPING_MAX_TRANSITIONS:
        return False, f"{len(recent)} transitions within {FLAPPING_WINDOW_SECONDS}s"
    return True, None