def get_window_start(timestamp, window_seconds):
    """
    Floors an epoch timestamp to the start of its clock-aligned window.
    E.g. window_seconds=60 -> floors to the start of the minute.
    """
    return (timestamp // window_seconds) * window_seconds


def update_accumulator(accumulators, sensor_id, sensor_type, window_start, value):
    """
    Updates (or creates) the running aggregate for a given sensor + window.
    Plain dict, not a class - consistent with Cleaner/Anomaly detector's style.
    """
    key = (sensor_id, window_start)
    acc = accumulators.get(key)
    if acc is None:
        accumulators[key] = {
            "sensor_id": sensor_id,
            "type": sensor_type,
            "window_start": window_start,
            "sum": value,
            "min": value,
            "max": value,
            "count": 1,
        }
    else:
        acc["sum"] += value
        acc["min"] = min(acc["min"], value)
        acc["max"] = max(acc["max"], value)
        acc["count"] += 1
    return accumulators


def finalize_window(acc, window_seconds):
    """ Converts a raw accumulator into the row shape ready for insertion. """
    return {
        "sensor_id": acc["sensor_id"],
        "type": acc["type"],
        "window_start": acc["window_start"],
        "window_end": acc["window_start"] + window_seconds,
        "avg_value": acc["sum"] / acc["count"],
        "min_value": acc["min"],
        "max_value": acc["max"],
        "sample_count": acc["count"],
    }


def get_completed_windows(accumulators, now, window_seconds):
    """
    Returns the keys of windows whose end has already passed relative to
    wall-clock time - triggers a flush regardless of whether new data ever
    arrives for that sensor again
    """
    return [
        key for key, acc in accumulators.items()
        if now >= acc["window_start"] + window_seconds
    ]