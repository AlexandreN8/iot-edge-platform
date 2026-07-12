from business_rules import (
    get_window_start,
    update_accumulator,
    finalize_window,
    get_completed_windows,
)


def test_get_window_start_floors_to_minute():
    # 12:34:57 -> should floor to 12:34:00
    ts = 1_700_000_097.0  # arbitrary epoch, seconds component = 57
    window_start = get_window_start(ts, 60)
    assert window_start == ts - (ts % 60)
    assert window_start % 60 == 0


def test_get_window_start_floors_to_hour():
    ts = 1_700_003_700.0  # arbitrary epoch with nonzero minute/second
    window_start = get_window_start(ts, 3600)
    assert window_start % 3600 == 0
    assert window_start <= ts


def test_update_accumulator_creates_new_entry():
    accumulators = {}
    update_accumulator(accumulators, "temp-001", "temperature", 1000, 20.0)
    acc = accumulators[("temp-001", 1000)]
    assert acc["count"] == 1
    assert acc["sum"] == 20.0
    assert acc["min"] == 20.0
    assert acc["max"] == 20.0


def test_update_accumulator_updates_existing_entry():
    accumulators = {}
    update_accumulator(accumulators, "temp-001", "temperature", 1000, 20.0)
    update_accumulator(accumulators, "temp-001", "temperature", 1000, 24.0)
    update_accumulator(accumulators, "temp-001", "temperature", 1000, 18.0)
    acc = accumulators[("temp-001", 1000)]
    assert acc["count"] == 3
    assert acc["sum"] == 62.0
    assert acc["min"] == 18.0
    assert acc["max"] == 24.0


def test_update_accumulator_keeps_sensors_separate():
    accumulators = {}
    update_accumulator(accumulators, "temp-001", "temperature", 1000, 20.0)
    update_accumulator(accumulators, "temp-002", "temperature", 1000, 99.0)
    assert accumulators[("temp-001", 1000)]["count"] == 1
    assert accumulators[("temp-002", 1000)]["count"] == 1
    assert accumulators[("temp-001", 1000)]["sum"] == 20.0


def test_finalize_window_computes_average_and_window_end():
    acc = {"sensor_id": "temp-001", "type": "temperature", "window_start": 1000, "sum": 60.0, "min": 18.0, "max": 24.0, "count": 3}
    row = finalize_window(acc, window_seconds=60)
    assert row["avg_value"] == 20.0
    assert row["window_end"] == 1060
    assert row["min_value"] == 18.0
    assert row["max_value"] == 24.0
    assert row["sample_count"] == 3


def test_get_completed_windows_flags_past_windows():
    accumulators = {
        ("temp-001", 1000): {"sensor_id": "temp-001", "type": "temperature", "window_start": 1000, "sum": 20.0, "min": 20.0, "max": 20.0, "count": 1},
    }
    # now is well past window_start + 60
    completed = get_completed_windows(accumulators, now=1100, window_seconds=60)
    assert ("temp-001", 1000) in completed


def test_get_completed_windows_ignores_still_open_windows():
    accumulators = {
        ("temp-001", 1000): {"sensor_id": "temp-001", "type": "temperature", "window_start": 1000, "sum": 20.0, "min": 20.0, "max": 20.0, "count": 1},
    }
    # now is still within the window
    completed = get_completed_windows(accumulators, now=1030, window_seconds=60)
    assert completed == []