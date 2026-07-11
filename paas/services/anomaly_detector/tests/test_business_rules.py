from business_rules import (
    check_statistical_anomaly,
    check_flapping_anomaly,
    WINDOW_SIZE,
    FLAPPING_WINDOW_SECONDS,
    FLAPPING_MAX_TRANSITIONS,
)


def make_payload(sensor_id="temp-001", type_="temperature", value=20.0, timestamp=1000.0):
    return {"sensor_id": sensor_id, "type": type_, "value": value, "timestamp": timestamp, "unit": "°C"}


# --- check_statistical_anomaly ---

def test_statistical_ignores_non_statistical_type():
    payload = make_payload(type_="opening", value=1)
    ok, reason = check_statistical_anomaly(payload, {})
    assert ok
    assert reason is None


def test_statistical_passes_with_insufficient_history():
    history = {"temp-001": [20.0] * (WINDOW_SIZE - 1)}
    payload = make_payload(value=99.0)
    ok, reason = check_statistical_anomaly(payload, history)
    assert ok


def test_statistical_passes_within_normal_deviation():
    history = {"temp-001": [20.0, 20.5, 19.5, 20.2, 19.8] * (WINDOW_SIZE // 5)}
    payload = make_payload(value=20.3)
    ok, reason = check_statistical_anomaly(payload, history)
    assert ok


def test_statistical_flags_large_deviation():
    history = {"temp-001": [19.8, 20.1, 19.9, 20.2, 20.0] * (WINDOW_SIZE // 5)}
    payload = make_payload(value=100.0)
    ok, reason = check_statistical_anomaly(payload, history)
    assert not ok
    assert "deviates" in reason


def test_statistical_passes_when_stddev_is_zero():
    # constant history means no computable deviation - never flagged, by design
    history = {"temp-001": [20.0] * WINDOW_SIZE}
    payload = make_payload(value=20.0)
    ok, reason = check_statistical_anomaly(payload, history)
    assert ok


# --- check_flapping_anomaly ---

def test_flapping_ignores_non_flapping_type():
    payload = make_payload(type_="temperature", value=20.0)
    ok, reason = check_flapping_anomaly(payload, {})
    assert ok


def test_flapping_passes_under_threshold():
    transitions = {"opening-001": [1000.0, 1010.0, 1020.0]}
    payload = make_payload(sensor_id="opening-001", type_="opening", value=1, timestamp=1030.0)
    ok, reason = check_flapping_anomaly(payload, transitions)
    assert ok


def test_flapping_flags_over_threshold():
    base = 1000.0
    transitions = {"opening-001": [base + i * 5 for i in range(FLAPPING_MAX_TRANSITIONS + 1)]}
    payload = make_payload(sensor_id="opening-001", type_="opening", value=1, timestamp=base + 50)
    ok, reason = check_flapping_anomaly(payload, transitions)
    assert not ok
    assert "transitions" in reason


def test_flapping_ignores_transitions_outside_window():
    old_transitions = {"opening-001": [0.0] * (FLAPPING_MAX_TRANSITIONS + 1)}
    payload = make_payload(
        sensor_id="opening-001", type_="opening", value=1,
        timestamp=old_transitions["opening-001"][0] + FLAPPING_WINDOW_SECONDS + 100,
    )
    ok, reason = check_flapping_anomaly(payload, old_transitions)
    assert ok