from business_rules import check_business_range, check_business_duplicate

def test_plausible_co2_passes():
    ok, reason = check_business_range({"type": "co2", "value": 800})
    assert ok is True

def test_implausible_co2_rejected():
    ok, reason = check_business_range({"type": "co2", "value": 99999})
    assert ok is False
    assert "outside plausible range" in reason

def test_unknown_type_passes_through():
    ok, reason = check_business_range({"type": "opening", "value": 1})
    assert ok is True  

def test_binary_sensor_repeat_is_not_duplicate():
    last_seen = {"opening-001": (0, 1000.0)}
    payload = {"sensor_id": "opening-001", "type": "opening", "value": 0, "timestamp": 1005.0}
    ok, reason = check_business_duplicate(payload, last_seen)
    assert ok is True 

def test_continuous_sensor_close_repeat_is_duplicate():
    last_seen = {"co2-001": (800, 1000.0)}
    payload = {"sensor_id": "co2-001", "type": "co2", "value": 800, "timestamp": 1005.0}
    ok, reason = check_business_duplicate(payload, last_seen)
    assert ok is False

def test_continuous_sensor_repeat_outside_window_is_ok():
    last_seen = {"co2-001": (800, 1000.0)}
    payload = {"sensor_id": "co2-001", "type": "co2", "value": 800, "timestamp": 1050.0}
    ok, reason = check_business_duplicate(payload, last_seen)
    assert ok is True  