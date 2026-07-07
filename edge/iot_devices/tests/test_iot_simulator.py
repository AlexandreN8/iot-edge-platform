from iot_simulator import generate_value, build_payload, maybe_inject_fault

def test_generate_value_continuous_within_bounds():
    sensor = {"type": "co2", "sensor_id": "co2-001", "min": 400, "max": 1500, "unit": "ppm"}
    for _ in range(20):
        value = generate_value(sensor)
        assert 400 <= value <= 1500

def test_generate_value_binary_returns_valid_choice():
    sensor = {"type": "opening", "sensor_id": "opening-001", "values": [0, 1], "unit": "bool"}
    for _ in range(20):
        value = generate_value(sensor)
        assert value in [0, 1]

def test_build_payload_structure():
    sensor = {"type": "co2", "sensor_id": "co2-001", "unit": "ppm"}
    payload = build_payload(sensor, 800)
    assert payload["sensor_id"] == "co2-001"
    assert payload["type"] == "co2"
    assert payload["value"] == 800
    assert payload["unit"] == "ppm"
    assert "timestamp" in payload

def test_maybe_inject_fault_returns_tuple():
    sensor = {"type": "co2", "sensor_id": "co2-001", "min": 400, "max": 1500, "unit": "ppm"}
    payload = build_payload(sensor, 800)
    result = maybe_inject_fault(sensor, payload)
    assert isinstance(result, tuple)
    assert len(result) == 2