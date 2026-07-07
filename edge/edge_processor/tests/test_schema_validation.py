from schema_validation import validate

def test_valid_co2_payload():
    payload = {"sensor_id": "co2-001", "type": "co2", "value": 800, "unit": "ppm"}
    assert validate(payload) is True

def test_missing_unit_rejected():
    payload = {"sensor_id": "co2-001", "type": "co2", "value": 800}
    assert validate(payload) is False

def test_unknown_type_rejected():
    payload = {"sensor_id": "x", "type": "unknown_type", "value": 1, "unit": "x"}
    assert validate(payload) is False