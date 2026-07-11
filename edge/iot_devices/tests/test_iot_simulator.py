import json
import random
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

def test_maybe_inject_fault_statistical_spike_reaches_extreme():
    """A statistical_spike fault should jump to the sensor's own min or max, staying business-plausible."""
    sensor = {"type": "temperature", "sensor_id": "temp-001", "min": 0, "max": 50, "unit": "°C"}
    payload = build_payload(sensor, 20.0)

    random.seed(1)  
    found_spike = False
    for _ in range(200):  
        msg, label = maybe_inject_fault(sensor, payload)
        if label == "statistical_spike":
            found_spike = True
            data = json.loads(msg)
            assert data["value"] in (sensor["min"], sensor["max"])
            break
    assert found_spike, "statistical_spike branch never triggered across 200 attempts"


def test_generate_opening_value_mostly_stable():
    """Opening sensor should stay stable most cycles, not toggle every call like the old uniform choice."""
    sensor = {"type": "opening", "sensor_id": "opening-test", "values": [0, 1], "unit": "bool"}
    values = [generate_value(sensor) for _ in range(30)]
    toggles = sum(1 for a, b in zip(values, values[1:]) if a != b)
    assert toggles < 15  