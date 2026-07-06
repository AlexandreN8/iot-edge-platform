SCHEMAS = {
    "co2": {"required": ["sensor_id", "value", "unit"]},
    "temperature": {"required": ["sensor_id", "value", "unit"]},
    "humidity": {"required": ["sensor_id", "value", "unit"]},
    "occupancy": {"required": ["sensor_id", "value", "unit"]},
    "power_consumption": {"required": ["sensor_id", "value", "unit"]},
    "opening": {"required": ["sensor_id", "value", "unit"]},
    "smoke": {"required": ["sensor_id", "value", "unit"]},
    "vibration": {"required": ["sensor_id", "value", "unit"]},
}


def validate(payload: dict) -> bool:
    schema = SCHEMAS.get(payload.get("type"))
    if not schema:
        return False
    return all(
        field in payload and payload[field] is not None
        for field in schema["required"]
    )