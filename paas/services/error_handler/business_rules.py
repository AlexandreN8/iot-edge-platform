def classify_fault_type(reason):
    """
    Extracts a low-cardinality fault-type label from a DLQ reason string,
    e.g. "out_of_range: value 5000 outside..." -> "out_of_range".
    """
    if not reason or ":" not in reason:
        return "unknown"
    category = reason.split(":", 1)[0].strip()
    return category if category else "unknown"