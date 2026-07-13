from business_rules import classify_fault_type


def test_classify_fault_type_extracts_category_before_colon():
    assert classify_fault_type("out_of_range: value 5000 outside plausible range") == "out_of_range"


def test_classify_fault_type_business_duplicate():
    assert classify_fault_type("business_duplicate: same value 20.0 repeated within 10s") == "business_duplicate"


def test_classify_fault_type_no_colon_returns_unknown():
    assert classify_fault_type("something without a colon") == "unknown"


def test_classify_fault_type_empty_string_returns_unknown():
    assert classify_fault_type("") == "unknown"


def test_classify_fault_type_none_returns_unknown():
    assert classify_fault_type(None) == "unknown"


def test_classify_fault_type_strips_whitespace():
    assert classify_fault_type("  out_of_range  : details") == "out_of_range"