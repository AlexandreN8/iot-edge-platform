from business_rules import is_targeted, format_status

def test_is_targeted_site_in_wave():
    assert is_targeted("site-001", ["site-001", "site-002"]) is True


def test_is_targeted_site_not_in_wave():
    assert is_targeted("site-003", ["site-001", "site-002"]) is False


def test_is_targeted_empty_wave():
    assert is_targeted("site-001", []) is False


def test_format_status_includes_all_fields():
    status = format_status("site-001", "abc123", "success", "healthcheck passed")
    assert status["site_id"] == "site-001"
    assert status["sha"] == "abc123"
    assert status["outcome"] == "success"
    assert status["detail"] == "healthcheck passed"