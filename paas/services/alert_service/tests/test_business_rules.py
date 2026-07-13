from business_rules import (
    classify_severity,
    should_send_email,
    build_email_content,
    COOLDOWN_SECONDS,
    GLOBAL_COOLDOWN_SECONDS,
)


def test_classify_severity_statistical_warning():
    reason = "value 100.0 deviates 3.2 stddev from recent mean 20.0"
    assert classify_severity(reason) == "warning"


def test_classify_severity_statistical_critical():
    reason = "value 100.0 deviates 15.3 stddev from recent mean 20.0"
    assert classify_severity(reason) == "critical"


def test_classify_severity_flapping_warning():
    reason = "6 transitions within 60s"
    assert classify_severity(reason) == "warning"


def test_classify_severity_flapping_critical():
    reason = "12 transitions within 60s"
    assert classify_severity(reason) == "critical"


def test_classify_severity_unrecognized_defaults_to_warning():
    reason = "something unexpected happened"
    assert classify_severity(reason) == "warning"


def test_should_send_email_first_time_for_sensor():
    assert should_send_email("temp-001", now=1000, last_email_sent={}, last_global_sent=None) is True


def test_should_send_email_within_per_sensor_cooldown():
    last_email_sent = {"temp-001": 1000}
    assert should_send_email(
        "temp-001", now=1000 + COOLDOWN_SECONDS - 1, last_email_sent=last_email_sent, last_global_sent=None
    ) is False


def test_should_send_email_after_per_sensor_cooldown():
    last_email_sent = {"temp-001": 1000}
    assert should_send_email(
        "temp-001", now=1000 + COOLDOWN_SECONDS, last_email_sent=last_email_sent, last_global_sent=1000
    ) is True


def test_should_send_email_blocked_by_global_cooldown_even_if_different_sensor():
    assert should_send_email(
        "temp-001", now=1010, last_email_sent={}, last_global_sent=1000
    ) is False


def test_should_send_email_passes_after_global_cooldown_expires():
    assert should_send_email(
        "temp-001", now=1000 + GLOBAL_COOLDOWN_SECONDS, last_email_sent={}, last_global_sent=1000
    ) is True


def test_build_email_content_includes_key_fields():
    subject, plain_body, html_body = build_email_content("temp-001", "temperature", 45.2, "value deviates 12 stddev", "critical")
    assert "temp-001" in subject
    assert "CRITICAL" in subject
    assert "45.2" in plain_body
    assert "temperature" in plain_body
    assert "temp-001" in html_body

def test_classify_severity_malformed_stddev_reason_defaults_to_warning():
    reason = "value deviates stddev from recent mean"  # no actual number to parse
    assert classify_severity(reason) == "warning"


def test_classify_severity_malformed_transitions_reason_defaults_to_warning():
    reason = "transitions within 60s"  # no leading number to parse
    assert classify_severity(reason) == "warning" 