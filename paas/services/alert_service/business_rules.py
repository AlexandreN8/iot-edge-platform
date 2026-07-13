
COOLDOWN_SECONDS = 1800         # 30 minutes, per sensor
GLOBAL_COOLDOWN_SECONDS = 120   # 2 minutes, across all sensors combined


def classify_severity(reason):
    """
    Derives a severity level from Anomaly detector's reason string.
    """
    if "stddev" in reason:
        try:
            deviation = float(reason.split("deviates")[1].split("stddev")[0].strip())
        except (IndexError, ValueError):
            return "warning"
        return "critical" if deviation >= 10 else "warning"

    if "transitions" in reason:
        try:
            count = int(reason.split()[0])
        except (IndexError, ValueError):
            return "warning"
        return "critical" if count >= 10 else "warning"

    return "warning"


def should_send_email(sensor_id, now, last_email_sent, last_global_sent):
    """
    Rate-limits email notifications on two independent axes - every anomaly
    is still recorded in the alerts table regardless, this only decides
    whether an email accompanies this particular one:
    - per-sensor cooldown: the same sensor can't re-trigger an email within
      COOLDOWN_SECONDS of its last one
    - global cooldown: no email at all can go out within
      GLOBAL_COOLDOWN_SECONDS of the last email sent, regardless of which
      sensor - caps overall inbox volume when several different sensors
      misbehave close together in time
    Both must pass for an email to be sent.
    """
    last_sent_for_sensor = last_email_sent.get(sensor_id)
    per_sensor_ok = last_sent_for_sensor is None or (now - last_sent_for_sensor) >= COOLDOWN_SECONDS

    global_ok = last_global_sent is None or (now - last_global_sent) >= GLOBAL_COOLDOWN_SECONDS

    return per_sensor_ok and global_ok


def build_email_content(sensor_id, sensor_type, value, reason, severity):
    """ Builds the subject/plain-text/HTML body trio for the alert email. """
    subject = f"[IoT Alert] {severity.upper()} - {sensor_id}"

    plain_body = (
        f"Sensor: {sensor_id} ({sensor_type})\n"
        f"Value: {value}\n"
        f"Reason: {reason}\n"
        f"Severity: {severity}\n"
    )

    severity_color = "#d32f2f" if severity == "critical" else "#f57c00"

    html_body = f"""
    <html>
      <body style="font-family: Arial, sans-serif; color: #333;">
        <div style="border-left: 4px solid {severity_color}; padding: 12px 16px; background: #f9f9f9;">
          <h2 style="margin: 0 0 8px 0; color: {severity_color};">
            {severity.upper()} anomaly detected
          </h2>
          <table style="border-collapse: collapse; margin-top: 8px;">
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Sensor</td><td><b>{sensor_id}</b></td></tr>
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Type</td><td>{sensor_type}</td></tr>
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Value</td><td>{value}</td></tr>
            <tr><td style="padding: 4px 12px 4px 0; color: #666;">Reason</td><td>{reason}</td></tr>
          </table>
        </div>
      </body>
    </html>
    """

    return subject, plain_body, html_body