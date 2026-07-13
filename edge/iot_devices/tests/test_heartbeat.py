import time
from heartbeat import touch_heartbeat

def test_touch_heartbeat_creates_file_with_recent_timestamp(tmp_path):
    path = tmp_path / "heartbeat"
    touch_heartbeat(str(path))

    assert path.exists()
    written_ts = float(path.read_text())
    assert abs(time.time() - written_ts) < 2