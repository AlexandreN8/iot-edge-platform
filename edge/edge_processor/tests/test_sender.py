import time
from sender import make_delivery_callback, touch_heartbeat
from buffer import init_db, insert, fetch_pending


def test_delivery_callback_marks_sent_on_success():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "temp-001"}')
    row_id = fetch_pending(conn)[0][0]

    class FakeMsg:
        def topic(self):
            return "raw"

        def partition(self):
            return 0

    callback = make_delivery_callback(conn, row_id)
    callback(err=None, msg=FakeMsg())

    assert fetch_pending(conn) == []  # mark_sent was called, row removed


def test_delivery_callback_does_not_mark_sent_on_failure():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "temp-001"}')
    row_id = fetch_pending(conn)[0][0]

    callback = make_delivery_callback(conn, row_id)
    callback(err="some kafka error", msg=None)

    assert len(fetch_pending(conn)) == 1  # row still pending, not marked sent


def test_touch_heartbeat_creates_file_with_recent_timestamp(tmp_path):
    path = tmp_path / "heartbeat_sender"
    touch_heartbeat(str(path))

    assert path.exists()
    written_ts = float(path.read_text())
    assert abs(time.time() - written_ts) < 2