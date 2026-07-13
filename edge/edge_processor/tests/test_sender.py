from sender import make_delivery_callback
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