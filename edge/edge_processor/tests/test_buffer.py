import json
import time
from buffer import init_db, insert, fetch_pending, mark_sent, purge_expired


def test_insert_and_fetch_pending():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "temp-001", "value": 20.0}')

    rows = fetch_pending(conn)

    assert len(rows) == 1
    assert rows[0][1] == '{"sensor_id": "temp-001", "value": 20.0}'


def test_fetch_pending_respects_limit():
    conn = init_db(":memory:")
    for i in range(5):
        insert(conn, f'{{"sensor_id": "s-{i}"}}')

    rows = fetch_pending(conn, limit=2)

    assert len(rows) == 2


def test_fetch_pending_orders_oldest_first():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "first"}')
    insert(conn, '{"sensor_id": "second"}')

    rows = fetch_pending(conn)

    assert json.loads(rows[0][1])["sensor_id"] == "first"
    assert json.loads(rows[1][1])["sensor_id"] == "second"


def test_mark_sent_removes_row():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "temp-001"}')
    row_id = fetch_pending(conn)[0][0]

    mark_sent(conn, row_id)

    assert fetch_pending(conn) == []


def test_purge_expired_removes_old_pending_rows():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "old"}')
    row_id = fetch_pending(conn)[0][0]
    conn.execute("UPDATE buffer SET buffered_at = ? WHERE id = ?", (time.time() - 100, row_id))
    conn.commit()

    purged_count = purge_expired(conn, ttl_seconds=50)

    assert purged_count == 1
    assert fetch_pending(conn) == []


def test_purge_expired_keeps_fresh_rows():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "fresh"}')

    purged_count = purge_expired(conn, ttl_seconds=3600)

    assert purged_count == 0
    assert len(fetch_pending(conn)) == 1


def test_purge_expired_only_touches_pending_status():
    conn = init_db(":memory:")
    insert(conn, '{"sensor_id": "already-sent"}')
    row_id = fetch_pending(conn)[0][0]
    conn.execute("UPDATE buffer SET status = 'sent', buffered_at = ? WHERE id = ?", (time.time() - 100, row_id))
    conn.commit()

    purged_count = purge_expired(conn, ttl_seconds=50)

    assert purged_count == 0  