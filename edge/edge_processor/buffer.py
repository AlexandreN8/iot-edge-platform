import sqlite3
import time

def init_db(path="buffer.db"):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""CREATE TABLE IF NOT EXISTS buffer (
        id INTEGER PRIMARY KEY,
        payload TEXT,
        status TEXT DEFAULT 'pending',
        buffered_at REAL NOT NULL
    )""")
    return conn

def insert(conn, payload_json):
    conn.execute(
        "INSERT INTO buffer (payload, buffered_at) VALUES (?, ?)",
        (payload_json, time.time()),
    )
    conn.commit()

def purge_expired(conn, ttl_seconds):
    """
    Deletes pending rows older than ttl_seconds - protects disk from
    unbounded growth during a prolonged Kafka outage. 
    Returns the number of rows purged, for logging.
    """
    cutoff = time.time() - ttl_seconds
    cursor = conn.execute(
        "DELETE FROM buffer WHERE status = 'pending' AND buffered_at < ?", (cutoff,)
    )
    conn.commit()
    return cursor.rowcount

def fetch_pending(conn, limit=50):
    cursor = conn.execute(
        "SELECT id, payload FROM buffer WHERE status = 'pending' ORDER BY buffered_at LIMIT ?", (limit,)
    )
    return cursor.fetchall()

def mark_sent(conn, row_id):
    conn.execute("DELETE FROM buffer WHERE id = ?", (row_id,))
    conn.commit()