import sqlite3

def init_db(path="buffer.db"):
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""CREATE TABLE IF NOT EXISTS buffer (
        id INTEGER PRIMARY KEY, payload TEXT, status TEXT DEFAULT 'pending'
    )""")
    return conn

def insert(conn, payload_json):
    conn.execute("INSERT INTO buffer (payload) VALUES (?)", (payload_json,))
    conn.commit()

def fetch_pending(conn, limit=50):
    cursor = conn.execute(
        "SELECT id, payload FROM buffer WHERE status = 'pending' LIMIT ?", (limit,)
    )
    return cursor.fetchall()

def mark_sent(conn, row_id):
    conn.execute("DELETE FROM buffer WHERE id = ?", (row_id,))
    conn.commit()