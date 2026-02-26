import psycopg2
from psycopg2.extras import RealDictCursor


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="fin_insight",
        user="postgres",
        password="12345678"
    )


def execute(query, params=None, fetch=False):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(query, params or [])

    result = None
    if fetch:
        result = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()

    return result