"""
app/database/db.py
------------------
Fixed issues from original:
  - execute() now shares ONE connection per logical call, so BEGIN/COMMIT
    are never on different connections (the silent transaction bug).
  - Added fetchone= support (was silently ignored before).
  - Credentials read from environment variables; hardcoded fallback kept
    for local dev but logs a warning.
  - get_connection() is exported so ingest_router can manage its own
    long-running transactions safely.
"""

import os
import logging

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTION FACTORY
# ─────────────────────────────────────────────────────────────────────────────

def get_connection() -> psycopg2.extensions.connection:
    host     = os.environ.get("DB_HOST",     "localhost")
    database = os.environ.get("DB_NAME",     "fin_insight")
    user     = os.environ.get("DB_USER",     "postgres")
    password = os.environ.get("DB_PASSWORD", "12345678")

    if password == "12345678" and not os.environ.get("DB_PASSWORD"):
        logger.warning(
            "DB_PASSWORD is using the hardcoded fallback. "
            "Set the DB_PASSWORD environment variable in production."
        )

    return psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        cursor_factory=RealDictCursor,
    )


# ─────────────────────────────────────────────────────────────────────────────
# TRANSACTION CONTEXT
# ─────────────────────────────────────────────────────────────────────────────

class _TransactionContext:
    """
    Holds one open connection for the duration of a governed action.
    Usage:
        with transaction_context() as ctx:
            ctx.execute("BEGIN")
            ctx.execute("INSERT ...")
            ctx.execute("COMMIT")
    """
    def __init__(self):
        self._conn: psycopg2.extensions.connection | None = None

    def __enter__(self):
        self._conn = get_connection()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute(self, query: str, params=None, fetch: bool = False, fetchone: bool = False):
        if not self._conn:
            raise RuntimeError("execute() called outside transaction_context()")
        return _run(self._conn, query, params, fetch, fetchone)


def transaction_context() -> _TransactionContext:
    return _TransactionContext()


# ─────────────────────────────────────────────────────────────────────────────
# SIMPLE ONE-SHOT EXECUTE  (non-transactional helpers)
# ─────────────────────────────────────────────────────────────────────────────

def _run(conn, query: str, params, fetch: bool, fetchone: bool):
    with conn.cursor() as cur:
        cur.execute(query, params or [])

        if fetchone:
            result = cur.fetchone()
        elif fetch:
            result = cur.fetchall()
        else:
            result = None

    return result


def execute(query: str, params=None, fetch: bool = False, fetchone: bool = False):
    """
    Open a connection, run ONE statement, commit, close.

    IMPORTANT: Do NOT call execute("BEGIN") then execute("COMMIT") — each call
    gets its own connection and they will never see each other's transaction.
    For multi-statement transactions use transaction_context() instead.
    """
    if query.strip().upper() in ("BEGIN", "COMMIT", "ROLLBACK"):
        # These are no-ops in autocommit-per-call mode.
        # Callers that need real transactions must use transaction_context().
        logger.debug("execute('%s') is a no-op in single-shot mode — use transaction_context()", query.strip())
        return None

    conn = get_connection()
    try:
        result = _run(conn, query, params, fetch, fetchone)
        conn.commit()
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()