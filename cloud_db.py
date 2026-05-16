try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import pool as psycopg2_pool
except Exception:
    psycopg2 = None
    psycopg2_pool = None
    RealDictCursor = None

import os
import re

import streamlit as st


DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_INTEGRITY_ERROR = (psycopg2.IntegrityError,) if psycopg2 is not None else tuple()
SQL_TYPE_KEYWORDS = {
    "TEXT",
    "INTEGER",
    "REAL",
    "NUMERIC",
    "BLOB",
    "DATE",
    "TIMESTAMP",
    "BOOLEAN",
    "DOUBLE",
    "PRECISION",
    "SERIAL",
}


@st.cache_resource(show_spinner=False)
def get_pg_pool(database_url):
    """Reutiliza conexões PostgreSQL no Streamlit Cloud para reduzir lentidão."""
    if psycopg2 is None or psycopg2_pool is None:
        return None
    return psycopg2_pool.SimpleConnectionPool(
        minconn=1,
        maxconn=8,
        dsn=database_url,
        cursor_factory=RealDictCursor,
    )


def ensure_database_url():
    if not DATABASE_URL:
        raise RuntimeError(
            "DATABASE_URL não configurada. Esta versão tactical_cloud_v5_postgres roda apenas com PostgreSQL."
        )


class PgCursorCompat:
    """Adaptador mínimo para deixar cursores PostgreSQL parecidos com sqlite3.Row."""

    def __init__(self, cursor, lastrowid=None, rows=None):
        self._cursor = cursor
        self.lastrowid = lastrowid
        self._rows = rows

    def fetchone(self):
        if self._rows is not None:
            return self._rows[0] if self._rows else None
        return self._cursor.fetchone()

    def fetchall(self):
        if self._rows is not None:
            return self._rows
        return self._cursor.fetchall()

    def __iter__(self):
        return iter(self.fetchall())


class PgConnCompat:
    """Compatibilidade básica para usar conn.execute(...) no PostgreSQL."""

    def __init__(self):
        ensure_database_url()
        if psycopg2 is None:
            raise RuntimeError("psycopg2-binary não está instalado. Adicione ao requirements.txt para usar PostgreSQL.")
        self._pool = get_pg_pool(DATABASE_URL)
        if self._pool is None:
            raise RuntimeError("Pool PostgreSQL não pôde ser inicializado.")
        self._conn = self._pool.getconn()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            self._conn.rollback()
        self._pool.putconn(self._conn)
        return False

    def _translate_sql(self, sql):
        """Converte SQL legado para PostgreSQL sem alterar a lógica principal da query."""
        sql = str(sql)
        stripped = sql.strip()
        upper = stripped.upper()
        if upper.startswith("PRAGMA") or "SQLITE_SEQUENCE" in upper:
            return None

        sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
        sql = sql.replace("strftime('%d/%m/%Y', e.event_date)", "to_char(e.event_date::date, 'DD/MM/YYYY')")
        sql = sql.replace('strftime("%d/%m/%Y", e.event_date)', "to_char(e.event_date::date, 'DD/MM/YYYY')")
        sql = sql.replace("?", "%s")

        aliases = []

        def quote_alias(match):
            alias = match.group(1)
            if alias.upper() in SQL_TYPE_KEYWORDS:
                return f"AS {alias}"
            aliases.append(alias)
            return f'AS "{alias}"'

        sql = re.sub(
            r"\bAS\s+([A-Za-z_À-ÿ][A-Za-z0-9_À-ÿ]*)",
            quote_alias,
            sql,
            flags=re.IGNORECASE,
        )

        for alias in sorted(set(aliases), key=len, reverse=True):
            sql = re.sub(
                rf'(?<![".])\b{re.escape(alias)}\b(?!")',
                f'"{alias}"',
                sql,
            )

        sql = re.sub(r"%(?!s)", "%%", sql)
        return sql

    def execute(self, sql, params=()):
        translated = self._translate_sql(sql)
        if translated is None:
            return PgCursorCompat(None, rows=[])
        cur = self._conn.cursor()
        lastrowid = None
        stripped = translated.strip()
        original_upper = str(sql).upper()
        if "OR IGNORE" in original_upper and stripped.upper().startswith("INSERT INTO") and "ON CONFLICT" not in stripped.upper():
            translated = stripped.rstrip(";") + " ON CONFLICT DO NOTHING"
            cur.execute(translated, params)
            return PgCursorCompat(cur)
        if stripped.upper().startswith("INSERT INTO") and "ON CONFLICT" not in stripped.upper() and "RETURNING" not in stripped.upper():
            translated = stripped.rstrip(";") + " RETURNING id"
            cur.execute(translated, params)
            try:
                row = cur.fetchone()
                if row and "id" in row:
                    lastrowid = row["id"]
            except Exception:
                lastrowid = None
            return PgCursorCompat(cur, lastrowid=lastrowid)
        if "INSERT INTO" in stripped.upper() and "ON CONFLICT DO NOTHING" not in stripped.upper() and "OR IGNORE" not in original_upper:
            cur.execute(translated, params)
        else:
            if "INSERT INTO" in stripped.upper() and "ON CONFLICT" not in stripped.upper():
                translated = stripped.rstrip(";") + " ON CONFLICT DO NOTHING"
            cur.execute(translated, params)
        return PgCursorCompat(cur)

    def executescript(self, sql_script):
        cur = self._conn.cursor()
        for statement in str(sql_script).split(";"):
            statement = statement.strip()
            if not statement or statement.upper().startswith("PRAGMA"):
                continue
            translated = self._translate_sql(statement)
            if translated:
                cur.execute(translated)
        return PgCursorCompat(cur)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def get_conn():
    return PgConnCompat()
