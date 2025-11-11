import logging
from typing import Any

from psycopg2 import sql
from psycopg2.extensions import cursor as BaseCursor

from .utils import _normalize_casts, _shorten


def _render_sql_with_params(cur, query, vars) -> str:
    """
    Render SQL + params into a final string:
    - For any psycopg2.sql object (SQL/Composed/Identifier), use as_string.
    - Bind params via mogrify; on failure, fall back to raw SQL text.
    - Normalize duplicated casts for cleaner logs.
    """
    try:
        qtxt = query.as_string(cur.connection) if hasattr(query, "as_string") else str(query)
    except Exception:
        qtxt = str(query)
    final = qtxt
    if vars is not None:
        try:
            final = cur.mogrify(qtxt, vars).decode()
        except Exception:
            final = qtxt
    # Normalize duplicated ::type::type in the rendered SQL
    return _normalize_casts(final)


class ErrorLoggingCursorParam(BaseCursor):
    """
    Cursor that logs the full SQL (with params bound) only when psycopg2.Error occurs.
    """

    def execute(self, query, vars=None):
        try:
            return super().execute(query, vars)
        except Exception as e:
            try:
                import psycopg2
                is_pg_error = isinstance(e, psycopg2.Error)
            except Exception:
                is_pg_error = True
            if is_pg_error:
                full_sql = _render_sql_with_params(self, query, vars)
                msg = _shorten(full_sql)
                print(f"[SQL-ERROR] {msg}")
                logging.error(f"[SQL-ERROR] {msg}")
            raise

    def executemany(self, query, vars_list):
        try:
            return super().executemany(query, vars_list)
        except Exception as e:
            try:
                import psycopg2
                is_pg_error = isinstance(e, psycopg2.Error)
            except Exception:
                is_pg_error = True
            if is_pg_error:
                first = (vars_list[0] if vars_list else None)
                full_sql = _render_sql_with_params(self, query, first)
                msg = _shorten(full_sql)
                print(f"[SQL-ERROR-MANY] {msg}")
                logging.error(f"[SQL-ERROR-MANY] {msg}")
            raise