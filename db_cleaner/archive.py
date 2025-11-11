import csv
import logging
import os
from datetime import datetime
from typing import List, Tuple

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from .sql_render import ErrorLoggingCursorParam
from .pg import get_column_types, build_typed_values_clause
from .utils import split_schema_table


def archive_to_csv(rows: List[Tuple], archive_dir: str, table_name: str) -> None:
    if not rows:
        return
    os.makedirs(archive_dir, exist_ok=True)
    filename = f"{table_name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    filepath = os.path.join(archive_dir, filename)
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    logging.info(f"[ARCHIVE] {table_name}: Archived {len(rows)} rows to {filepath}")
    print(f"[ARCHIVE] {table_name}: Archived {len(rows)} rows to {filepath}")


def select_rows_for_archive(conn: PGConnection, table: str, key_columns: List[str], keys: List[Tuple]) -> List[Tuple]:
    """
    Select rows from a table for archiving based on key columns and values.
    """
    if not keys:
        return []
    schema, tbl = split_schema_table(table)
    key_types = get_column_types(conn, schema, tbl, key_columns)
    vals_sql, vals_params = build_typed_values_clause(keys, key_types)

    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cols_sql = sql.SQL(",").join([sql.Identifier(c) for c in key_columns])
        q = sql.SQL("SELECT * FROM {tbl} WHERE ({cols}) IN (VALUES {vals})").format(
            tbl=sql.Identifier(schema, tbl),
            cols=cols_sql,
            vals=vals_sql,
        )
        cur.execute(q, vals_params)
        return cur.fetchall()