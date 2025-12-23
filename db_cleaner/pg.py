from typing import List, Tuple, Dict, Any, Optional

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from .sql_render import ErrorLoggingCursorParam
from .utils import split_schema_table


def get_column_types(conn: PGConnection, schema: str, table: str, columns: List[str]) -> List[str]:
    q = """
    SELECT a.attname, t.typname
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = %s::regclass
      AND a.attname = ANY(%s)
      AND a.attnum > 0
    """
    regclass = f"{schema}.{table}"
    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cur.execute(q, (regclass, columns))
        rows = cur.fetchall()
    typemap = {name: typ for name, typ in rows}
    return [typemap[c] for c in columns]


def build_typed_values_clause(parent_keys: List[Tuple], col_types: List[str]) -> Tuple[sql.SQL, List[Any]]:
    if not parent_keys:
        return sql.SQL(""), []
    params: List[Any] = []
    row_templates: List[sql.SQL] = []
    for tup in parent_keys:
        if len(tup) != len(col_types):
            raise ValueError("Each meta-length of parent_keys needs to be consistent with col_types.")
        cols = []
        for i, typ in enumerate(col_types):
            cols.append(sql.SQL("%s::{}").format(sql.SQL(typ)))
            params.append(tup[i])
        row_templates.append(sql.SQL("(") + sql.SQL(",").join(cols) + sql.SQL(")"))
    return sql.SQL(",").join(row_templates), params


def get_primary_key_columns(conn: PGConnection, schema: str, table: str) -> List[str]:
    """
    Retrieves the primary key column names of a given table in a given schema.
    """
    q = """
    SELECT a.attname
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
    WHERE i.indisprimary = TRUE
      AND n.nspname = %s AND c.relname = %s
    ORDER BY array_position(i.indkey, a.attnum);
    """
    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]


def build_values_clause(cur, tuples: List[Tuple]) -> sql.SQL:
    parts = []
    for t in tuples:
        frag = cur.mogrify("(" + ",".join(["%s"] * len(t)) + ")", t).decode()
        parts.append(sql.SQL(frag))
    return sql.SQL(",").join(parts)


def build_conditions_sql(conditions: Optional[List[Dict[str, Any]]]) -> Tuple[sql.SQL, List[Any]]:
    if not conditions:
        return sql.SQL(""), []
    clauses = []
    params: List[Any] = []
    for cond in conditions:
        if "raw_sql" in cond:
            raw = cond["raw_sql"]
            clauses.append(sql.SQL(raw))
            params.extend(cond.get("params", []))
            continue
        col = cond["column"]
        op = cond["op"].upper().strip()
        if op in ("IS NULL", "IS NOT NULL"):
            clauses.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(op)))
        elif op == "IN":
            vals = cond.get("value", [])
            if not isinstance(vals, (list, tuple)):
                raise ValueError("conditions.IN expects a list value.")
            placeholders = sql.SQL(",").join([sql.Placeholder()] * len(vals))
            clauses.append(sql.SQL("{} IN ({})").format(sql.Identifier(col), placeholders))
            params.extend(vals)
        else:
            clauses.append(sql.SQL("{} {} {}").format(sql.Identifier(col), sql.SQL(op), sql.Placeholder()))
            params.append(cond.get("value"))
    where_sql = sql.SQL(" AND ").join(clauses)
    # Prefix with AND so it can be appended to an existing WHERE clause.
    return sql.SQL(" AND ").join([sql.SQL(""), where_sql]), params


def fetch_batch(conn: PGConnection,
                table: str,
                key_columns: List[str],
                date_column: str,
                cutoff,                # can be None
                batch_size: int,
                conditions: Optional[List[Dict[str, Any]]] = None) -> List[Tuple]:
    schema, tbl = split_schema_table(table)
    keys_sql = sql.SQL(",").join([sql.Identifier(c) for c in key_columns])
    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        base = sql.SQL("SELECT {keys} FROM {tbl} WHERE TRUE").format(
            keys=keys_sql, tbl=sql.Identifier(schema, tbl)
        )
        params: List[Any] = []
        if cutoff is not None:
            base = base + sql.SQL(" AND {} < %s").format(sql.Identifier(date_column))
            params.append(cutoff)

        cond_sql, cond_params = build_conditions_sql(conditions)
        q = base + cond_sql
        
        # Only add ORDER BY if we have a cutoff date (for deterministic batch processing)
        # Without cutoff, ORDER BY adds unnecessary overhead for large tables
        if cutoff is not None:
            q = q + sql.SQL(" ORDER BY {} ASC").format(sql.Identifier(date_column))
            
        q = q + sql.SQL(" LIMIT %s")
        params.extend(cond_params)
        params.append(batch_size)

        cur.execute(q, params)
        rows = cur.fetchall()
    return [tuple(r) for r in rows]