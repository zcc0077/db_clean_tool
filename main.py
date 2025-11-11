import csv
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional, Set

import yaml
import psycopg2
from sqlalchemy import create_engine
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection
from psycopg2.extensions import cursor as BaseCursor


# ---------- Load Config ----------
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- Logging ----------
def setup_logging(log_file: str) -> None:
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

def _shorten(text: str, max_len: int = 2000) -> str:
    """Limit very long SQL prints to avoid flooding logs."""
    return (text[:max_len] + "...") if len(text) > max_len else text

def _normalize_casts(text: str) -> str:
    """
    Collapse duplicated casts like '::uuid::uuid' -> '::uuid'.
    Works for any typename: ::type::type -> ::type.
    """
    return re.sub(r"::([A-Za-z0-9_]+)\s*::\1", r"::\1", text)

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

# ---------- Helpers ----------
def qualify_table(name: str) -> str:
    if "." in name:
        return name
    return f"public.{name}"

def format_pg_error(e: psycopg2.Error) -> str:
    diag = getattr(e, "diag", None)
    parts = []

    def add(label: str, value):
        if value:
            parts.append(f"{label}={value}")

    add("message", getattr(diag, "message_primary", None) or getattr(e, "pgerror", None) or str(e))
    add("detail", getattr(diag, "message_detail", None))
    add("hint", getattr(diag, "hint", None))
    add("context", getattr(diag, "context", None))
    add("schema", getattr(diag, "schema_name", None))
    add("table", getattr(diag, "table_name", None))
    add("column", getattr(diag, "column_name", None))
    add("constraint", getattr(diag, "constraint_name", None))
    add("sqlstate", getattr(e, "pgcode", None))
    add("routine", getattr(diag, "routine", None) if diag else None)

    return " | ".join(parts) if parts else str(e)


def split_schema_table(qualified: str) -> Tuple[str, str]:
    if "." in qualified:
        s, t = qualified.split(".", 1)
        return s, t
    return "public", qualified


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
    

def select_rows_for_archive(conn, table: str, key_columns: List[str], keys: List[Tuple]) -> List[Tuple]:
    """
    Select rows from a table for archiving based on key columns and values.
    
    Args:
        conn (PGConnection): The PostgreSQL connection object.
        table (str): The name of the table to select from.
        key_columns (List[str]): The names of the columns to filter by.
        keys (List[Tuple]): The values of the columns to filter by.
    
    Returns:
        List[Tuple]: The selected rows.
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


# ---------- Introspection ----------
def fetch_needed_parent_keys(conn, table: str,
                             current_key_columns: List[str],
                             parent_keys: List[Tuple],
                             needed_columns: List[str]) -> List[Tuple]:
    schema, tbl = split_schema_table(table)
    key_types = get_column_types(conn, schema, tbl, current_key_columns)
    vals_sql, vals_params = build_typed_values_clause(parent_keys, key_types)

    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        keys_sql = sql.SQL(",").join([sql.Identifier(c) for c in current_key_columns])
        need_sql = sql.SQL(",").join([sql.Identifier(c) for c in needed_columns])
        q = sql.SQL(
            "SELECT {need} FROM {tbl} WHERE ({keys}) IN (VALUES {vals})"
        ).format(
            tbl=sql.Identifier(schema, tbl),
            keys=keys_sql,
            vals=vals_sql,
            need=need_sql,
        )
        cur.execute(q, vals_params)
        rows = cur.fetchall()
        return [tuple(r) for r in rows]


def get_column_types(conn, schema: str, table: str, columns: List[str]) -> List[str]:
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

    Args:
        conn (PGConnection): The PostgreSQL connection object.
        schema (str): The name of the schema containing the table.
        table (str): The name of the table from which to retrieve primary key column names.

    Returns:
        List[str]: A list of primary key column names.
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


def auto_find_fk_relations(conn: PGConnection, parent_qualified: str) -> List[Dict[str, Any]]:
    """
    Find all foreign key relations in a PostgreSQL database that involve a given parent table.

    Given a parent table, this function will return a list of dictionaries, where each dictionary
    contains information about a foreign key relation involving the parent table. The dictionary will
    contain the following keys:

    - "name": the name of the table that the foreign key relation is to.
    - "parent_table": the name of the parent table that the foreign key relation is from.
    - "mapping": a dictionary that maps the columns of the child table to the columns of the parent table.

    :param conn: A connection to the PostgreSQL database.
    :param parent_qualified: The name of the parent table, qualified with its schema if necessary.
    :return: A list of dictionaries, each containing information about a foreign key relation.
    """
    parent_schema, parent_table = split_schema_table(parent_qualified)
    q = """
    SELECT
      n_child.nspname AS child_schema,
      c_child.relname AS child_table,
      array_agg(a_child.attname ORDER BY array_position(c.conkey, a_child.attnum)) AS child_columns,
      array_agg(a_parent.attname ORDER BY array_position(c.confkey, a_parent.attnum)) AS parent_columns
    FROM pg_constraint c
      JOIN pg_class c_child ON c.conrelid = c_child.oid
      JOIN pg_namespace n_child ON n_child.oid = c_child.relnamespace
      JOIN pg_class c_parent ON c.confrelid = c_parent.oid
      JOIN pg_namespace n_parent ON n_parent.oid = c_parent.relnamespace
      JOIN pg_attribute a_child ON a_child.attrelid = c_child.oid AND a_child.attnum = ANY (c.conkey)
      JOIN pg_attribute a_parent ON a_parent.attrelid = c_parent.oid AND a_parent.attnum = ANY (c.confkey)
    WHERE c.contype = 'f'
      AND n_parent.nspname = %s AND c_parent.relname = %s
    GROUP BY child_schema, child_table;
    """
    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cur.execute(q, (parent_schema, parent_table))
        rows = cur.fetchall()
    rels = []
    for child_schema, child_table, child_cols, parent_cols in rows:
        rels.append({
            "name": f"{child_schema}.{child_table}",
            "parent_table": f"{parent_schema}.{parent_table}",
            "mapping": {
                "child_columns": list(child_cols),
                "parent_columns": list(parent_cols),
            },
        })
    return rels


# ---------- SQL Builders ----------
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
    return sql.SQL(" AND ").join([sql.SQL(""), where_sql]), params


# ---------- Fetch batch ----------
def fetch_batch(conn: PGConnection,
                table: str,
                key_columns: List[str],
                date_column: str,
                cutoff: datetime,
                batch_size: int) -> List[Tuple]:
    """
    Fetch a batch of parent keys older than cutoff.
    Returns keys only: List[Tuple].
    """
    schema, tbl = split_schema_table(table)
    keys_sql = sql.SQL(",").join([sql.Identifier(c) for c in key_columns])
    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        q = sql.SQL(
            "SELECT {keys} FROM {tbl} WHERE {date_col} < %s ORDER BY {date_col} ASC LIMIT %s"
        ).format(
            keys=keys_sql,
            tbl=sql.Identifier(schema, tbl),
            date_col=sql.Identifier(date_column),
        )
        cur.execute(q, (cutoff, batch_size))
        rows = cur.fetchall()
    return [tuple(r) for r in rows]


# ---------- Child operations ----------
def count_child_matches(conn, child_table: str, mapping: Dict[str, List[str]],
                        parent_keys: List[Tuple], conditions: Optional[List[Dict[str, Any]]] = None) -> int:
    schema, tbl = split_schema_table(child_table)
    child_cols = mapping["child_columns"]
    child_types = get_column_types(conn, schema, tbl, child_cols)
    vals_sql, vals_params = build_typed_values_clause(parent_keys, child_types)

    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cols_sql = sql.SQL(",").join([sql.Identifier(c) for c in child_cols])
        base = sql.SQL("SELECT COUNT(*) FROM {tbl} WHERE ({cols}) IN (VALUES {vals})").format(
            tbl=sql.Identifier(schema, tbl),
            cols=cols_sql,
            vals=vals_sql,
        )
        cond_sql, cond_params = build_conditions_sql(conditions)
        q = base + cond_sql
        cur.execute(q, vals_params + cond_params)
        return cur.fetchone()[0]


def select_child_pks(conn, child_table: str, mapping: Dict[str, List[str]],
                     parent_keys: List[Tuple], conditions: Optional[List[Dict[str, Any]]] = None) -> List[Tuple]:
    schema, tbl = split_schema_table(child_table)
    pk_cols = get_primary_key_columns(conn, schema, tbl) or mapping["child_columns"]
    child_cols = mapping["child_columns"]
    child_types = get_column_types(conn, schema, tbl, child_cols)
    vals_sql, vals_params = build_typed_values_clause(parent_keys, child_types)

    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cols_sql = sql.SQL(",").join([sql.Identifier(c) for c in child_cols])
        pk_sql = sql.SQL(",").join([sql.Identifier(c) for c in pk_cols])
        base = sql.SQL("SELECT {pk} FROM {tbl} WHERE ({cols}) IN (VALUES {vals})").format(
            tbl=sql.Identifier(schema, tbl),
            cols=cols_sql,
            vals=vals_sql,
            pk=pk_sql,
        )
        cond_sql, cond_params = build_conditions_sql(conditions)
        q = base + cond_sql
        cur.execute(q, vals_params + cond_params)
        return [tuple(r) for r in cur.fetchall()]


# Reserved for certain scenarios, but it is no longer called at the parent level.
def delete_child_returning_pk(conn, child_table: str, mapping: Dict[str, List[str]],
                              parent_keys: List[Tuple], conditions: Optional[List[Dict[str, Any]]] = None) -> List[Tuple]:
    schema, tbl = split_schema_table(child_table)
    pk_cols = get_primary_key_columns(conn, schema, tbl) or mapping["child_columns"]
    child_cols = mapping["child_columns"]
    child_types = get_column_types(conn, schema, tbl, child_cols)
    vals_sql, vals_params = build_typed_values_clause(parent_keys, child_types)

    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cols_sql = sql.SQL(",").join([sql.Identifier(c) for c in child_cols])
        pk_sql = sql.SQL(",").join([sql.Identifier(c) for c in pk_cols])
        base = sql.SQL("DELETE FROM {tbl} WHERE ({cols}) IN (VALUES {vals})").format(
            tbl=sql.Identifier(schema, tbl),
            cols=cols_sql,
            vals=vals_sql,
        )
        cond_sql, cond_params = build_conditions_sql(conditions)
        ret = sql.SQL(" RETURNING {pk}").format(pk=pk_sql)
        q = base + cond_sql + ret
        cur.execute(q, vals_params + cond_params)
        rows = cur.fetchall()
        print(f"[DELETE] {child_table}: Deleted {len(rows)} rows.")
        logging.info(f"[DELETE] {child_table}: Deleted {len(rows)} rows.")
        return [tuple(r) for r in rows]


def delete_parent(conn, table: str, key_columns: List[str], parent_keys: List[Tuple]) -> int:
    schema, tbl = split_schema_table(table)
    key_types = get_column_types(conn, schema, tbl, key_columns)
    vals_sql, vals_params = build_typed_values_clause(parent_keys, key_types)

    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cols_sql = sql.SQL(",").join([sql.Identifier(c) for c in key_columns])
        q = sql.SQL("DELETE FROM {tbl} WHERE ({cols}) IN (VALUES {vals})").format(
            tbl=sql.Identifier(schema, tbl),
            cols=cols_sql,
            vals=vals_sql,
        )
        cur.execute(q, vals_params)
        return cur.rowcount


# ---------- Relations ----------
def normalize_manual_relations(manual: List[Dict[str, Any]], main_table: str) -> List[Dict[str, Any]]:
    out = []
    for r in manual or []:
        rr = dict(r)
        rr["name"] = qualify_table(rr["name"])
        rr["parent_table"] = qualify_table(rr.get("parent_table") or main_table)
        mp = rr.get("mapping") or {}
        cc = mp.get("child_columns") or []
        pc = mp.get("parent_columns") or []
        if not cc or not pc or len(cc) != len(pc):
            raise ValueError(f"Invalid mapping in relation {rr}")
        out.append(rr)
    return out


def filter_relations(relations: List[Dict[str, Any]], skip_tables: Set[str], skip_columns: Set[str]) -> List[Dict[str, Any]]:
    norm_skip_tables = set()
    for t in (skip_tables or []):
        norm_skip_tables.add(t)
        norm_skip_tables.add(qualify_table(t))
    out = []
    for r in relations:
        child_name = r["name"]
        child_short = child_name.split(".")[-1]
        if child_name in norm_skip_tables or child_short in norm_skip_tables:
            continue
        child_cols = r["mapping"]["child_columns"]
        if any(c in skip_columns for c in child_cols):
            continue
        out.append(r)
    return out


def union_relations(manual: List[Dict[str, Any]], auto: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged = []
    seen = set()
    for r in (manual or []) + (auto or []):
        key = (r["parent_table"], r["name"],
               tuple(r["mapping"]["child_columns"]), tuple(r["mapping"]["parent_columns"]))
        if key in seen:
            continue
        seen.add(key)
        merged.append(r)
    return merged


def ensure_auto_children(conn: PGConnection, relations_graph: Dict[str, List[Dict[str, Any]]],
                         table: str, skip_tables: Set[str], skip_columns: Set[str]) -> None:
    auto_rels = auto_find_fk_relations(conn, table)
    auto_rels = filter_relations(auto_rels, skip_tables, skip_columns)
    existing = relations_graph.setdefault(table, [])
    existing_keys = set(
        (r["parent_table"], r["name"],
         tuple(r["mapping"]["child_columns"]), tuple(r["mapping"]["parent_columns"]))
        for r in existing
    )
    for r in auto_rels:
        key = (r["parent_table"], r["name"],
               tuple(r["mapping"]["child_columns"]), tuple(r["mapping"]["parent_columns"]))
        if key not in existing_keys:
            existing.append(r)
            existing_keys.add(key)


# ---------- Cascade Driver ----------
def cascade_delete(conn: PGConnection,
                   current_table: str,
                   current_key_columns: List[str],
                   parent_keys: List[Tuple],
                   relations_graph: Dict[str, List[Dict[str, Any]]],
                   dry_run: bool,
                   auto_discover: bool,
                   discovered_auto: Set[str],
                   skip_tables: Set[str],
                   skip_columns: Set[str],
                   edge_path: Optional[Set[Tuple]] = None,
                   delete_totals: Optional[Dict[str, int]] = None,
                   archive: bool = False,
                   archive_buffers: Optional[Dict[str, List[Tuple]]] = None) -> None:
    if edge_path is None:
        edge_path = set()
    if delete_totals is None:
        delete_totals = {}
    if archive_buffers is None:
        archive_buffers = {}    

    if auto_discover and current_table not in discovered_auto:
        ensure_auto_children(conn, relations_graph, current_table, skip_tables, skip_columns)
        discovered_auto.add(current_table)

    # Recursively handle child relationships: First into child table and delete the child table itself
    for rel in relations_graph.get(current_table, []):
        child_table = rel["name"]
        mapping = rel["mapping"]
        conditions = rel.get("conditions")

        parent_cols = mapping["parent_columns"]
        child_cols = mapping["child_columns"]
        edge_key = (current_table, child_table, tuple(parent_cols), tuple(child_cols))
        if edge_key in edge_path:
            logging.warning(f"[CYCLE] Infinite loop detected:{current_table} -> {child_table},Skipping...")
            print(f"[CYCLE] Infinite loop detected:{current_table} -> {child_table}, Skipping...")
            continue
        edge_path.add(edge_key)

        # Generate the values of the parent column for matching the child table
        if set(parent_cols).issubset(set(current_key_columns)):
            idx = {col: i for i, col in enumerate(current_key_columns)}
            parent_keys_for_child = [tuple(pk[idx[c]] for c in parent_cols) for pk in parent_keys]
        else:
            parent_keys_for_child = fetch_needed_parent_keys(
                conn=conn,
                table=current_table,
                current_key_columns=current_key_columns,
                parent_keys=parent_keys,
                needed_columns=parent_cols,
            )
            if not parent_keys_for_child:
                print(f"[WARN] Failed to find the parent column value <{parent_cols}> for the child relation in [{current_table}], skip the child table [{child_table}]")
                logging.warning(f"[WARN] <{current_table}> is missing the parent column [{parent_cols}] required for the child relationship, skip <{child_table}>")
                edge_path.remove(edge_key)
                continue

        # Not deleting a child table at the parent level: In actual operation, first select the primary key of the child table and recursively return it to the child table
        if dry_run:
            cnt = count_child_matches(conn, child_table, mapping, parent_keys_for_child, conditions)
            print(f"[DRY-RUN] Would delete {cnt} rows from {child_table} (child of {current_table}).")
            logging.info(f"[DRY-RUN] Would delete {cnt} rows from {child_table} (child of {current_table}).")
            
            delete_totals[child_table] = delete_totals.get(child_table, 0) + cnt

            child_pks = select_child_pks(conn, child_table, mapping, parent_keys_for_child, conditions)
        else:
            child_pks = select_child_pks(conn, child_table, mapping, parent_keys_for_child, conditions)

        if child_pks:
            cs, ct = split_schema_table(child_table)
            child_pk_cols = get_primary_key_columns(conn, cs, ct) or mapping["child_columns"]
            cascade_delete(
                conn=conn,
                current_table=child_table,
                current_key_columns=child_pk_cols,
                parent_keys=child_pks,
                relations_graph=relations_graph,
                dry_run=dry_run,
                auto_discover=auto_discover,
                discovered_auto=discovered_auto,
                skip_tables=skip_tables,
                skip_columns=skip_columns,
                edge_path=edge_path,
                delete_totals=delete_totals, 
                archive=archive,
                archive_buffers=archive_buffers, 
            )

        edge_path.remove(edge_key)

    # Delete the current table itself at the end (or dry run)
    if dry_run:
        schema, tbl = split_schema_table(current_table)
        key_types = get_column_types(conn, schema, tbl, current_key_columns)
        vals_sql, vals_params = build_typed_values_clause(parent_keys, key_types)
        with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
            cols_sql = sql.SQL(",").join([sql.Identifier(c) for c in current_key_columns])
            q = sql.SQL("SELECT COUNT(*) FROM {tbl} WHERE ({cols}) IN (VALUES {vals})").format(
                tbl=sql.Identifier(schema, tbl),
                cols=cols_sql,
                vals=vals_sql,
            )
            cur.execute(q, vals_params)
            cnt = cur.fetchone()[0]
        print(f"[DRY-RUN] Would delete {cnt} rows from {current_table} (parent).")
        logging.info(f"[DRY-RUN] Would delete {cnt} rows from {current_table} (parent).")

        delete_totals[current_table] = delete_totals.get(current_table, 0) + cnt
    else:
        if archive:
            rows = select_rows_for_archive(conn, current_table, current_key_columns, parent_keys)
            if rows:
                archive_buffers.setdefault(current_table, []).extend(rows)
                
        deleted = delete_parent(conn, current_table, current_key_columns, parent_keys)
        print(f"[DELETE] {current_table}: Deleted {deleted} rows (parent).")
        logging.info(f"[DELETE] {current_table}: Deleted {deleted} rows (parent).")
        delete_totals[current_table] = delete_totals.get(current_table, 0) + deleted


# ---------- Clean One Table ----------
def clean_table(conn: PGConnection,
                conf: Dict[str, Any],
                skip_tables: Set[str],
                skip_columns: Set[str],
                dry_run: bool,
                auto_discover: bool) -> None:
    table = qualify_table(conf["name"])
    key_columns: List[str] = conf["key_columns"]
    date_col: str = conf["date_column"]
    time_out: int = conf["time_out"]

    if conf["name"] in skip_tables or date_col in skip_columns:
        logging.warning(f"[SKIP] Table '{table}' skipped due to filter rules")
        print(f"[SKIP] Table '{table}' skipped due to filter rules")
        return

    expire_days = int(conf["expire_days"])
    batch_size = int(conf["batch_size"])
    archive = bool(conf.get("archive", False))
    archive_path = conf.get("archive_path")

    manual_relations_cfg = conf.get("related", []) or []
    manual_relations = normalize_manual_relations(manual_relations_cfg, main_table=table)
    manual_relations = filter_relations(manual_relations, skip_tables, skip_columns)

    auto_relations = auto_find_fk_relations(conn, table) if auto_discover else []
    auto_relations = filter_relations(auto_relations, skip_tables, skip_columns)

    merged = union_relations(manual_relations, auto_relations)
    relations_graph: Dict[str, List[Dict[str, Any]]] = {}
    for r in merged:
        relations_graph.setdefault(r["parent_table"], []).append(r)

    cutoff_date = datetime.now() - timedelta(days=expire_days)
    total_deleted = 0
    run_delete_totals: Dict[str, int] = {}

    logging.info(f"[START] Cleaning table '{table}' before {cutoff_date}")
    print(f"[START] Cleaning table '{table}' older than {expire_days} days (before {cutoff_date})")

    while True:
        keys = fetch_batch(
            conn, table, key_columns, date_col, cutoff_date, batch_size
        )
        if not keys:
            print(f"[DONE] Table '{table}' cleaned, total deleted: {total_deleted}")
            logging.info(f"[DONE] Table '{table}' cleaned, total deleted: {total_deleted}")
            if run_delete_totals and not dry_run:
                print("[TOTAL] Per-table deletion summary:")
                for tbl, cnt in run_delete_totals.items():
                    print(f"  - {tbl}: {cnt} rows.")
                    logging.info(f"[TOTAL] {tbl}: {cnt} rows.")
            break

        discovered_auto: Set[str] = set()
        if dry_run:
            print(f"[DRY-RUN] {table}: Would delete up to {len(keys)} rows in this batch.")
            logging.info(f"[DRY-RUN] {table}: Would delete up to {len(keys)} rows in this batch.")
            
            dry_run_totals: Dict[str, int] = {}
            cascade_delete(
                conn=conn,
                current_table=table,
                current_key_columns=key_columns,
                parent_keys=keys,
                relations_graph=relations_graph,
                dry_run=True,
                auto_discover=auto_discover,
                discovered_auto=discovered_auto,
                skip_tables=skip_tables,
                skip_columns=skip_columns,
                edge_path=set(),
                delete_totals=dry_run_totals,
            )
            if dry_run_totals:
                print("[SUMMARY] Dry-run totals (per table):")
                for tbl, cnt in dry_run_totals.items():
                    print(f" - {tbl}: {cnt} rows.")
                    logging.info(f"[SUMMARY] (dry-run) {tbl}: {cnt} rows.")
            break

        try:
            batch_delete_totals: Dict[str, int] = {}
            archive_buffers: Dict[str, List[Tuple]] = {} 
            
            # The statement that temporarily raises or closes this session times out
            if time_out:
                with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
                    try:
                        cur.execute("SET LOCAL statement_timeout = %s", (f"{time_out}s",))  # or "0" to disable timeout
                    except Exception:
                        cur.execute("SET statement_timeout = %s", (f"{time_out}s",))    # or "0" to disable timeout

            # child -> parent (within a single transaction)
            cascade_delete(
                conn=conn,
                current_table=table,
                current_key_columns=key_columns,
                parent_keys=keys,
                relations_graph=relations_graph,
                dry_run=False,
                auto_discover=auto_discover,
                discovered_auto=discovered_auto,
                skip_tables=skip_tables,
                skip_columns=skip_columns,
                edge_path=set(),
                delete_totals=batch_delete_totals,
                archive=archive,
                archive_buffers=archive_buffers,
            )

            conn.commit()

            # archive until transaction is committed
            if archive and archive_path and archive_buffers:
                for tbl_name, rows in archive_buffers.items():
                    if rows:
                        archive_to_csv(rows, archive_path, tbl_name)

            total_deleted += len(keys)

            if batch_delete_totals:
                print("[SUMMARY] Per-table deletion in this batch:")
                for tbl, cnt in batch_delete_totals.items():
                    print(f"  - {tbl}: {cnt} rows.")
                    logging.info(f"[SUMMARY] {tbl}: {cnt} rows in this batch.")

                for tbl, cnt in batch_delete_totals.items():
                    run_delete_totals[tbl] = run_delete_totals.get(tbl, 0) + cnt

            print(f"[BATCH] {table}: Completed batch of {len(keys)} keys. Total deleted parents: {total_deleted}")
            logging.info(f"[BATCH] {table}: Completed batch of {len(keys)} keys. Total parents: {total_deleted}")

        except psycopg2.Error as e:
            conn.rollback()
            detail = format_pg_error(e)
            logging.error(f"[ERROR] {table} psycopg2.Error: {detail}")
            print(f"[ERROR] Rolled back transaction for '{table}': {detail}")
            break
        except Exception as e:
            conn.rollback()
            logging.error(f"[ERROR] Transaction rolled back for table '{table}': {e}")
            print(f"[ERROR] Rolled back transaction for '{table}': {e}")
            break

        time.sleep(0.2)


# ---------- Main ----------
def main():
    cfg = load_config("./config.yaml")

    db_uri = cfg["db_uri"]
    skip_tables = set(cfg.get("skip_tables", []))
    skip_columns = set(cfg.get("skip_columns", []))
    dry_run = bool(cfg.get("dry_run", True))
    auto_discover = bool(cfg.get("auto_discover_related", True))

    setup_logging(cfg.get("log_file", "./cleaner.log"))

    engine = create_engine(db_uri)
    conn: PGConnection = engine.raw_connection()
    try:
        for conf in cfg["tables"]:
            clean_table(conn, conf, skip_tables, skip_columns, dry_run, auto_discover)
    finally:
        conn.close()


if __name__ == "__main__":
    main()