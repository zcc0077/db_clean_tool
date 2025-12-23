from typing import List, Tuple, Dict, Any, Set

from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from .sql_render import ErrorLoggingCursorParam
from .utils import qualify_table, split_schema_table


def auto_find_fk_relations(conn: PGConnection, parent_qualified: str, exclude_cascade: bool = True) -> List[Dict[str, Any]]:
    """
    Find all foreign key relations in a PostgreSQL database that involve a given parent table.
    
    Args:
        conn: Database connection
        parent_qualified: Parent table name (schema.table)
        exclude_cascade: If True, exclude foreign keys with CASCADE delete action to avoid redundant processing
    """
    parent_schema, parent_table = split_schema_table(parent_qualified)
    
    # Query includes confdeltype to identify CASCADE delete actions
    # confdeltype: 'a' = NO ACTION, 'r' = RESTRICT, 'c' = CASCADE, 'n' = SET NULL, 'd' = SET DEFAULT
    q = """
    SELECT
      n_child.nspname AS child_schema,
      c_child.relname AS child_table,
      array(
        SELECT a.attname 
        FROM pg_attribute a 
        WHERE a.attrelid = c_child.oid AND a.attnum = ANY(c.conkey)
        ORDER BY array_position(c.conkey, a.attnum)
      ) AS child_columns,
      array(
        SELECT a.attname 
        FROM pg_attribute a 
        WHERE a.attrelid = c_parent.oid AND a.attnum = ANY(c.confkey)
        ORDER BY array_position(c.confkey, a.attnum)
      ) AS parent_columns,
      c.confdeltype AS delete_action,
      c.conname AS constraint_name
    FROM pg_constraint c
      JOIN pg_class c_child ON c.conrelid = c_child.oid
      JOIN pg_namespace n_child ON n_child.oid = c_child.relnamespace
      JOIN pg_class c_parent ON c.confrelid = c_parent.oid
      JOIN pg_namespace n_parent ON n_parent.oid = c_parent.relnamespace
    WHERE c.contype = 'f'
      AND n_parent.nspname = %s AND c_parent.relname = %s;
    """
    with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
        cur.execute(q, (parent_schema, parent_table))
        rows = cur.fetchall()
    
    rels = []
    skipped_cascade = []
    
    for child_schema, child_table, child_cols, parent_cols, delete_action, constraint_name in rows:
        child_table_qualified = f"{child_schema}.{child_table}"
        
        # Skip tables with CASCADE delete if exclude_cascade is True
        if exclude_cascade and delete_action == 'c':
            skipped_cascade.append({
                "table": child_table_qualified,
                "constraint": constraint_name,
                "delete_action": "CASCADE"
            })
            continue
            
        rels.append({
            "name": child_table_qualified,
            "parent_table": f"{parent_schema}.{parent_table}",
            "mapping": {
                "child_columns": list(child_cols),
                "parent_columns": list(parent_cols),
            },
            "delete_action": delete_action,
            "constraint_name": constraint_name,
        })
    
    # Log skipped CASCADE relationships for transparency
    if skipped_cascade:
        import logging
        print(f"[AUTO-DISCOVER] Skipped {len(skipped_cascade)} tables with CASCADE delete rules:")
        logging.info(f"[AUTO-DISCOVER] Skipped {len(skipped_cascade)} tables with CASCADE delete rules:")
        for item in skipped_cascade:
            print(f"  - {item['table']} (constraint: {item['constraint']})")
            logging.info(f"  - {item['table']} (constraint: {item['constraint']})")
    
    return rels


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
                         table: str, skip_tables, skip_columns, exclude_cascade: bool = True) -> None:
    auto_rels = auto_find_fk_relations(conn, table, exclude_cascade=exclude_cascade)
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
            
