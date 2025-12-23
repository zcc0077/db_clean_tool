import logging
import os
import time
import sys
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional, Set

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PGConnection

from .sql_render import ErrorLoggingCursorParam
from .relations import ensure_auto_children, auto_find_fk_relations, normalize_manual_relations, filter_relations, union_relations
from .pg import (
    get_column_types,
    build_typed_values_clause,
    get_primary_key_columns,
    build_conditions_sql,
    fetch_batch,
)
from .archive import select_rows_for_archive, archive_to_csv
from .utils import qualify_table, split_schema_table, format_pg_error, format_duration


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
        ensure_auto_children(conn, relations_graph, current_table, skip_tables, skip_columns, exclude_cascade=True)
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

        # Not deleting a child table at the parent level: select child PKs and recurse
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


def clean_table(conn: PGConnection,
                conf: Dict[str, Any],
                skip_tables: Set[str],
                skip_columns: Set[str],
                dry_run: bool) -> None:
    table = qualify_table(conf["name"])
    key_columns: List[str] = conf["key_columns"]
    date_col: str = conf["date_column"]
    time_out: int = conf["time_out"]
    enable: bool = conf.get("enable", True)
    if not enable:
        logging.warning(f"[SKIP] Table '{table}' skipped due to disabled")
        print(f"[SKIP] Table '{table}' skipped due to disabled")
        return

    if conf["name"] in skip_tables or date_col in skip_columns:
        logging.warning(f"[SKIP] Table '{table}' skipped due to filter rules")
        print(f"[SKIP] Table '{table}' skipped due to filter rules")
        return

    auto_discover = bool(conf.get("auto_discover_related", False))
    expire_days = os.getenv('EXPIRY_DAYS')
    if expire_days is not None:
        expire_days = int(expire_days)
        print(f"✓ Using environment variable to set expire_days = {expire_days}")
    else:
        expire_days = int(conf["expire_days"]) or 45
        
    batch_size = int(conf["batch_size"])
    archive = os.getenv('ARCHIVE')
    if archive is not None:
        archive = archive.lower() in ('true', '1', 'yes', 'on')
        print(f"✓ Using environment variable to set archive = {archive}")
    else:
        archive = bool(conf.get("archive", False))
    
    archive_path = conf.get("archive_path")
    
    conditions = conf.get("conditions", []) or []
    disable_cutoff = bool(conf.get("disable_cutoff", False))

    manual_relations_cfg = conf.get("related", []) or []
    manual_relations = normalize_manual_relations(manual_relations_cfg, main_table=table)
    manual_relations = filter_relations(manual_relations, skip_tables, skip_columns)

    exclude_cascade = bool(conf.get("exclude_cascade_fk", True))
    auto_relations = auto_find_fk_relations(conn, table, exclude_cascade=exclude_cascade) if auto_discover else []
    auto_relations = filter_relations(auto_relations, skip_tables, skip_columns)

    merged = union_relations(manual_relations, auto_relations)
    relations_graph: Dict[str, List[Dict[str, Any]]] = {}
    for r in merged:
        relations_graph.setdefault(r["parent_table"], []).append(r)

    cutoff_date = None if disable_cutoff else (datetime.now() - timedelta(days=expire_days)).replace(hour=0, minute=0, second=0, microsecond=0)
    total_deleted = 0
    run_delete_totals: Dict[str, int] = {}

    logging.info(f"[START] Cleaning table '{table}' " +
                 (f"with cutoff {cutoff_date} " if cutoff_date else "without default cutoff ") +
                 (f"and {len(conditions)} parent condition(s)."))
    print(f"[START] Cleaning table '{table}' " +
          (f"older than {expire_days} days (before {cutoff_date}) " if cutoff_date else "without default cutoff ") +
          (f"with {len(conditions)} extra parent condition(s)."))

    while True:
        keys = fetch_batch(
            conn, table, key_columns, date_col, cutoff_date, batch_size, conditions=conditions
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
        
        # Temporarily set statement timeout for this session/transaction
        if time_out:
            with conn.cursor(cursor_factory=ErrorLoggingCursorParam) as cur:
                try:
                    cur.execute("SET LOCAL statement_timeout = %s", (f"{time_out}s",))
                except Exception:
                    cur.execute("SET statement_timeout = %s", (f"{time_out}s",))
                    
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
            batch_start_time = time.time()
            batch_delete_totals: Dict[str, int] = {}
            archive_buffers: Dict[str, List[Tuple]] = {}

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
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time

            if batch_delete_totals:
                print("[SUMMARY] Per-table deletion in this batch:")
                for tbl, cnt in batch_delete_totals.items():
                    print(f"  - {tbl}: {cnt} rows.")
                    logging.info(f"[SUMMARY] {tbl}: {cnt} rows in this batch.")

                for tbl, cnt in batch_delete_totals.items():
                    run_delete_totals[tbl] = run_delete_totals.get(tbl, 0) + cnt

            print(f"[BATCH] {table}: Completed batch of {len(keys)} keys in {format_duration(batch_duration)}. Total deleted parents: {total_deleted}")
            logging.info(f"[BATCH] {table}: Completed batch of {len(keys)} keys in {format_duration(batch_duration)}. Total parents: {total_deleted}")

        except psycopg2.Error as e:
            conn.rollback()
            detail = format_pg_error(e)
            logging.error(f"[ERROR] {table} psycopg2.Error: {detail}")
            print(f"[ERROR] Rolled back transaction for '{table}': {detail}")
            sys.exit(1)
            break
        except Exception as e:
            conn.rollback()
            logging.error(f"[ERROR] Transaction rolled back for table '{table}': {e}")
            print(f"[ERROR] Rolled back transaction for '{table}': {e}")
            sys.exit(1)
            break

        time.sleep(0.2)