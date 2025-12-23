import os
import time
from typing import Set

from sqlalchemy import create_engine
from psycopg2.extensions import connection as PGConnection

from .config import load_config
from .utils import setup_logging, format_duration
from .cleaner import clean_table


def main():
    cfg = load_config(os.environ.get("DB_CLEANER_CONFIG", "./config/config.yaml"))

    db_uri = cfg["db_uri"]
    skip_tables: Set[str] = set(cfg.get("skip_tables", []))
    skip_columns: Set[str] = set(cfg.get("skip_columns", []))
    dry_run = bool(cfg.get("dry_run", True))

    log_file = cfg.get("log_file", "./cleaner.log")
    log_rotate = cfg.get("log_rotate")  # dict or None
    log_console = bool(cfg.get("log_console", True))
    setup_logging(log_file, rotate=log_rotate, console=log_console)

    print(f"[INFO] Starting database cleanup process...")
    
    engine = create_engine(db_uri)
    conn: PGConnection = engine.raw_connection()
    overall_start_time = time.time()

    try:
        for conf in cfg["tables"]:
            table_start_time = time.time()
            table_name = conf.get("name", "unknown")
            
            clean_table(conn, conf, skip_tables, skip_columns, dry_run)
            
            table_end_time = time.time()
            table_duration = table_end_time - table_start_time
            print(f"[TIMING] Table '{table_name}' completed in {format_duration(table_duration)}")
    finally:
        conn.close()
        
    overall_end_time = time.time()
    overall_duration = overall_end_time - overall_start_time
    print(f"[TIMING] Total cleanup completed in {format_duration(overall_duration)}")
    

if __name__ == "__main__":
    main()