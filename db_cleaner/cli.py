import os
from typing import Set

from sqlalchemy import create_engine
from psycopg2.extensions import connection as PGConnection

from .config import load_config
from .utils import setup_logging
from .cleaner import clean_table


def main():
    cfg = load_config(os.environ.get("DB_CLEANER_CONFIG", "./config/config.yaml"))

    db_uri = cfg["db_uri"]
    skip_tables: Set[str] = set(cfg.get("skip_tables", []))
    skip_columns: Set[str] = set(cfg.get("skip_columns", []))
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