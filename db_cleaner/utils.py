import logging
import re
from typing import List, Tuple, Dict, Any, Optional

import psycopg2


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


def qualify_table(name: str) -> str:
    if "." in name:
        return name
    return f"public.{name}"


def split_schema_table(qualified: str) -> Tuple[str, str]:
    if "." in qualified:
        s, t = qualified.split(".", 1)
        return s, t
    return "public", qualified


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