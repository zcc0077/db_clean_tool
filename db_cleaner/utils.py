import logging
import os
import re
from typing import Tuple, Optional, Dict, Any

import psycopg2
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler


def setup_logging(log_file: str,
                  rotate: Optional[Dict[str, Any]] = None,
                  console: bool = True) -> None:
    """
    Configure logging with rotation.

    rotate:
      - Timed rotation (default):
        {"type": "timed", "when": "D", "interval": 1, "backup_count": 7}
      - Size rotation:
        {"type": "size", "max_bytes": 10485760, "backup_count": 10}
    console: also log to stderr if True.
    """
    # Ensure log directory exists.
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    if rotate and (rotate.get("type") == "size"):
        handler = RotatingFileHandler(
            log_file,
            maxBytes=int(rotate.get("max_bytes", 10 * 1024 * 1024)),
            backupCount=int(rotate.get("backup_count", 10)),
            encoding="utf-8",
        )
    else:
        # Default to timed rotation daily.
        when = str(rotate.get("when", "D")) if rotate else "D"
        interval = int(rotate.get("interval", 1)) if rotate else 1
        backup_count = int(rotate.get("backup_count", 7)) if rotate else 7
        handler = TimedRotatingFileHandler(
            log_file,
            when=when,
            interval=interval,
            backupCount=backup_count,
            encoding="utf-8",
            utc=True,
        )

    handler.setFormatter(fmt)
    logger.addHandler(handler)

    if console:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)


def _shorten(text: str, max_len: int = 2000) -> str:
    return (text[:max_len] + "...") if len(text) > max_len else text


def _normalize_casts(text: str) -> str:
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


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to a human-readable string with appropriate units.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string (e.g., "2.5s", "1m 30s", "1h 5m 30s")
    """
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:  # Less than 1 hour
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        if remaining_seconds < 1:
            return f"{minutes}m"
        return f"{minutes}m {remaining_seconds:.1f}s"
    else:  # 1 hour or more
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        remaining_seconds = seconds % 60
        if remaining_seconds < 1:
            if remaining_minutes == 0:
                return f"{hours}h"
            return f"{hours}h {remaining_minutes}m"
        return f"{hours}h {remaining_minutes}m {remaining_seconds:.1f}s"