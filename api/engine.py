"""TextSQL engine singleton shared across all routers.

main.py calls set_engine() on startup; routers call get_engine() per request.
Keeping this in a dedicated module avoids circular imports between main and routers.
"""

from __future__ import annotations

import os

from fastapi import HTTPException

from text2sql import TextSQL


_engine: TextSQL | None = None


def get_engine() -> TextSQL:
    if _engine is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    return _engine


def set_engine(engine: TextSQL | None) -> None:
    global _engine
    _engine = engine


def build_engine() -> TextSQL:
    db = os.environ.get("TEXT2SQL_DB")
    if not db:
        raise RuntimeError(
            "TEXT2SQL_DB is required. "
            'Example: TEXT2SQL_DB="postgresql+psycopg://user:pass@localhost/db?options=-csearch_path%3Dgold"'
        )
    return TextSQL(
        connection_string=db,
        model=os.environ.get("TEXT2SQL_MODEL") or "anthropic:claude-sonnet-4-6",
        canonical_queries=os.environ.get("TEXT2SQL_CANONICAL") or None,
        examples=os.environ.get("TEXT2SQL_SCENARIOS") or None,
        trace_file=os.environ.get("TEXT2SQL_TRACES") or None,
        metadata_hint=os.environ.get("TEXT2SQL_METADATA_HINT") or None,
        enforce_read_only=os.environ.get("TEXT2SQL_ENFORCE_RO", "false").lower() in ("1", "true", "yes"),
    )
