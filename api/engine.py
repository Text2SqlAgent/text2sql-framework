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
    semantic_cache = _build_semantic_cache()

    return TextSQL(
        connection_string=db,
        model=os.environ.get("TEXT2SQL_MODEL") or "anthropic:claude-sonnet-4-6",
        # Optional tier overrides — when set, the supervisor (model=) delegates
        # schema discovery to model_light and narrative rendering to model_heavy.
        # If both are unset, the agent runs single-tier on `model`.
        model_light=os.environ.get("TEXT2SQL_MODEL_LIGHT") or None,
        model_heavy=os.environ.get("TEXT2SQL_MODEL_HEAVY") or None,
        canonical_queries=os.environ.get("TEXT2SQL_CANONICAL") or None,
        examples=os.environ.get("TEXT2SQL_SCENARIOS") or None,
        trace_file=os.environ.get("TEXT2SQL_TRACES") or None,
        metadata_hint=os.environ.get("TEXT2SQL_METADATA_HINT") or None,
        enforce_read_only=os.environ.get("TEXT2SQL_ENFORCE_RO", "false").lower() in ("1", "true", "yes"),
        semantic_cache=semantic_cache,
    )


def _build_semantic_cache():
    """Build a SemanticCache from env vars, or return None if disabled / unavailable.

    Reads:
        TEXT2SQL_SEMANTIC_CACHE   = "true" / "1" / "yes" to enable.
        TEXT2SQL_CACHE_THRESHOLD  = cosine threshold (default 0.93).
        TEXT2SQL_CACHE_TTL        = seconds (default 3600).
        TEXT2SQL_CACHE_MAX        = max entries (default 1000).

    Returns None when disabled or when sentence-transformers isn't installed
    (the cache is opt-in via the [cache] extra).
    """
    if os.environ.get("TEXT2SQL_SEMANTIC_CACHE", "false").lower() not in ("1", "true", "yes"):
        return None

    from text2sql.semantic_cache import SemanticCache, make_default_embedder

    embedder = make_default_embedder()
    if embedder is None:
        import warnings
        warnings.warn(
            "TEXT2SQL_SEMANTIC_CACHE=true but sentence-transformers is not installed. "
            "Install via `uv pip install -e \".[cache]\"` to enable. Cache disabled.",
            stacklevel=2,
        )
        return None

    return SemanticCache(
        embedder=embedder,
        threshold=float(os.environ.get("TEXT2SQL_CACHE_THRESHOLD", "0.93")),
        ttl_seconds=int(os.environ.get("TEXT2SQL_CACHE_TTL", "3600")),
        max_entries=int(os.environ.get("TEXT2SQL_CACHE_MAX", "1000")),
    )
