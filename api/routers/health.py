"""Health, traces, and canonical endpoints — no auth required."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query

from api.engine import get_engine

router = APIRouter(tags=["utility"])


@router.get("/health")
def health() -> dict[str, Any]:
    eng = get_engine()
    return {
        "status": "ok",
        "db": "ok" if eng.db.test_connection() else "fail",
        "canonical_count": len(eng.canonical_store.queries) if eng.canonical_store else 0,
        "tracer_enabled": eng.tracer is not None,
    }


@router.get("/traces")
def traces(limit: int = Query(default=20, ge=1, le=500)) -> list[dict[str, Any]]:
    eng = get_engine()
    if not eng.tracer:
        return []
    return [
        {
            "question": tr.question,
            "final_sql": tr.final_sql,
            "success": tr.success,
            "canonical_query": getattr(tr, "canonical_query", None),
            "duration_seconds": tr.duration_seconds,
            "user_id": getattr(tr, "user_id", None),
            "user_role": getattr(tr, "user_role", None),
            "started_at": tr.start_time,
        }
        for tr in list(eng.tracer.traces)[-limit:][::-1]
    ]


@router.get("/canonical")
def canonical_list() -> dict[str, Any]:
    eng = get_engine()
    if not eng.canonical_store:
        return {"queries": []}
    return {
        "queries": [
            {"name": q.name, "aliases": q.aliases, "description": q.description}
            for q in eng.canonical_store.queries
        ]
    }
