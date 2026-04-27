"""FastAPI app exposing the text2sql engine to the chat UI.

Runtime configuration via environment variables:

    TEXT2SQL_DB                Postgres connection string for agent_reader.
    TEXT2SQL_MODEL             "anthropic:claude-sonnet-4-6" (default).
    TEXT2SQL_CANONICAL         Path to canonical.md (optional).
    TEXT2SQL_SCENARIOS         Path to scenarios.md (optional).
    TEXT2SQL_TRACES            Path to traces JSONL (optional).
    TEXT2SQL_METADATA_HINT     Free-form hint for the system prompt
                               (e.g. "Base currency PEN; fiscal year starts Jan; tz America/Lima").
    TEXT2SQL_ENFORCE_RO        "true" to fail fast on writable connection (default true).
    ANTHROPIC_API_KEY          Required for Anthropic models.

Run:
    uvicorn api.main:app --reload --port 8000
"""

from __future__ import annotations

import io
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Iterable

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware

from text2sql import TextSQL

from api.models import AskRequest, AskResponse, HealthResponse, TraceSummary


# ---------------------------------------------------------------------------
# Engine lifecycle
# ---------------------------------------------------------------------------

_ENGINE: TextSQL | None = None


def _build_engine() -> TextSQL:
    db = os.environ.get("TEXT2SQL_DB")
    if not db:
        raise RuntimeError(
            "TEXT2SQL_DB environment variable is required. Example: "
            'TEXT2SQL_DB="postgresql://agent_reader:pwd@localhost/customer_demo?options=-csearch_path%3Dgold"'
        )

    enforce_ro = os.environ.get("TEXT2SQL_ENFORCE_RO", "true").lower() in ("1", "true", "yes")
    canonical = os.environ.get("TEXT2SQL_CANONICAL") or None
    scenarios = os.environ.get("TEXT2SQL_SCENARIOS") or None
    traces    = os.environ.get("TEXT2SQL_TRACES")    or None
    model     = os.environ.get("TEXT2SQL_MODEL")     or "anthropic:claude-sonnet-4-6"
    hint      = os.environ.get("TEXT2SQL_METADATA_HINT") or None

    return TextSQL(
        connection_string=db,
        model=model,
        canonical_queries=canonical,
        examples=scenarios,
        trace_file=traces,
        metadata_hint=hint,
        enforce_read_only=enforce_ro,
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ENGINE
    _ENGINE = _build_engine()
    yield
    _ENGINE = None


app = FastAPI(
    title="text2sql chat API",
    version="0.1.0",
    description="HTTP wrapper around TextSQL.ask() for the chat UI.",
    lifespan=lifespan,
)

# Permissive CORS — tighten for production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("TEXT2SQL_CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)


def _engine() -> TextSQL:
    if _ENGINE is None:
        raise HTTPException(status_code=503, detail="Engine not initialized")
    return _ENGINE


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    eng = _engine()
    db_ok = "ok" if eng.db.test_connection() else "fail"
    canonical_count = (
        len(eng.canonical_store.queries) if eng.canonical_store else 0
    )
    return HealthResponse(
        status="ok",
        db=db_ok,
        canonical_count=canonical_count,
        tracer_enabled=eng.tracer is not None,
    )


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest) -> AskResponse:
    eng = _engine()
    started = time.time()
    try:
        result = eng.ask(
            req.question,
            max_rows=req.max_rows,
            user_id=req.user_id,
            user_role=req.user_role,
            metadata={"session_id": req.session_id} if req.session_id else None,
        )
    except Exception as e:
        # The engine itself shouldn't raise for a bad SQL — only for
        # programmer error or DB unreachable. Surface with 500.
        raise HTTPException(status_code=500, detail=str(e))

    duration = round(time.time() - started, 3)

    # Pull canonical info from the most recent trace, if tracing is on.
    canonical_query: str | None = None
    canonical_score: float | None = None
    if eng.tracer and eng.tracer.traces:
        last = eng.tracer.traces[-1]
        canonical_query = getattr(last, "canonical_query", None)
        score = getattr(last, "canonical_score", 0.0)
        canonical_score = float(score) if score else None

    columns = list(result.data[0].keys()) if result.data else []
    truncated = req.max_rows is not None and len(result.data) >= req.max_rows

    return AskResponse(
        question=result.question,
        sql=result.sql,
        data=[_jsonify(row) for row in result.data],
        columns=columns,
        row_count=len(result.data),
        truncated=truncated,
        error=result.error,
        commentary=result.commentary,
        canonical_query=canonical_query,
        canonical_score=canonical_score,
        tool_calls_made=result.tool_calls_made,
        iterations=result.iterations,
        duration_seconds=duration,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
    )


@app.post("/ask/csv")
def ask_csv(req: AskRequest):
    """Same as /ask but streams a CSV file back instead of JSON."""
    eng = _engine()
    try:
        result = eng.ask(
            req.question,
            max_rows=req.max_rows,
            user_id=req.user_id,
            user_role=req.user_role,
            metadata={"session_id": req.session_id} if req.session_id else None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    if result.error:
        raise HTTPException(status_code=400, detail=result.error)

    # Reuse SQLResult.to_csv via an in-memory buffer
    buf = io.StringIO()
    if result.data:
        import csv
        fieldnames = list(result.data[0].keys())
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result.data)

    filename = _slugify(req.question)[:60] or "query"
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}.csv"'},
    )


@app.get("/traces", response_model=list[TraceSummary])
def traces(limit: int = Query(default=20, ge=1, le=500)) -> list[TraceSummary]:
    eng = _engine()
    if not eng.tracer:
        return []

    out: list[TraceSummary] = []
    # Most recent first
    for tr in list(eng.tracer.traces)[-limit:][::-1]:
        out.append(TraceSummary(
            question=tr.question,
            final_sql=tr.final_sql,
            success=tr.success,
            canonical_query=getattr(tr, "canonical_query", None),
            duration_seconds=tr.duration_seconds,
            user_id=getattr(tr, "user_id", None),
            user_role=getattr(tr, "user_role", None),
            started_at=tr.start_time,
        ))
    return out


@app.get("/canonical")
def canonical_list() -> dict[str, Any]:
    """List configured canonical queries — useful for the UI to surface as suggested questions."""
    eng = _engine()
    if not eng.canonical_store:
        return {"queries": []}
    return {
        "queries": [
            {
                "name": q.name,
                "aliases": q.aliases,
                "description": q.description,
            }
            for q in eng.canonical_store.queries
        ]
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jsonify(row: dict) -> dict:
    """Convert non-JSON-serializable types (datetime, Decimal) to strings."""
    import datetime as dt
    from decimal import Decimal

    out = {}
    for k, v in row.items():
        if isinstance(v, (dt.date, dt.datetime)):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            # floats are fine for the UI's purposes; if you need exact decimals,
            # change this to str(v).
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _slugify(s: str) -> str:
    out = []
    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
        elif out and out[-1] != "_":
            out.append("_")
    return "".join(out).strip("_")
