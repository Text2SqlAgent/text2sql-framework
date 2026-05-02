"""Ask endpoints — wraps TextSQL.ask() with conversation persistence and auth.

POST /ask        — JSON response with SQL, data, and metadata.
POST /ask/csv    — Same query returned as a downloadable CSV file.

Both endpoints accept an optional conversation_id to maintain multi-turn
history. When omitted, a new conversation is created automatically and its
ID is returned so the client can continue the thread on the next call.
"""

from __future__ import annotations

import csv
import io
import time
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from api.auth import require_api_key
from api.db import Conversation, Message, get_session

router = APIRouter(tags=["ask"], dependencies=[Depends(require_api_key)])

# Max number of prior message pairs to include as history context.
_HISTORY_PAIRS = 10


# ---------------------------------------------------------------------------
# Request / response models (ask-specific extensions of the base models)
# ---------------------------------------------------------------------------

class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    conversation_id: Optional[str] = None   # null → create new conversation
    user_id: Optional[str] = None
    user_role: Optional[str] = None
    max_rows: int = Field(default=200, ge=1, le=10_000)


class AskResponse(BaseModel):
    conversation_id: str
    message_id: str
    question: str
    sql: str
    data: list[dict[str, Any]]
    columns: list[str]
    row_count: int
    truncated: bool
    error: Optional[str]
    commentary: str
    canonical_query: Optional[str]
    canonical_score: Optional[float]
    tool_calls_made: int
    iterations: int
    duration_seconds: float
    input_tokens: int
    output_tokens: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_engine():
    from api.engine import get_engine
    return get_engine()


def _jsonify(row: dict) -> dict:
    import datetime as dt
    from decimal import Decimal
    out = {}
    for k, v in row.items():
        if isinstance(v, (dt.date, dt.datetime)):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
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


def _load_history(conversation_id: str, db: Session) -> list[dict]:
    """Return the last N message pairs as a list of role/content dicts."""
    msgs = db.scalars(
        select(Message)
        .where(Message.conversation_id == conversation_id)
        .order_by(Message.created_at.desc())
        .limit(_HISTORY_PAIRS * 2)  # user + assistant per pair
    ).all()

    # Reverse to chronological order
    msgs = list(reversed(msgs))
    history = []
    for m in msgs:
        content = m.content
        if m.role == "assistant" and m.sql_generated:
            content = f"{m.content}\n\n```sql\n{m.sql_generated}\n```".strip()
        history.append({"role": m.role, "content": content})
    return history


def _get_or_create_conversation(
    conversation_id: Optional[str],
    user_id: Optional[str],
    user_role: Optional[str],
    db: Session,
) -> Conversation:
    if conversation_id:
        conv = db.scalar(select(Conversation).where(Conversation.id == conversation_id))
        if conv is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        return conv

    conv = Conversation(user_id=user_id, user_role=user_role)
    db.add(conv)
    db.commit()
    db.refresh(conv)
    return conv


def _persist_exchange(
    conv: Conversation,
    question: str,
    result,
    duration: float,
    db: Session,
) -> Message:
    """Write the user message + assistant message rows; update conversation metadata."""
    is_canonical = bool(result.commentary and result.commentary.startswith("[canonical:"))

    # User message
    user_msg = Message(
        conversation_id=conv.id,
        role="user",
        content=question,
    )
    db.add(user_msg)

    # Assistant message — content is commentary; SQL stored separately
    full_content = result.commentary or ""
    assistant_msg = Message(
        conversation_id=conv.id,
        role="assistant",
        content=full_content,
        sql_generated=result.sql or None,
        row_count=len(result.data),
        tool_calls_made=result.tool_calls_made,
        iterations=result.iterations,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        duration_seconds=duration,
        is_canonical=is_canonical,
        error=result.error,
    )
    db.add(assistant_msg)

    # Auto-title the conversation from the first question (truncated)
    if conv.title is None:
        conv.title = question[:100]

    # Touch updated_at
    db.execute(
        update(Conversation)
        .where(Conversation.id == conv.id)
        .values(updated_at=__import__("sqlalchemy").func.now(), title=conv.title)
    )
    db.commit()
    db.refresh(assistant_msg)
    return assistant_msg


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/ask", response_model=AskResponse)
def ask(req: AskRequest, db: Session = Depends(get_session)) -> AskResponse:
    eng = _get_engine()
    conv = _get_or_create_conversation(req.conversation_id, req.user_id, req.user_role, db)

    history = _load_history(str(conv.id), db) if req.conversation_id else []

    started = time.time()
    try:
        result = eng.ask(
            req.question,
            max_rows=req.max_rows,
            user_id=req.user_id,
            user_role=req.user_role,
            message_history=history or None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    duration = round(time.time() - started, 3)

    assistant_msg = _persist_exchange(conv, req.question, result, duration, db)

    canonical_query: Optional[str] = None
    canonical_score: Optional[float] = None
    if eng.tracer and eng.tracer.traces:
        last = eng.tracer.traces[-1]
        canonical_query = getattr(last, "canonical_query", None)
        score = getattr(last, "canonical_score", 0.0)
        canonical_score = float(score) if score else None

    columns = list(result.data[0].keys()) if result.data else []
    truncated = len(result.data) >= req.max_rows

    return AskResponse(
        conversation_id=str(conv.id),
        message_id=str(assistant_msg.id),
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


@router.post("/ask/csv")
def ask_csv(req: AskRequest, db: Session = Depends(get_session)) -> Response:
    """Same as POST /ask but returns a downloadable CSV file."""
    eng = _get_engine()
    conv = _get_or_create_conversation(req.conversation_id, req.user_id, req.user_role, db)
    history = _load_history(str(conv.id), db) if req.conversation_id else []

    started = time.time()
    try:
        result = eng.ask(
            req.question,
            max_rows=req.max_rows,
            user_id=req.user_id,
            user_role=req.user_role,
            message_history=history or None,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    duration = round(time.time() - started, 3)

    if result.error:
        raise HTTPException(status_code=400, detail=result.error)

    _persist_exchange(conv, req.question, result, duration, db)

    buf = io.StringIO()
    if result.data:
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
