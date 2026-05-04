"""Ask endpoints — wraps TextSQL.ask() with conversation persistence and auth.

POST /ask         — JSON response with SQL, data, and metadata. Sync.
POST /ask/csv     — Same query returned as a downloadable CSV file.
POST /ask/stream  — Server-Sent Events. Streams agent progress in real time
                    (tool_call → tool_result → sql → rows → narrative → done),
                    so the user sees activity while the agent loop runs
                    instead of staring at a 30-second blank screen.

Both /ask and /ask/stream accept an optional conversation_id to maintain
multi-turn history. When omitted, a new conversation is created automatically
and its ID is returned so the client can continue the thread on the next call.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import time
from typing import Any, AsyncIterator, Optional

from fastapi import APIRouter, Depends, HTTPException, Response
from langchain_core.messages import HumanMessage
from pydantic import BaseModel, Field
from sqlalchemy import select, update
from sqlalchemy.orm import Session
from sse_starlette.sse import EventSourceResponse

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


# ---------------------------------------------------------------------------
# Streaming endpoint
# ---------------------------------------------------------------------------

# Event types the client should expect from /ask/stream:
#
#   started         { conversation_id, question }
#   tool_call       { name, args }                      — agent is calling a tool
#   tool_result     { name, content_preview }           — tool returned (truncated)
#   subagent        { name, status }                    — analyst / etc. invoked
#   sql             { sql }                             — final SQL extracted
#   rows            { columns, data, row_count }        — query executed
#   commentary      { commentary }                      — narrative (post-hallucination-guard)
#   done            { duration_seconds, tool_calls_made, message_id }
#   error           { error }
#
# All payloads are JSON. The client should:
#   - Render `tool_call` as a status line ("Querying schema...", "Writing SQL...")
#   - Render `sql` in a code block as soon as it arrives
#   - Render `rows` as a table immediately
#   - Append `commentary` (which already includes the verification warning footer
#     if the analyst cited unverified numbers)
#
# Why SSE vs WebSocket: one-way streaming, plays nicely with proxies/CDNs,
# trivial reconnection. Production AI streaming (Vercel AI SDK 5, Anthropic's
# own UIs) overwhelmingly chooses SSE for this shape.

@router.post("/ask/stream")
async def ask_stream(req: AskRequest, db: Session = Depends(get_session)) -> EventSourceResponse:
    """Stream agent progress events while the SQL agent runs.

    Same input as POST /ask. Returns an SSE stream of typed events; the
    client gets a status line within milliseconds even when the full agent
    loop takes 30 seconds.
    """
    eng = _get_engine()
    conv = _get_or_create_conversation(req.conversation_id, req.user_id, req.user_role, db)
    history = _load_history(str(conv.id), db) if req.conversation_id else []

    async def event_stream() -> AsyncIterator[dict]:
        started = time.time()
        yield _sse("started", {
            "conversation_id": str(conv.id),
            "question": req.question,
        })

        # Try canonical / semantic-cache first via the same path /ask uses.
        # These short-circuit without touching the LLM, so streaming them as
        # tool_call/sql/rows/done back-to-back gives the same client UX as a
        # full agent run — just faster.
        try:
            short_circuit_result = _maybe_short_circuit(
                eng, req.question, req.max_rows, req.user_id, req.user_role,
            )
        except Exception as e:
            yield _sse("error", {"error": str(e)})
            return

        if short_circuit_result is not None:
            duration = round(time.time() - started, 3)
            yield _sse("sql", {"sql": short_circuit_result.sql})
            yield _sse("rows", _rows_payload(short_circuit_result.data, req.max_rows))
            if short_circuit_result.commentary:
                yield _sse("commentary", {"commentary": short_circuit_result.commentary})
            assistant_msg = _persist_exchange(conv, req.question, short_circuit_result, duration, db)
            yield _sse("done", {
                "duration_seconds": duration,
                "tool_calls_made": 0,
                "message_id": str(assistant_msg.id),
                "via": "canonical_or_cache",
            })
            return

        # Full agent path. Stream graph updates as they happen.
        messages_in = list(history) + [{"role": "user", "content": req.question}]
        lc_messages = [HumanMessage(content=m["content"]) for m in messages_in if m.get("role") == "user"]

        collected_messages: list[Any] = []
        underlying_graph = eng.generator.agent.agent  # CompiledStateGraph
        seen_msg_ids: set[str] = set()
        tool_calls_made = 0

        try:
            async for chunk in underlying_graph.astream(
                {"messages": lc_messages},
                config={"recursion_limit": 50},
                stream_mode="updates",
            ):
                # chunk is a dict {node_name: {messages: [...]}}
                for _node_name, node_update in chunk.items():
                    if not isinstance(node_update, dict):
                        continue
                    for msg in node_update.get("messages", []) or []:
                        # Deduplicate (some graph nodes echo prior messages)
                        msg_id = getattr(msg, "id", None) or id(msg)
                        if msg_id in seen_msg_ids:
                            continue
                        seen_msg_ids.add(msg_id)
                        collected_messages.append(msg)

                        for evt in _msg_to_events(msg):
                            if evt["event"] == "tool_call":
                                tool_calls_made += 1
                            yield _sse(evt["event"], evt["data"])

        except Exception as e:
            yield _sse("error", {"error": f"Agent stream failed: {e}"})
            return

        # Stream is done — parse the final result (extracts SQL + executes).
        # Run in a thread since _parse_result and DB exec are blocking.
        try:
            final_result = await asyncio.to_thread(
                eng.generator._parse_result,
                req.question,
                collected_messages,
                req.max_rows,
            )
        except Exception as e:
            yield _sse("error", {"error": f"Result parsing failed: {e}"})
            return

        if final_result.error:
            yield _sse("error", {"error": final_result.error, "sql": final_result.sql})
            return

        yield _sse("sql", {"sql": final_result.sql})
        yield _sse("rows", _rows_payload(final_result.data, req.max_rows))
        if final_result.commentary:
            yield _sse("commentary", {"commentary": final_result.commentary})

        # Persist the exchange (mirrors what /ask does).
        duration = round(time.time() - started, 3)
        assistant_msg = await asyncio.to_thread(
            _persist_exchange, conv, req.question, final_result, duration, db,
        )

        yield _sse("done", {
            "duration_seconds": duration,
            "tool_calls_made": tool_calls_made or final_result.tool_calls_made,
            "message_id": str(assistant_msg.id),
            "input_tokens": final_result.input_tokens,
            "output_tokens": final_result.output_tokens,
        })

    return EventSourceResponse(event_stream())


# ---------------------------------------------------------------------------
# Streaming helpers
# ---------------------------------------------------------------------------

def _sse(event_name: str, payload: dict) -> dict:
    """Wrap a payload as an sse-starlette dict event."""
    return {"event": event_name, "data": json.dumps(payload, default=str)}


def _rows_payload(data: list[dict], max_rows: int) -> dict:
    """Standard rows event payload — column names + jsonified rows + count."""
    columns = list(data[0].keys()) if data else []
    return {
        "columns": columns,
        "data": [_jsonify(r) for r in data],
        "row_count": len(data),
        "truncated": len(data) >= max_rows,
    }


def _msg_to_events(msg: Any) -> list[dict]:
    """Map a single LangGraph message into the typed SSE events the client expects.

    AI message with tool_calls   → one tool_call event per call (or subagent)
    Tool message (result)        → tool_result event with truncated content
    Other message types          → no event (kept in collected_messages for parsing)
    """
    events: list[dict] = []

    if hasattr(msg, "tool_calls") and msg.tool_calls:
        for tc in msg.tool_calls:
            name = tc.get("name", "")
            args = tc.get("args", {})
            # Distinguish subagent dispatch (the 'task' tool) from regular tool calls.
            if name == "task":
                subagent_type = args.get("subagent_type", "?")
                events.append({
                    "event": "subagent",
                    "data": {"name": subagent_type, "status": "invoked"},
                })
            else:
                # Truncate args for transit; the full args are visible in the trace.
                args_preview = {k: (str(v)[:200] + ("…" if len(str(v)) > 200 else "")) for k, v in args.items()}
                events.append({
                    "event": "tool_call",
                    "data": {"name": name, "args": args_preview},
                })
        return events

    # ToolMessage — has .name and .tool_call_id
    if hasattr(msg, "name") and hasattr(msg, "tool_call_id") and msg.name:
        content = str(msg.content) if not isinstance(msg.content, str) else msg.content
        preview = content[:300] + ("…" if len(content) > 300 else "")
        events.append({
            "event": "tool_result",
            "data": {"name": msg.name, "content_preview": preview},
        })

    return events


def _maybe_short_circuit(eng, question: str, max_rows: int,
                         user_id: Optional[str], user_role: Optional[str]):
    """If the question hits a canonical or semantic-cache, return its SQLResult; else None.

    We can't introspect the chain inside TextSQL.ask() without modifying it,
    so we re-run the canonical/cache lookups here ourselves. The agent path
    is the slow one — it's the one we want to stream — so re-running the
    cheap lookups twice is fine.
    """
    if eng.canonical_store is not None:
        match = eng.canonical_store.match(question)
        if match is not None:
            return eng._run_canonical(
                question=question, match=match, max_rows=max_rows,
                user_id=user_id, user_role=user_role, metadata=None,
            )
    if eng.semantic_cache is not None:
        cached = eng.semantic_cache.lookup(question)
        if cached is not None:
            return eng._run_cached(
                question=question, cached=cached, max_rows=max_rows,
                user_id=user_id, user_role=user_role, metadata=None,
            )
    return None
