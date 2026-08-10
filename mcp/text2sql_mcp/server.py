"""MCP tools for keyless, coding-assistant-hosted Text2SQL investigation.

The default lifecycle (`start_query` -> `run_python` -> `finish_query`) uses the
MCP client's model and needs no provider API key. The legacy autonomous `query`
tool remains available and only constructs a model client when called.
"""

from __future__ import annotations

import os
import sys
import threading
import uuid
from typing import Any

from mcp.server.mcpserver import MCPServer

_sessions = {}
_trace_reader = None
_session_lock = threading.RLock()
_engine = None
_TRUTHY = {"1", "true", "yes"}
_MAX_ACTIVE_SESSIONS = 32


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in _TRUTHY


def _database_url() -> str:
    value = os.environ.get("TEXT2SQL_DATABASE_URL")
    if not value:
        raise RuntimeError(
            "TEXT2SQL_DATABASE_URL is not set. Provide a SQLAlchemy URL, "
            "for example sqlite:///analytics.db"
        )
    return value


def _new_session():
    """Construct one isolated host-agent query workspace."""
    from text2sql import ExternalAgentSession

    trace_mode = os.environ.get("TEXT2SQL_TRACE_MODE", "").strip().lower()
    if not trace_mode:
        trace_mode = "database" if _env_flag("TEXT2SQL_TRACE_TO_DB") else "local"
    return ExternalAgentSession(
        _database_url(),
        workspace_dir=os.environ.get("TEXT2SQL_WORKSPACE_DIR", ".text2sql"),
        trace_mode=trace_mode,
        trace_file=os.environ.get("TEXT2SQL_TRACE_FILE") or None,
        trace_database_url=os.environ.get("TEXT2SQL_TRACE_DATABASE_URL") or None,
        examples=os.environ.get("TEXT2SQL_EXAMPLES") or None,
        instructions=os.environ.get("TEXT2SQL_INSTRUCTIONS") or None,
    )


def _query_session(query_id: str):
    with _session_lock:
        session = _sessions.get(query_id)
    if session is None:
        raise ValueError("Unknown or completed query_id; call start_query first")
    return session


def _get_engine():
    """Lazily construct the legacy autonomous agent only if `query` is called."""
    global _engine
    if _engine is not None:
        return _engine

    from text2sql import TextSQL

    kwargs: dict[str, Any] = {}
    if model := os.environ.get("TEXT2SQL_MODEL"):
        kwargs["model"] = model
    if instructions := os.environ.get("TEXT2SQL_INSTRUCTIONS"):
        kwargs["instructions"] = instructions
    if examples := os.environ.get("TEXT2SQL_EXAMPLES"):
        kwargs["examples"] = examples
    if _env_flag("TEXT2SQL_TRACE_TO_DB"):
        kwargs["trace_to_db"] = True
    _engine = TextSQL(_database_url(), **kwargs)
    return _engine


mcp = MCPServer("text2sql")


@mcp.tool()
def start_query(question: str) -> dict:
    """Start an isolated database investigation and return its `query_id`."""
    with _session_lock:
        if len(_sessions) >= _MAX_ACTIVE_SESSIONS:
            raise RuntimeError("Too many unfinished queries; finish or abort an existing query")
        query_id = str(uuid.uuid4())
        session = _new_session()
        context = session.start_query(question)
        _sessions[query_id] = session
    return {"query_id": query_id, **context}


@mcp.tool()
def run_python(query_id: str, code: str) -> str:
    """Run restricted persistent Python for one active `query_id`.

    Available capabilities include `db.list_tables()`, `db.describe(table)`,
    `db.schema()`, read-only `db.query(sql)`, `traces.recent()`, and
    `skills.list()/read(name)`. Variables persist within the query.
    """
    return _query_session(query_id).run_python(code)


@mcp.tool()
def finish_query(query_id: str, sql: str, max_rows: int = 100) -> dict:
    """Verify final SQL for `query_id`, return rows, and persist its trace."""
    session = _query_session(query_id)
    result = session.finish_query(sql, max_rows=max_rows)
    if not session.active:
        with _session_lock:
            _sessions.pop(query_id, None)
    return result


@mcp.tool()
def abort_query(query_id: str, error: str = "Host agent aborted the query") -> dict:
    """Persist an abandoned `query_id` as a failed trace and release it."""
    session = _query_session(query_id)
    result = session.abort_query(error)
    with _session_lock:
        _sessions.pop(query_id, None)
    return result


@mcp.tool()
def recent_traces(limit: int = 10) -> list[dict]:
    """Read recent completed traces for debugging or a future improvement agent."""
    global _trace_reader
    with _session_lock:
        if _trace_reader is None:
            _trace_reader = _new_session()
        reader = _trace_reader
    return reader.recent_traces(limit)


@mcp.tool()
def query(question: str, max_rows: int = 100) -> dict:
    """Legacy autonomous query requiring a configured model provider API key."""
    result = _get_engine().ask(question, max_rows=max_rows)
    return {
        "sql": result.sql,
        "data": result.data,
        "error": result.error,
        "row_count": len(result.data),
        "tool_calls_made": result.tool_calls_made,
    }


def main() -> None:
    try:
        mcp.run()
    except Exception as exc:
        print(f"text2sql-mcp failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
