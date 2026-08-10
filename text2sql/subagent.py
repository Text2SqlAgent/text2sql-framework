"""Keyless workspace lifecycle for coding-assistant-hosted database agents."""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from sqlalchemy.engine import make_url

from text2sql.connection import Database
from text2sql.examples import ExampleStore
from text2sql.sandbox import MAX_ROWS_PER_QUERY
from text2sql.state import MemoryStateStore, SQLiteStateStore
from text2sql.tools import _is_read_only
from text2sql.tracing import Tracer
from text2sql.workspace import PythonWorkspace


def _normalize_sql(value: str) -> str:
    """Preserve SQL literal contents while tolerating outer whitespace/one semicolon."""
    return value.strip().rstrip(";").strip()


class ExternalAgentSession:
    """A persistent Text2SQL workspace whose reasoning is supplied by a host agent.

    Claude Code, Cursor, or another MCP client owns the model loop. This class
    owns only database capabilities, query lifecycle, verification, and traces,
    so it never needs a model provider API key.
    """

    def __init__(
        self,
        database_url: str,
        *,
        workspace_dir: str | Path | None = ".text2sql",
        trace_mode: str = "local",
        trace_file: str | Path | None = None,
        trace_database_url: str | None = None,
        trace_database_schema: str | None = None,
        examples: str | None = None,
        instructions: str | None = None,
        skills_dir: str | Path | None = None,
    ):
        if trace_mode not in {"local", "database", "off"}:
            raise ValueError("trace_mode must be 'local', 'database', or 'off'")
        source_backend = make_url(database_url).get_backend_name()
        if trace_mode == "database" and not trace_database_url and source_backend == "databricks":
            raise ValueError(
                "Databricks tracing requires trace_database_url. Use a separate "
                "supported trace database instead of writing into the source catalog."
            )

        self.db = Database(database_url)
        self.trace_db = None
        self.instructions = instructions or ""
        self._lock = threading.RLock()
        self._active_question: str | None = None
        self._history_start = 0

        if workspace_dir is None:
            state_store = MemoryStateStore()
            workspace_path = None
        else:
            workspace_path = Path(workspace_dir)
            state_store = SQLiteStateStore(workspace_path / "state.db")

        output_path = None
        trace_db = None
        database_schema = None
        if trace_mode == "local":
            output_path = str(trace_file or ((workspace_path or Path(".text2sql")) / "traces.jsonl"))
        elif trace_mode == "database":
            if trace_database_url:
                self.trace_db = Database(trace_database_url)
                trace_db = self.trace_db
                if self.trace_db.dialect == "postgresql":
                    database_schema = trace_database_schema or "text2sql"
                elif trace_database_schema:
                    raise ValueError("trace_database_schema is supported only for PostgreSQL")
            else:
                trace_db = self.db
                database_schema = trace_database_schema

        self.tracer = Tracer(
            output_path=output_path, db=trace_db, database_schema=database_schema
        ) if trace_mode != "off" else None
        example_store = ExampleStore(examples) if examples else None
        self.workspace = PythonWorkspace(
            self.db,
            state_store,
            tracer=self.tracer,
            example_store=example_store,
            allow_self_modification=False,
            skills_dir=skills_dir,
        )

    @property
    def active(self) -> bool:
        return self._active_question is not None

    def start_query(self, question: str) -> dict[str, Any]:
        """Start a host-agent query and return lightweight workspace context."""
        if not isinstance(question, str) or not question.strip():
            raise ValueError("question must be non-empty text")
        with self._lock:
            if self.active:
                self._end_trace("", False, "Superseded by a new query before finish_query")
            self._active_question = question.strip()
            self._history_start = len(self.workspace.db.query_history)
            if self.tracer:
                self.tracer.start_query(self._active_question)
            return {
                "dialect": self.db.dialect,
                "skills": self.workspace.skill_names(),
                "instructions": self.instructions,
                "message": (
                    "Workspace ready. Use run_python with db/schema/traces/skills capabilities; "
                    "test the exact final SQL with db.query(), then call finish_query()."
                ),
            }

    def run_python(self, code: str) -> str:
        """Execute one restricted persistent Python cell for the active query."""
        with self._lock:
            if not self.active:
                return "No active query. Call start_query(question) first."
            if self.tracer:
                self.tracer.record_tool_start()
            result = self.workspace.execute(code)
            if self.tracer:
                self.tracer.record_tool_call("run_python", {"code": code}, result)
            return result

    def finish_query(self, sql: str, max_rows: int = 100) -> dict[str, Any]:
        """Verify the exact tested SQL, execute a bounded result, and persist its trace."""
        with self._lock:
            if not self.active:
                return self._result(sql, [], "No active query. Call start_query(question) first.")
            if not isinstance(sql, str) or not sql.strip():
                return self._result("", [], "sql must be non-empty text")
            if not _is_read_only(sql):
                return self._result(sql, [], "Only read-only SQL is permitted")

            history = self.workspace.db.query_history
            tested = history[self._history_start:]
            if not tested or _normalize_sql(sql) != _normalize_sql(tested[-1]):
                return self._result(
                    sql,
                    [],
                    "Final SQL must exactly match the last successful db.query() in this query.",
                )

            try:
                limit = max(1, min(int(max_rows), MAX_ROWS_PER_QUERY))
                rows = self.db.execute_read_only(sql, max_rows=limit)
            except Exception as exc:
                error = f"SQL execution failed: {exc}"
                self._end_trace(sql, False, error)
                return self._result(sql, [], error)

            self._end_trace(sql, True, None)
            return self._result(sql, rows, None)

    def abort_query(self, error: str = "Host agent aborted the query") -> dict[str, Any]:
        """Finish an abandoned query as a failed trace."""
        with self._lock:
            if not self.active:
                return {"aborted": False, "error": "No active query"}
            self._end_trace("", False, str(error)[:2000])
            return {"aborted": True, "error": str(error)[:2000]}

    def recent_traces(self, limit: int = 10) -> list[dict]:
        if not self.tracer:
            return []
        return self.tracer.stored_traces(limit)

    def _end_trace(self, sql: str, success: bool, error: str | None) -> None:
        if self.tracer:
            self.tracer.end_query(sql, success, error=error)
        self._active_question = None
        self._history_start = len(self.workspace.db.query_history)

    @staticmethod
    def _result(sql: str, rows: list[dict], error: str | None) -> dict[str, Any]:
        return {
            "sql": sql,
            "data": rows,
            "error": error,
            "row_count": len(rows),
        }
