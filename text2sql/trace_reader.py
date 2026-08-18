"""Read Text2SQL traces from JSONL or framework-owned database tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from text2sql.tracing import Tracer

if TYPE_CHECKING:
    from text2sql.connection import Database


class TraceReader:
    """Storage-agnostic, read-only access to completed query traces."""

    def __init__(
        self,
        *,
        jsonl_path: str | Path | None = None,
        database: Database | None = None,
    ):
        if bool(jsonl_path) == bool(database):
            raise ValueError("Configure exactly one trace source: jsonl_path or database")
        self.jsonl_path = Path(jsonl_path) if jsonl_path else None
        self.database = database

    def source(self) -> dict[str, Any]:
        """Describe the configured source without exposing connection secrets."""
        if self.jsonl_path:
            return {
                "kind": "jsonl",
                "path": str(self.jsonl_path),
                "format": "one complete trace per line; tool_calls are nested",
            }
        return {
            "kind": "database",
            "dialect": self.database.dialect,
            "tables": ["text2sql_traces", "text2sql_tool_calls"],
            "relationship": "text2sql_tool_calls.trace_id = text2sql_traces.id",
        }

    def recent(self, limit: int = 20, failures_only: bool = False) -> list[dict]:
        """Return newest traces first, including their ordered tool calls."""
        limit = max(1, min(int(limit), 500))
        if self.jsonl_path:
            traces = Tracer.load_traces(str(self.jsonl_path))
            records = [trace.to_dict() for trace in reversed(traces)]
            if failures_only:
                records = [record for record in records if not record["success"]]
            return records[:limit]
        return self._recent_from_database(limit, failures_only)

    def summary(self, limit: int = 100) -> dict[str, Any]:
        """Return aggregate signals useful for an evidence-driven review."""
        traces = self.recent(limit=limit)
        if not traces:
            return {"total_queries": 0, "trace_source": self.source()}

        successful = [trace for trace in traces if trace.get("success")]
        total_sql_attempts = sum(int(trace.get("sql_attempts") or 0) for trace in traces)
        total_sql_errors = sum(int(trace.get("sql_errors") or 0) for trace in traces)
        successful_tokens = sum(
            int(trace.get("input_tokens") or 0) + int(trace.get("output_tokens") or 0)
            for trace in successful
        )
        return {
            "total_queries": len(traces),
            "successful_queries": len(successful),
            "success_rate": round(len(successful) / len(traces), 3),
            "total_tokens": sum(
                int(trace.get("input_tokens") or 0) + int(trace.get("output_tokens") or 0)
                for trace in traces
            ),
            "avg_tokens_per_success": round(
                successful_tokens / max(len(successful), 1), 1
            ),
            "avg_tool_calls": round(
                sum(int(trace.get("total_tool_calls") or 0) for trace in traces) / len(traces),
                1,
            ),
            "avg_schema_queries": round(
                sum(int(trace.get("schema_queries") or 0) for trace in traces) / len(traces),
                1,
            ),
            "sql_error_rate": round(total_sql_errors / max(total_sql_attempts, 1), 3),
            "trace_source": self.source(),
        }

    def _recent_from_database(self, limit: int, failures_only: bool) -> list[dict]:
        where = "WHERE success = 0" if failures_only else ""
        with self.database.engine.connect() as connection:
            rows = connection.execute(text(
                "SELECT id, question, final_sql, success, error, duration_seconds, "
                "total_tool_calls, sql_attempts, sql_errors, schema_queries, "
                "schema_backtracking_count, llm_iterations, input_tokens, "
                "output_tokens, created_at FROM text2sql_traces "
                f"{where} ORDER BY created_at DESC"
            )).mappings().fetchmany(limit)

            records = []
            for row in rows:
                record = dict(row)
                record["trace_id"] = record.pop("id")
                record["success"] = bool(record["success"])
                calls = connection.execute(text(
                    "SELECT sequence, name, arguments, result, execution_ms, "
                    "llm_think_ms, created_at FROM text2sql_tool_calls "
                    "WHERE trace_id = :trace_id ORDER BY sequence"
                ), {"trace_id": record["trace_id"]}).mappings().all()
                record["tool_calls"] = [self._tool_call(dict(call)) for call in calls]
                records.append(record)
        return records

    @staticmethod
    def _tool_call(call: dict) -> dict:
        raw_arguments = call.pop("arguments", "{}")
        try:
            call["arguments"] = json.loads(raw_arguments)
        except (json.JSONDecodeError, TypeError):
            call["arguments"] = {"raw": str(raw_arguments)}
        call["result_preview"] = call.pop("result", "")
        return call
