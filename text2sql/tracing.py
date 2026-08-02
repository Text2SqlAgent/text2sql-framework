"""Tracing — captures the full agentic loop for analysis.

Records every tool call, its result, and whether the LLM actually used it.
This data powers the paid analytics layer:

- Example scenario effectiveness: how often is each scenario looked up vs. actually
  used in the final SQL? Are scenarios mislabeled? Missing?
- Schema exploration patterns: how many catalog queries before writing SQL?
  Does the LLM struggle to find the right tables?
- SQL retry rate: how often does the first attempt fail? What errors are common?
- Tool call efficiency: total calls per question, wasted calls, etc.
- Search term mismatches: LLM searched "revenue" but column is "amt_ttl_grs"
- Join failures: LLM couldn't find join paths between tables
"""

from __future__ import annotations

import json
import re
import time
import uuid
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, TYPE_CHECKING

from sqlalchemy import text as _sql_text

if TYPE_CHECKING:
    from text2sql.connection import Database

logger = logging.getLogger(__name__)


@dataclass
class ToolCallTrace:
    """A single tool call within a query trace."""
    name: str
    arguments: dict
    result_preview: str
    timestamp: float = 0.0
    execution_ms: float = 0.0   # how long the tool took to execute
    llm_think_ms: float = 0.0   # how long the LLM spent thinking before this call


@dataclass
class ExampleUsage:
    """Tracks whether a looked-up example was actually used."""
    scenario_queried: str
    content_returned: str  # first 300 chars
    used_in_final_sql: bool = False  # did key terms from the example appear in the final SQL?


@dataclass
class SearchVsFound:
    """Tracks what the LLM searched for vs. what it found and used."""
    search_term: str
    columns_returned: List[str] = field(default_factory=list)
    column_used_in_sql: str = ""


@dataclass
class JoinAttempt:
    """Tracks a join attempt between two tables."""
    left_table: str
    right_table: str
    join_condition: str
    success: bool = True
    error_message: str = ""


@dataclass
class StructuredSQLError:
    """Parsed SQL error with type classification."""
    error_type: str  # column_not_found, table_not_found, ambiguous, syntax, other
    raw_message: str
    sql_attempted: str = ""


@dataclass
class QueryTrace:
    """Full trace of a single question -> SQL flow."""
    question: str
    final_sql: str
    success: bool
    error: Optional[str] = None

    # Timing
    start_time: float = 0.0
    end_time: float = 0.0
    duration_seconds: float = 0.0

    # Loop stats
    total_tool_calls: int = 0
    llm_iterations: int = 0
    sql_attempts: int = 0  # how many execute_sql calls
    sql_errors: int = 0    # how many returned SQL Error

    # Schema exploration
    schema_queries: int = 0  # execute_sql calls that query information_schema / metadata

    # Example usage
    example_lookups: List[ExampleUsage] = field(default_factory=list)

    # Raw tool calls
    tool_calls: List[ToolCallTrace] = field(default_factory=list)

    # === Enriched fields (Phase 1) ===

    # What keywords the LLM searched for in schema exploration
    search_terms_used: List[str] = field(default_factory=list)

    # Search term → columns returned → column used in final SQL
    columns_searched_vs_found: List[SearchVsFound] = field(default_factory=list)

    # Join attempts with success/fail
    join_attempts: List[JoinAttempt] = field(default_factory=list)

    # Parsed SQL errors
    sql_errors_structured: List[StructuredSQLError] = field(default_factory=list)

    # How many times the LLM went back to metadata after starting SQL
    schema_backtracking_count: int = 0

    # LLM reasoning text between tool calls
    llm_reasoning_steps: List[str] = field(default_factory=list)

    # Token usage
    input_tokens: int = 0
    output_tokens: int = 0

    def to_dict(self):
        return asdict(self)


def _is_schema_query(sql):
    # type: (str) -> bool
    """Heuristic: is this SQL querying the metadata catalog?"""
    sql_upper = sql.upper()
    indicators = [
        "INFORMATION_SCHEMA",
        "PG_CATALOG",
        "PG_DESCRIPTION",
        "SQLITE_MASTER",
        "SYS.TABLES",
        "SYS.COLUMNS",
        "TABLE_INFO",
        "PRAGMA",
        "SHOW TABLES",
        "SHOW COLUMNS",
        "DESCRIBE ",
    ]
    return any(ind in sql_upper for ind in indicators)


def _example_was_used(example_content, final_sql):
    # type: (str, str) -> bool
    """Heuristic: did the LLM actually use guidance from the example in its SQL?

    Checks if table/column names mentioned in the example appear in the final SQL.
    """
    if not final_sql or not example_content:
        return False

    # Extract words that look like table/column identifiers from the example
    # (lowercase alphanumeric + underscore, at least 3 chars)
    identifiers = set(re.findall(r'\b([a-z][a-z0-9_]{2,})\b', example_content.lower()))
    sql_lower = final_sql.lower()

    # Filter to identifiers that are likely table/column names (not common English words)
    common_words = {
        "the", "and", "for", "from", "with", "that", "this", "are", "not",
        "join", "left", "right", "inner", "outer", "where", "group", "order",
        "select", "insert", "update", "delete", "into", "values", "table",
        "create", "drop", "alter", "null", "true", "false", "between",
        "like", "having", "limit", "offset", "union", "distinct", "case",
        "when", "then", "else", "end", "sum", "count", "avg", "min", "max",
        "stored", "across", "tables", "always", "minus", "query", "use",
        "should", "need", "column", "columns",
    }
    identifiers -= common_words

    if not identifiers:
        return False

    matches = sum(1 for ident in identifiers if ident in sql_lower)
    # If at least 30% of the example's identifiers appear in the SQL, consider it used
    return matches / len(identifiers) >= 0.3 if identifiers else False


def _classify_sql_error(error_message):
    # type: (str) -> str
    """Classify a SQL error message into a category."""
    msg = error_message.lower()
    if any(x in msg for x in ["no such column", "column", "unknown column", "not found"]):
        if "table" in msg:
            return "table_not_found"
        return "column_not_found"
    if any(x in msg for x in ["no such table", "table", "relation", "doesn't exist"]):
        return "table_not_found"
    if "ambiguous" in msg:
        return "ambiguous"
    if any(x in msg for x in ["syntax", "parse", "unexpected"]):
        return "syntax"
    return "other"


def _extract_search_terms(sql):
    # type: (str) -> List[str]
    """Extract search terms from LIKE/ILIKE clauses in schema queries."""
    terms = []
    # Match LIKE '%term%' or ILIKE '%term%' patterns
    for match in re.finditer(r"(?:I?LIKE)\s+'%([^%']+)%'", sql, re.IGNORECASE):
        terms.append(match.group(1).lower())
    # Match = 'term' patterns in WHERE clauses on column_name/table_name
    for match in re.finditer(
        r"(?:column_name|table_name)\s*=\s*'([^']+)'", sql, re.IGNORECASE
    ):
        terms.append(match.group(1).lower())
    return terms


def _extract_join_info(sql):
    # type: (str) -> List[JoinAttempt]
    """Extract JOIN clauses from SQL."""
    joins = []
    # Match JOIN table ON condition
    for match in re.finditer(
        r"JOIN\s+(\w+)\s+(?:\w+\s+)?ON\s+(\w+\.\w+\s*=\s*\w+\.\w+)",
        sql, re.IGNORECASE,
    ):
        right_table = match.group(1)
        condition = match.group(2)
        # Try to extract left table from the condition
        parts = condition.split("=")
        left_table = parts[0].strip().split(".")[0] if "." in parts[0] else ""
        joins.append(JoinAttempt(
            left_table=left_table,
            right_table=right_table,
            join_condition=condition.strip(),
        ))
    return joins


def _extract_columns_from_result(result_preview):
    # type: (str) -> List[str]
    """Extract column names from a schema query result."""
    columns = []
    lines = result_preview.strip().split("\n")
    if len(lines) < 3:
        return columns
    # Look for column_name or name in headers
    headers = [h.strip().lower() for h in lines[0].split("|")]
    col_idx = None
    for i, h in enumerate(headers):
        if h in ("column_name", "name", "column"):
            col_idx = i
            break
    if col_idx is None:
        return columns
    for line in lines[2:]:
        if line.startswith("(") or line.startswith("..."):
            break
        values = [v.strip() for v in line.split("|")]
        if col_idx < len(values):
            columns.append(values[col_idx])
    return columns


# ---------------------------------------------------------------------------
# Database trace sink
#
# Deterministic framework-owned write path. This is NOT reachable from the
# agent loop: the LLM only ever sees `tools.execute_sql`, which is gated by
# `_is_read_only`. These statements are fixed strings with bound parameters,
# executed directly against the SQLAlchemy engine — the model cannot influence
# the SQL text, the table names, or whether this runs at all.
#
# DDL is intentionally lowest-common-denominator so the exact same statement
# works on SQLite, PostgreSQL and MySQL: no SERIAL/AUTOINCREMENT, no
# DEFAULT now(), no dialect-specific types. `id`/`created_at` are generated in
# Python. `success` is stored as 1/0 in an INTEGER for the same reason.
# ---------------------------------------------------------------------------

_CREATE_TRACES_TABLE = """
CREATE TABLE IF NOT EXISTS text2sql_traces (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    question TEXT,
    final_sql TEXT,
    success INTEGER,
    error TEXT,
    duration_seconds REAL,
    total_tool_calls INTEGER,
    sql_attempts INTEGER,
    sql_errors INTEGER,
    schema_queries INTEGER,
    schema_backtracking_count INTEGER,
    llm_iterations INTEGER,
    input_tokens INTEGER,
    output_tokens INTEGER,
    created_at VARCHAR(32)
)
"""

_CREATE_TOOL_CALLS_TABLE = """
CREATE TABLE IF NOT EXISTS text2sql_tool_calls (
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    trace_id VARCHAR(36),
    sequence INTEGER,
    name VARCHAR(255),
    arguments TEXT,
    result TEXT,
    execution_ms REAL,
    llm_think_ms REAL,
    created_at VARCHAR(32)
)
"""

# Best-effort: MySQL has no CREATE INDEX IF NOT EXISTS. Failure here is
# non-fatal — the index is a lookup optimization, not a correctness need.
_CREATE_TOOL_CALLS_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_text2sql_tool_calls_trace_id "
    "ON text2sql_tool_calls (trace_id)"
)

_INSERT_TRACE = """
INSERT INTO text2sql_traces (
    id, question, final_sql, success, error, duration_seconds,
    total_tool_calls, sql_attempts, sql_errors, schema_queries,
    schema_backtracking_count, llm_iterations, input_tokens, output_tokens,
    created_at
) VALUES (
    :id, :question, :final_sql, :success, :error, :duration_seconds,
    :total_tool_calls, :sql_attempts, :sql_errors, :schema_queries,
    :schema_backtracking_count, :llm_iterations, :input_tokens, :output_tokens,
    :created_at
)
"""

_INSERT_TOOL_CALL = """
INSERT INTO text2sql_tool_calls (
    id, trace_id, sequence, name, arguments, result,
    execution_ms, llm_think_ms, created_at
) VALUES (
    :id, :trace_id, :sequence, :name, :arguments, :result,
    :execution_ms, :llm_think_ms, :created_at
)
"""


def _utc_now_iso():
    # type: () -> str
    """Timestamp generated in Python so the DDL needs no DEFAULT clause."""
    return datetime.now(timezone.utc).isoformat()


class Tracer:
    """Collects traces and optionally writes them to a JSONL file.

    Supports auto-sync to a remote dashboard via ``api_key``, and persistence
    into the user's own database via ``db``.

    The three sinks (file, dashboard, database) are independent and freely
    combinable — enabling one never disables another.
    """

    def __init__(self, output_path=None, api_key=None, api_url=None, batch_size=5, db=None):
        # type: (Optional[str], Optional[str], Optional[str], int, Optional[Database]) -> None
        self.traces = []  # type: List[QueryTrace]
        self.output_path = Path(output_path) if output_path else None
        self._current = None  # type: Optional[QueryTrace]

        # Dashboard sync
        self._api_key = api_key
        self._api_url = (api_url or "https://text2sql-dashboard.vercel.app").rstrip("/")
        self._batch_size = batch_size
        self._sync_buffer = []  # type: List[dict]
        self._http_client = None  # lazy-init

        # Database sink — reuses the caller's existing Database/engine. Tables
        # are created lazily on the first write, never in __init__.
        self._db = db
        self._db_enabled = db is not None
        self._db_tables_ready = False

        # Timing state for per-step latency tracking
        self._last_event_time = 0.0  # when the last tool result came back (or query started)
        self._tool_start_time = 0.0  # when the current tool started executing

    def start_query(self, question):
        # type: (str) -> None
        """Start tracing a new query."""
        now = time.time()
        self._current = QueryTrace(
            question=question,
            final_sql="",
            success=False,
            start_time=now,
        )
        self._last_event_time = now
        self._tool_start_time = 0.0

    def record_tool_start(self):
        # type: () -> None
        """Mark the start of a tool execution. Call before the tool runs."""
        self._tool_start_time = time.time()

    def record_tool_call(self, name, arguments, result):
        # type: (str, dict, str) -> None
        """Record a tool call with timing. Call after the tool returns."""
        if not self._current:
            return

        now = time.time()

        # Compute execution time (tool_start → now)
        execution_ms = 0.0
        if self._tool_start_time > 0:
            execution_ms = round((now - self._tool_start_time) * 1000, 1)

        # Compute LLM think time (last_event → tool_start or now)
        llm_think_ms = 0.0
        if self._last_event_time > 0:
            think_end = self._tool_start_time if self._tool_start_time > 0 else now
            llm_think_ms = round((think_end - self._last_event_time) * 1000, 1)

        self._current.tool_calls.append(ToolCallTrace(
            name=name,
            arguments=arguments,
            result_preview=result,
            timestamp=now,
            execution_ms=execution_ms,
            llm_think_ms=llm_think_ms,
        ))
        self._current.total_tool_calls += 1

        # Reset timing state
        self._last_event_time = now
        self._tool_start_time = 0.0

        if name == "execute_sql":
            sql = arguments.get("sql", "")
            self._current.sql_attempts += 1

            if "SQL Error" in result:
                self._current.sql_errors += 1
                self._current.sql_errors_structured.append(StructuredSQLError(
                    error_type=_classify_sql_error(result),
                    raw_message=result[:300],
                    sql_attempted=sql,
                ))
                # Check for failed joins in the SQL
                for join in _extract_join_info(sql):
                    join.success = False
                    join.error_message = result[:200]
                    self._current.join_attempts.append(join)

            if _is_schema_query(sql):
                self._current.schema_queries += 1
                # Extract search terms
                terms = _extract_search_terms(sql)
                self._current.search_terms_used.extend(terms)
                # Extract column names from result
                columns_found = _extract_columns_from_result(result)
                for term in terms:
                    self._current.columns_searched_vs_found.append(SearchVsFound(
                        search_term=term,
                        columns_returned=columns_found,
                    ))
            else:
                # Non-schema SQL after we've already done schema queries = potential backtracking
                # if followed by another schema query later, we'll count it
                pass

        elif name == "lookup_example":
            self._current.example_lookups.append(ExampleUsage(
                scenario_queried=arguments.get("scenario", ""),
                content_returned=result[:300],
            ))

    def record_token_usage(self, input_tokens, output_tokens):
        # type: (int, int) -> None
        """Accumulate token usage from an LLM response."""
        if not self._current:
            return
        self._current.input_tokens += input_tokens
        self._current.output_tokens += output_tokens

    def record_reasoning(self, text):
        # type: (str) -> None
        """Record LLM reasoning text (AIMessage content between tool calls)."""
        if not self._current or not text or not text.strip():
            return
        self._current.llm_reasoning_steps.append(text.strip())

    def end_query(self, sql, success, error=None, iterations=0):
        # type: (str, bool, Optional[str], int) -> QueryTrace
        """Finish tracing a query and return the trace."""
        if not self._current:
            return QueryTrace(question="", final_sql=sql, success=success, error=error)

        self._current.final_sql = sql
        self._current.success = success
        self._current.error = error
        self._current.llm_iterations = iterations
        self._current.end_time = time.time()
        self._current.duration_seconds = round(
            self._current.end_time - self._current.start_time, 2
        )

        # Analyze example usage
        for ex in self._current.example_lookups:
            ex.used_in_final_sql = _example_was_used(ex.content_returned, sql)

        # Fill in column_used_in_sql for search-vs-found entries
        if sql:
            sql_lower = sql.lower()
            for svf in self._current.columns_searched_vs_found:
                for col in svf.columns_returned:
                    if col.lower() in sql_lower:
                        svf.column_used_in_sql = col
                        break

        # Extract successful joins from final SQL
        if sql:
            for join in _extract_join_info(sql):
                # Only add if not already recorded as a failure
                existing = {
                    (j.left_table.lower(), j.right_table.lower())
                    for j in self._current.join_attempts
                }
                key = (join.left_table.lower(), join.right_table.lower())
                if key not in existing:
                    self._current.join_attempts.append(join)

        # Count schema backtracking: schema query that comes after a non-schema SQL attempt
        self._current.schema_backtracking_count = _count_backtracking(
            self._current.tool_calls
        )

        trace = self._current
        self.traces.append(trace)
        self._current = None

        # Write to file if configured
        if self.output_path:
            self._write_trace(trace)

        # Write to the user's own database if configured
        if self._db_enabled:
            self._write_trace_to_db(trace)

        # Buffer for dashboard sync
        if self._api_key:
            self._sync_buffer.append(trace.to_dict())
            if len(self._sync_buffer) >= self._batch_size:
                self._flush_sync()

        return trace

    def _write_trace(self, trace):
        # type: (QueryTrace) -> None
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "a") as f:
            f.write(json.dumps(trace.to_dict(), default=str) + "\n")

    # --- Database sink -----------------------------------------------------

    def _disable_db_tracing(self, what, exc):
        # type: (str, Exception) -> None
        """Log once and permanently stop trying to write traces to the DB.

        Tracing is observability, never a hard dependency of the query loop —
        a read-only role, a missing grant or a dialect quirk must degrade to a
        single warning, not an exception and not a retry on every query.
        """
        if self._db_enabled:
            logger.warning(
                "Database tracing disabled — %s: %s. "
                "Traces will not be written to text2sql_traces / text2sql_tool_calls.",
                what,
                exc,
            )
        self._db_enabled = False

    def _ensure_db_tables(self):
        # type: () -> bool
        """Create the trace tables on first write. Idempotent."""
        if self._db_tables_ready:
            return True
        if not self._db_enabled or self._db is None:
            return False

        try:
            # .begin() (not .connect()) — DDL/DML needs a committed transaction.
            with self._db.engine.begin() as conn:
                conn.execute(_sql_text(_CREATE_TRACES_TABLE))
                conn.execute(_sql_text(_CREATE_TOOL_CALLS_TABLE))
        except Exception as exc:
            self._disable_db_tracing("could not create trace tables", exc)
            return False

        try:
            with self._db.engine.begin() as conn:
                conn.execute(_sql_text(_CREATE_TOOL_CALLS_INDEX))
        except Exception as exc:
            logger.debug("Could not create trace_id index (non-fatal): %s", exc)

        self._db_tables_ready = True
        return True

    def _write_trace_to_db(self, trace):
        # type: (QueryTrace) -> None
        """Insert one summary row plus one row per tool call."""
        if not self._ensure_db_tables():
            return

        trace_id = str(uuid.uuid4())
        created_at = _utc_now_iso()

        try:
            with self._db.engine.begin() as conn:
                conn.execute(_sql_text(_INSERT_TRACE), {
                    "id": trace_id,
                    "question": trace.question,
                    "final_sql": trace.final_sql,
                    "success": 1 if trace.success else 0,
                    "error": trace.error,
                    "duration_seconds": trace.duration_seconds,
                    "total_tool_calls": trace.total_tool_calls,
                    "sql_attempts": trace.sql_attempts,
                    "sql_errors": trace.sql_errors,
                    "schema_queries": trace.schema_queries,
                    "schema_backtracking_count": trace.schema_backtracking_count,
                    "llm_iterations": trace.llm_iterations,
                    "input_tokens": trace.input_tokens,
                    "output_tokens": trace.output_tokens,
                    "created_at": created_at,
                })

                for i, tc in enumerate(trace.tool_calls):
                    try:
                        arguments = json.dumps(tc.arguments, default=str)
                    except (TypeError, ValueError):
                        arguments = str(tc.arguments)
                    conn.execute(_sql_text(_INSERT_TOOL_CALL), {
                        "id": str(uuid.uuid4()),
                        "trace_id": trace_id,
                        "sequence": i,
                        "name": tc.name,
                        "arguments": arguments,
                        "result": tc.result_preview,
                        "execution_ms": tc.execution_ms,
                        "llm_think_ms": tc.llm_think_ms,
                        "created_at": created_at,
                    })
        except Exception as exc:
            self._disable_db_tracing("failed to write trace", exc)
            return

        logger.debug(
            "Wrote trace %s (%d tool calls) to the database",
            trace_id,
            len(trace.tool_calls),
        )

    def _flush_sync(self):
        # type: () -> None
        """POST buffered traces to the dashboard API."""
        if not self._sync_buffer or not self._api_key:
            return

        try:
            import httpx  # optional dependency
        except ImportError:
            logger.warning(
                "httpx not installed — cannot sync traces to dashboard. "
                "Install with: pip install text2sql-framework[dashboard]"
            )
            self._sync_buffer.clear()
            return

        if self._http_client is None:
            self._http_client = httpx.Client(timeout=10.0)

        payload = {"traces": self._sync_buffer}
        try:
            resp = self._http_client.post(
                f"{self._api_url}/api/v1/traces",
                json=payload,
                headers={"Authorization": f"Bearer {self._api_key}"},
            )
            if resp.status_code == 200 or resp.status_code == 201:
                logger.debug("Synced %d traces to dashboard", len(self._sync_buffer))
            else:
                logger.warning("Dashboard sync failed (%d): %s", resp.status_code, resp.text[:200])
        except Exception as exc:
            logger.warning("Dashboard sync error: %s", exc)
        finally:
            self._sync_buffer.clear()

    def flush(self):
        # type: () -> None
        """Explicitly flush any buffered traces to the dashboard."""
        self._flush_sync()

    @classmethod
    def load_traces(cls, path):
        # type: (str) -> List[QueryTrace]
        """Load traces from a JSONL file into QueryTrace objects."""
        traces = []
        p = Path(path)
        if not p.exists():
            return traces
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                trace = _dict_to_query_trace(data)
                traces.append(trace)
        return traces

    def summary(self):
        # type: () -> dict
        """Aggregate stats across all traced queries."""
        if not self.traces:
            return {"total_queries": 0}

        total = len(self.traces)
        successful = sum(1 for t in self.traces if t.success)
        total_tool_calls = sum(t.total_tool_calls for t in self.traces)
        total_sql_attempts = sum(t.sql_attempts for t in self.traces)
        total_sql_errors = sum(t.sql_errors for t in self.traces)
        total_schema_queries = sum(t.schema_queries for t in self.traces)
        total_example_lookups = sum(len(t.example_lookups) for t in self.traces)
        examples_used = sum(
            1 for t in self.traces
            for ex in t.example_lookups
            if ex.used_in_final_sql
        )

        avg_tool_calls = total_tool_calls / total
        avg_duration = sum(t.duration_seconds for t in self.traces) / total

        return {
            "total_queries": total,
            "success_rate": round(successful / total, 3),
            "avg_tool_calls_per_query": round(avg_tool_calls, 1),
            "avg_duration_seconds": round(avg_duration, 2),
            "total_sql_attempts": total_sql_attempts,
            "sql_error_rate": round(total_sql_errors / max(total_sql_attempts, 1), 3),
            "avg_schema_queries_per_question": round(total_schema_queries / total, 1),
            "total_example_lookups": total_example_lookups,
            "example_utilization_rate": round(
                examples_used / max(total_example_lookups, 1), 3
            ),
        }

    def example_report(self):
        # type: () -> List[Dict]
        """Per-scenario breakdown: how often looked up, how often actually used."""
        scenario_stats = {}  # type: Dict[str, Dict]
        for trace in self.traces:
            for ex in trace.example_lookups:
                name = ex.scenario_queried
                if name not in scenario_stats:
                    scenario_stats[name] = {"lookups": 0, "used": 0}
                scenario_stats[name]["lookups"] += 1
                if ex.used_in_final_sql:
                    scenario_stats[name]["used"] += 1

        report = []
        for name, stats in sorted(scenario_stats.items()):
            report.append({
                "scenario": name,
                "lookups": stats["lookups"],
                "used_in_sql": stats["used"],
                "utilization_rate": round(stats["used"] / max(stats["lookups"], 1), 3),
            })
        return report


def _count_backtracking(tool_calls):
    # type: (List[ToolCallTrace]) -> int
    """Count how many times the LLM went back to schema queries after writing SQL."""
    count = 0
    has_written_sql = False
    for tc in tool_calls:
        if tc.name != "execute_sql":
            continue
        sql = tc.arguments.get("sql", "")
        if not sql:
            continue
        if _is_schema_query(sql):
            if has_written_sql:
                count += 1
        else:
            has_written_sql = True
    return count


def _dict_to_query_trace(data):
    # type: (dict) -> QueryTrace
    """Reconstruct a QueryTrace from a dict (loaded from JSONL)."""
    tool_calls = [
        ToolCallTrace(**tc) for tc in data.pop("tool_calls", [])
    ]
    example_lookups = [
        ExampleUsage(**ex) for ex in data.pop("example_lookups", [])
    ]
    columns_searched = [
        SearchVsFound(**c) for c in data.pop("columns_searched_vs_found", [])
    ]
    join_attempts = [
        JoinAttempt(**j) for j in data.pop("join_attempts", [])
    ]
    sql_errors_structured = [
        StructuredSQLError(**e) for e in data.pop("sql_errors_structured", [])
    ]
    search_terms = data.pop("search_terms_used", [])
    reasoning = data.pop("llm_reasoning_steps", [])
    backtrack = data.pop("schema_backtracking_count", 0)

    trace = QueryTrace(
        question=data.get("question", ""),
        final_sql=data.get("final_sql", ""),
        success=data.get("success", False),
        error=data.get("error"),
        start_time=data.get("start_time", 0),
        end_time=data.get("end_time", 0),
        duration_seconds=data.get("duration_seconds", 0),
        total_tool_calls=data.get("total_tool_calls", 0),
        llm_iterations=data.get("llm_iterations", 0),
        sql_attempts=data.get("sql_attempts", 0),
        sql_errors=data.get("sql_errors", 0),
        schema_queries=data.get("schema_queries", 0),
        tool_calls=tool_calls,
        example_lookups=example_lookups,
        search_terms_used=search_terms,
        columns_searched_vs_found=columns_searched,
        join_attempts=join_attempts,
        sql_errors_structured=sql_errors_structured,
        schema_backtracking_count=backtrack,
        llm_reasoning_steps=reasoning,
        input_tokens=data.get("input_tokens", 0),
        output_tokens=data.get("output_tokens", 0),
    )
    return trace
