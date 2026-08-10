"""Persistent Python workspace for the Python-first text-to-SQL agent.

The model receives one tool, ``run_python``. Database access and agent state are
capabilities inside that workspace rather than separate model-facing tools.
This is an in-process restriction, not a hardened security boundary.
"""

from __future__ import annotations

import ast
import contextlib
import io
import json
import math
import re
import statistics
import threading
from pathlib import Path
from typing import Any

from text2sql.sandbox import (
    MAX_AST_NODES,
    MAX_CODE_CHARS,
    MAX_OUTPUT_CHARS,
    MAX_ROWS_PER_QUERY,
    MAX_SQL_CALLS,
    _ALLOWED_NODES,
    _SAFE_BUILTINS,
    _SAFE_METHODS,
)
from text2sql.tools import _is_read_only

_CAPABILITY_METHODS = {
    "query", "list_tables", "describe", "schema", "dialect",
    "recent", "search", "list", "read", "write", "lookup",
}
_RESERVED_NAMES = {
    "db", "traces", "skills", "prompt", "examples", "math", "statistics",
} | set(_SAFE_BUILTINS)
MAX_STATE_CHARS = 50_000
_SAFE_SLUG = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$")


class _WorkspaceValidator(ast.NodeVisitor):
    def generic_visit(self, node: ast.AST) -> None:
        if type(node).__name__ not in _ALLOWED_NODES:
            raise ValueError(f"Unsupported Python syntax: {type(node).__name__}")
        super().generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id.startswith("_"):
            raise ValueError("Names beginning with '_' are not allowed.")
        if isinstance(node.ctx, ast.Store) and node.id in _RESERVED_NAMES:
            raise ValueError(f"'{node.id}' is a protected workspace capability")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr.startswith("_"):
            raise ValueError("Private attribute access is not allowed.")
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            raise ValueError("Workspace capability attributes cannot be replaced or deleted.")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute):
            allowed = _SAFE_METHODS | _CAPABILITY_METHODS
            if node.func.attr not in allowed:
                if not (
                    isinstance(node.func.value, ast.Name)
                    and node.func.value.id in {"math", "statistics"}
                ):
                    raise ValueError(f"Method '{node.func.attr}' is not allowed.")
        self.generic_visit(node)


def _bounded_json(value: Any) -> str:
    try:
        encoded = json.dumps(value, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        encoded = str(value)
    if len(encoded) > MAX_OUTPUT_CHARS:
        return encoded[:MAX_OUTPUT_CHARS] + "\n[Result truncated]"
    return encoded


class DatabaseCapability:
    """Read-only database API exposed inside the workspace as ``db``."""

    def __init__(self, database, tracer=None):
        self._database = database
        self._tracer = tracer
        self._calls = 0
        self._query_history: list[str] = []

    @property
    def query_history(self) -> tuple[str, ...]:
        return tuple(self._query_history)

    def reset_budget(self) -> None:
        self._calls = 0

    def query(self, sql: str) -> list[dict]:
        if not isinstance(sql, str):
            raise TypeError("db.query() requires a SQL string")
        if not _is_read_only(sql):
            raise ValueError("db.query() only permits read-only SQL")
        self._calls += 1
        if self._calls > MAX_SQL_CALLS:
            raise RuntimeError(f"At most {MAX_SQL_CALLS} database queries are allowed per Python call")
        if self._tracer:
            self._tracer.record_tool_start()
        try:
            rows = self._database.execute(sql, max_rows=MAX_ROWS_PER_QUERY)
        except Exception as exc:
            if self._tracer:
                self._tracer.record_tool_call("execute_sql", {"sql": sql}, f"SQL Error: {exc}")
            raise
        self._query_history.append(sql)
        if self._tracer:
            self._tracer.record_tool_call(
                "execute_sql", {"sql": sql}, _bounded_json(rows)
            )
        return rows

    def list_tables(self) -> list[str]:
        return self._database.get_inspector().get_table_names()

    def describe(self, table: str) -> dict:
        if table not in self.list_tables():
            raise ValueError(f"Unknown table: {table}")
        return self._database.get_schema_summary()[table]

    def schema(self) -> dict:
        return self._database.get_schema_summary()

    def dialect(self) -> str:
        return self._database.dialect


class TraceCapability:
    """Read-only access to completed and current traces as ``traces``."""

    def __init__(self, tracer):
        self._tracer = tracer

    def recent(self, limit: int = 10) -> list[dict]:
        if not self._tracer:
            return []
        limit = max(0, min(int(limit), 100))
        return self._tracer.stored_traces(limit)

    def search(self, text: str, limit: int = 10) -> list[dict]:
        needle = str(text).lower()
        matches = []
        for trace in reversed(self.recent(100)):
            if needle in json.dumps(trace, default=str).lower():
                matches.append(trace)
                if len(matches) >= max(0, min(int(limit), 100)):
                    break
        return matches


class SkillCapability:
    """State-backed reusable instructions exposed as ``skills``."""

    def __init__(self, state_store, writable: bool = False):
        self._state = state_store
        self._writable = writable

    def _name(self, name: str) -> str:
        if not isinstance(name, str) or not _SAFE_SLUG.fullmatch(name):
            raise ValueError("Skill names may contain only letters, numbers, '-' and '_'")
        return name

    def list(self) -> list[str]:
        return sorted(self._state.list("skills"))

    def read(self, name: str) -> str:
        name = self._name(name)
        value = self._state.get("skills", name)
        if value is None:
            raise ValueError(f"Unknown skill: {name}")
        return str(value)[:MAX_OUTPUT_CHARS]

    def write(self, name: str, content: str) -> str:
        if not self._writable:
            raise PermissionError("Skill writes are disabled; enable allow_self_modification explicitly")
        name = self._name(name)
        if not isinstance(content, str):
            raise TypeError("Skill content must be text")
        if len(content) > MAX_STATE_CHARS:
            raise ValueError(f"Skill content exceeds {MAX_STATE_CHARS:,} characters")
        self._state.put("skills", name, content)
        return f"Saved skill '{name}'. It will be available to future runs."


class PromptCapability:
    """Editable prompt addendum exposed as ``prompt``."""

    def __init__(self, state_store, writable: bool = False):
        self._state = state_store
        self._writable = writable

    def read(self) -> str:
        return str(self._state.get("prompt", "addendum", ""))[:MAX_OUTPUT_CHARS]

    def write(self, content: str) -> str:
        if not self._writable:
            raise PermissionError("Prompt writes are disabled; enable allow_self_modification explicitly")
        if not isinstance(content, str):
            raise TypeError("Prompt content must be text")
        if len(content) > MAX_STATE_CHARS:
            raise ValueError(f"Prompt content exceeds {MAX_STATE_CHARS:,} characters")
        self._state.put("prompt", "addendum", content)
        return "Saved the agent prompt addendum. It takes effect on the next ask()."


class ExampleCapability:
    """Existing curated examples exposed through Python as ``examples``."""

    def __init__(self, example_store):
        self._store = example_store

    def list(self) -> list[str]:
        return self._store.list_scenarios() if self._store else []

    def lookup(self, scenario: str) -> str:
        if not self._store:
            return "No example scenarios configured."
        return self._store.lookup(scenario)


class _BoundedWriter(io.StringIO):
    def write(self, value: str) -> int:
        remaining = MAX_OUTPUT_CHARS - self.tell()
        if remaining > 0:
            super().write(value[:remaining])
        return len(value)


class PythonWorkspace:
    """Persistent, restricted namespace used across an agent's Python calls."""

    def __init__(
        self, database, state_store, tracer=None, example_store=None,
        allow_self_modification: bool = False,
    ):
        self.db = DatabaseCapability(database, tracer=tracer)
        self.traces = TraceCapability(tracer)
        self.skills = SkillCapability(state_store, writable=allow_self_modification)
        self.prompt = PromptCapability(state_store, writable=allow_self_modification)
        self.allow_self_modification = allow_self_modification
        self._lock = threading.RLock()
        self.examples = ExampleCapability(example_store)
        self._namespace = {
            "__builtins__": dict(_SAFE_BUILTINS),
            "math": math,
            "statistics": statistics,
            "db": self.db,
            "traces": self.traces,
            "skills": self.skills,
            "prompt": self.prompt,
            "examples": self.examples,
        }

    def execute(self, code: str) -> str:
        if not isinstance(code, str) or not code.strip():
            return "Empty Python code."
        if len(code) > MAX_CODE_CHARS:
            return f"Code exceeds the {MAX_CODE_CHARS:,}-character limit."
        try:
            tree = ast.parse(code, mode="exec")
            if sum(1 for _ in ast.walk(tree)) > MAX_AST_NODES:
                return f"Code exceeds the {MAX_AST_NODES:,}-AST-node limit."
            _WorkspaceValidator().visit(tree)
        except (SyntaxError, ValueError) as exc:
            return f"Python blocked: {exc}"

        with self._lock:
            self.db.reset_budget()
            self._namespace.pop("result", None)
            output = _BoundedWriter()
            try:
                with contextlib.redirect_stdout(output):
                    exec(compile(tree, "<text2sql-workspace>", "exec"), self._namespace, self._namespace)
            except Exception as exc:
                return f"Python error: {type(exc).__name__}: {exc}"

            parts = [output.getvalue().strip()] if output.getvalue().strip() else []
            if "result" in self._namespace:
                parts.append(_bounded_json(self._namespace["result"]))
            return "\n".join(parts)[:MAX_OUTPUT_CHARS] or "Python completed. Assign `result` or call print(...)."

    def make_tool(self):
        workspace = self

        def run_python(code: str) -> str:
            """Run Python in the persistent text-to-SQL workspace.

            Available APIs: ``db`` (read-only query/schema access), ``traces``
            (recent/search), ``skills`` (list/read/write), ``prompt``
            (read/write addendum), and ``examples`` (list/lookup). Variables
            persist across calls. Assign to ``result`` or call ``print``.
            """
            return workspace.execute(code)

        return run_python

    def prompt_addendum(self) -> str:
        return self.prompt.read()

    def skill_names(self) -> list[str]:
        return self.skills.list()
