"""Tool definitions for text2sql — execute_sql + lookup_example.

`make_tools()` returns plain Python functions (closures) bound to a specific
Database instance (and optional ExampleStore). They carry type-hinted
signatures and docstrings, which the native agent loop turns into JSON tool
schemas and the LangChain integrations turn into LangChain tools via
`to_langchain_tools()`. Neither `make_tools` nor this module imports LangChain
at module load — the LangChain import is lazy and only happens if you call
`to_langchain_tools()`.
"""

from __future__ import annotations

import re
import sqlglot
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from text2sql.connection import Database
    from text2sql.examples import ExampleStore

_DESTRUCTIVE_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE|GRANT|REVOKE|EXEC|EXECUTE|CALL)\b",
    re.IGNORECASE,
)


def _is_read_only(sql: str) -> bool:
    """Check that SQL contains exactly one read-only statement."""
    try:
        statements = sqlglot.parse(sql)
    except sqlglot.errors.ParseError:
        # Fall back to the conservative lexical guard for unsupported SQL syntax.
        stripped = re.sub(r"--[^\n]*", "", sql)
        stripped = re.sub(r"/\*.*?\*/", "", stripped, flags=re.DOTALL)
        stripped = stripped.strip().rstrip(";").strip()
        if not stripped or ";" in stripped:
            return False

        first_word = stripped.split()[0].upper()
        allowed_starts = ("SELECT", "WITH", "EXPLAIN", "DESCRIBE", "SHOW", "PRAGMA")
        if first_word not in allowed_starts:
            return False
        return not _DESTRUCTIVE_PATTERN.search(stripped)

    # Reject empty input and multiple statements.
    if len(statements) != 1 or statements[0] is None:
        return False

    expression = statements[0]
    # SQLGlot represents SHOW as a generic command, but it is read-only metadata.
    if expression.key == "command":
        command_name = str(expression.this).upper()
        if command_name == "SHOW":
            return True

        # Validate the SQL inside EXPLAIN separately.
        if command_name == "EXPLAIN":
            inner = expression.args.get("expression")
            inner_sql = getattr(inner, "this", "")
            return isinstance(inner_sql, str) and _is_read_only(inner_sql)

        return False

    # Allow only supported query types.
    allowed = {
        "select",
        "union",
        "intersect",
        "except",
        "show",
        "describe",
        "pragma",
        "explain",
    }
    if expression.key not in allowed:
        return False

    # Reject write operations even when nested inside a query.
    blocked = {
        "insert",
        "update",
        "delete",
        "create",
        "drop",
        "alter",
        "truncate",
        "merge",
        "replace",
        "grant",
        "revoke",
        "command",
    }
    return not any(node.key in blocked for node in expression.walk())


def _format_results(rows: list[dict]) -> str:
    """Format query results as a pipe-delimited table string."""
    if not rows:
        return "Query executed successfully. 0 rows returned."
    cols = list(rows[0].keys())
    lines = [" | ".join(str(c) for c in cols)]
    lines.append("-" * len(lines[0]))
    for row in rows:
        lines.append(" | ".join(str(row[c]) for c in cols))
    lines.append(f"({len(rows)} rows)")
    return "\n".join(lines)


def make_tools(db: Database, example_store: ExampleStore | None = None) -> list:
    """Create text2sql tools bound to a specific database and optional example store.

    Returns plain Python functions (no LangChain dependency). Each function has a
    type-hinted signature and a docstring; the native agent loop derives a JSON
    tool schema from these, and `to_langchain_tools()` wraps them for the optional
    LangChain integrations.
    """

    def execute_sql(sql: str) -> str:
        """Execute a read-only SQL query and return results.

        Use this for everything: exploring schema metadata (information_schema,
        PRAGMA, etc.), testing your queries, and running the final answer.
        If the query errors, read the error message, fix, and retry.
        Only SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, and PRAGMA are allowed.
        """
        if not sql.strip():
            return "Empty SQL query."
        if not _is_read_only(sql):
            return "Blocked: only SELECT/WITH/SHOW/DESCRIBE/PRAGMA queries are allowed."
        try:
            rows = db.execute(sql)
            return _format_results(rows)
        except Exception as e:
            return f"SQL Error: {e}"

    tools = [execute_sql]

    if example_store:
        def lookup_example(scenario: str) -> str:
            """Look up a curated example scenario by keyword.

            Returns guidance on which tables, columns, and joins to use for a
            specific business concept (e.g. 'customer home address', 'net revenue',
            'active customers'). Call this when the user's question involves a
            business concept you're unsure about.
            """
            return example_store.lookup(scenario)

        tools.append(lookup_example)

    return tools


def to_langchain_tools(tools: list) -> list:
    """Wrap plain text2sql tool functions as LangChain tools.

    Used by the optional LangChain integrations (`Text2SqlMiddleware` and the
    deepagents backend in `agent_langchain`). Imports LangChain lazily so the
    core package never requires it.
    """
    from langchain_core.tools import tool as _lc_tool

    return [_lc_tool(fn) for fn in tools]


def execute_tool(name: str, arguments: dict, db=None, example_store=None) -> str:
    """Execute a tool by name — standalone helper for testing and direct use."""
    if name == "execute_sql":
        if not db:
            return "SQL execution not available."
        sql = arguments.get("sql", "")
        if not sql.strip():
            return "Empty SQL query."
        if not _is_read_only(sql):
            return "Blocked: only SELECT/WITH/SHOW/DESCRIBE/PRAGMA queries are allowed."
        try:
            rows = db.execute(sql)
            return _format_results(rows)
        except Exception as e:
            return f"SQL Error: {e}"

    elif name == "lookup_example":
        if not example_store:
            return "No example scenarios configured."
        scenario = arguments.get("scenario", "")
        return example_store.lookup(scenario)

    else:
        return f"Unknown tool: {name}"
