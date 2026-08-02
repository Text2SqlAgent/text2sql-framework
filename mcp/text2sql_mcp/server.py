"""MCP server exposing text2sql-framework as a single `query` tool.

Configuration is read from environment variables:
  TEXT2SQL_DATABASE_URL  (required)  SQLAlchemy URL, e.g. sqlite:///mydb.db
  TEXT2SQL_MODEL         (optional)  LangChain model id (default: anthropic:claude-sonnet-4-6)
  TEXT2SQL_INSTRUCTIONS  (optional)  Free-text business rules / hints
  TEXT2SQL_EXAMPLES      (optional)  Path to a scenarios.md file

Plus the usual provider key — ANTHROPIC_API_KEY or OPENAI_API_KEY —
which text2sql-framework reads via LangChain.
"""

from __future__ import annotations

import os
import sys
from typing import Any

from mcp.server.fastmcp import FastMCP

_engine = None


def _get_engine():
    """Lazily build the TextSQL engine on first use."""
    global _engine
    if _engine is not None:
        return _engine

    db_url = os.environ.get("TEXT2SQL_DATABASE_URL")
    if not db_url:
        raise RuntimeError(
            "TEXT2SQL_DATABASE_URL is not set. "
            "Provide a SQLAlchemy connection string, e.g. sqlite:///mydb.db"
        )

    from text2sql import TextSQL

    kwargs: dict[str, Any] = {}
    if model := os.environ.get("TEXT2SQL_MODEL"):
        kwargs["model"] = model
    if instructions := os.environ.get("TEXT2SQL_INSTRUCTIONS"):
        kwargs["instructions"] = instructions
    if examples := os.environ.get("TEXT2SQL_EXAMPLES"):
        kwargs["examples"] = examples

    _engine = TextSQL(db_url, **kwargs)
    return _engine


mcp = FastMCP("text2sql")


@mcp.tool()
def query(question: str, max_rows: int = 100) -> dict:
    """Ask the database a natural-language question.

    The agent explores the schema, writes SQL, executes it, and self-corrects
    on errors before returning. Read-only — only SELECT-style statements.

    Args:
        question: The natural-language question, e.g. "top 5 customers by revenue".
        max_rows: Cap on rows returned in `data`. Defaults to 100.

    Returns:
        dict with:
          sql:    the final verified SQL
          data:   list of row dicts (capped at max_rows)
          error:  error message if execution failed, else None
          row_count:        number of rows in `data`
          tool_calls_made:  how many SQL calls the agent made while exploring
    """
    engine = _get_engine()
    result = engine.ask(question, max_rows=max_rows)
    return {
        "sql": result.sql,
        "data": result.data,
        "error": result.error,
        "row_count": len(result.data),
        "tool_calls_made": result.tool_calls_made,
    }


def main() -> None:
    """Entry point for the `text2sql-mcp` console script."""
    try:
        mcp.run()
    except Exception as exc:
        print(f"text2sql-mcp failed: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
