"""Subagents for tier-routed query handling.

The supervisor model (medium tier) is the default agent that handles SQL
synthesis. It optionally delegates two phases to tier-specialized subagents:

  - schema_explorer (Light tier)  — pure metadata lookups
                                    (information_schema, pg_views, etc.)
  - analyst        (Heavy tier)   — narrative / business interpretation
                                    of result rows. No tools, just reasoning.

Subagents are exposed to the supervisor via deepagents' built-in `task` tool.
The supervisor calls `task(subagent_type="schema_explorer", description=...)`
or `task(subagent_type="analyst", description=...)` when it judges that phase
warrants the specialized model.

This module is customer-agnostic. Wiring lives here; per-customer config
(which models, which examples) flows in via the SQLGenerator.
"""

from __future__ import annotations

from typing import Any


SCHEMA_EXPLORER_PROMPT = """You are a Postgres schema explorer.

You receive a description of what the supervisor needs to know about the schema
(table relevance, column names, comments, foreign keys). Your job is to run a
small number of metadata queries and return a clean summary.

## Tools available
- `execute_sql` — run any read-only SQL.

## Rules
- ONLY query metadata. Use information_schema, pg_views, pg_description,
  pg_indexes. Do NOT run SELECT against application data tables.
- Aim for 2-4 tool calls total. If you can't determine relevance in that budget,
  return what you have with a note.
- If the supervisor asks about a specific entity (customer, product, etc.),
  prefer the gold.* views and silver.* tables; ignore bronze.* unless asked.
- Report back in a compact structured format — the supervisor will read this and
  use it to write SQL. Keep it short, actionable, and free of speculation.

## Output format
```
Relevant tables/views:
- <schema>.<name>: <one-line purpose>
  key columns: <col1>, <col2>, ...
  note: <comment from pg_description if useful>
```

Then a one-line "Recommended starting point" suggesting which view best answers
the question.
"""


ANALYST_PROMPT = """You are a business analyst for a data product.

You receive:
  - The user's original question (in Spanish or English).
  - The SQL the supervisor ran.
  - The result rows as a markdown table.
  - Any business context (canonical names, currency, scope).

Your job is to produce the user-facing answer:
  1. Render the data as a clean markdown table (or the most appropriate format
     for the question — totals, single value, list, etc.).
  2. Add 1-3 sentences of business interpretation. Highlight what's notable
     (largest, smallest, surprising patterns, units, currency, scope).
  3. Match the user's language. If they asked in Spanish, answer in Spanish.

## Rules
- DO NOT modify the SQL or the data. Numbers must come from the rows verbatim.
- DO NOT speculate beyond what the data shows. If the data is insufficient to
  answer, say so plainly.
- DO NOT call any tools. You are a pure reasoning agent.
- Be concise. Avoid filler ("Here are the results...", "I hope this helps...").
- For currency values, include the currency code if known (UYU, USD, BRL, etc.).
- For percentages and ratios, include the absolute numbers behind them.
"""


def make_schema_explorer(
    tools: list,
    model: Any | None = None,
) -> dict:
    """Build the schema_explorer subagent spec for deepagents.

    Args:
        tools: list of tools to expose to the subagent. Should contain at least
               execute_sql (so the explorer can hit information_schema).
        model: a chat model instance or 'provider:model' string. None inherits
               the supervisor's model (degenerate single-tier mode).

    Returns:
        SubAgent TypedDict ready to pass to deepagents.create_deep_agent(subagents=...).
    """
    spec: dict = {
        "name": "schema_explorer",
        "description": (
            "Use BEFORE writing SQL when you need to discover relevant tables, "
            "views, columns, or relationships. Pass a clear description of what "
            "the user is asking about (e.g. 'find tables for customer revenue "
            "by month'). Returns a structured schema summary. Do not use this "
            "for application-data queries — it only inspects metadata."
        ),
        "system_prompt": SCHEMA_EXPLORER_PROMPT,
        "tools": tools,
    }
    if model is not None:
        spec["model"] = model
    return spec


def make_analyst(
    model: Any | None = None,
) -> dict:
    """Build the analyst subagent spec for deepagents.

    The analyst has NO tools — it's a pure reasoning agent. The supervisor must
    pass the SQL, the result rows, and any context inside the `description`
    argument when invoking via the `task` tool.

    Args:
        model: a chat model instance or 'provider:model' string. None inherits
               the supervisor's model.

    Returns:
        SubAgent TypedDict ready to pass to deepagents.create_deep_agent(subagents=...).
    """
    spec: dict = {
        "name": "analyst",
        "description": (
            "Use AFTER successful SQL execution when the user asked for "
            "narrative, insight, a formatted report, or a multi-row table that "
            "needs interpretation. Pass: (1) the original user question, (2) "
            "the SQL you ran, (3) the result rows as a markdown table, (4) any "
            "business context. Returns a clean user-facing answer in the user's "
            "language. Skip this for simple factual questions like 'how many X?'."
        ),
        "system_prompt": ANALYST_PROMPT,
        "tools": [],  # no tools — pure reasoning
    }
    if model is not None:
        spec["model"] = model
    return spec
