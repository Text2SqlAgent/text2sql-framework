"""SQL generation using the Deep Agents SDK.

The LLM gets pre-loaded tools (execute_sql, lookup_example) and a system prompt
with dialect-specific guidance on where schema metadata lives. Deep Agents handles
the agentic loop, context compaction, and provider abstraction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from text2sql.agent import create_deep_agent

from text2sql.connection import Database
from text2sql.dialects import get_dialect_guide
from text2sql.examples import ExampleStore
from text2sql.tools import make_tools
from text2sql.tracing import Tracer


@dataclass
class SQLResult:
    """Result of a text-to-SQL query."""

    question: str
    sql: str
    data: list = field(default_factory=list)
    error: Optional[str] = None
    commentary: str = ""
    tool_calls_made: int = 0
    iterations: int = 0
    input_tokens: int = 0
    output_tokens: int = 0

    @property
    def success(self) -> bool:
        return self.error is None and self.sql != ""

    def __str__(self) -> str:
        if self.error:
            return f"Error: {self.error}\nSQL: {self.sql}"
        return f"SQL: {self.sql}\n({len(self.data)} rows)"


SYSTEM_PROMPT = """You are a SQL expert. Translate natural language questions into SQL queries.

This is a **{dialect}** database.

{dialect_guide}
{custom_metadata}
## Tools

- `execute_sql` — run any read-only SQL (SELECT, WITH, SHOW, DESCRIBE, PRAGMA). Use this to explore the schema metadata and test queries. Use LIMIT to keep result sets under 100 rows when possible.
{example_tool_note}
## Workflow

1. EXPLORE: Query the schema metadata (see above) to find relevant tables and columns
   - Search by keyword: filter table/column names with LIKE or ILIKE
   - Look at column descriptions/comments if available
2. INSPECT: Query full column lists for candidate tables to see exact names and types
3. RELATIONSHIPS: Query foreign keys to find how tables join
4. EXAMPLES: If the question involves a business concept you're unsure about, use `lookup_example` to get guidance{example_list_note}
5. WRITE & EXECUTE: Write your SQL and execute it to verify it works
6. FIX: If it errors, read the error, fix, and re-execute

## Rules
- ALWAYS explore the schema first — never guess table or column names
- Use exact names from the metadata catalog
- Write {dialect} SQL syntax
- You MUST execute your final SQL via `execute_sql` before responding. Never return SQL you haven't run.
- If execution fails, read the error, fix the SQL, and execute again. Repeat until it works.
- Once the query executes successfully, your final response MUST include the SQL inside a ```sql code block. You may include brief commentary outside the code block if helpful. The results are captured automatically and displayed to the user separately.
{instructions}"""


class SQLGenerator:
    """Creates a Deep Agent pre-loaded with text2sql tools."""

    def __init__(
        self,
        db: Database,
        model: str = "anthropic:claude-sonnet-4-6",
        instructions: str | None = None,
        custom_metadata: str | None = None,
        example_store: ExampleStore | None = None,
        tracer: Tracer | None = None,
    ):
        self.db = db
        self.model = model
        self.instructions = instructions
        self.custom_metadata = custom_metadata
        self.example_store = example_store
        self.tracer = tracer

        self.tools = make_tools(db, example_store)
        self.system_prompt = self._build_system_prompt()

        self.agent = create_deep_agent(
            model=model,
            tools=self.tools,
            system_prompt=self.system_prompt,
        )

    def _build_system_prompt(self) -> str:
        dialect = self.db.dialect
        dialect_guide = get_dialect_guide(dialect)

        custom = ""
        if self.custom_metadata:
            custom = f"\n## Custom Metadata\n{self.custom_metadata}\n"

        instructions = ""
        if self.instructions:
            instructions = f"\n## Instructions\n{self.instructions}\n"

        example_tool_note = ""
        example_list_note = ""
        if self.example_store:
            example_tool_note = "- `lookup_example` — look up a curated example scenario by keyword (e.g. \"net revenue\", \"customer address\"). Returns guidance on which tables/columns/joins to use.\n"
            scenarios = self.example_store.list_scenarios()
            if scenarios:
                example_list_note = "\n   Available examples: {}".format(", ".join(scenarios))

        return SYSTEM_PROMPT.format(
            dialect=dialect,
            dialect_guide=dialect_guide,
            custom_metadata=custom,
            instructions=instructions,
            example_tool_note=example_tool_note,
            example_list_note=example_list_note,
        )

    def ask(self, question: str, max_rows: int | None = None) -> SQLResult:
        if self.tracer:
            self.tracer.start_query(question)

        result = self.agent.invoke(
            {"messages": [{"role": "user", "content": question}]}
        )

        return self._parse_result(question, result["messages"], max_rows=max_rows)

    def _parse_result(self, question: str, messages: list, max_rows: int | None = None) -> SQLResult:
        """Extract SQL from the agent's final text response, then execute it.

        The agent is instructed to respond with ONLY the final SQL in its last
        message (no tool calls). We parse that SQL out and execute it ourselves,
        giving the caller control over max_rows.
        """
        tool_calls_made = 0
        args_by_id: dict[str, dict] = {}
        # Track message timestamps for computing LLM think time vs tool execution time
        last_ai_timestamp = self.tracer._current.start_time if self.tracer and self.tracer._current else 0.0

        for msg in messages:
            resp_meta = getattr(msg, "response_metadata", {}) if hasattr(msg, "response_metadata") else {}
            msg_time = resp_meta.get("timestamp", 0)

            # Accumulate token usage from AIMessages
            usage = resp_meta.get("usage", {})
            if usage and self.tracer:
                self.tracer.record_token_usage(
                    usage.get("input_tokens", 0),
                    usage.get("output_tokens", 0),
                )

            # AIMessage with tool_calls
            if hasattr(msg, "tool_calls") and msg.tool_calls:
                # Capture reasoning text from AIMessage content
                if self.tracer and hasattr(msg, "content"):
                    content = msg.content
                    if isinstance(content, str) and content.strip():
                        self.tracer.record_reasoning(content)
                    elif isinstance(content, list):
                        for block in content:
                            if isinstance(block, dict) and block.get("type") == "text":
                                text = block.get("text", "")
                                if text.strip():
                                    self.tracer.record_reasoning(text)

                for tc in msg.tool_calls:
                    tool_calls_made += 1
                    args_by_id[tc["id"]] = tc["args"]

                # This AI message represents LLM thinking — record the timestamp
                if msg_time:
                    last_ai_timestamp = msg_time

            # AIMessage without tool_calls — pure reasoning
            elif hasattr(msg, "content") and hasattr(msg, "type") and getattr(msg, "type", None) == "ai":
                if self.tracer:
                    content = msg.content
                    if isinstance(content, str) and content.strip():
                        self.tracer.record_reasoning(content)
                if msg_time:
                    last_ai_timestamp = msg_time

            # ToolMessage with result — record for tracing
            if hasattr(msg, "name") and hasattr(msg, "tool_call_id"):
                content = msg.content if isinstance(msg.content, str) else str(msg.content)
                tc_id = getattr(msg, "tool_call_id", None)

                if self.tracer:
                    # Use record_tool_start to set LLM think time boundary
                    if last_ai_timestamp > 0:
                        self.tracer._last_event_time = self.tracer._last_event_time or last_ai_timestamp
                        self.tracer._tool_start_time = last_ai_timestamp

                    args = args_by_id.get(tc_id, {})
                    self.tracer.record_tool_call(msg.name, args, content)

        # Extract SQL from the agent's final message (the one with no tool calls)
        final_sql = ""
        commentary = ""
        error = None
        if messages:
            last_msg = messages[-1]
            final_text = last_msg.content if hasattr(last_msg, "content") else str(last_msg)
            if isinstance(final_text, list):
                final_text = " ".join(
                    b.get("text", "") for b in final_text if isinstance(b, dict)
                )
            final_sql, commentary = _extract_sql_from_response(str(final_text))

        if not final_sql:
            final_text = messages[-1].content if messages else ""
            error = f"No SQL produced. Response: {str(final_text)[:300]}"

        # Execute the SQL the agent specified in its response
        data = []
        if final_sql and not error:
            try:
                rows = self.db.execute(final_sql)
                if max_rows is not None:
                    rows = rows[:max_rows]
                data = rows
            except Exception as e:
                error = f"Final execution failed: {e}"

        if self.tracer:
            self.tracer.end_query(
                sql=final_sql,
                success=error is None and final_sql != "",
                error=error,
                iterations=tool_calls_made,
            )

        # Pull token counts from the trace
        input_tokens = 0
        output_tokens = 0
        if self.tracer and self.tracer.traces:
            last_trace = self.tracer.traces[-1]
            input_tokens = last_trace.input_tokens
            output_tokens = last_trace.output_tokens

        return SQLResult(
            question=question,
            sql=final_sql,
            data=data,
            error=error,
            commentary=commentary,
            tool_calls_made=tool_calls_made,
            iterations=tool_calls_made,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )



def _extract_sql_from_response(text: str) -> tuple[str, str]:
    """Extract SQL and commentary from the agent's final text response.

    The agent wraps its final SQL in a ```sql code block. Everything outside
    the code block is commentary.

    Returns:
        (sql, commentary) tuple
    """
    import re

    if not text or not text.strip():
        return "", ""

    # Try to extract from ```sql ... ``` code block
    match = re.search(r'```(?:sql)?\s*\n?(.*?)\n?```', text, re.DOTALL)
    if match:
        sql = match.group(1).strip()
        # Commentary is everything outside the code block
        commentary = re.sub(r'```(?:sql)?\s*\n?.*?\n?```', '', text, flags=re.DOTALL).strip()
        return sql, commentary

    # Fallback: the whole response might be SQL — look for SELECT/WITH
    stripped = text.strip()
    match = re.search(
        r'((?:WITH\b|SELECT\b).*)',
        stripped,
        re.DOTALL | re.IGNORECASE,
    )
    if match:
        sql = match.group(1).strip()
        if ';' in sql:
            sql = sql[:sql.rindex(';') + 1]
        return sql, ""

    return "", text.strip()
