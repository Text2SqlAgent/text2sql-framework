# text2sql-mcp

<!-- mcp-name: io.github.cpenniman12/text2sql-mcp -->

MCP server for [text2sql-framework](https://github.com/cpenniman12/text2sql-framework). Plugs into Claude Desktop, Cursor, Goose, or any other MCP-compatible assistant and lets it ask a SQL database questions in natural language.

The agent explores the schema, writes SQL, executes it against the real DB, and self-corrects on errors — no RAG layer, no schema descriptions, no pre-computed embeddings.

## Install

Out of the box, `text2sql-mcp` supports **SQLite + Anthropic**:

```bash
pip install text2sql-mcp
# or
uvx text2sql-mcp
```

For other databases or LLM providers, install with the matching extra so the right driver gets installed:

| You want… | Install command |
| --- | --- |
| SQLite (default) | `uvx text2sql-mcp` |
| Postgres | `uvx 'text2sql-mcp[postgres]'` |
| MySQL | `uvx 'text2sql-mcp[mysql]'` |
| Snowflake | `uvx 'text2sql-mcp[snowflake]'` |
| BigQuery | `uvx 'text2sql-mcp[bigquery]'` |
| OpenAI models | add `openai`, e.g. `uvx 'text2sql-mcp[postgres,openai]'` |

## Configure

Set environment variables in your MCP client config:

| Variable | Required | Description |
| --- | --- | --- |
| `TEXT2SQL_DATABASE_URL` | yes | SQLAlchemy URL, e.g. `sqlite:///mydb.db`, `postgresql://user:pass@host/db` |
| `ANTHROPIC_API_KEY` *or* `OPENAI_API_KEY` | yes | LLM provider key |
| `TEXT2SQL_MODEL` | no | LangChain model id (default: `anthropic:claude-sonnet-4-6`) |
| `TEXT2SQL_INSTRUCTIONS` | no | Business rules / hints, e.g. "Revenue = net of refunds." |
| `TEXT2SQL_EXAMPLES` | no | Path to a scenarios.md file for the agent's `lookup_example` tool |
| `TEXT2SQL_TRACE_MODE` | no | `local` (default), `database`, or `off` |
| `TEXT2SQL_TRACE_FILE` | no | Local JSONL path; defaults to `.text2sql/traces.jsonl` |
| `TEXT2SQL_TRACE_TO_DB` | no | Write and read traces from the source database's two trace tables |
| `TEXT2SQL_TRACE_DATABASE_URL` | no | Write and read traces in a separate trace database; credentials are never returned by MCP |

### Claude Desktop / Cursor / generic MCP

```json
{
  "mcpServers": {
    "text2sql": {
      "command": "uvx",
      "args": ["text2sql-mcp"],
      "env": {
        "TEXT2SQL_DATABASE_URL": "sqlite:///mydb.db",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

### Goose CLI

```bash
goose configure
# Add Extension → Command-line Extension
# Name: text2sql
# Command: uvx text2sql-mcp
# Env: TEXT2SQL_DATABASE_URL, ANTHROPIC_API_KEY
```

## Tools

- **`query(question, max_rows=100)`** — ask the database a natural-language question. Returns `{sql, data, error, row_count, tool_calls_made}`.
- **`trace_source()`** — report whether traces come from JSONL or the `text2sql_traces` / `text2sql_tool_calls` tables.
- **`recent_traces(limit=20, failures_only=false)`** — return recent completed investigations with ordered tool calls.
- **`trace_summary(limit=100)`** — aggregate success, token, SQL-error, schema-query, and tool-call signals.

The trace tools do not construct an LLM client, so Claude Code, Codex, Cursor,
or another MCP host can perform the improvement analysis with its own model.

## How it works

Under the hood this is a thin wrapper around [text2sql-framework](https://github.com/cpenniman12/text2sql-framework), which uses LangChain Deep Agents to do iterative tool-calling against a single `execute_sql` tool. See the framework README for benchmarks (19/20 on Spider zero-shot across 80 tables) and architecture details.

## License

MIT
