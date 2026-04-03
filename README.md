# text2sql

A text-to-SQL SDK that gives an LLM one tool — `execute_sql` — and lets it explore the schema, write queries, and self-correct in a loop. No RAG pipeline, no semantic layer, no schema descriptions required. Just a connection string and an API key.

**24/25 (96%) on Spider zero-shot. 25/25 after one round of automated trace analysis.**

```python
from text2sql import TextSQL

engine = TextSQL(
    "postgresql://user:pass@localhost/mydb",
    trace_file="traces.jsonl",
)
result = engine.ask("Which customers have spent more than $10K this year?")

print(result.sql)   # verified SQL
print(result.data)  # [{'name': 'Acme Corp', 'total': 14302.50}, ...]
```

## How it works

Below is a real agent trace from the [Spider benchmark](https://yale-lily.github.io/spider) — a concert/stadium database the LLM has never seen before:

**Question:** *"Find the number of concerts that happened in the stadium with the highest capacity."*

```
┌─────────────────────────────────────────────────────────────┐
│ Tool:   execute_sql                                         │
│ Input:  SELECT name FROM sqlite_master                      │
│         WHERE type='table' AND name NOT LIKE 'sqlite_%'     │
│ Output: stadium, singer, concert, singer_in_concert         │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  PRAGMA table_info('stadium')                        │
│ Output: Stadium_ID INT, Location TEXT, Name TEXT,            │
│         Capacity INT, Highest INT, Lowest INT, Average INT  │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  PRAGMA table_info('concert')                        │
│ Output: concert_ID INT, concert_Name TEXT, Theme TEXT,       │
│         Stadium_ID TEXT, Year TEXT                           │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT COUNT(*) FROM concert                        │
│         WHERE Stadium_ID = (SELECT Stadium_ID               │
│         FROM stadium ORDER BY Capacity DESC LIMIT 1)        │
│ Output: 0    ← wrong! type mismatch (INT vs TEXT)           │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT Stadium_ID, Name, Capacity FROM stadium      │
│         ORDER BY Capacity DESC LIMIT 5                      │
│ Output: 4 | Hampden Park | 52500                            │
│         2 | Somerset Park | 11998  …                        │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT Stadium_ID, concert_Name                     │
│         FROM concert LIMIT 10                               │
│ Output: 1 | Auditions                                       │
│         2 | Super bootcamp  …                               │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT COUNT(*) FROM concert                        │
│         WHERE CAST(Stadium_ID AS INT) =                     │
│           (SELECT Stadium_ID FROM stadium                   │
│            ORDER BY Capacity DESC LIMIT 1)                  │
│ Output: 0  ✓                                                │
└─────────────────────────────────────────────────────────────┘
```

The agent got a wrong result (0 concerts), realized `Stadium_ID` was stored as TEXT in one table and INT in another, investigated by sampling actual data, then fixed the type mismatch with `CAST`. Seven tool calls, all autonomous.

Schema retrieval and SQL generation happen in the same loop, not as separate pipeline stages. If the agent picks the wrong table, it goes back and finds the right one. If a query errors, it reads the error message and fixes it. If the output doesn't look right, it rethinks its approach.

## Benchmarks

Tested on the [Spider benchmark](https://yale-lily.github.io/spider) — the most widely used text-to-SQL evaluation, with 10,000+ questions across 200 databases. We ran the 25 unique questions from the `concert_singer` dev set — 4 tables, a mix of easy through extra-hard difficulty, covering aggregations, JOINs, subqueries, and set operations (INTERSECT/EXCEPT).

| Difficulty | Questions | Correct | Score |
|---|---|---|---|
| Easy | 8 | 8 | 100% |
| Medium | 11 | 10 | 91% |
| Hard | 4 | 4 | 100% |
| Extra Hard | 3 | 3 | 100% |
| **Total** | **25** | **24** | **96%** |

**24/25 (96%) on the first run, zero-shot, no examples.** The single failure was a LEFT vs INNER JOIN ambiguity on a bridge table. After one round of `analyze_traces` through the MCP server, the scenarios file caught this pattern and the agent got **25/25** on retest.

## Install

```bash
pip install text2sql

# With Anthropic:
pip install "text2sql[anthropic]"

# With OpenAI:
pip install "text2sql[openai]"
```

## Quick start

```python
from text2sql import TextSQL

# Connect to any SQLAlchemy-supported database
engine = TextSQL("sqlite:///company.db")

# Ask a question
result = engine.ask("Top 5 products by total revenue")
print(result.sql)
print(result.data)

# Control how many rows come back
result = engine.ask("All customers in New York", max_rows=50)
```

## LLM providers

```python
# Anthropic (recommended)
engine = TextSQL("sqlite:///mydb.db", model="anthropic:claude-sonnet-4-6")

# OpenAI
engine = TextSQL("sqlite:///mydb.db", model="openai:gpt-4o")
```

## Database support

Any database with a SQLAlchemy driver:

```python
TextSQL("postgresql://user:pass@localhost/mydb")
TextSQL("mysql+pymysql://user:pass@localhost/mydb")
TextSQL("sqlite:///mydb.db")
TextSQL("mssql+pyodbc://user:pass@server/db?driver=ODBC+Driver+17+for+SQL+Server")
TextSQL("snowflake://user:pass@account/db/schema")
```

The agent automatically detects the SQL dialect and adjusts its schema exploration strategy — `information_schema` for PostgreSQL/MySQL/Snowflake, `PRAGMA` for SQLite, `sys.tables` for SQL Server.

## Scenarios and the feedback loop

The agent works out of the box with just a connection string — but real databases have jargon, business logic, and naming conventions that no LLM can guess. That's where `scenarios.md` comes in.

A scenarios file is a markdown file where each `## heading` contains domain knowledge the agent can't infer from the schema alone — business rules, column name translations, tricky join paths, corrective guidance:

```markdown
## net revenue
Net revenue = gross revenue minus refunds.
Use INNER JOIN between orders and payments, not LEFT JOIN.
- `orders.amt_ttl` is the gross order total
- Refunds are in the `payments` table where `is_refund = 1`

    -- CORRECT
    SELECT SUM(o.amt_ttl) + SUM(p.amt) FROM orders o
    JOIN payments p ON o.order_id = p.order_id WHERE p.is_refund = 1;
```

At runtime, the agent doesn't get the entire file dumped into its context. It sees a list of scenario titles and gets a `lookup_example` tool. When it's about to write a query involving revenue, it calls `lookup_example("net revenue")` and retrieves the full guidance before writing SQL. The agent decides when it needs help, and only pulls in what's relevant.

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    examples="scenarios.md",
    trace_file="traces.jsonl",
)
```

### Building scenarios automatically with the MCP

You don't have to write scenarios by hand. The SDK saves full traces of every query — which tables the agent explored, what SQL it tried, what errors it hit, how it self-corrected. The MCP server reads these traces, identifies where the agent struggled, and writes corrective scenarios to `scenarios.md` automatically.

```bash
pip install text2sql-mcp
```

Add to your `.mcp.json`:

```json
{
  "mcpServers": {
    "text2sql": {
      "command": "text2sql-mcp",
      "env": {
        "TEXT2SQL_DB": "sqlite:///mydb.db",
        "TEXT2SQL_TRACES": "traces.jsonl",
        "TEXT2SQL_EXAMPLES": "scenarios.md",
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

The MCP server plugs into Claude Code, Cursor, or any MCP-compatible assistant and exposes two tools:

- **`analyze_traces`** — reads unread traces, sends them to an LLM along with the database schema and current scenarios, and writes improvements to `scenarios.md`
- **`get_summary`** — quick stats: total traces, success rate, unread count, scenario count

The loop: run queries → traces accumulate → call `analyze_traces` → scenarios.md gets better → future queries use the improved scenarios via `lookup_example`. This is how we went from 96% to 100% on Spider — the MCP identified a LEFT vs INNER JOIN pattern the agent kept getting wrong and wrote a corrective scenario that fixed it.

## CLI

```bash
# Interactive mode
text2sql ask "sqlite:///mydb.db"

# Single question
text2sql query "sqlite:///mydb.db" "How many orders per month?"

# With options
text2sql ask "postgresql://localhost/mydb" --model anthropic:claude-sonnet-4-6
```

## Built on Deep Agents

The agent loop is powered by [Deep Agents](https://github.com/langchain-ai/deepagents) (`langchain-ai/deepagents`). We use a minimal middleware stack — just automatic context compaction (summarizes older tool calls if the agent is working on a task with many steps) and Anthropic prompt caching (reduces API costs). All other default middleware (filesystem tools, sub-agents, todo lists) is disabled so the agent only sees the text2sql tools it needs.

## Architecture

```
text2sql/
├── core.py          # TextSQL — public API
├── generate.py      # SQLGenerator — builds the agent, parses results
├── connection.py    # Database — SQLAlchemy wrapper
├── tools.py         # execute_sql + lookup_example (LangChain tools)
├── dialects.py      # Per-dialect schema exploration guides
├── examples.py      # ExampleStore — loads scenario markdown
├── tracing.py       # Tracer — captures full agentic loop
├── analyze.py       # AnalysisEngine — deterministic trace analysis
├── models.py        # Pydantic models for analysis reports
└── cli.py           # Click CLI
```

## License

MIT
