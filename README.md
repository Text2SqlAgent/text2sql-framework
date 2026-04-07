# text2sql

Until recently, LLMs couldn't reliably chain more than a handful of tool calls before losing the thread. Though frontier models now make dozens, hundreds, or even thousands of iterative tool calls from a single prompt, reading each result and deciding what to do next. This unlocks a different shape of text-to-SQL system: instead of pre-computing which schema elements are relevant, you can hand the LLM one tool (`execute_sql`) and let it explore the schema, write queries, test them against real data, and self-correct before returning a final answer. This SDK requires no RAG, semantic layer, schema descriptions, etc. All that is needed is a connection string and a frontier model, as shown in the example below.

As models keep getting better at recursive tool use, the right move is to keep rearchitecting the harness so it constrains the LLM as little as possible (every guardrail you remove is capability you get back).

**19/20 (95%) on Spider zero-shot across 80 tables and 20 databases. 20/20 after adding one scenario.**

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

Below is a real agent trace from the [Spider benchmark](https://yale-lily.github.io/spider). The agent is connected to a single database containing **80 tables** from 20 different schemas — it has to find the right tables for each question on its own.

**Question:** *"List the name of singers in ascending order of net worth."*

```
┌─────────────────────────────────────────────────────────────┐
│ Tool:   execute_sql                                         │
│ Input:  SELECT name FROM sqlite_master                      │
│         WHERE type='table' AND name NOT LIKE 'sqlite_%'     │
│ Output: battle, ship, death, continents, countries,         │
│         car_makers, model_list, car_names, cars_data,       │
│         stadium, singer, concert, singer_in_concert,        │
│         course, teacher, … (80 tables)                      │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  PRAGMA table_info('singer')                         │
│ Output: Singer_ID INT, Name TEXT, Country TEXT,              │
│         Song_Name TEXT, Song_release_year TEXT,              │
│         Age INT, Is_male BOOL                               │
│         ← no Net_Worth column — wrong table                 │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  PRAGMA table_info('singer_solo')                    │
│ Output: Singer_ID INT, Name TEXT, Birth_Year REAL,          │
│         Net_Worth_Millions REAL, Citizenship TEXT            │
│         ← found it                                          │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT Name FROM singer_solo                        │
│         ORDER BY Net_Worth_Millions ASC                     │
│ Output: Abigail Johnson, Susanne Klatten,                   │
│         Gina Rinehart, Iris Fontbona, …  ✓                  │
└─────────────────────────────────────────────────────────────┘
```

The agent saw 80 tables, found two `singer` tables, inspected both, identified which one had the `Net_Worth_Millions` column, and wrote the correct query. Four tool calls, all autonomous.

Schema retrieval and SQL generation happen in the same loop, not as separate pipeline stages. If the agent picks the wrong table, it goes back and finds the right one. If a query errors, it reads the error message and fixes it. If the output doesn't look right, it rethinks its approach.

## Benchmarks

Tested on the [Spider benchmark](https://yale-lily.github.io/spider) — the most widely used text-to-SQL evaluation, with 10,000+ questions across 200 databases. We merged all 20 dev-set databases into a single 80-table database and ran 20 questions — one per database, randomly selected. The agent had to navigate 80 tables to find the right ones for each question.

**19/20 (95%) zero-shot, no examples.** The single failure was an ambiguous question — *"What is maximum and minimum death toll caused each time?"* — where the agent returned per-battle results instead of a global aggregate. After adding a one-line scenario clarifying that "each time" means overall, the agent used `lookup_example` to retrieve the guidance and got it right: **20/20.**

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
