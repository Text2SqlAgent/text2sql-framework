# text2sql

Give any AI agent a way to talk to your relational databases.

**text2sql** is a Python SDK that translates natural language into SQL using agentic schema retrieval. The agent explores your database schema on its own — searching tables, inspecting columns, testing queries — then returns verified SQL and results. Works with any SQL database (PostgreSQL, MySQL, SQLite, SQL Server, Snowflake, BigQuery) and any LLM provider.

```python
from text2sql import TextSQL

engine = TextSQL("postgresql://user:pass@localhost/mydb")
result = engine.ask("Which customers have spent more than $10K this year?")

print(result.sql)   # verified SQL
print(result.data)  # [{'name': 'Acme Corp', 'total': 14302.50}, ...]
```

## How it works

The text2sql SDK takes full advantage of LLMs' ability to make many tool calls and work in a loop. This architecture lets the agent take as many steps as it needs — more tool calls for harder questions, fewer for simple ones. Here is the current architecture. Access to an execute_sql tool enables the LLM to ensure accuracey before returning the result to the user.

```
A typical flow might be:
User: "What's our revenue by product category?"
                    │
                    ▼
        ┌─────────────────────┐
        │   Agent receives    │
        │     question        │
        └────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  1. EXPLORE            │  ← queries information_schema 
     │  List all tables
     └───────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  2. LOOKUP EXAMPLES    │  ← calls lookup_example("revenue by category")
     │  Check for business    │    gets guidance: which tables, columns, joins
     │  context (when applicable)      to use for this concept
     └───────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  3. INSPECT            │  ← PRAGMA table_info('order_items')
     │  Check column names,   │    sees: item_id, order_id, prod_id, qty,
     │  types, keys           │    unit_px_at_sale, discount_pct, line_total
     └───────────┬────────────┘
                 │
     ┌───────────▼────────────┐
     │  4. WRITE & EXECUTE    │  ← runs the SQL, sees the actual results
     │  Test the query        │
     │                        │    ┌─ SQL error? read the error, fix, retry
     │                        │    ├─ Wrong columns? go back to schema
     │                        │    ├─ Results look off? try a different table
     │                        │    └─ Results make sense? done
     └───────────┬────────────┘
                 │ (loop until satisfied)
                 │
     ┌───────────▼────────────┐
     │  5. RETURN              │  ← final verified SQL sent back
     │  Verified SQL           │    middleware re-executes with your row limit
     └────────────────────────┘
```

The agent sees the actual query results at every step. If it writes a query and the output doesn't look right — empty results, unexpected columns, numbers that don't make sense — it goes back to the schema, tries different tables or joins, and re-executes. This self-correction loop runs until the agent is confident the results answer the question.

This is fundamentally different from many other text2sql frameworks, where there's no feedback loop at all. The agent self-corrects at the **retrieval** stage (picked the wrong table? go find the right one), at the **SQL** stage (syntax error? read it and fix), and at the **results** stage (output doesn't match the question? rethink the approach).

This matters most on real-world schemas:
- **50+ tables** where most are irrelevant to any given question
- **Cryptic column names** (`amt_ttl`, `qty_oh`, `unit_px`) that need exploration to understand
- **Multiple valid sources** for the same concept (revenue in `orders`, `payments`, reporting tables)
- **Missing or inconsistent FK naming** (`customer_id` in one table, `cust_id` in another)

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


The agent gets a `lookup_example` tool. An example might be a query where you anticipate your agent to fail. For example, if you company always adjusts revenue to local currency, you might include an example called 'calculating revenue' which the LLM would call for any revenue question. When the LLM calls `lookup_example("net revenue") itll be returned an MD file explaining the correct methodology for adjusting revenue.


## Custom instructions

For global rules that apply to every query:

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    instructions="""
    - Revenue always means net revenue (after refunds)
    - Always exclude test accounts (email LIKE '%@test.%')
    - Date columns are stored as TEXT in ISO format
    """,
)
```

## Tracing

Every query is traced — which tables the agent explored, what SQL it tried, what errors it hit, how it self-corrected:

```python
engine = TextSQL(
    "sqlite:///mydb.db",
    trace_file="traces.jsonl",  # writes traces to disk
)

result = engine.ask("Top customers by spend")

# See aggregate stats
print(engine.trace_summary())
# {
#   'total_queries': 1,
#   'success_rate': 1.0,
#   'avg_tool_calls_per_query': 4.2,
#   'sql_error_rate': 0.0,
#   ...
# }
```

## Circular - The text2sql Agent Tracing Platform (Coming soon!)

Writing examples and instructions by hand works, but how do you know *which* examples to write? and how do you ensure those edits are effective? The soon to be release agentic tracing platform understands that Text-to-SQL agents fail in specific, predictable ways — wrong columns, missing joins, misunderstood business logic. Generic observability platforms show you the trace, but they can't tell you why the SQL was wrong or how to fix it. Circular will analyze your traces with deep knowledge of how LLMs fail at SQL generation, then gives you specific edits to make your agent better. The platform reads your agent traces, identifies where your LLM is struggling — wrong tables, bad joins, misunderstood business terms — and gives you specific fixes. 

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    api_key="t2s_live_abc123...",  # enables auto-sync to dashboard
)
```

Traces sync automatically in the background. Your data never leaves your database — only the agent's tool call metadata (which tables it searched, what SQL it tried, what errors it hit) is sent to the dashboard.

## CLI

```bash
# Interactive mode
text2sql ask "sqlite:///mydb.db"

# Single question
text2sql query "sqlite:///mydb.db" "How many orders per month?"

# With options
text2sql ask "postgresql://localhost/mydb" --model anthropic:claude-sonnet-4-6
```

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
