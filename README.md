# text2sql

In the past few months we've seen LLMs massively improve at tool calling. This text to sql architecture takes full advangtage of that new capability by giving the LLM maximum flexability to pull in the context it needs. The architecture is very simple - you just give the LLM access to an execute sql tool so that it can explore the data schema and then test queries in a loop before returing the final answer. 

```python
from text2sql import TextSQL

engine = TextSQL("postgresql://user:pass@localhost/mydb")
result = engine.ask("Which customers have spent more than $10K this year?")

print(result.sql)   # verified SQL
print(result.data)  # [{'name': 'Acme Corp', 'total': 14302.50}, ...]
```

## How it works

To demonstrate how this architecture works, ive pasted in an agent trace below: 

```
Question: "For the client whose loan was approved first on 1993/7/5,
           what is the balance increase rate from 1993/3/22 to 1998/12/27?"

┌─────────────────────────────────────────────────────────────┐
│ Tool:   execute_sql                                         │
│ Input:  SELECT name FROM sqlite_master                      │
│         WHERE type='table' AND name NOT LIKE 'sqlite_%'     │
│ Output: account, card, client, disp, district, loan,        │
│         order, trans                                        │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT sql FROM sqlite_master                       │
│         WHERE type='table'                                  │
│           AND name IN ('loan', 'trans', 'disp')             │
│ Output: loan(loan_id, account_id, date, amount, duration…)  │
│         trans(trans_id, account_id, date, type, balance…)    │
│         disp(disp_id, client_id, account_id, type…)         │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT loan_id, account_id FROM loan                │
│         WHERE date = '1993-07-05'                           │
│         ORDER BY loan_id LIMIT 1                            │
│ Output: loan_id=5314  account_id=1787                       │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT balance FROM trans                           │
│         WHERE account_id = 1787 AND date = '1993-03-22'     │
│ Output: balance = 1100                                      │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  SELECT balance FROM trans                           │
│         WHERE account_id = 1787 AND date = '1998-12-27'     │
│ Output: balance = 5835                                      │
├─────────────────────────────────────────────────────────────┤
│ Tool:   execute_sql                                         │
│ Input:  WITH first_loan AS (…),                             │
│              balance_start AS (…),                          │
│              balance_end AS (…)                             │
│         SELECT ROUND((end - start) / start * 100, 2)        │
│           AS increase_rate                                  │
│ Output: increase_rate = 430.45%  ✓                          │
└─────────────────────────────────────────────────────────────┘
```


The agent sees the actual query results at every step. If it writes a query and the output doesn't look right — empty results, unexpected columns, numbers that don't make sense — it goes back to the schema, tries different tables or joins, and re-executes. This self-correction loop runs until the agent is confident the results answer the question.

This is fundamentally different from one-shot RAG, where there's no feedback loop at all. The agent self-corrects at the **retrieval** stage (picked the wrong table? go find the right one), at the **SQL** stage (syntax error? read it and fix), and at the **results** stage (output doesn't match the question? rethink the approach).


## Benchmarks

Tested on the [BIRD financial benchmark](https://bird-bench.github.io/) — 8 questions (6 challenging, 2 moderate) against a real Czech banking schema with 8 tables.

**6/8 on the first run. 8/8 after one round of trace-driven improvements.**

→ [Full benchmark results and traces](bird_financial_benchmark.md)

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

## Example scenarios (optional)

Real databases have jargon, business logic, and naming conventions that no LLM can guess. The `examples` parameter lets you teach the agent your domain:

```markdown
<!-- scenarios.md -->

## net revenue
Net revenue = gross revenue minus refunds.
- `orders.amt_ttl` is the gross order total
- Refunds are in the `payments` table where `is_refund = 1`
- Net = SUM(orders.amt_ttl) + SUM(payments.amt) WHERE is_refund = 1
  (refund amounts are stored as negative values)

## active customers
A customer is "active" if they placed at least one non-cancelled order
in the last 12 months.
```sql
SELECT DISTINCT cust_id FROM orders
WHERE order_date >= DATE('now', '-12 months')
  AND status != 'cancelled'
```

```

```python
engine = TextSQL(
    "postgresql://localhost/mydb",
    examples="scenarios.md",
)
```

The agent gets a `lookup_example` tool. When a question involves a business concept like "net revenue" or "active customers," the agent calls `lookup_example("net revenue")` and gets your guidance before writing SQL.

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
